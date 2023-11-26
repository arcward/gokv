package cmd

import (
	"context"
	"crypto"
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/arcward/gokv/build"
	"github.com/arcward/gokv/server"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var serverCmd = &cobra.Command{
	Use:          "server",
	Short:        "Starts a server",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		opts := &cliOpts
		ctx := cmd.Context()

		serverConfig := &opts.Server

		if opts.hashAlgorithm.Value != crypto.Hash(0) {
			serverConfig.HashAlgorithm = opts.hashAlgorithm.Value
		}
		u, err := parseURL(opts.Address)
		if err != nil {
			return fmt.Errorf("invalid address: %w", err)
		}

		opts.Address = u.String()

		lis, err := net.Listen(
			u.Scheme,
			u.Host,
		)
		if err != nil {
			return fmt.Errorf("failed to listen: %w", err)
		}

		snapshotDir := opts.SnapshotDir
		snapshotInterval := opts.SnapshotInterval

		if snapshotDir != "" {
			snapshotDir, err := filepath.Abs(snapshotDir)
			if err != nil {
				return fmt.Errorf(
					"failed to get absolute path for snapshot directory '%s': %w",
					snapshotDir,
					err,
				)
			}
			info, err := os.Stat(snapshotDir)
			if os.IsNotExist(err) {
				return fmt.Errorf(
					"snapshot directory '%s' does not exist: %w",
					snapshotDir,
					err,
				)
			}
			if !info.IsDir() {
				return fmt.Errorf(
					"snapshot directory is not a directory: %w",
					err,
				)
			}
			if snapshotInterval == 0 {
				return fmt.Errorf("snapshot interval must be greater than 0")
			}
		}

		var out io.Writer
		if cliOpts.Quiet {
			out = io.Discard
		} else {
			out = os.Stdout
		}
		log.SetOutput(out)

		handlerOptions := &slog.HandlerOptions{
			Level:     cliOpts.LogLevel,
			AddSource: true,
		}
		var handler slog.Handler
		if cliOpts.LogJSON {
			handler = slog.NewJSONHandler(out, handlerOptions)
		} else {
			handler = slog.NewTextHandler(out, handlerOptions)
		}
		defaultLogger = slog.New(handler).WithGroup("gokv")
		slog.SetDefault(defaultLogger)

		if build.Version != "" {
			defaultLogger = defaultLogger.With(
				slog.String(
					"version",
					build.Version,
				),
			)
		}

		serverConfig.Logger = defaultLogger

		var snapshotData []byte
		var loadSnapshot string

		if opts.LoadSnapshot != "" {
			loadSnapshot = opts.LoadSnapshot
		} else if opts.SnapshotDir != "" {
			loadSnapshot, err = latestSnapshot(opts.SnapshotDir)
			if err != nil {
				return fmt.Errorf(
					"failed to load latest snapshot from %s: %w",
					snapshotDir,
					err,
				)
			}
		}

		if loadSnapshot != "" {
			loadSnapshot, err = filepath.Abs(loadSnapshot)
			if err != nil {
				return fmt.Errorf(
					"failed to get absolute path for snapshot file '%s': %w",
					loadSnapshot,
					err,
				)
			}
			snapshotData, err = os.ReadFile(loadSnapshot)
			if err != nil {
				return fmt.Errorf(
					"failed to read snapshot file '%s': %w",
					loadSnapshot,
					err,
				)
			}
		}

		var srv *server.KeyValueStore
		if snapshotData == nil {
			defaultLogger.Info(
				"creating new server",
			)
			srv = server.NewServer(serverConfig)
		} else {
			defaultLogger.Info(
				"creating new server from snapshot",
				slog.Group("snapshot", slog.String("file", loadSnapshot)),
			)
			srv, err = server.NewServerFromSnapshot(
				snapshotData,
				serverConfig,
			)
			if err != nil {
				return fmt.Errorf("failed to load snapshot: %w", err)
			}
		}
		srv.WithEventStream(ctx)
		cliOpts.server = srv

		snapshotter, err := server.NewSnapshotter(
			srv,
			snapshotDir,
			snapshotInterval,
			cliOpts.SnapshotLimit,
		)

		var runSnapshotter bool
		if snapshotInterval > 0 {
			runSnapshotter = true
		}

		var snapshotOnShutdown bool
		if snapshotInterval > 0 || snapshotDir != "" {
			snapshotOnShutdown = true
		}

		if err != nil {
			return fmt.Errorf("failed to create snapshotter: %w", err)
		}
		var grpcServer *grpc.Server

		if opts.SSLCertfile != "" && opts.SSLKeyfile != "" {
			creds, err := credentials.NewServerTLSFromFile(
				opts.SSLCertfile,
				opts.SSLKeyfile,
			)
			if err != nil {
				log.Fatalf("failed to load TLS keys: %s", err.Error())
			}
			grpcServer = grpc.NewServer(
				grpc.UnaryInterceptor(unaryLoggingInterceptor(opts.server)),
				grpc.Creds(creds),
			)
		} else {
			grpcServer = grpc.NewServer(grpc.UnaryInterceptor(unaryLoggingInterceptor(opts.server)))
		}
		opts.grpcServer = grpcServer

		pb.RegisterKeyValueStoreServer(
			grpcServer,
			cliOpts.server,
		)

		defaultLogger.Info(
			"starting server",
			"config", *serverConfig,
			slog.String("listen_address", lis.Addr().String()),
			slog.String("config_file", opts.configFile),
			slog.String("ssl_certfile", opts.SSLCertfile),
			slog.String("ssl_keyfile", opts.SSLKeyfile),
			slog.String("log_level", cliOpts.LogLevel.String()),
			slog.Group(
				"snapshot",
				slog.String("dir", snapshotDir),
				slog.Duration("interval", snapshotInterval),
			),
		)
		if runSnapshotter {
			go snapshotter.Run(ctx)
		}
		wg := &sync.WaitGroup{}

		go func() {
			select {
			case <-ctx.Done():
				wg.Add(1)
				defer wg.Done()
				grpcServer.GracefulStop()
				if snapshotOnShutdown {
					snapshotter.Stop()
					finalSnapshotFilename := fmt.Sprintf(
						"gokv-snapshot-%d.json.gz",
						time.Now().Unix(),
					)
					finalSnapshot := filepath.Join(
						snapshotDir,
						finalSnapshotFilename,
					)
					err = snapshotter.Snapshot(finalSnapshot)
					if err == nil {
						snapshotter.Logger().Info(
							"saved snapshot on shutdown",
							slog.String("file", finalSnapshot),
						)
					} else {
						snapshotter.Logger().Error(
							"failed to save final snapshot",
							slog.String("file", finalSnapshot),
							slog.String("error", err.Error()),
						)
					}
					snapshotter.Logger().Info(
						fmt.Sprintf(
							"snapshots created during runtime: %v",
							snapshotter.Snapshots,
						),
					)
				}
			}
		}()

		err = grpcServer.Serve(lis)
		if err == nil {
			fmt.Println("done")
		} else {
			defaultLogger.Error(
				fmt.Sprintf(
					"Failed to serve: %v",
					err,
				),
			)
		}
		wg.Wait()
		return err
	},
}

func latestSnapshot(dir string) (filename string, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	var latestTime time.Time
	var latestFile string
	pattern := regexp.MustCompile(`gokv-snapshot-(\d+)\.json\.gz`)

	for _, file := range files {
		matches := pattern.FindStringSubmatch(file.Name())
		if len(matches) == 2 {
			var ts int64
			ts, err = strconv.ParseInt(matches[1], 10, 64)
			if err != nil {
				continue
			}
			fileTime := time.Unix(ts, 0)
			if fileTime.After(latestTime) {
				latestTime = fileTime
				latestFile = file.Name()
			}
		}
	}
	if latestFile == "" {
		return "", nil
	}
	return filepath.Join(dir, latestFile), nil
}

func unaryLoggingInterceptor(srv *server.KeyValueStore) grpc.UnaryServerInterceptor {
	f := func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		p, ok := peer.FromContext(ctx)
		if ok && p != nil {
			srv.Logger().InfoContext(
				ctx, "received call",
				slog.Any("metadata", md),
				slog.Group(
					"peer",
					slog.String("address", p.Addr.String()),
					slog.Any("auth_type", p.AuthInfo),
				),
			)
		} else {
			srv.Logger().InfoContext(
				ctx, "received call",
				slog.Any("metadata", md),
			)
		}

		return handler(ctx, req)
	}
	return f
}

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.PersistentFlags().StringVar(
		&cliOpts.configFile,
		"config",
		"",
		"config file (default is $HOME/.gokv.yaml)",
	)
	serverCmd.Flags().Uint64Var(
		&cliOpts.Server.MaxNumberOfKeys,
		"max-keys",
		server.DefaultMaxKeys,
		"Maximum number of keys",
	)
	serverCmd.Flags().Uint64Var(
		&cliOpts.Server.MaxValueSize,
		"max-value-size",
		server.DefaultMaxValueSize,
		"Maximum size of a value (in bytes)",
	)
	serverCmd.Flags().Uint64Var(
		&cliOpts.Server.MaxKeySize,
		"max-key-size",
		server.DefaultMaxKeySize,
		"Maximum size of a key (in bytes)",
	)
	serverCmd.Flags().Int64Var(
		&cliOpts.Server.RevisionLimit,
		"revision-limit",
		server.DefaultRevisionLimit,
		"Maximum size of history. 0=disabled, -1=unlimited",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.Server.KeepExpiredKeys,
		"keep-expired-keys",
		false,
		"Do not delete expired keys",
	)
	serverCmd.Flags().Var(
		&cliOpts.hashAlgorithm,
		"hash-algorithm",
		"Hash algorithm to use for key hashing",
	)
	//serverCmd.Flags().StringVar(
	//	&cliOpts.Server.Backup,
	//	"backup",
	//	"",
	//	"Backup file to load on startup and write on shutdown",
	//)
	serverCmd.Flags().StringVar(
		&cliOpts.LoadSnapshot,
		"load-snapshot",
		"",
		"Loads the specified snapshot file on startup",
	)
	serverCmd.Flags().StringVar(
		&cliOpts.SnapshotDir,
		"snapshot-dir",
		"",
		"Directory to store snapshots. If specified, the server will "+
			"load the most recent snapshot found in the directory on startup, "+
			"matching the filename gokv-snapshot-*.json.gz. On shutdown, the "+
			"server will write a final snapshot, regardless of the snapshot "+
			"interval.",
	)
	serverCmd.MarkFlagDirname("snapshot-dir")
	serverCmd.Flags().DurationVar(
		&cliOpts.SnapshotInterval,
		"snapshot-interval",
		0,
		"Interval to take snapshots",
	)
	serverCmd.Flags().IntVar(
		&cliOpts.SnapshotLimit,
		"snapshot-limit",
		0,
		"Maximum number of snapshots to keep. 0=unlimited. Otherwise, "+
			"whenever a new snapshot is created, the oldest snapshot(s) over the "+
			"limit will be deleted (only snapshots created by the same process "+
			"will be removed, not all snapshots in the directory).",
	)
}
