package cmd

import (
	"context"
	"expvar"
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/arcward/keyquarry/build"
	"github.com/arcward/keyquarry/server"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"regexp"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Starts a server",
	//SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		cc := &cliOpts
		if cc.ProfileCPU {
			f, _ := os.Create("cpuprofile")
			profErr := pprof.StartCPUProfile(f)
			if profErr != nil {
				return profErr
			}

			defer pprof.StopCPUProfile()
			go func() {
				log.Println(http.ListenAndServe("localhost:5001", nil))
			}()
		}
		opts := &cliOpts
		serverConfig := &opts.ServerOpts

		if viper.GetString("snapshot.secret_key") != "" {
			viper.Set("encrypt_snapshots", true)
		}
		err := viper.Unmarshal(
			serverConfig, viper.DecodeHook(
				mapstructure.ComposeDecodeHookFunc(
					mapstructure.StringToTimeDurationHookFunc(),
					mapstructure.StringToSliceHookFunc(","),
					decodeHashType(),
				),
			),
		)
		if err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}

		if serverConfig.PruneInterval > 0 && serverConfig.PruneInterval < serverConfig.MinPruneInterval {
			return fmt.Errorf(
				"prune interval must be at least %s",
				serverConfig.MinPruneInterval,
			)
		}
		if serverConfig.PruneInterval > 0 && serverConfig.PruneTarget > serverConfig.PruneThreshold {
			return fmt.Errorf(
				"prune target must be less than prune threshold",
			)
		}
		if serverConfig.PruneTarget == 0 {
			serverConfig.PruneTarget = serverConfig.PruneThreshold
		}

		ctx := cmd.Context()
		//if !rootCmd.Flags().Changed("log-level") {
		//	serverConfig.LogLevel = viper.GetString("log_level")
		//}
		log.SetOutput(os.Stdout)

		logLevel, _ := getLogLevel(serverConfig.LogLevel)
		handlerOptions := &slog.HandlerOptions{
			Level:     logLevel,
			AddSource: true,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				switch a.Key {
				case slog.SourceKey:
					source := a.Value.Any().(*slog.Source)
					source.File = filepath.Base(source.File)
				}
				return a
			},
		}
		var handler slog.Handler
		if cliOpts.LogJSON {
			handler = slog.NewJSONHandler(os.Stdout, handlerOptions)
		} else {
			handler = slog.NewTextHandler(os.Stdout, handlerOptions)
		}
		defaultLogger = slog.New(handler)
		slog.SetDefault(defaultLogger)

		u, err := parseURL(serverConfig.ListenAddress)
		if err != nil {
			return fmt.Errorf("invalid address: %w", err)
		}
		if u.Scheme == "unix" {
			_, err = os.Stat(u.Host)
			if err == nil || !os.IsNotExist(err) {
				return fmt.Errorf(
					"socket file '%s' already exists",
					u.Host,
				)
			}
		}

		serverConfig.ListenAddress = u.String()
		lis, err := net.Listen(
			u.Scheme,
			u.Host,
		)
		if err != nil {
			return fmt.Errorf("failed to listen: %w", err)
		}

		snapshotDir := serverConfig.Snapshot.Dir

		if snapshotDir != "" {
			snapshotDir, err := filepath.Abs(snapshotDir)
			if err != nil {
				return fmt.Errorf(
					"failed to get absolute path for snapshot directory '%s': %w",
					snapshotDir,
					err,
				)
			}
			err = os.MkdirAll(snapshotDir, 0700)
			if err == nil {
				defaultLogger.Info(
					"Created snapshot directory",
					"dir",
					snapshotDir,
				)
			} else if !os.IsExist(err) {
				return fmt.Errorf(
					"failed to create snapshot directory '%s': %w",
					snapshotDir,
					err,
				)
			}
			serverConfig.Snapshot.Dir = snapshotDir
		}

		var snapshotInitFile string
		var snapshotData []byte
		var snapshotErr error
		if serverConfig.Snapshot.LoadFile != "" {
			snapshotInitFile = serverConfig.Snapshot.LoadFile
		} else if serverConfig.Snapshot.Enabled {
			snapshotInitFile, snapshotErr = latestSnapshot(
				serverConfig.Snapshot.Dir,
				serverConfig.Snapshot.Encrypt,
			)
		}

		if snapshotErr != nil {
			return fmt.Errorf(
				"failed to load latest snapshot from %s: %w",
				snapshotDir,
				snapshotErr,
			)
		}

		if snapshotInitFile != "" {
			snapshotData, snapshotErr = os.ReadFile(snapshotInitFile)
			if snapshotErr != nil {
				return fmt.Errorf(
					"failed to read snapshot file '%s': %w",
					serverConfig.Snapshot.LoadFile,
					err,
				)
			}
			if snapshotData == nil {
				return fmt.Errorf(
					"snapshot file '%s' is empty",
					serverConfig.Snapshot.LoadFile,
				)
			}
		}

		if build.Version != "" {
			defaultLogger = defaultLogger.With(
				slog.String(
					"version",
					build.Version,
				),
			)
		}

		serverConfig.Logger = defaultLogger
		srv := cliOpts.server
		if snapshotData == nil {
			defaultLogger.Info(
				"creating new server",
			)
			srv, err = server.NewServer(serverConfig)
			if err != nil {
				return fmt.Errorf("failed to create server: %w", err)
			}
		} else if serverConfig.Snapshot.Encrypt {
			defaultLogger.Info(
				"creating new server from encrypted snapshot",
				slog.Group("snapshot", slog.String("file", snapshotInitFile)),
			)
			srv, err = server.NewServerFromEncryptedSnapshot(
				serverConfig.Snapshot.SecretKey,
				snapshotData,
				serverConfig,
			)
			if err != nil {
				return fmt.Errorf("failed to load snapshot: %w", err)
			}
		} else {
			defaultLogger.Info(
				"creating new server from snapshot",
				slog.Group("snapshot", slog.String("file", snapshotInitFile)),
			)
			srv, err = server.NewServerFromSnapshot(
				snapshotData,
				serverConfig,
			)
			if err != nil {
				return fmt.Errorf("failed to load snapshot: %w", err)
			}
		}

		opts.server = srv
		var grpcServer *grpc.Server

		baseOpts := &cliOpts
		if serverConfig.SSLCertfile != "" && serverConfig.SSLKeyfile != "" {
			creds, err := credentials.NewServerTLSFromFile(
				serverConfig.SSLCertfile,
				serverConfig.SSLKeyfile,
			)
			if err != nil {
				log.Fatalf("failed to load TLS keys: %s", err.Error())
			}
			grpcServer = grpc.NewServer(
				grpc.UnaryInterceptor(server.ClientIDInterceptor(baseOpts.server)),
				grpc.Creds(creds),
			)
		} else {
			grpcServer = grpc.NewServer(
				grpc.UnaryInterceptor(
					server.ClientIDInterceptor(baseOpts.server),
				),
			)
		}
		baseOpts.grpcServer = grpcServer

		pb.RegisterKeyValueStoreServer(
			grpcServer,
			cliOpts.server,
		)
		settings := []slog.Attr{slog.String("config.file", baseOpts.configFile)}

		for k, v := range viper.AllSettings() {
			if k == "secret_key" || k == "privileged_client_ids" {
				continue
			}
			settings = append(
				settings,
				slog.Any(fmt.Sprintf("config.%s", k), v),
			)
		}
		defaultLogger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"starting server",
			settings...,
		)

		sctx, scancel := context.WithCancel(ctx)

		wg := &sync.WaitGroup{}

		// Start the event stream and snapshotter - when the context is
		// cancelled, that propagates to both, and the Start() function
		// should return.
		srvDone := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			expvar.Publish(
				"keyquarry",
				expvar.Func(func() any { return srv.GetStats() }),
			)
			if e := srv.Start(sctx); e != nil {
				panic(fmt.Errorf("failed to start server: %w", e))
			}
			srvDone <- struct{}{}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				// When the main context is cancelled, gracefully stop
				// the GRPC server. When that's done, we cancel the
				// KeyValueStore context that was passed to Start().
				// We don't use the same context or a child context for
				// that, because it might be cancelled before the server
				// is done shutting down.
				// Then, we wait for the signal that Start() has returned,
				// so we know it's safe to call Stop(), which will stop the
				// event stream, close its channels, and take a final
				// snapshot.
				defaultLogger.Info("shutting down server")
				grpcServer.GracefulStop()

				socketCleanupDone := make(chan struct{}, 1)
				if u.Scheme == "unix" {
					go func() {
						socketErr := os.Remove(u.Host)
						if socketErr != nil && !os.IsNotExist(socketErr) {
							defaultLogger.Error(
								"failed to remove socket file",
								slog.String("error", socketErr.Error()),
							)
						}
						socketCleanupDone <- struct{}{}
					}()
				} else {
					socketCleanupDone <- struct{}{}
				}

				// cancel KeyValueStore context, wait for Start() to return,
				// then stop. Finally, wait for the socket cleanup to finish.
				scancel()
				<-srvDone
				defaultLogger.Debug("got done signal, waiting for server to stop")
				stopErr := srv.Stop()
				if stopErr != nil {
					defaultLogger.Error(
						"failed to stop server",
						slog.String("error", stopErr.Error()),
					)
				}
				defaultLogger.Debug("waiting for socket cleanup, if needed")
				<-socketCleanupDone
				return
			}
		}()

		if err = grpcServer.Serve(lis); err != nil {
			defaultLogger.Error(
				"failed to serve",
				slog.String("error", err.Error()),
			)
		}
		// Waits for KeyValueStore.Start() to return, for the GRPC
		// server to shut down gracefully, for KeyValueStore.Stop() to
		// finish, and possibly for a unix socket file to be removed.
		wg.Wait()
		defaultLogger.Info("server stopped")
		return nil
	},
}

func latestSnapshot(dir string, encrypted bool) (filename string, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	var latestTime time.Time
	var latestFile string
	var pattern *regexp.Regexp
	if encrypted {
		pattern = regexp.MustCompile(`(\d+)\.json\.aes\.gz`)
	} else {
		pattern = regexp.MustCompile(`(\d+)\.json\.gz`)
	}

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

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.Flags().StringVar(
		&cliOpts.ServerOpts.ListenAddress,
		"listen",
		defaultAddress,
		"Address to listen on",
	)

	serverCmd.Flags().StringVar(
		&cliOpts.ServerOpts.SSLCertfile,
		"ssl-certfile",
		"",
		"SSL certificate file",
	)
	_ = serverCmd.MarkFlagFilename("ssl-certfile")
	serverCmd.Flags().StringVar(
		&cliOpts.ServerOpts.SSLKeyfile,
		"ssl-keyfile",
		"",
		"SSL key file",
	)
	serverCmd.Flags().StringVar(
		&cliOpts.configFile,
		"config",
		"",
		"config file (default is keyquarry-server.env)",
	)
	serverCmd.Flags().Uint64Var(
		&cliOpts.ServerOpts.MaxNumberOfKeys,
		"max-keys",
		server.DefaultMaxKeys,
		"Maximum number of keys",
	)
	serverCmd.Flags().Uint64Var(
		&cliOpts.ServerOpts.MaxValueSize,
		"max-value-bytes",
		server.DefaultMaxValueSize,
		"Maximum size of a value (in bytes)",
	)
	serverCmd.Flags().Uint64Var(
		&cliOpts.ServerOpts.MaxKeySize,
		"max-key-size",
		server.DefaultMaxKeySize,
		"Maximum size of a key (in bytes)",
	)
	serverCmd.Flags().Int64Var(
		&cliOpts.ServerOpts.RevisionLimit,
		"revision-limit",
		server.DefaultRevisionLimit,
		"Maximum size of history. 0=disabled, -1=unlimited",
	)
	//serverCmd.Flags().BoolVar(
	//	&cliOpts.ServerOpts.KeepExpiredKeys,
	//	"keep-expired-keys",
	//	false,
	//	"Do not delete expired keys",
	//)
	//serverCmd.Flags().StringVar(
	//	&cliOpts.ServerOpts.HashAlgorithm,
	//	"hash-algorithm",
	//	"",
	//	"Hash algorithm to use for key hashing",
	//)
	snapopts := &cliOpts.ServerOpts.Snapshot

	serverCmd.Flags().StringVar(
		&snapopts.LoadFile,
		"load-snapshot",
		"",
		"Loads the specified snapshot file on startup",
	)
	serverCmd.Flags().StringVar(
		&snapopts.Dir,
		"snapshot-dir",
		"",
		"Directory t	LogLevel      o store snapshots. If specified, the server will "+
			"load the most recent snapshot found in the directory on startup, "+
			"matching the filename keyquarry-snapshot-*.json.gz. On shutdown, the "+
			"server will write a final snapshot, regardless of the snapshot "+
			"interval.",
	)
	_ = serverCmd.MarkFlagDirname("snapshot-dir")
	serverCmd.Flags().DurationVar(
		&snapopts.Interval,
		"snapshot-interval",
		0,
		"Interval to take snapshots",
	)
	serverCmd.Flags().IntVar(
		&snapopts.Limit,
		"snapshot-limit",
		server.DefaultSnapshotLimit,
		"Maximum number of snapshots to keep. 0=unlimited. Otherwise, "+
			"whenever a new snapshot is created, the oldest snapshot(s) over the "+
			"limit will be deleted (only snapshots created by the same process "+
			"will be removed, not all snapshots in the directory).",
	)
	serverCmd.Flags().BoolVar(
		&snapopts.Encrypt,
		"encrypt-snapshots",
		false,
		"Encrypt snapshots at rest (requires setting KEYQUARRY_SNAPSHOT_SECRET_KEY). If a secret key has been set, this defaults to true",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.ServerOpts.Snapshot.Enabled,
		"snapshot",
		false,
		"Enable snapshots",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.ServerOpts.Readonly,
		"readonly",
		false,
		"Starts the server in read-only mode",
	)
}
