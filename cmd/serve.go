package cmd

import (
	"database/sql"
	"errors"
	"expvar"
	"log/slog"
	"os/user"

	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/arcward/keyquarry/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
)

var serverCmd = &cobra.Command{
	Use:          "serve",
	Short:        "Starts a server",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		opts := &cliOpts
		serverConfig := &opts.ServerOpts

		err := viper.Unmarshal(serverConfig)
		if err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}

		ctx := cmd.Context()

		log.SetOutput(os.Stdout)

		var srv *server.Server

		if serverConfig.StartFresh || !serverConfig.Snapshot.Enabled {
			srv, err = server.New(serverConfig)
			if err != nil {
				return fmt.Errorf("failed to create server: %w", err)
			}
		}

		if srv == nil && serverConfig.Snapshot.Enabled {
			dialect := server.GetDialect(serverConfig.Snapshot.Database)
			if dialect == nil {
				return fmt.Errorf("unsupported or invalid database driver in connection string")
			}

			if err = dialect.InitDB(
				ctx,
				serverConfig.Snapshot.Database,
			); err != nil {
				return fmt.Errorf("unable to initialize database: %w", err)
			}

			srv, err = server.NewFromLatestSnapshot(ctx, serverConfig)
			// the returned error may just be the db being empty, or no
			// snapshot records for the configured server name
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf(
					"failed to create server from snapshot: %w",
					err,
				)
			}
		}

		if srv == nil {
			srv, err = server.New(serverConfig)
			if err != nil {
				return fmt.Errorf("failed to create server: %w", err)
			}
		}

		defaultLogger = srv.Config().Logger
		slog.SetDefault(defaultLogger)

		if serverConfig.ExpVar {
			expvar.Publish(
				server.InternalClientID,
				expvar.Func(
					func() any {
						var serverMetrics *pb.ServerMetrics

						if srv != nil {
							serverMetrics = srv.GetStats()
						}
						return serverMetrics
					},
				),
			)
		}
		return srv.Serve(cmd.Context())
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.Flags().StringVar(
		&cliOpts.ServerOpts.ListenAddress,
		"listen",
		server.DefaultAddress,
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

	serverCmd.Flags().StringVar(
		&cliOpts.ServerOpts.MonitorAddress,
		"monitor-address",
		server.DefaultMonitorAddress,
		"HTTP server listen address when using prometheus metrics, pprof or expvar",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.ServerOpts.Prometheus,
		"prometheus",
		false,
		"Enable prometheus /metrics endpoint",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.ServerOpts.PPROF,
		"pprof",
		false,
		"Enables /debug/pprof",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.ServerOpts.ExpVar,
		"expvar",
		false,
		"Enables /debug/vars",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.ServerOpts.OTLPTrace,
		"trace",
		false,
		"Enables opentelemetry traces",
	)

	snapopts := &cliOpts.ServerOpts.Snapshot

	serverCmd.Flags().DurationVar(
		&snapopts.Interval,
		"snapshot-interval",
		0,
		"Interval to take snapshots",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.ServerOpts.StartFresh,
		"fresh",
		false,
		"Start with a blank slate (ignores existing snapshot data)",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.ServerOpts.Readonly,
		"readonly",
		false,
		"Starts the server in read-only mode",
	)

	var defaultServerName string
	hostname, _ := os.Hostname()
	if hostname != "" {
		defaultServerName = hostname
	}

	u, _ := user.Current()
	if u != nil {
		username := u.Username
		if username != "" {
			defaultServerName = fmt.Sprintf(
				"%s@%s",
				username,
				defaultServerName,
			)
		}
	}
	serverCmd.Flags().StringVar(
		&cliOpts.ServerOpts.Name,
		"name",
		defaultServerName,
		"Sets the server name to associate with snapshots",
	)
}
