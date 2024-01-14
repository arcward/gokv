package cmd

import (
	"context"
	"database/sql"
	"errors"
	"expvar"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	msdk "go.opentelemetry.io/otel/sdk/metric"
	tsdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace/noop"

	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/arcward/keyquarry/build"
	"github.com/arcward/keyquarry/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var serverCmd = &cobra.Command{
	Use:          "server",
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

		traceProvider, meterProvider, setupErr := setupOTelSDK(
			ctx,
			serverConfig.ServiceName,
			build.Version,
			serverConfig.Trace,
			serverConfig.Metrics,
		)
		printError(setupErr)

		if traceProvider == nil {
			otel.SetTracerProvider(noop.TracerProvider{})
		} else {
			otel.SetTracerProvider(traceProvider)
			defer func() {
				tpCtx, tpCancel := context.WithTimeout(
					context.Background(),
					30*time.Second,
				)
				_ = traceProvider.Shutdown(tpCtx)
				tpCancel()
			}()
		}
		if meterProvider == nil {
			otel.SetMeterProvider(mnoop.MeterProvider{})
		} else {
			otel.SetMeterProvider(meterProvider)
			defer func() {
				mpCtx, mpCancel := context.WithTimeout(
					context.Background(),
					30*time.Second,
				)
				_ = meterProvider.Shutdown(mpCtx)
				mpCancel()
			}()
		}

		if serverConfig.PruneInterval > 0 &&
			serverConfig.PruneInterval < serverConfig.MinPruneInterval {
			return fmt.Errorf(
				"prune interval must be at least %s",
				serverConfig.MinPruneInterval,
			)
		}
		if serverConfig.PruneInterval > 0 &&
			serverConfig.PruneTarget > serverConfig.PruneThreshold {
			return fmt.Errorf(
				"prune target must be less than prune threshold",
			)
		}
		if serverConfig.PruneTarget == 0 {
			serverConfig.PruneTarget = serverConfig.PruneThreshold
		}

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

		if opts.StartFresh || serverConfig.Snapshot.Database == "" {
			srv, err = server.NewServer(serverConfig)
			if err != nil {
				return fmt.Errorf("failed to create server: %w", err)
			}
		} else if serverConfig.Snapshot.Database != "" {
			dialect := server.GetDialect(serverConfig.Snapshot.Database)
			if dialect == nil {
				return fmt.Errorf("unsupported or invalid database driver in connection string")
			}
			err = dialect.InitDB(ctx, serverConfig.Snapshot.Database)
			if err != nil {
				return fmt.Errorf("unable to initialize database: %w", err)
			}
			srv, err = server.NewServerFromLatestSnapshot(ctx, serverConfig)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					defaultLogger.Warn(
						"no snapshot found, creating new server",
					)
					srv, err = server.NewServer(serverConfig)
					if err != nil {
						return fmt.Errorf("failed to create server: %w", err)
					}
				} else {
					return fmt.Errorf(
						"failed to create server from snapshot: %w",
						err,
					)
				}

			}
		} else {
			srv, err = server.NewServer(serverConfig)
			if err != nil {
				return fmt.Errorf("failed to create server: %w", err)
			}
		}
		opts.server = srv

		baseOpts := &cliOpts
		statsHandler := otelgrpc.NewServerHandler()
		serverOpts := []grpc.ServerOption{
			grpc.StatsHandler(statsHandler),
			grpc.UnaryInterceptor(server.ClientIDInterceptor(baseOpts.server)),
			grpc.KeepaliveParams(
				keepalive.ServerParameters{
					Time:    server.DefaultKeepaliveTime,
					Timeout: server.DefaultKeepaliveTimeout,
				},
			),
		}

		if serverConfig.SSLCertfile != "" && serverConfig.SSLKeyfile != "" {
			creds, err := credentials.NewServerTLSFromFile(
				serverConfig.SSLCertfile,
				serverConfig.SSLKeyfile,
			)
			if err != nil {
				log.Fatalf("failed to load TLS keys: %s", err.Error())
			}
			serverOpts = append(serverOpts, grpc.Creds(creds))
		}
		grpcServer := grpc.NewServer(serverOpts...)
		baseOpts.grpcServer = grpcServer

		pb.RegisterKeyValueStoreServer(
			grpcServer,
			cliOpts.server,
		)
		settings := []slog.Attr{slog.String("config.file", baseOpts.configFile)}
		var monitorServer *http.Server
		for k, v := range viper.AllSettings() {
			if k == "privileged_client_id" {
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

		if srv.MonitorMux != nil {
			monitorServer = &http.Server{
				Addr:    serverConfig.MonitorAddress,
				Handler: srv.MonitorMux,
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				fmt.Println("starting monitor server")
				defaultLogger.Info(
					"starting monitor server",
					"address",
					serverConfig.MonitorAddress,
				)
				if httpErr := monitorServer.ListenAndServe(); httpErr != nil {
					if !errors.Is(httpErr, http.ErrServerClosed) {
						defaultLogger.Error(
							"failed to start monitor server",
							slog.String("error", httpErr.Error()),
						)
					}
				}
			}()
		}

		// Start the event stream and snapshotter - when the context is
		// cancelled, that propagates to both, and the Start() function
		// should return.
		srvDone := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()

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
				defaultLogger.Info(
					"shutting down server",
					"timeout",
					serverConfig.GracefulStopTimeout,
				)
				grpcServerStopped := make(chan struct{}, 1)
				if serverConfig.GracefulStopTimeout == 0 {
					grpcServer.GracefulStop()
					grpcServerStopped <- struct{}{}
					close(grpcServerStopped)
				} else {
					gsTimer := time.NewTimer(serverConfig.GracefulStopTimeout)
					go func() {
						grpcServer.GracefulStop()
						grpcServerStopped <- struct{}{}
						close(grpcServerStopped)
					}()
					select {
					case <-gsTimer.C:
						defaultLogger.Warn(
							"graceful stop timed out, forcing stop",
							"timeout",
							serverConfig.GracefulStopTimeout,
						)
						grpcServer.Stop()
					case <-grpcServerStopped:
						defaultLogger.Info("grpc server stopped gracefully")
					}
				}

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

				if monitorServer != nil {
					monitorCtx, monitorCancel := context.WithTimeout(
						context.Background(),
						15*time.Second,
					)
					shutErr := monitorServer.Shutdown(monitorCtx)
					monitorCancel()
					defaultLogger.Error(
						"error shutting down monitor endpoints",
						"error",
						shutErr,
					)
				}
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
		&cliOpts.ServerOpts.MaxKeyLength,
		"max-key-length",
		server.DefaultMaxKeyLength,
		"Maximum length of a key (in bytes)",
	)
	serverCmd.Flags().Int64Var(
		&cliOpts.ServerOpts.RevisionLimit,
		"revision-limit",
		server.DefaultRevisionLimit,
		"Maximum size of history. 0=disabled, -1=unlimited",
	)
	snapopts := &cliOpts.ServerOpts.Snapshot

	serverCmd.Flags().DurationVar(
		&snapopts.Interval,
		"snapshot-interval",
		0,
		"Interval to take snapshots",
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

	expvar.Publish(
		"keyquarry",
		expvar.Func(
			func() any {
				var serverMetrics *pb.ServerMetrics
				currentSrv := cliOpts.server
				if currentSrv != nil {
					serverMetrics = currentSrv.GetStats()
				}
				return serverMetrics
			},
		),
	)
}

func setupOTelSDK(
	ctx context.Context,
	serviceName,
	serviceVersion string,
	traces bool,
	metrics bool,
) (
	traceProvider *tsdk.TracerProvider,
	metricProvider *msdk.MeterProvider,
	err error,
) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	// Set up propagator.
	prop := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	otel.SetTextMapPropagator(prop)
	if traces {
		traceExporter, te := otlptracegrpc.New(ctx)
		if te != nil {
			return nil, nil, te
		}

		traceProvider = tsdk.NewTracerProvider(
			tsdk.WithBatcher(
				traceExporter,
				// Default is 5s. Set to 1s for demonstrative purposes.
				tsdk.WithBatchTimeout(time.Second),
			),
			tsdk.WithResource(res),
		)
	}

	if metrics {
		metricExporter, merr := otlpmetricgrpc.New(ctx)
		if merr != nil {
			return nil, nil, err
		}

		metricProvider = msdk.NewMeterProvider(
			msdk.WithResource(res),
			msdk.WithReader(
				msdk.NewPeriodicReader(
					metricExporter,
					// Default is 1m. Set to 3s for demonstrative purposes.
					msdk.WithInterval(3*time.Second),
				),
			),
		)
	}

	return traceProvider, metricProvider, nil
}
