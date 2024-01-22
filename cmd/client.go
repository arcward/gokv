package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/arcward/keyquarry/build"
	"github.com/arcward/keyquarry/client"
	"github.com/arcward/keyquarry/server"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"log/slog"
	"os"
	"os/user"
)

// clientCmd represents the client command
var clientCmd = &cobra.Command{
	Use:          "client",
	Short:        "Client operations",
	SilenceUsage: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		opts := &cliOpts

		var logLevel slog.Level
		switch {
		case opts.clientOpts.Verbose:
			logLevel = slog.LevelDebug
		default:
			logLevel, _ = getLogLevel(opts.LogLevel)
			if logLevel == slog.LevelInfo && !rootCmd.Flags().Changed("log-level") {
				logLevel = slog.LevelWarn
			}
		}

		u, err := parseURL(opts.clientOpts.Address)
		if err != nil {
			return fmt.Errorf("invalid address: %w", err)
		}

		switch u.Scheme {
		case "unix":
			_, err = os.Stat(u.Host)
			if err != nil && os.IsNotExist(err) {
				return fmt.Errorf("socket file '%s' not found", u.Host)
			}
		default:
			opts.clientOpts.Address = u.Host
		}

		var outp io.Writer
		switch {
		case opts.clientOpts.Quiet:
			outp = io.Discard
		default:
			outp = os.Stderr
		}
		log.SetOutput(outp)

		handlerOptions := &slog.HandlerOptions{
			Level:     logLevel,
			AddSource: true,
		}

		var handler slog.Handler
		switch {
		case opts.LogJSON:
			handler = slog.NewJSONHandler(outp, handlerOptions)
		default:
			handler = slog.NewTextHandler(outp, handlerOptions)
		}
		defaultLogger = slog.New(handler).With("logger", "default")

		if build.Version != "" {
			defaultLogger = defaultLogger.With(
				slog.String(
					"version",
					build.Version,
				),
			)
		}
		slog.SetDefault(defaultLogger)

		dialOpts := []grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithKeepaliveParams(
				keepalive.ClientParameters{
					Time:    opts.clientOpts.DialKeepAliveTime,
					Timeout: opts.clientOpts.DialKeepAliveTimeout,
				},
			),
		}

		switch {
		case opts.clientOpts.NoTLS:
			defaultLogger.Warn("connection not encrypted")
			dialOpts = append(
				dialOpts,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
		default:
			tlsSet := false
			if opts.clientOpts.CACert != "" {
				caCreds, err := credentials.NewClientTLSFromFile(
					opts.clientOpts.CACert,
					"",
				)
				if err != nil {
					log.Fatalln(err)
				}
				dialOpts = append(
					dialOpts,
					grpc.WithTransportCredentials(caCreds),
				)
				tlsSet = true
			}

			if opts.clientOpts.InsecureSkipVerify {
				tlsConfig := &tls.Config{InsecureSkipVerify: true}
				insecureCreds := credentials.NewTLS(tlsConfig)
				dialOpts = append(
					dialOpts,
					grpc.WithTransportCredentials(insecureCreds),
				)
				tlsSet = true
			}

			if !tlsSet {
				defaultLogger.Warn("insecure connection")
				dialOpts = append(
					dialOpts,
					grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
				)
			}
		}

		kvClient := opts.client
		if kvClient == nil {
			kvClient = client.New(
				opts.clientOpts.Address,
				opts.clientOpts.ClientID,
				defaultLogger,
				nil,
				dialOpts...,
			)
			opts.client = kvClient
		}

		opts.client = kvClient
		connCtx, connCancel := context.WithTimeout(
			ctx,
			opts.clientOpts.DialTimeout,
		)
		dialErr := kvClient.Dial(connCtx, true)
		connCancel()
		if dialErr != nil {
			statusCode := status.Code(dialErr)
			if statusCode != codes.AlreadyExists {
				fmt.Printf("err: %s", dialErr.Error())
				log.Fatalf("unable to connect: %s\n", dialErr.Error())
			}
		}

		go func() {
			for {
				select {
				case <-ctx.Done():
					log.Printf("closing connection")
					if opts.client != nil {
						err = opts.client.CloseConnection()
						if err != nil {
							log.Printf(
								"error closing connection: %s",
								err.Error(),
							)
						}
						os.Exit(1)
					}
					return
				}
			}
		}()

		return err
	},
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(clientCmd)
	clientCmd.PersistentFlags().StringVarP(
		&cliOpts.clientOpts.Address,
		"address",
		"a",
		server.DefaultAddress,
		"Address to connect to/listen from",
	)

	var clientID string
	clientID, _ = os.Hostname()
	clientUser, _ := user.Current()
	if clientUser != nil && clientUser.Username != "" {
		switch clientID {
		case "":
			clientID = clientUser.Username
		default:
			clientID = fmt.Sprintf("%s@%s", clientUser.Username, clientID)
		}
	}
	clientCmd.PersistentFlags().StringVar(
		&cliOpts.clientOpts.ClientID,
		"client-id",
		clientID,
		"Set the client ID (default: {user}@{host}). Identifies the client to the server for updates, unlocking, etc.",
	)
	clientCmd.PersistentFlags().StringVar(
		&cliOpts.clientOpts.CACert,
		"ca-certfile",
		"",
		"SSL certificate file",
	)

	cobra.CheckErr(clientCmd.MarkPersistentFlagFilename("ca-certfile"))
	clientCmd.PersistentFlags().BoolVar(
		&cliOpts.clientOpts.Quiet,
		"quiet",
		false,
		"Disables log output",
	)
	clientCmd.PersistentFlags().BoolVar(
		&cliOpts.clientOpts.InsecureSkipVerify,
		"insecure",
		false,
		"disables certificate verification",
	)
	clientCmd.PersistentFlags().BoolVar(
		&cliOpts.clientOpts.NoTLS,
		"no-tls",
		false,
		"disables TLS",
	)
	clientCmd.PersistentFlags().BoolVarP(
		&cliOpts.clientOpts.Verbose,
		"verbose",
		"v",
		false,
		"(Client only) Enables debug-level logging",
	)
	clientCmd.PersistentFlags().IntVar(
		&cliOpts.clientOpts.IndentJSON,
		"indent",
		0,
		"Number of spaces to indent client JSON output",
	)

	clientCmd.PersistentFlags().DurationVar(
		&cliOpts.clientOpts.DialTimeout,
		"dial-timeout",
		client.DefaultDialTimeout,
		"Timeout for dialing the server",
	)
	clientCmd.PersistentFlags().DurationVar(
		&cliOpts.clientOpts.DialKeepAliveTime,
		"dial-keepalive-time",
		client.DefaultDialKeepAliveTime,
		"Duration between keepalive pings sent to the server when no activity is seen",
	)
	clientCmd.PersistentFlags().DurationVar(
		&cliOpts.clientOpts.DialKeepAliveTimeout,
		"dial-keepalive-timeout",
		client.DefaultDialKeepAliveTimeout,
		"Time to wait after --dial-keepalive-time ping before assuming the connection is dead",
	)
}
