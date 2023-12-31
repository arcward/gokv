package cmd

import (
	"context"
	"fmt"
	"github.com/arcward/keyquarry/build"
	"github.com/arcward/keyquarry/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
		if opts.ClientOpts.Verbose {
			logLevel = slog.LevelDebug
		} else {
			logLevel, _ = getLogLevel(opts.LogLevel)
			if logLevel == slog.LevelInfo && !rootCmd.Flags().Changed("log-level") {
				logLevel = slog.LevelWarn
			}
		}

		cfg := &opts.ClientOpts

		u, err := parseURL(opts.ClientOpts.Address)
		if err != nil {
			return fmt.Errorf("invalid address: %w", err)
		}
		if u.Scheme == "unix" {
			_, err = os.Stat(u.Host)
			if err != nil && os.IsNotExist(err) {
				return fmt.Errorf("socket file '%s' not found", u.Host)
			}
			opts.ClientOpts.Address = opts.ClientOpts.Address
			//opts.ClientOpts.Address = u.String()
		} else {
			opts.ClientOpts.Address = u.Host
		}

		var outp io.Writer
		if opts.ClientOpts.Quiet {
			outp = io.Discard
		} else {
			outp = os.Stderr
		}
		log.SetOutput(outp)

		handlerOptions := &slog.HandlerOptions{
			Level:     logLevel,
			AddSource: true,
		}

		var handler slog.Handler

		if opts.LogJSON {
			handler = slog.NewJSONHandler(outp, handlerOptions)
		} else {
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

		cfg.Logger = defaultLogger

		if cfg.ClientID == "" {
			clientID := viper.GetString("client_id")
			if clientID == "" {
				hostname, err := os.Hostname()
				if err != nil {
					return err
				}
				user, err := user.Current()
				if err != nil {
					return err
				}
				clientID = fmt.Sprintf("%s@%s", user.Username, hostname)
			}
			cfg.ClientID = clientID
		}

		kvClient := opts.client
		if kvClient == nil {
			kvClient = client.NewClient(cfg, grpc.WithBlock())
			opts.client = kvClient
		}

		opts.client = kvClient
		connCtx, connCancel := context.WithTimeout(
			ctx,
			opts.ClientOpts.DialTimeout,
		)
		dialErr := kvClient.Dial(connCtx, true)
		connCancel()
		if dialErr != nil {
			statusCode := status.Code(dialErr)
			if statusCode != codes.AlreadyExists {
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
		&cliOpts.ClientOpts.Address,
		"address",
		"a",
		defaultAddress,
		"Address to connect to/listen from",
	)
	clientCmd.PersistentFlags().StringVar(
		&cliOpts.ClientOpts.ClientID,
		"client-id",
		"",
		"Set the client ID (default: {user}@{host}). Identifies the client to the server for updates, unlocking, etc.",
	)
	clientCmd.PersistentFlags().StringVar(
		&cliOpts.ClientOpts.CACert,
		"ca-certfile",
		"",
		"SSL certificate file",
	)
	clientCmd.MarkFlagFilename("ca-certfile")
	clientCmd.PersistentFlags().BoolVar(
		&cliOpts.ClientOpts.Quiet,
		"quiet",
		false,
		"Disables log output",
	)
	clientCmd.PersistentFlags().BoolVar(
		&cliOpts.ClientOpts.InsecureSkipVerify,
		"insecure",
		false,
		"disables certificate verification",
	)
	clientCmd.PersistentFlags().BoolVar(
		&cliOpts.ClientOpts.NoTLS,
		"no-tls",
		false,
		"disables TLS",
	)
	clientCmd.PersistentFlags().BoolVarP(
		&cliOpts.ClientOpts.Verbose,
		"verbose",
		"v",
		false,
		"(Client only) Enables debug-level logging",
	)
	clientCmd.PersistentFlags().IntVar(
		&cliOpts.ClientOpts.IndentJSON,
		"indent",
		0,
		"Number of spaces to indent client JSON output",
	)
}
