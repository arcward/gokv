package cmd

import (
	"fmt"
	"github.com/arcward/gokv/build"
	"github.com/arcward/gokv/client"
	"github.com/spf13/cobra"
	"io"
	"log"
	"log/slog"
	"os"
)

// clientCmd represents the client command
var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Client operations",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		opts := &cliOpts

		if opts.Verbose {
			opts.LogLevel.Value = slog.LevelDebug
		}
		if opts.SSLCertfile != "" {
			opts.Client.SSLCertfile = opts.SSLCertfile
		}

		cfg := opts.Client
		cfg.Context = ctx

		u, err := parseURL(opts.Address)
		if err != nil {
			return fmt.Errorf("invalid address: %w", err)
		}
		if u.Scheme == "unix" {
			opts.Address = u.String()
		} else {
			opts.Address = u.Host
		}
		cfg.Address = opts.Address

		if opts.LogLevel.Value == 0 && !opts.Verbose && !rootCmd.Flags().Changed("log-level") {
			opts.LogLevel.Value = slog.LevelWarn
		}

		var out io.Writer
		if opts.Quiet {
			out = io.Discard
		} else {
			out = os.Stderr
		}
		log.SetOutput(out)

		handlerOptions := &slog.HandlerOptions{
			Level:     cliOpts.LogLevel,
			AddSource: true,
		}

		var handler slog.Handler

		if opts.LogJSON {
			handler = slog.NewJSONHandler(out, handlerOptions)
		} else {
			handler = slog.NewTextHandler(out, handlerOptions)
		}
		defaultLogger = slog.New(handler).WithGroup("gokv")
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
		client := client.NewClient(cfg)
		opts.client = client
		err = client.Dial()
		if err != nil {
			log.Fatalf("unable to connect: %s\n", err.Error())
		}

		go func() {
			for {
				select {
				case <-ctx.Done():
					log.Printf("closing connection")
					err = opts.client.CloseConnection()
					if err != nil {
						log.Printf("error closing connection: %s", err.Error())
					}
					os.Exit(1)
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

	clientCmd.PersistentFlags().BoolVar(
		&cliOpts.Client.InsecureSkipVerify,
		"insecure",
		false,
		"disables certificate verification",
	)
	clientCmd.PersistentFlags().BoolVar(
		&cliOpts.Client.NoTLS,
		"no-tls",
		false,
		"disables TLS",
	)
	clientCmd.PersistentFlags().BoolVarP(
		&cliOpts.Verbose,
		"verbose",
		"v",
		false,
		"(Client only) Enables debug-level logging",
	)
	clientCmd.PersistentFlags().IntVar(
		&cliOpts.IndentJSON,
		"indent",
		0,
		"Number of spaces to indent client JSON output",
	)
}
