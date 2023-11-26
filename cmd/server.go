package cmd

import (
	"crypto"
	"encoding/json"
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/arcward/gokv/server"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"log/slog"
	"net"
	"os"
	"strings"
)

var hashAlgorithms = map[string]crypto.Hash{
	crypto.MD5.String():    crypto.MD5,
	crypto.SHA1.String():   crypto.SHA1,
	crypto.SHA256.String(): crypto.SHA256,
	crypto.SHA512.String(): crypto.SHA512,
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Starts a server",
	RunE: func(cmd *cobra.Command, args []string) error {
		opts := &cliOpts

		network, addr, found := strings.Cut(opts.Address, "://")
		if !found {
			network = "tcp"
			addr = opts.Address
		}
		if network == "" {
			network = "tcp"
		}

		lis, err := net.Listen(
			network,
			addr,
		)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		var s *grpc.Server
		if opts.SSL.Certfile != "" && opts.SSL.Keyfile != "" {
			creds, err := credentials.NewServerTLSFromFile(
				opts.SSL.Certfile,
				opts.SSL.Keyfile,
			)
			if err != nil {
				log.Fatalf("failed to load TLS keys: %s", err.Error())
			}
			s = grpc.NewServer(grpc.Creds(creds))
		} else {
			s = grpc.NewServer()
		}
		opts.grpcServer = s

		var exists bool
		if cliOpts.ServerBackup != "" {
			_, err = os.Stat(cliOpts.ServerBackup)
			if err == nil {
				exists = true
			}
		}
		logger := slog.New(
			slog.NewTextHandler(
				os.Stdout,
				&slog.HandlerOptions{Level: cliOpts.LogLevel},
			),
		)
		opts.Server.Logger = logger
		logger = logger.WithGroup("gokv_server")
		server := server.NewServer(opts.Server)
		cliOpts.server = server

		if exists {
			log.Printf("loading backup file: %s", cliOpts.ServerBackup)
			serverData, err := os.ReadFile(cliOpts.ServerBackup)
			if err != nil {
				log.Fatalf(
					"failed to read backup file '%s': %s",
					cliOpts.ServerBackup,
					err.Error(),
				)
			}
			err = json.Unmarshal(serverData, &server)
		}

		pb.RegisterKeyValueStoreServer(
			s,
			cliOpts.server,
		)
		cliOpts.Server.Logger.Info(
			"starting server",
			slog.String("address", opts.Address),
			slog.Group(
				"config",
				slog.Bool(
					"HashValues",
					cliOpts.server.Config().Hashing.Enabled,
				),
				slog.String("ssl-certfile", opts.SSL.Certfile),
				slog.String("ssl-keyfile", opts.SSL.Keyfile),
				slog.String("backup", opts.ServerBackup),
				slog.Uint64(
					"MaxNumberOfKeys",
					cliOpts.server.Config().MaxNumberOfKeys,
				),
				slog.Uint64(
					"DefaultMaxValueSize",
					cliOpts.server.Config().MaxValueSize,
				),
				slog.Group(
					"History",
					slog.Int64(
						"RevisionLimit",
						cliOpts.server.Config().RevisionLimit,
					),
				),
			),
		)

		if err := s.Serve(lis); err != nil {
			cliOpts.server.Config().Logger.Error(
				fmt.Sprintf(
					"Failed to serve: %v",
					err,
				),
			)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	algoNames := make([]string, 0, len(hashAlgorithms))
	for name, algo := range hashAlgorithms {
		if algo.Available() {
			algoNames = append(algoNames, name)
		}
	}
	serverCmd.Flags().Uint64Var(
		&cliOpts.Server.MaxNumberOfKeys,
		"max-keys",
		0,
		"Maximum number of keys",
	)
	serverCmd.Flags().Uint64Var(
		&cliOpts.Server.MaxValueSize,
		"max-value-size",
		server.DefaultMaxValueSize,
		"Maximum size of a value",
	)
	serverCmd.Flags().Uint64Var(
		&cliOpts.Server.MaxKeySize,
		"max-key-size",
		server.DefaultMaxKeySize,
		"Maximum size of a key",
	)
	serverCmd.Flags().Int64Var(
		&cliOpts.Server.RevisionLimit,
		"revision-limit",
		5,
		"Maximum size of history. 0=disabled, -1=unlimited",
	)
	serverCmd.Flags().BoolVar(
		&cliOpts.Server.KeepExpiredKeys,
		"keep-expired-keys",
		false,
		"Do not delete expired keys",
	)
	serverCmd.Flags().StringVar(
		&cliOpts.ServerBackup,
		"backup",
		"",
		"JSON backup file to load on startup and write on shutdown",
	)
	serverCmd.MarkFlagFilename("backup", "json")
}
