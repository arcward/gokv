package cmd

import (
	"github.com/arcward/gokv/client"
	"github.com/spf13/cobra"
	"log"
	"os"
)

// clientCmd represents the client command
var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Client operations",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		opts := &cliOpts
		cfg := opts.Client
		cfg.Context = ctx
		cfg.Address = opts.Address
		client := client.NewClient(cfg)
		opts.client = client
		err := client.Dial()
		if err != nil {
			log.Fatalf("unable to connect: %s", err.Error())
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
		//opts.client = api.NewKeyValueStoreClient(conn)
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
}
