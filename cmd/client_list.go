package cmd

import (
	"fmt"
	"github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
	"log"
)

var listCmd = &cobra.Command{
	Use:   "list-keys",
	Short: "Outputs a list of keys",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		opts := &cliOpts
		kv, err := opts.client.ListKeys(
			ctx, &api.ListKeysRequest{
				Pattern: opts.clientOpts.ListKeyOpts.Pattern,
				Limit:   opts.clientOpts.ListKeyOpts.Limit,
			},
		)
		if err != nil {
			return err
		}
		log.Printf("keys: %#v", kv)
		for _, k := range kv.Keys {
			fmt.Println(k)
		}

		return nil
	},
}

func init() {
	clientCmd.AddCommand(listCmd)
	clientCmd.Flags().StringVar(
		&cliOpts.clientOpts.ListKeyOpts.Pattern,
		"pattern",
		"",
		"pattern to match keys against",
	)
	clientCmd.Flags().Uint64Var(
		&cliOpts.clientOpts.ListKeyOpts.Limit,
		"limit",
		0,
		"limit the number of keys returned",
	)
}
