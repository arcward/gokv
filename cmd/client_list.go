package cmd

import (
	"fmt"
	"github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
	"log"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Outputs a list of keys",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		opts := &cliOpts
		kv, err := opts.client.ListKeys(
			ctx, &api.ListKeysRequest{
				Pattern:         opts.clientOpts.ListKeyOpts.Pattern,
				Limit:           opts.clientOpts.ListKeyOpts.Limit,
				IncludeReserved: opts.clientOpts.ListKeyOpts.IncludeReserved,
			},
		)
		if err != nil {
			return err
		}
		log.Printf("keys: %#v", kv)
		for _, k := range kv.Keys {
			_, err = fmt.Fprintln(out, k)
			printError(err)
		}

		return nil
	},
}

func init() {
	clientCmd.AddCommand(listCmd)
	listCmd.Flags().StringVar(
		&cliOpts.clientOpts.ListKeyOpts.Pattern,
		"pattern",
		"",
		"pattern to match keys against",
	)
	listCmd.Flags().Uint64Var(
		&cliOpts.clientOpts.ListKeyOpts.Limit,
		"limit",
		0,
		"limit the number of keys returned",
	)
	listCmd.Flags().BoolVar(
		&cliOpts.clientOpts.ListKeyOpts.IncludeReserved,
		"include-reserved",
		false,
		"include reserved keys in list",
	)
}
