package cmd

import (
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
)

var infoCmd = &cobra.Command{
	Use:   "inspect [flags] [key]",
	Short: "Get information about a key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		key := args[0]
		opts := &cliOpts
		kv, err := opts.client.Inspect(
			ctx,
			&pb.InspectRequest{
				Key:            key,
				IncludeValue:   opts.clientOpts.InspectIncludeValue,
				IncludeMetrics: opts.clientOpts.InspectIncludeMetrics,
			},
		)
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(infoCmd)
	infoCmd.Flags().BoolVar(
		&cliOpts.clientOpts.InspectIncludeValue,
		"with-value",
		false,
		"Includes the key's base64-encoded value in the output",
	)
	infoCmd.Flags().BoolVar(
		&cliOpts.clientOpts.InspectIncludeMetrics,
		"with-metrics",
		false,
		"Includes key metrics in the output",
	)
}
