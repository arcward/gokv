package cmd

import (
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
)

var keyMetricsCmd = &cobra.Command{
	Use:   "key-metric [key]",
	Short: "Get key metrics",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		key := args[0]
		opts := &cliOpts
		kv, err := opts.client.GetKeyMetric(
			ctx,
			&pb.KeyMetricRequest{
				Key: key,
			},
		)
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(keyMetricsCmd)
}
