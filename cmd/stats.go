package cmd

import (
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Get server stats",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		opts := &cliOpts
		kv, err := opts.client.Stats(ctx, &pb.EmptyRequest{})
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(statsCmd)
}
