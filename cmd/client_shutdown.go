package cmd

import (
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
)

var shutdownCmd = &cobra.Command{
	Use:   "server-shutdown",
	Short: "Triggers server shutdown",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()

		opts := &cliOpts
		req := &pb.ShutdownRequest{}

		kv, err := opts.client.Shutdown(ctx, req)
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(shutdownCmd)
}
