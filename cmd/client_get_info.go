package cmd

import (
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
)

var infoCmd = &cobra.Command{
	Use:   "info [key]",
	Short: "Get information about a key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		key := args[0]
		opts := &cliOpts
		kv, err := opts.client.GetKeyInfo(ctx, &pb.Key{Key: key})
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(infoCmd)
}
