package cmd

import (
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
	"strconv"
)

var pruneCmd = &cobra.Command{
	Use:   "prune [pruneTo] [ignoreKeys...]",
	Short: "Triggers pruning",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		pruneTo, err := strconv.Atoi(args[0])
		printError(err)
		var ignoreKeys []string
		if len(args) > 1 {
			ignoreKeys = append(ignoreKeys, args[1:]...)
		}

		opts := &cliOpts
		req := &pb.PruneRequest{
			PruneTo:    uint64(pruneTo),
			IgnoreKeys: ignoreKeys,
		}

		kv, err := opts.client.Prune(ctx, req)
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(pruneCmd)
}
