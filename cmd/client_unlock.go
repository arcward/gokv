package cmd

import (
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
)

var unlockCmd = &cobra.Command{
	Use:   "unlock [flags] [key]",
	Short: "Unlocks a key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		key := args[0]
		opts := &cliOpts
		kv, err := opts.client.Unlock(
			ctx,
			&pb.UnlockRequest{
				Key: key,
			},
		)
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(unlockCmd)
}
