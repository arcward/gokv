package cmd

import (
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
)

var popCmd = &cobra.Command{
	Use:   "pop [key]",
	Short: "Retrieve a key's value and delete it",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()

		key := args[0]
		opts := &cliOpts
		kv, err := opts.client.Pop(ctx, &pb.PopRequest{Key: key})
		printError(err)
		_, err = fmt.Fprintln(out, string(kv.Value))
		printError(err)
	},
}

func init() {
	clientCmd.AddCommand(popCmd)
}
