package cmd

import (
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Get the value of a key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		key := args[0]
		opts := &cliOpts
		var value []byte
		if opts.clientOpts.GetKeyVersion == 0 {
			kv, err := opts.client.Get(ctx, &pb.Key{Key: key})
			printError(err)
			value = kv.Value
		} else {
			kv, err := opts.client.GetRevision(
				ctx,
				&pb.GetRevisionRequest{
					Key:     key,
					Version: opts.clientOpts.GetKeyVersion,
				},
			)
			printError(err)
			value = kv.Value
		}
		_, err := fmt.Fprintln(out, string(value))
		printError(err)
		return nil
	},
}

func init() {
	clientCmd.AddCommand(getCmd)
	opts := &cliOpts
	getCmd.Flags().Int64Var(
		&opts.clientOpts.GetKeyVersion,
		"revision",
		0,
		"Get the value of a key at a specific version. 0 means latest. "+
			"A positive number means the version number. A negative number "+
			"specifies the number of versions back from the latest.",
	)
}
