package cmd

import (
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
)

var lockCmd = &cobra.Command{
	Use:   "lock [flags] [key]",
	Short: "Locks a key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		key := args[0]

		opts := &cliOpts
		kv, err := opts.client.Lock(
			ctx,
			&pb.LockRequest{
				Key:      key,
				Duration: uint32(opts.clientOpts.LockTimeout.Seconds()),
			},
		)
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(lockCmd)
	lockCmd.Flags().DurationVar(
		&cliOpts.clientOpts.LockTimeout,
		"lock-timeout",
		0,
		"Lock timeout",
	)
}
