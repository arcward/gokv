package cmd

import (
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
	"time"
)

var lockCmd = &cobra.Command{
	Use:          "lock [flags] [key] [duration]",
	Short:        "Locks a key",
	Args:         cobra.ExactArgs(2),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		key := args[0]
		duration := args[1]
		d, err := time.ParseDuration(duration)
		if err != nil {
			return fmt.Errorf("invalid duration '%s': %w", duration, err)
		}

		opts := &cliOpts

		kv, err := opts.client.Lock(
			ctx,
			&pb.LockRequest{
				Key:             key,
				Duration:        durationpb.New(d),
				CreateIfMissing: opts.clientOpts.LockCreateIfMissing,
			},
		)
		printError(err)
		printResult(kv)
		return nil
	},
}

func init() {
	clientCmd.AddCommand(lockCmd)
	lockCmd.Flags().BoolVar(
		&cliOpts.clientOpts.LockCreateIfMissing,
		"create-if-missing",
		false,
		"Create the key if it does not exist",
	)
}
