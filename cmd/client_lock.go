package cmd

import (
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
	"log"
)

var lockCmd = &cobra.Command{
	Use:   "lock [flags] [key]",
	Short: "Locks a key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		key := args[0]

		log.Printf("setting key: %s", key)
		opts := &cliOpts
		kv, err := opts.client.Lock(
			ctx,
			&pb.LockRequest{
				Key:      key,
				Duration: uint32(opts.clientOpts.LockTimeout.Seconds()),
			},
		)
		if err != nil {
			return err
		}
		fmt.Println(kv.String())

		return nil
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
