package cmd

import (
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
	"strings"
)

var setCmd = &cobra.Command{
	Use:   "set [key] [value]",
	Short: "Sets the value of a key",
	Args:  cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		var key string
		var value []byte
		var err error
		key = args[0]

		if len(args) == 2 {
			value = []byte(args[1])
		} else {
			val, err := readStdin()
			printError(err)
			value = []byte(strings.Join(val, "\n"))
		}
		opts := &cliOpts
		kv, err := opts.client.Set(
			ctx,
			&pb.KeyValue{
				Key:          key,
				Value:        value,
				ExpireIn:     uint32(opts.clientOpts.ExpireKeyIn.Seconds()),
				Lock:         opts.clientOpts.Lock,
				LockDuration: uint32(opts.clientOpts.LockTimeout.Seconds()),
			},
		)
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(setCmd)
	setCmd.Flags().DurationVar(
		&cliOpts.clientOpts.ExpireKeyIn,
		"expires",
		0,
		"Expire key in specified duration (e.g. 1h30m)",
	)
	setCmd.Flags().BoolVar(
		&cliOpts.clientOpts.Lock,
		"lock",
		false,
		"Lock the key",
	)
	setCmd.Flags().DurationVar(
		&cliOpts.clientOpts.LockTimeout,
		"lock-timeout",
		0,
		"Lock timeout",
	)
}
