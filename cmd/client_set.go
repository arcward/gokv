package cmd

import (
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
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

		switch len(args) {
		case 2:
			value = []byte(args[1])
		default:
			val, err := readStdin()
			printError(err)
			value = []byte(strings.Join(val, "\n"))
		}

		opts := &cliOpts
		req := &pb.KeyValue{Key: key, Value: value}

		if opts.clientOpts.LockTimeout > 0 {
			req.LockDuration = durationpb.New(opts.clientOpts.LockTimeout)
		}

		if opts.clientOpts.KeyLifespan.Seconds() > 0 {
			req.Lifespan = durationpb.New(opts.clientOpts.KeyLifespan)
		}

		kv, err := opts.client.Set(ctx, req)
		printError(err)
		printResult(kv)
	},
}

func init() {
	clientCmd.AddCommand(setCmd)
	setCmd.Flags().DurationVar(
		&cliOpts.clientOpts.KeyLifespan,
		"lifespan",
		0,
		"Expire key in specified duration (e.g. 1h30m)",
	)
	setCmd.Flags().DurationVar(
		&cliOpts.clientOpts.LockTimeout,
		"lock",
		0,
		"Lock key for specified duration (e.g. 1h30m)",
	)
}
