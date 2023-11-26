package cmd

import (
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
	"log"
	"strings"
)

var setCmd = &cobra.Command{
	Use:   "set [key] [value]",
	Short: "Sets the value of a key",
	Args:  cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		var key string
		var value []byte
		var err error
		key = args[0]

		if len(args) == 2 {
			value = []byte(args[1])
		} else {
			val, err := readStdin()
			if err != nil {
				return err
			}
			value = []byte(strings.Join(val, "\n"))
		}
		opts := &cliOpts

		log.Printf("setting key: %s", key)
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
		if err != nil {
			fmt.Println(err)
			return nil
		}
		fmt.Println(kv.String())

		return nil
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
