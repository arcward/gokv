package cmd

import (
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
)

var watchKeyCmd = &cobra.Command{
	Use:   "watch-key [key]",
	Short: "Watches server key events",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		opts := &cliOpts
		client := opts.client
		key := args[0]
		stream, e := client.WatchKeyValue(
			ctx,
			&pb.WatchKeyValueRequest{Key: key},
		)
		printError(e)
		defer func() {
			_ = client.CloseConnection()
		}()

		for {
			if ctx.Err() != nil {
				break
			}
			ev, err := stream.Recv()
			printError(err)
			if ev != nil {
				fmt.Printf("%s\n", ev)
			}
			fmt.Printf("saw event: %s %#v\n", ev.KeyEvent, ev.KeyEvent)
			switch *ev.KeyEvent.Enum() {
			case pb.KeyEvent_DELETED, pb.KeyEvent_EXPIRED, pb.KeyEvent_EXPUNGED:
				return
			}
		}
	},
}

func init() {
	clientCmd.AddCommand(watchKeyCmd)
}
