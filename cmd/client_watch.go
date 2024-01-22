package cmd

import (
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
)

var watchCmd = &cobra.Command{
	Use:   "watch",
	Short: "Watches server key events",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		opts := &cliOpts
		client := opts.client

		stream, e := client.WatchStream(ctx, &pb.WatchRequest{})
		printError(e)

		for {
			if ctx.Err() != nil {
				break
			}
			ev, err := stream.Recv()
			printError(err)
			if ev != nil {
				fmt.Printf("%s\n", ev)
			}
		}
	},
}

func init() {
	clientCmd.AddCommand(watchCmd)
}
