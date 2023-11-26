package cmd

import (
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Get server stats",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		opts := &cliOpts
		kv, err := opts.client.Stats(ctx, &pb.EmptyRequest{})
		if err != nil {
			return err
		}
		fmt.Println(kv.String())

		return nil
	},
}

func init() {
	clientCmd.AddCommand(statsCmd)
}
