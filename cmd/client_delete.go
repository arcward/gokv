package cmd

import (
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
	"log"
)

var deleteCmd = &cobra.Command{
	Use:   "delete [key]",
	Short: "Delete a key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		key := args[0]
		log.Printf("deleting key: %s", key)
		opts := &cliOpts
		kv, err := opts.client.Delete(ctx, &pb.Key{Key: key})
		if err != nil {
			return err
		}
		fmt.Println(kv.String())

		return nil
	},
}

func init() {
	clientCmd.AddCommand(deleteCmd)
}
