package cmd

import (
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
	"log"
)

var infoCmd = &cobra.Command{
	Use:   "info [key]",
	Short: "Get information about a key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		key := args[0]
		log.Printf("getting value for key '%s'", key)
		opts := &cliOpts
		kv, err := opts.client.GetKeyInfo(ctx, &pb.Key{Key: key})
		if err != nil {
			return err
		}
		fmt.Println(kv.String())

		return nil
	},
}

func init() {
	clientCmd.AddCommand(infoCmd)
}
