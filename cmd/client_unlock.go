package cmd

import (
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
	"log"
)

var unlockCmd = &cobra.Command{
	Use:   "unlock [flags] [key]",
	Short: "Unlocks a key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		key := args[0]
		opts := &cliOpts
		log.Printf("unlocking key: %s", key)
		kv, err := opts.client.Unlock(
			ctx,
			&pb.UnlockRequest{Key: key},
		)
		if err != nil {
			return err
		}
		fmt.Println(kv.String())

		return nil
	},
}

func init() {
	clientCmd.AddCommand(unlockCmd)
}
