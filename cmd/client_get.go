package cmd

import (
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
	"log"
	"os"
)

var getCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Get the value of a key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		key := args[0]
		log.Printf("getting value for key '%s'", key)
		opts := &cliOpts
		kv, err := opts.client.Get(ctx, &pb.Key{Key: key})
		if err != nil {
			fmt.Printf("error getting key: %s\n", err.Error())
			os.Exit(1)
		}
		fmt.Println(string(kv.Value))

		return nil
	},
}

func init() {
	clientCmd.AddCommand(getCmd)
}
