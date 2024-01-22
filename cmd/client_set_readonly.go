package cmd

import (
	"errors"
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
	"strings"
)

var setReadonlyCmd = &cobra.Command{
	Use:          "readonly [on|off]",
	Short:        "Enable/disable readonly mode on the server",
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		var enable bool
		choice := strings.ToLower(args[0])
		switch choice {
		case "on":
			enable = true
		case "off":
			enable = false
		default:
			return errors.New("invalid choice (must be: on or off)")
		}

		req := &pb.ReadOnlyRequest{
			Enable: enable,
		}
		opts := &cliOpts
		kv, err := opts.client.SetReadOnly(ctx, req)
		printError(err)
		printResult(kv)
		return nil
	},
}

func init() {
	clientCmd.AddCommand(setReadonlyCmd)
}
