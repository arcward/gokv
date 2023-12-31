package cmd

import (
	"fmt"
	"github.com/arcward/keyquarry/build"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Display version information",
	Run: func(cmd *cobra.Command, args []string) {
		opts := &cliOpts
		fmt.Println("keyquarry version:", build.Version)
		if opts.ShowDetailedVersion {
			fmt.Println("git commit:", build.Commit)
			fmt.Println("build time:", build.Time)
			fmt.Println("build user:", build.User)

		}

	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
	versionCmd.Flags().BoolVar(
		&cliOpts.ShowDetailedVersion,
		"all",
		false,
		"Show detailed build information",
	)
}
