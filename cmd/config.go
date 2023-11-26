package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
	"os"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Display configuration",
	Run: func(cmd *cobra.Command, args []string) {
		var cfg CLIConfig

		cp := &cfg
		err := viper.Unmarshal(cp)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		d, err := yaml.Marshal(cp)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(string(d))
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
}
