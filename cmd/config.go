package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Display configuration",
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf(
			"config file: %s", viper.ConfigFileUsed(),
		)
		settings := viper.AllSettings()
		yamlConfig, err := yaml.Marshal(settings)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(string(yamlConfig))
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
}
