package cmd

import (
	"bufio"
	"fmt"
	"github.com/arcward/keyquarry/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func getInput(
	prompt string,
	defaultOption string,
	requireSelection bool,
	choices ...string,
) (
	string,
	error,
) {
	if len(choices) == 0 {
		if defaultOption == "" {
			prompt = fmt.Sprintf("%s: ", prompt)
		} else {
			prompt = fmt.Sprintf("%s [%s]: ", prompt, defaultOption)
		}
	} else {
		if defaultOption == "" {
			prompt = fmt.Sprintf(
				"%s (%s): ",
				prompt,
				strings.Join(choices, ", "),
			)
		} else {
			prompt = fmt.Sprintf(
				"%s (%s) [%s]: ",
				prompt,
				strings.Join(choices, ", "),
				defaultOption,
			)
		}
	}
	fmt.Printf("%s", prompt)
	scanner := bufio.NewScanner(os.Stdin)

	scanner.Scan()
	err := scanner.Err()
	if err != nil {
		return "", err
	}
	line := scanner.Text()
	if requireSelection && defaultOption == "" && line == "" {
		return "", fmt.Errorf("no choice selected")
	}
	if line == "" {
		if defaultOption != "" {
			return defaultOption, nil
		}
		if len(choices) > 0 && requireSelection {
			return "", fmt.Errorf("no choice selected")
		}
	} else {
		if len(choices) > 0 {
			for _, choice := range choices {
				if line == choice {
					return line, nil
				}
			}
			return "", fmt.Errorf("invalid choice")
		}
	}
	return line, nil
}

func getTrueFalse(prompt string, defaultTrue bool) (bool, error) {
	if defaultTrue {
		prompt = fmt.Sprintf("%s [Y/n]: ", prompt)
	} else {
		prompt = fmt.Sprintf("%s [y/N]: ", prompt)
	}

	fmt.Printf("%s", prompt)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	err := scanner.Err()
	if err != nil {
		return false, err
	}
	line := scanner.Text()
	if line == "" {
		return defaultTrue, nil
	}
	if line == "y" || line == "Y" {
		return true, nil
	}
	if line == "n" || line == "N" {
		return false, nil
	}
	return false, fmt.Errorf("invalid choice")
}

var initServerCmd = &cobra.Command{
	Use:          "init-server",
	Short:        "Create a new server configuration",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("keyquarry server setup")

		_ = viper.ReadInConfig()

		serverConfig := &cliOpts.ServerOpts

		err := viper.Unmarshal(serverConfig)
		if err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}

		currentCfg := viper.ConfigFileUsed()
		if currentCfg != "" {
			currentCfg, _ = filepath.Abs(currentCfg)
			fmt.Printf(
				"Configuring based on existing config file: %s\n",
				currentCfg,
			)
		}

		revisionLimit, err := getInput(
			"Revision limit",
			viper.GetString("revision_limit"),
			false,
		)
		if err != nil {
			return err
		}
		viper.Set("revision_limit", revisionLimit)

		maxKeys := viper.GetUint64("max_keys")
		if maxKeys == 0 {
			maxKeys = server.DefaultMaxKeys
		}

		vmaxKeys, err := getInput(
			"Maximum number of keys",
			fmt.Sprintf("%d", maxKeys),
			false,
		)
		if err != nil {
			return err
		}
		viper.Set("max_keys", vmaxKeys)

		defaultPruneInterval := viper.GetString("prune_interval")
		if defaultPruneInterval == "" {
			defaultPruneInterval = "1h"
		}

		pruneInterval, err := getInput(
			"Prune interval (example: 1h) (0s=never)",
			defaultPruneInterval,
			false,
		)

		currentEager := viper.GetBool("eager_prune")
		eagerPrune, err := getTrueFalse(
			"Eager prune",
			currentEager,
		)
		viper.Set("eager_prune", eagerPrune)

		if err != nil {
			return err
		}
		_, err = time.ParseDuration(pruneInterval)
		if err != nil {
			return err
		}
		viper.Set("prune_interval", pruneInterval)

		enableSSL, err := getTrueFalse("Enable SSL/TLS", false)
		if err != nil {
			return err
		}

		if enableSSL {
			sslCertfile, err := getInput(
				"SSL certificate file",
				viper.GetString("ssl_certfile"),
				true,
			)
			if err != nil {
				return err
			}
			sslCertfile, err = filepath.Abs(sslCertfile)
			if err != nil {
				return err
			}

			_, err = os.Stat(sslCertfile)
			if err != nil {
				if os.IsNotExist(err) {
					return fmt.Errorf("file %s does not exist", sslCertfile)
				}
			}
			viper.Set("ssl_certfile", sslCertfile)

			sslKeyfile, err := getInput(
				"SSL key file",
				viper.GetString("ssl_keyfile"),
				true,
			)

			if err != nil {
				return err
			}
			sslKeyfile, err = filepath.Abs(sslKeyfile)
			if err != nil {
				return err
			}

			_, err = os.Stat(sslKeyfile)
			if err != nil {
				if os.IsNotExist(err) {
					return fmt.Errorf("file %s does not exist", sslCertfile)
				}
			}

			viper.Set("ssl_keyfile", sslKeyfile)
		}

		createSnapshots, err := getTrueFalse(
			"Enable periodic snapshots to DB",
			false,
		)
		if err != nil {
			return err
		}
		if createSnapshots {
			viper.Set("snapshot.enabled", true)

			currentDBConn := viper.GetString("snapshot.database")
			snapshotDB, err := getInput(
				"Snapshot database connection string",
				currentDBConn,
				false,
			)
			if err != nil {
				return err
			}
			viper.Set("snapshot.database", snapshotDB)
		} else {
			viper.Set("snapshot.enabled", false)
		}

		privilegedClientID, err := getInput(
			"client ID with privileged access",
			viper.GetString("privileged_client_id"),
			false,
		)
		if err != nil {
			return err
		}
		viper.Set("privileged_client_id", privilegedClientID)

		currentAddr := viper.GetString("listen_address")
		if currentAddr == "" {
			currentAddr = server.DefaultAddress
		}
		addr, err := getInput("Listen address", currentAddr, false)
		if err != nil {
			return err
		}
		_, err = parseURL(addr)
		if err != nil {
			return err
		}

		viper.Set("listen_address", addr)

		logLevel, err := getInput(
			"Log level",
			slog.LevelInfo.String(),
			true,
			slog.LevelError.String(),
			slog.LevelWarn.String(),
			slog.LevelInfo.String(),
			slog.LevelDebug.String(),
		)
		if err != nil {
			return err
		}
		viper.Set("log_level", logLevel)

		defaultFormat := "text"
		if viper.GetBool("log_json") {
			defaultFormat = "json"
		}

		logFormat, err := getInput(
			"Log format",
			defaultFormat,
			false,
			"text", "json",
		)
		if err != nil {
			return err
		}
		if logFormat == "json" {
			viper.Set("log_json", true)
		} else {
			viper.Set("log_json", false)
		}

		cfgDir, _ := os.UserConfigDir()
		if cfgDir != "" {
			cfgDir = filepath.Join(cfgDir, "keyquarry")
		}
		defaultConfigPath := filepath.Join(cfgDir, "keyquarry-server.env")
		if currentCfg != "" {
			defaultConfigPath = currentCfg
		}
		configPath, err := getInput(
			"Write configuration to",
			defaultConfigPath,
			false,
		)
		if err != nil {
			return err
		}
		var isDefaultConfigPath bool
		if configPath == defaultConfigPath {
			isDefaultConfigPath = true
		} else {
			configPath, err = filepath.Abs(configPath)
			if err != nil {
				return err
			}
		}

		err = os.MkdirAll(filepath.Dir(configPath), 0700)
		if err != nil && !os.IsExist(err) {
			return fmt.Errorf(
				"failed to create config directory '%s': %w",
				filepath.Dir(configPath),
				err,
			)
		}

		err = viper.WriteConfigAs(configPath)
		if err != nil {
			return fmt.Errorf(
				"failed to write config to '%s': %w",
				configPath,
				err,
			)
		}

		fmt.Printf("Configuration written to: %s\n", configPath)
		if isDefaultConfigPath {
			fmt.Printf("Start the server with:\n  $ keyquarry server\n")
		} else {
			fmt.Printf(
				"Start the server with:\n  $ keyquarry server --config %s \n",
				configPath,
			)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(initServerCmd)
}
