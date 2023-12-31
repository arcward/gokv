package cmd

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/arcward/keyquarry/server"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/term"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
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
	//fmt.Println()
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

		//ctx := cmd.Context()
		fmt.Println("keyquarry server setup")

		_ = viper.ReadInConfig()

		serverConfig := &cliOpts.ServerOpts

		err := viper.Unmarshal(
			serverConfig, viper.DecodeHook(
				mapstructure.ComposeDecodeHookFunc(
					mapstructure.StringToTimeDurationHookFunc(),
					mapstructure.StringToSliceHookFunc(","),
					decodeHashType(),
				),
			),
		)

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

		available := []string{}
		for _, h := range availableHashes() {
			available = append(available, h.String())
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

		hash, err := getInput(
			"Hash algorithm",
			viper.GetString("hash_algorithm"),
			false,
			available...,
		)
		if err != nil {
			return err
		}
		viper.Set("hash_algorithm", hash)

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
			"Create on-disk snapshots",
			false,
		)
		if err != nil {
			return err
		}
		if createSnapshots {
			viper.Set("snapshot.enabled", true)

			encryptSnapshots, err := getTrueFalse(
				"Encrypt snapshots",
				viper.GetBool("snapshot.encrypt"),
			)
			if err != nil {
				return err
			}
			viper.Set("snapshot.encrypt", encryptSnapshots)
			if encryptSnapshots {

				var goodPassword bool
				var secretKey []byte

				currentKey := viper.GetString("snapshot.secret_key")

				if currentKey != "" {
					setNewKey, err := getTrueFalse("Set new secret key?", true)
					if err != nil {
						return err
					}
					if !setNewKey {
						keyData := []byte(currentKey)
						if len(keyData) == 32 || len(keyData) == 16 {
							goodPassword = true
							secretKey = make([]byte, len(keyData))
							copy(secretKey, keyData)
						}
					}
				}

				for !goodPassword {
					print("Secret key (length 16 or 32, or leave blank to generate a random key): ")
					secretKey, err = term.ReadPassword(int(os.Stdin.Fd()))
					fmt.Printf("\n")
					if err != nil {
						return err
					}
					keyLength := len(secretKey)
					switch keyLength {
					case 0:
						secretKey = make([]byte, 32)
						_, err = rand.Read(secretKey)
						if err != nil {
							return err
						}
						hexKey := hex.EncodeToString(secretKey)
						copy(secretKey, []byte(hexKey))
						goodPassword = true
					case 16, 32:
						goodPassword = true

					default:
						fmt.Printf(
							"Secret key must be 16 or 32 bytes, got %d\n",
							keyLength,
						)
					}

				}
				viper.Set("snapshot.secret_key", string(secretKey))

			}

			snapshotDefaultDir := viper.GetString("snapshot.dir")
			if snapshotDefaultDir == "" {
				snapshotDefaultDir, _ = filepath.Abs("./snapshots")
			}

			snapshotDir, err := getInput(
				"Snapshot directory",
				snapshotDefaultDir,
				false,
			)
			if err != nil {
				return err
			}
			if snapshotDir != "" {
				snapshotDir, err = filepath.Abs(snapshotDir)
				if err != nil {
					return err
				}

				_, err = os.Stat(snapshotDir)
				if err != nil {
					if os.IsNotExist(err) {
						err = os.MkdirAll(snapshotDir, 0755)
						if err != nil {
							return err
						}
					} else {
						return err
					}
				}
				viper.Set("snapshot.dir", snapshotDir)

				snapshotInterval, err := getInput(
					"Snapshot interval (example: 1h) (0s=only on shutdown)",
					viper.GetString("snapshot.interval"),
					false,
				)
				if err != nil {
					return err
				}
				_, err = time.ParseDuration(snapshotInterval)
				if err != nil {
					return err
				}
				viper.Set("snapshot.interval", snapshotInterval)

				snapshotLimit, err := getInput(
					"Snapshot limit",
					viper.GetString("snapshot.limit"),
					false,
				)
				if err != nil {
					return err
				}

				limit, err := strconv.Atoi(snapshotLimit)
				if err != nil {
					return err
				}
				if limit < 1 {
					return fmt.Errorf("snapshot limit must be greater than 0")
				}
				viper.Set("snapshot.limit", snapshotLimit)

			}
		} else {
			viper.Set("snapshot.enabled", false)
		}

		privilegedClientID, err := getInput(
			"client ID with privileged access",
			viper.GetString("privileged_client_ids"),
			false,
		)
		if err != nil {
			return err
		}
		viper.Set("privileged_client_id", privilegedClientID)

		if createSnapshots {

		}

		currentAddr := viper.GetString("listen_address")
		if currentAddr == "" {
			currentAddr = defaultAddress
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
