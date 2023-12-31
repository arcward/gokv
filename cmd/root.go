package cmd

import (
	"bufio"
	"context"
	"crypto"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/arcward/keyquarry/client"
	"github.com/arcward/keyquarry/server"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

const (
	defaultNetwork = "tcp"
	defaultHost    = "localhost"
	defaultPort    = 33969
)

var out io.Writer = os.Stdout

var defaultAddress = fmt.Sprintf(
	"%s:%d",
	defaultHost,
	defaultPort,
)

type CLIConfig struct {
	ServerOpts server.Config `json:"server" yaml:"server" mapstructure:"server"`

	ClientOpts client.Config //`json:"client" yaml:"client" mapstructure:"client"`

	configFile          string
	clientOpts          clientOptions
	server              *server.KeyValueStore
	grpcServer          *grpc.Server
	client              *client.Client
	LogLevel            string `json:"log_level" yaml:"log_level" mapstructure:"log_level"`
	LogJSON             bool   `json:"log_json" yaml:"log_json" mapstructure:"log_json"`
	ShowDetailedVersion bool
	ProfileCPU          bool
}

func decodeHashType() mapstructure.DecodeHookFuncType {
	// Wrapped in a function call to add optional input parameters (eg. separator)
	return func(
		f reflect.Type, // data type
		t reflect.Type, // target data type
		data interface{}, // raw data
	) (interface{}, error) {
		// Check if the data type matches the expected one

		if f.Kind() != reflect.String {
			return data, nil
		}

		// Check if the target type matches the expected one
		if t != reflect.TypeOf(crypto.Hash(0)) {
			return data, nil
		}

		// Format/decode/parse the data and return the new value
		hashes := []crypto.Hash{
			crypto.MD4,
			crypto.MD5,
			crypto.SHA1,
			crypto.SHA224,
			crypto.SHA256,
			crypto.SHA384,
			crypto.SHA512,
			crypto.MD5SHA1,
			crypto.RIPEMD160,
			crypto.SHA3_224,
			crypto.SHA3_256,
			crypto.SHA3_384,
			crypto.SHA3_512,
			crypto.SHA512_224,
			crypto.SHA512_256,
			crypto.BLAKE2s_256,
			crypto.BLAKE2b_256,
			crypto.BLAKE2b_384,
			crypto.BLAKE2b_512,
		}
		for _, h := range hashes {
			if strings.ToUpper(data.(string)) == h.String() {
				return h, nil
			}
		}
		return crypto.Hash(0), nil
	}
}

type hashFlag struct {
	Value crypto.Hash
}

func (f *hashFlag) String() string {
	return f.Value.String()
}

func (f *hashFlag) Set(s string) error {
	s = strings.ToUpper(s)
	val := f.getHash(s)
	available := []string{}
	for _, hash := range availableHashes() {
		available = append(available, hash.String())
	}

	if s != "" && val == crypto.Hash(0) {
		return fmt.Errorf(
			"invalid hash algorithm '%s' (expected one of: %s)",
			s,
			strings.Join(available, ", "),
		)
	}
	if !val.Available() {
		return fmt.Errorf(
			"hash algorithm '%s' is not available (available: %s)",
			s,
			strings.Join(available, ", "),
		)
	}
	f.Value = val
	return nil
}

func availableHashes() []crypto.Hash {
	hashes := []crypto.Hash{
		crypto.MD4,
		crypto.MD5,
		crypto.SHA1,
		crypto.SHA224,
		crypto.SHA256,
		crypto.SHA384,
		crypto.SHA512,
		crypto.MD5SHA1,
		crypto.RIPEMD160,
		crypto.SHA3_224,
		crypto.SHA3_256,
		crypto.SHA3_384,
		crypto.SHA3_512,
		crypto.SHA512_224,
		crypto.SHA512_256,
		crypto.BLAKE2s_256,
		crypto.BLAKE2b_256,
		crypto.BLAKE2b_384,
		crypto.BLAKE2b_512,
	}
	available := []crypto.Hash{}
	for _, hash := range hashes {
		if hash.Available() {
			available = append(available, hash)
		}
	}
	return available
}

func (f *hashFlag) getHash(s string) crypto.Hash {
	switch s {
	case "MD4":
		return crypto.MD4
	case "MD5":
		return crypto.MD5
	case "SHA-1":
		return crypto.SHA1
	case "SHA-224":
		return crypto.SHA224
	case "SHA-256":
		return crypto.SHA256
	case "SHA-384":
		return crypto.SHA384
	case "SHA-512":
		return crypto.SHA512
	case "MD5+SHA1":
		return crypto.MD5SHA1
	case "RIPEMD-160":
		return crypto.RIPEMD160
	case "SHA3-224":
		return crypto.SHA3_224
	case "SHA3-256":
		return crypto.SHA3_256
	case "SHA3-384":
		return crypto.SHA3_384
	case "SHA3-512":
		return crypto.SHA3_512
	case "SHA-512/224":
		return crypto.SHA512_224
	case "SHA-512/256":
		return crypto.SHA512_256
	case "BLAKE2s-256":
		return crypto.BLAKE2s_256
	case "BLAKE2b-256":
		return crypto.BLAKE2b_256
	case "BLAKE2b-384":
		return crypto.BLAKE2b_384
	case "BLAKE2b-512":
		return crypto.BLAKE2b_512
	default:
		return crypto.Hash(0) // Unknown or unsupported hash
	}
}

func (f hashFlag) Type() string {
	return "crypto.Hash"
}

func getLogLevel(levelName string) (slog.Level, bool) {
	levelName = strings.ToUpper(levelName)
	switch levelName {
	case slog.LevelDebug.String():
		return slog.LevelDebug, true
	case slog.LevelInfo.String():
		return slog.LevelInfo, true
	case slog.LevelWarn.String():
		return slog.LevelWarn, true
	case slog.LevelError.String():
		return slog.LevelError, true
	case "EVENT":
		return server.LevelEvent, true
	default:
		return slog.LevelInfo, false
	}
}

type clientOptions struct {
	KeyLifespan         time.Duration
	LockTimeout         time.Duration
	LockCreateIfMissing bool
	ListKeyOpts         struct {
		Pattern         string
		Limit           uint64
		IncludeReserved bool
	}
	GetKeyVersion uint64

	Verbose     bool   `json:"verbose" yaml:"verbose" mapstructure:"verbose"`
	SSLKeyfile  string `json:"ssl_keyfile" yaml:"ssl_keyfile" mapstructure:"ssl_keyfile"`
	SSLCertfile string `json:"ssl_certfile" yaml:"ssl_certfile" mapstructure:"ssl_certfile"`
	Quiet       bool
	IndentJSON  int
}

var cliOpts CLIConfig = CLIConfig{
	ServerOpts: *server.DefaultConfig(),
	ClientOpts: client.DefaultConfig(),
}
var defaultLogger = slog.Default().With("logger", "cli")

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "keyquarry",
	Short: "Key-value store client/server with gRPC",
	Long:  ``,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(ctx context.Context) {

	err := rootCmd.ExecuteContext(ctx)

	if err != nil {
		defaultLogger.Error(
			"error during execution",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
}

// parseURL takes the given string, and attempts to parse it to
// a URL with scheme 'tcp' or 'unix', setting a default port if
// one isn't specified (TCP only)
func parseURL(s string) (*url.URL, error) {
	var network string
	var host string

	n, h, found := strings.Cut(s, "://")
	if found {
		host = h
		if n == "" {
			network = defaultNetwork
		} else {
			network = n
		}
	} else {
		host = n
		network = defaultNetwork
	}

	u := &url.URL{Scheme: network, Host: host}
	if u.Port() == "" && u.Scheme != "unix" {
		u.Host = fmt.Sprintf("%s:%d", u.Host, defaultPort)
	}
	return u, nil
}

func init() {
	cobra.OnInitialize(initConfig)

	_ = serverCmd.MarkFlagFilename("ssl-keyfile")
	rootCmd.PersistentFlags().StringVar(
		&cliOpts.LogLevel,
		"log-level",
		slog.LevelInfo.String(),
		"Log level (debug, info, warn, error). Server logs will "+
			"be written to stdout, while client logs will be written to "+
			"stderr.",
	)
	rootCmd.PersistentFlags().BoolVar(
		&cliOpts.LogJSON,
		"log-json",
		false,
		"Log in JSON format",
	)
	rootCmd.PersistentFlags().BoolVar(
		&cliOpts.ProfileCPU,
		"profile",
		false,
		"Profile CPU",
	)

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	var err error
	cobra.CheckErr(
		viper.BindPFlag(
			"listen_address",
			serverCmd.Flags().Lookup("listen"),
		),
	)
	viper.SetDefault(
		"listen_address",
		defaultAddress,
	)

	e := viper.BindPFlag(
		"ssl_certfile",
		serverCmd.Flags().Lookup("ssl-certfile"),
	)
	cobra.CheckErr(e)

	cobra.CheckErr(
		viper.BindPFlag(
			"ssl_keyfile",
			serverCmd.Flags().Lookup("ssl-keyfile"),
		),
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"max_value_size",
			serverCmd.Flags().Lookup("max-value-bytes"),
		),
	)
	viper.SetDefault("max_value_size", server.DefaultMaxValueSize)

	cobra.CheckErr(
		viper.BindPFlag(
			"max_key_size",
			serverCmd.Flags().Lookup("max-key-size"),
		),
	)
	viper.SetDefault("max_key_size", server.DefaultMaxKeySize)

	cobra.CheckErr(
		viper.BindPFlag(
			"max_keys",
			serverCmd.Flags().Lookup("max-keys"),
		),
	)
	viper.SetDefault("max_keys", 0)

	cobra.CheckErr(
		viper.BindPFlag(
			"revision_limit",
			serverCmd.Flags().Lookup("revision-limit"),
		),
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"log_level",
			rootCmd.Flags().Lookup("log-level"),
		),
	)
	viper.SetDefault("log_level", slog.LevelInfo.String())

	cobra.CheckErr(
		viper.BindPFlag(
			"log_json",
			rootCmd.Flags().Lookup("log-json"),
		),
	)
	viper.SetDefault("log_json", false)

	viper.SetDefault("log_events", true)
	viper.SetDefault("hash_algorithm", crypto.MD5.String())

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot.dir",
			serverCmd.Flags().Lookup("snapshot-dir"),
		),
	)
	var defaultSnapshotDir string
	defaultSnapshotDir, _ = os.UserCacheDir()
	if defaultSnapshotDir != "" {
		defaultSnapshotDir = filepath.Join(
			defaultSnapshotDir,
			"keyquarry",
			"snapshots",
		)
	}
	viper.SetDefault("snapshot.dir", defaultSnapshotDir)

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot.interval",
			serverCmd.Flags().Lookup("snapshot-interval"),
		),
	)
	viper.SetDefault("snapshot.interval", "0")

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot.limit",
			serverCmd.Flags().Lookup("snapshot-limit"),
		),
	)
	viper.SetDefault("snapshot.limit", server.DefaultSnapshotLimit)

	viper.SetDefault("snapshot.secret_key", "")

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot.encrypt",
			serverCmd.Flags().Lookup("encrypt-snapshots"),
		),
	)
	viper.SetDefault("snapshot.encrypt", true)

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot.enabled",
			serverCmd.Flags().Lookup("snapshot"),
		),
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"readonly",
			serverCmd.Flags().Lookup("readonly"),
		),
	)
	viper.SetDefault("readonly", false)

	viper.SetDefault("max_lock_duration", server.DefaultMaxLockDuration)
	viper.SetDefault("min_lock_duration", server.DefaultMinLockDuration)
	viper.SetDefault("min_lifespan", server.DefaultMinExpiry)

	viper.SetDefault("prune_threshold", server.DefaultPruneThreshold)
	viper.SetDefault("prune_target", server.DefaultPruneTarget)
	viper.SetDefault("min_prune_interval", server.MinPruneInterval)
	viper.SetDefault("eager_prune", true)
	viper.SetDefault(
		"event_stream_buffer_size",
		server.DefaultEventStreamBufferSize,
	)
	viper.SetDefault(
		"event_stream_send_timeout",
		server.DefaultEventStreamSendTimeout,
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"client_id", clientCmd.PersistentFlags().Lookup("client-id"),
		),
	)

	viper.SetEnvPrefix("KEYQUARRY")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv() // read in environment variables that match

	userConfigDir, _ := os.UserConfigDir()
	if userConfigDir != "" {
		userConfigDir = filepath.Join(userConfigDir, "keyquarry")
		viper.AddConfigPath(userConfigDir)
	}
	viper.AddConfigPath(".")
	viper.SetConfigType("env")
	viper.SetConfigName("keyquarry-server")
	if cliOpts.configFile == "" {
		if err = viper.ReadInConfig(); err != nil {
			var configFileNotFoundError viper.ConfigFileNotFoundError
			if !errors.As(err, &configFileNotFoundError) {
				defaultLogger.Error(err.Error())
				os.Exit(1)
			}
		}
	} else {
		// use config file specified in the flag
		defaultLogger.Info("using config file: " + cliOpts.configFile)
		viper.SetConfigFile(cliOpts.configFile)
		if err = viper.ReadInConfig(); err != nil {
			defaultLogger.Error(err.Error())
			os.Exit(1)
		}
	}

	cobra.CheckErr(viper.Unmarshal(&cliOpts))

	if cliOpts.configFile == "" {
		cliOpts.configFile = viper.ConfigFileUsed()
	}

}

func bindFlags(cmd *cobra.Command, v *viper.Viper, flagMap map[string]string) {
	return
	cmd.Flags().VisitAll(
		func(f *pflag.Flag) {
			// Determine the naming convention of the flags when represented in the config file
			configName := f.Name
			fmt.Println("checking", configName)

			viperName, exists := flagMap[configName]
			if f.Changed {
				fmt.Println(
					configName,
					"changed",
					viperName,
					f.Value,
					f.DefValue,
					exists,
				)
			}
			if exists {
				if v.IsSet(viperName) {
					fmt.Println(
						configName,
						"is set",
						viperName,
						fmt.Sprintf("%#v", v.Get(viperName)),
					)
				}

				if !f.Changed && v.IsSet(viperName) {
					val := v.Get(viperName)

					fmt.Println(
						"overriding value",
						f.Name,
						"from",
						f.Value,
						"to",
						val,
					)
					cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))

				}
			} else {
				if !f.Changed && v.IsSet(configName) {
					val := v.Get(configName)
					fmt.Println(
						"overriding value",
						f.Name,
						"from",
						f.Value,
						"to",
						val,
					)
					cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
				}
			}

			// Apply the viper config value to the flag when the flag is not set and viper has a value

		},
	)
}

func readStdin() ([]string, error) {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return nil, err
	}
	if (stat.Mode() & os.ModeNamedPipe) == 0 {
		return nil, nil
	}
	data := make([]string, 0)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		row := scanner.Text()
		if row != "" {
			data = append(data, row)
		}
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

func pmJSON(m proto.Message, indent string) (string, error) {
	if indent == "" {
		data, err := json.Marshal(m)
		return string(data), err
	}
	js, err := json.MarshalIndent(m, "", indent)
	return string(js), err
}

func printResult(m proto.Message) {
	var indent string
	opts := &cliOpts
	if opts.ClientOpts.IndentJSON > 0 {
		indent = strings.Repeat(" ", cliOpts.ClientOpts.IndentJSON)
	}
	js, err := pmJSON(m, indent)
	printError(err)
	_, err = fmt.Fprintln(out, js)
	printError(err)
}

func printError(err error) {
	if err != nil {
		defaultLogger.Error(err.Error())
		os.Exit(1)
	}
}
