package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/arcward/keyquarry/client"
	"github.com/arcward/keyquarry/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
)

var out io.Writer = os.Stdout

type CLIConfig struct {
	ServerOpts server.Config `json:"server" yaml:"server" mapstructure:"server"`
	// If true, snapshots will be ignored on startup. If snapshots are
	// enabled, they will still be created.
	StartFresh          bool `json:"start_fresh" yaml:"start_fresh" mapstructure:"start_fresh"`
	ClientOpts          client.Config
	configFile          string
	clientOpts          clientOptions
	server              *server.KeyValueStore
	grpcServer          *grpc.Server
	client              *client.Client
	LogLevel            string `json:"log_level" yaml:"log_level" mapstructure:"log_level"`
	LogJSON             bool   `json:"log_json" yaml:"log_json" mapstructure:"log_json"`
	ShowDetailedVersion bool
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
	InspectIncludeValue   bool
	InspectIncludeMetrics bool
	InspectDecodeValue    bool
	GetKeyVersion         int64
	Verbose               bool `json:"verbose" yaml:"verbose" mapstructure:"verbose"`
	Quiet                 bool
	IndentJSON            int
}

var cliOpts = CLIConfig{
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
			network = server.DefaultNetwork
		} else {
			network = n
		}
	} else {
		host = n
		network = server.DefaultNetwork
	}

	u := &url.URL{Scheme: network, Host: host}
	if u.Port() == "" && u.Scheme != "unix" {
		u.Host = fmt.Sprintf("%s:%d", u.Host, server.DefaultPort)
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
		server.DefaultAddress,
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
			"max_key_length",
			serverCmd.Flags().Lookup("max-key-length"),
		),
	)
	viper.SetDefault("max_key_length", server.DefaultMaxKeyLength)

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
	viper.SetDefault("log_events", false)

	viper.SetDefault("monitor_address", server.DefaultMonitorAddress)
	viper.SetDefault("pprof", false)
	viper.SetDefault("expvar", false)
	viper.SetDefault("metrics", false)
	viper.SetDefault("trace", false)

	viper.SetDefault(
		"graceful_shutdown_timeout",
		server.DefaultGracefulStopTimeout,
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot.interval",
			serverCmd.Flags().Lookup("snapshot-interval"),
		),
	)
	viper.SetDefault("snapshot.interval", "0")
	viper.SetDefault("snapshot.database", "")
	viper.SetDefault(
		"snapshot.expire_after",
		server.DefaultSnapshotExpireAfter.String(),
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot.enabled",
			serverCmd.Flags().Lookup("snapshot"),
		),
	)

	viper.SetDefault("snapshot.database", "")
	viper.SetDefault("event_stream_subscriber_limit", 0)
	// service name used in traces
	viper.SetDefault("service_name", "keyquarry")
	cobra.CheckErr(
		viper.BindPFlag(
			"readonly",
			serverCmd.Flags().Lookup("readonly"),
		),
	)

	var defaultServerName string
	hostname, _ := os.Hostname()
	if hostname != "" {
		defaultServerName = hostname
	}

	u, _ := user.Current()
	if u != nil {
		username := u.Username
		if username != "" {
			defaultServerName = fmt.Sprintf(
				"%s@%s",
				username,
				defaultServerName,
			)
		}
	}

	viper.SetDefault("name", defaultServerName)
	viper.SetDefault("readonly", false)
	viper.SetDefault("start_fresh", false)
	viper.SetDefault("max_lock_duration", server.DefaultMaxLockDuration)
	viper.SetDefault("min_lock_duration", server.DefaultMinLockDuration)
	viper.SetDefault("min_lifespan", server.DefaultMinExpiry)
	viper.SetDefault("privileged_client_id", "")
	viper.SetDefault("prune_interval", "0")
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
	cobra.CheckErr(viper.Unmarshal(&cliOpts.ServerOpts))
	if cliOpts.configFile == "" {
		cliOpts.configFile = viper.ConfigFileUsed()
	}

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
