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
	"google.golang.org/protobuf/proto"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// cliOpts holds the CLI config, set by flags
var (
	cliOpts = cliConfig{
		ServerOpts: *server.NewConfig(),
		clientOpts: clientOptions{},
	}
	defaultLogger = slog.Default().With("logger", "cli")
	rootCmd       = &cobra.Command{
		Use:   "keyquarry",
		Short: "Key-value store client/server with gRPC",
	}
	out io.Writer = os.Stdout
)

type cliConfig struct {
	ServerOpts          server.Config `json:"server" yaml:"server" mapstructure:"server"`
	configFile          string
	clientOpts          clientOptions
	client              *client.Client
	LogLevel            string `json:"log_level" yaml:"log_level" mapstructure:"log_level"`
	LogJSON             bool   `json:"log_json" yaml:"log_json" mapstructure:"log_json"`
	ShowDetailedVersion bool
}

type clientOptions struct {
	// Address is the address of the server to connect to
	Address string

	// ClientID is the ID of the client. If not specified, a random ID will be
	// generated.
	ClientID string

	// Verbose enables debug logging
	Verbose bool

	// Quiet disables all logging
	Quiet bool

	// IndentJSON specifies the number of spaces to indent JSON output
	IndentJSON int

	// CACert is the path to the CA certificate file
	CACert string

	// InsecureSkipVerify disables certificate verification
	InsecureSkipVerify bool

	// NoTLS disables TLS
	NoTLS bool

	// DialTimeout is the timeout for establishing a connection
	DialTimeout time.Duration

	// DialKeepAliveTime is the duration between keepalive pings sent to the server
	// when no activity is detected
	DialKeepAliveTime time.Duration

	// DialKeepAliveTimeout is the timeout for keepalive pings sent to the server
	DialKeepAliveTimeout time.Duration

	// KeyLifespan is for commands that allow specifying a key lifespan
	KeyLifespan time.Duration

	// LockTimeout specifies a lock duration for commands that lock a key
	LockTimeout time.Duration

	// LockCreateIfMissing sets `create_if_missing` for the lock command
	LockCreateIfMissing bool

	// ListKeyOpts holds options for list-keys
	ListKeyOpts struct {
		Pattern         string
		Limit           uint64
		IncludeReserved bool
	}

	// InspectIncludeValue sets `include_value` for the inspect command
	InspectIncludeValue bool

	// InspectIncludeMetrics sets `include_metrics` for the inspect command
	InspectIncludeMetrics bool

	// InspectDecodeValue sets `decode_value` for the inspect command
	InspectDecodeValue bool

	// GetKeyVersion specifies a revision to retrieve for the Get command
	GetKeyVersion int64
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(ctx context.Context) {
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		defaultLogger.Error(
			"error during execution",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
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
	cobra.CheckErr(
		viper.BindPFlag(
			"start_fresh",
			serverCmd.Flags().Lookup("fresh"),
		),
	)
	cobra.CheckErr(
		viper.BindPFlag(
			"ssl_certfile",
			serverCmd.Flags().Lookup("ssl-certfile"),
		),
	)
	cobra.CheckErr(
		viper.BindPFlag(
			"ssl_keyfile",
			serverCmd.Flags().Lookup("ssl-keyfile"),
		),
	)

	viper.SetDefault("max_value_size", server.DefaultMaxValueSize)
	viper.SetDefault("max_key_length", server.DefaultMaxKeyLength)
	viper.SetDefault("max_keys", 0)

	cobra.CheckErr(
		viper.BindPFlag(
			"log_level",
			rootCmd.Flags().Lookup("log-level"),
		),
	)
	viper.SetDefault("log_level", server.LevelNoticeName)

	cobra.CheckErr(
		viper.BindPFlag(
			"log_json",
			rootCmd.Flags().Lookup("log-json"),
		),
	)
	viper.SetDefault("log_json", false)

	viper.SetDefault("log_events", false)

	viper.SetDefault("monitor_address", server.DefaultMonitorAddress)
	cobra.CheckErr(
		viper.BindPFlag(
			"monitor_address", serverCmd.Flags().Lookup("monitor-address"),
		),
	)

	viper.SetDefault("pprof", false)
	cobra.CheckErr(viper.BindPFlag("pprof", serverCmd.Flags().Lookup("pprof")))

	viper.SetDefault("expvar", false)
	cobra.CheckErr(
		viper.BindPFlag(
			"expvar",
			serverCmd.Flags().Lookup("expvar"),
		),
	)

	viper.SetDefault("prometheus", false)
	cobra.CheckErr(
		viper.BindPFlag(
			"prometheus",
			serverCmd.Flags().Lookup("prometheus"),
		),
	)

	viper.SetDefault("trace", false)
	cobra.CheckErr(viper.BindPFlag("trace", serverCmd.Flags().Lookup("trace")))

	viper.SetDefault("tracer_name", server.DefaultTracerName)

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

	viper.SetDefault("snapshot.enabled", false)
	viper.SetDefault("snapshot.interval", "0")
	viper.SetDefault("snapshot.database", "")

	// service name used in traces
	viper.SetDefault("service_name", "keyquarry")

	viper.SetDefault("readonly", false)
	cobra.CheckErr(
		viper.BindPFlag(
			"readonly",
			serverCmd.Flags().Lookup("readonly"),
		),
	)

	viper.SetDefault("revision_limit", server.DefaultRevisionLimit)
	viper.SetDefault("persistent_revisions", false)
	viper.SetDefault("keep_key_history_after_delete", false)

	cobra.CheckErr(viper.BindPFlag("name", serverCmd.Flags().Lookup("name")))

	viper.SetDefault("start_fresh", false)
	cobra.CheckErr(viper.BindPFlag("trace", serverCmd.Flags().Lookup("fresh")))

	viper.SetDefault("max_lock_duration", server.DefaultMaxLockDuration)
	viper.SetDefault("min_lock_duration", server.DefaultMinLockDuration)
	viper.SetDefault("min_lifespan", server.DefaultMinLifespan)
	viper.SetDefault("privileged_client_id", "")

	viper.SetDefault("prune_interval", "0")
	viper.SetDefault("prune_at", 0)
	viper.SetDefault("prune_to", 0)

	viper.SetDefault("eager_prune_at", 0)
	viper.SetDefault("eager_prune_to", 0)

	viper.SetDefault(
		"event_stream_buffer_size",
		server.DefaultEventStreamBufferSize,
	)
	viper.SetDefault(
		"event_stream_send_timeout",
		server.DefaultEventStreamSendTimeout,
	)
	viper.SetDefault("event_stream_subscriber_limit", 0)

	viper.SetEnvPrefix("KEYQUARRY")

	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()

	userConfigDir, _ := os.UserConfigDir()
	if userConfigDir != "" {
		userConfigDir = filepath.Join(userConfigDir, "keyquarry")
		viper.AddConfigPath(userConfigDir)
	}
	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")
	viper.SetConfigName("keyquarry")

	switch cliOpts.configFile {
	case "":
		if err = viper.ReadInConfig(); err != nil {
			var configFileNotFoundError viper.ConfigFileNotFoundError
			if !errors.As(err, &configFileNotFoundError) {
				defaultLogger.Error(err.Error())
				os.Exit(1)
			}
		}
	default:
		// use config file specified in the flag
		// defaultLogger.Info("using config file: " + cliOpts.configFile)
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

// readStdin reads from stdin, returning a slice of non-empty lines
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

// pmJSON marshals the given proto.Message with the given indent
func pmJSON(m proto.Message, indent string) (string, error) {
	if indent == "" {
		data, err := json.Marshal(m)
		return string(data), err
	}
	js, err := json.MarshalIndent(m, "", indent)
	return string(js), err
}

// printResult prints the given proto.Message to stdout (indented as needed),
// or exits with status code 1 if an error occurs
func printResult(m proto.Message) {
	var indent string
	opts := &cliOpts
	if opts.clientOpts.IndentJSON > 0 {
		indent = strings.Repeat(" ", cliOpts.clientOpts.IndentJSON)
	}
	js, err := pmJSON(m, indent)
	printError(err)
	_, err = fmt.Fprintln(out, js)
	printError(err)
}

// printError is a helper function which, if the given error is not nil,
// prints the error to stderr and exits with status code 1
func printError(err error) {
	if err != nil {
		defaultLogger.Error(err.Error())
		os.Exit(1)
	}
}

// getLogLevel returns the slog.Level for the given level name, or
// false if the level name is invalid. This accounts for custom
// levels server.LevelEvent and server.LevelNotice
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
	case server.LevelEventName:
		return server.LevelEvent, true
	case server.LevelNoticeName:
		return server.LevelNotice, true
	default:
		return server.LevelNotice, false
	}
}

// parseURL takes the given string, and attempts to parse it to
// a URL with scheme 'tcp' or 'unix', setting a default port if
// one isn't specified (TCP only)
func parseURL(s string) (*url.URL, error) {
	var network string
	var host string

	n, h, found := strings.Cut(s, "://")
	switch {
	case found:
		host = h
		if n == "" {
			network = server.DefaultNetwork
		} else {
			network = n
		}
	default:
		host = n
		network = server.DefaultNetwork
	}

	u := &url.URL{Scheme: network, Host: host}
	if u.Port() == "" && u.Scheme != "unix" {
		u.Host = fmt.Sprintf("%s:%d", u.Host, server.DefaultPort)
	}
	return u, nil
}
