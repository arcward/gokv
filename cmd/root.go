package cmd

import (
	"bufio"
	"context"
	"crypto"
	"errors"
	"fmt"
	"github.com/arcward/gokv/client"
	"github.com/arcward/gokv/server"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	defaultNetwork = "tcp"
	defaultHost    = "localhost"
	defaultPort    = 33969
)

var defaultAddress = fmt.Sprintf(
	"%s:%d",
	defaultHost,
	defaultPort,
)

type CLIConfig struct {
	Address             string              `json:"address" yaml:"address" mapstructure:"address"`
	Verbose             bool                `json:"verbose" yaml:"verbose" mapstructure:"verbose"`
	Server              server.Config       `json:"server" yaml:"server" mapstructure:"server"`
	SSLKeyfile          string              `json:"ssl_keyfile" yaml:"ssl_keyfile" mapstructure:"ssl_keyfile"`
	SSLCertfile         string              `json:"ssl_certfile" yaml:"ssl_certfile" mapstructure:"ssl_certfile"`
	Client              client.ClientConfig // `json:"client" yaml:"client" mapstructure:"client"`
	configFile          string
	clientOpts          clientOptions
	server              *server.KeyValueStore
	grpcServer          *grpc.Server
	client              *client.Client
	LogLevel            logLevelFlag `json:"log_level" yaml:"log_level"`
	LogJSON             bool         `json:"log_json" yaml:"log_json"`
	hashAlgorithm       hashFlag
	Quiet               bool
	IndentJSON          int
	LoadSnapshot        string
	SnapshotDir         string
	SnapshotInterval    time.Duration
	SnapshotLimit       int
	ShowDetailedVersion bool
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
	for _, hash := range f.availableHashes() {
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

func (f *hashFlag) availableHashes() []crypto.Hash {
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

type logLevelFlag struct {
	Value slog.Level
}

func (f *logLevelFlag) String() string {
	return f.Value.String()
}

func (f logLevelFlag) Level() slog.Level {
	return f.Value
}

func (f *logLevelFlag) Set(s string) error {
	s = strings.ToUpper(s)
	levels := []slog.Level{
		slog.LevelInfo,
		slog.LevelDebug,
		slog.LevelWarn,
		slog.LevelError,
	}
	for _, level := range levels {
		if s == level.String() {
			f.Value = level
			return nil
		}
	}
	return fmt.Errorf(
		"invalid log level '%s' (expected one of: %s %s %s %s)",
		s,
		levels[0],
		levels[1],
		levels[2],
		levels[3],
	)
}

func (f logLevelFlag) Type() string {
	return "log_level"
}

type clientOptions struct {
	ExpireKeyIn time.Duration
	Lock        bool
	LockTimeout time.Duration
	ListKeyOpts struct {
		Pattern string
		Limit   uint64
	}
}

var cliOpts CLIConfig
var defaultLogger = slog.Default().WithGroup("gokv")

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "gokv",
	Short: "Key-value store client/server with gRPC",
	Long:  ``,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(ctx context.Context) {
	//ctx, cancel := context.WithCancel(context.Background())
	//signals := make(chan os.Signal, 1)
	//wg := &sync.WaitGroup{}
	//signal.Notify(
	//	signals,
	//	os.Interrupt,
	//	syscall.SIGHUP,
	//	syscall.SIGTERM,
	//	syscall.SIGINT,
	//)

	//defer func() {
	//	signal.Stop(signals)
	//	cancel()
	//}()
	//go func() {
	//	defer func() {
	//		fmt.Println("done")
	//	}()
	//	select {
	//	case <-ctx.Done():
	//		// only wait for the server to stop if it's running
	//		// this avoids blocking the client
	//		wg.Add(1)
	//		defer wg.Done()
	//		fmt.Println("called cancel")
	//		if cliOpts.grpcServer != nil {
	//			fmt.Println("graceful stop")
	//			cliOpts.grpcServer.GracefulStop()
	//		}
	//		opts := &cliOpts
	//		opts.server.Stop()
	//		//case <-signals:
	//		//	cancel()
	//	}
	//}()

	//go func() {
	//	defer func() {
	//		fmt.Println("done")
	//	}()
	//	select {
	//	case <-ctx.Done():
	//		fmt.Println("finished")
	//	case <-signals:
	//		// only wait for the server to stop if it's running
	//		// this avoids blocking the client
	//		wg.Add(1)
	//		defer wg.Done()
	//		cancel()
	//		fmt.Println("called cancel")
	//		if cliOpts.grpcServer != nil {
	//			fmt.Println("graceful stop")
	//			cliOpts.grpcServer.GracefulStop()
	//		}
	//		opts := &cliOpts
	//		opts.server.Stop()
	//	}
	//}()
	err := rootCmd.ExecuteContext(ctx)
	//fmt.Println("waiting")
	//wg.Wait()
	if err != nil {
		defaultLogger.Error(err.Error())
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

	rootCmd.PersistentFlags().StringVarP(
		&cliOpts.Address,
		"address",
		"a",
		defaultAddress,
		"Address to connect to/listen from",
	)

	rootCmd.PersistentFlags().StringVar(
		&cliOpts.SSLCertfile,
		"ssl-certfile",
		"",
		"SSL certificate file",
	)
	_ = serverCmd.MarkFlagFilename("ssl-certfile")
	rootCmd.PersistentFlags().StringVar(
		&cliOpts.SSLKeyfile,
		"ssl-keyfile",
		"",
		"SSL key file",
	)
	_ = serverCmd.MarkFlagFilename("ssl-keyfile")
	rootCmd.PersistentFlags().Var(
		&cliOpts.LogLevel,
		"log-level",
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
		&cliOpts.Quiet,
		"quiet",
		false,
		"Disables log output",
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
			"address",
			rootCmd.Flags().Lookup("address"),
		),
	)
	viper.SetDefault(
		"address",
		defaultAddress,
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"ssl_certfile",
			rootCmd.Flags().Lookup("ssl-certfile"),
		),
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"ssl_keyfile",
			rootCmd.Flags().Lookup("ssl-keyfile"),
		),
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"max_value_size",
			serverCmd.Flags().Lookup("max-value-size"),
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
	viper.SetDefault("revision_limit", 5)

	cobra.CheckErr(
		viper.BindPFlag(
			"log_level",
			rootCmd.Flags().Lookup("log-level"),
		),
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"hash_algorithm",
			serverCmd.Flags().Lookup("hash-algorithm"),
		),
	)
	viper.SetDefault("hash_algorithm", "")

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot_dir",
			serverCmd.Flags().Lookup("snapshot-dir"),
		),
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot_interval",
			serverCmd.Flags().Lookup("snapshot-interval"),
		),
	)

	cobra.CheckErr(
		viper.BindPFlag(
			"snapshot_limit",
			serverCmd.Flags().Lookup("snapshot-limit"),
		),
	)

	viper.SetEnvPrefix("GOKV")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv() // read in environment variables that match

	// Find home directory.
	home, e := os.UserHomeDir()
	if e != nil {
		defaultLogger.Error(
			"unable to find home directory",
			slog.String("error", e.Error()),
		)
	}

	// Search config in home directory with name ".gokv" (without extension).
	if home != "" {
		viper.AddConfigPath(home)
	}
	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")
	viper.SetConfigName(".gokv-server")

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
	marshaler := jsonpb.Marshaler{Indent: indent}
	js, err := marshaler.MarshalToString(m)
	return js, err
}

func printResult(m proto.Message) {
	var indent string
	opts := &cliOpts
	if opts.IndentJSON > 0 {
		indent = strings.Repeat(" ", cliOpts.IndentJSON)
	}
	js, err := pmJSON(m, indent)
	printError(err)
	fmt.Println(js)
}

func printError(err error) {
	if err != nil {
		defaultLogger.Error(err.Error())
		os.Exit(1)
	}
}
