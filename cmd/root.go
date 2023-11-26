package cmd

import (
	"bufio"
	"context"
	"fmt"
	"github.com/arcward/gokv/client"
	"github.com/arcward/gokv/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultPort = 33969
)

type CLIConfig struct {
	Address      string              `json:"address" yaml:"address" mapstructure:"address"`
	Verbose      bool                `json:"verbose" yaml:"verbose" mapstructure:"verbose"`
	Server       server.Config       `json:"server" yaml:"server" mapstructure:"server"`
	SSL          sslConfig           `json:"ssl" yaml:"ssl" mapstructure:"ssl"`
	Client       client.ClientConfig `json:"client" yaml:"client" mapstructure:"client"`
	configFile   string
	ServerBackup string `json:"server_backup" yaml:"server_backup" mapstructure:"server_backup"`
	clientOpts   clientOptions
	server       *server.KeyValueStore
	grpcServer   *grpc.Server
	client       *client.Client
	LogLevel     logLevelFlag `json:"log_level" yaml:"log_level"`
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

type sslConfig struct {
	Keyfile  string `json:"keyfile" yaml:"keyfile" mapstructure:"keyfile"`
	Certfile string `json:"certfile" yaml:"certfile" mapstructure:"certfile"`
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

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "gokv",
	Short: "Key-value store client/server with gRPC",
	Long:  ``,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		opts := &cliOpts
		var vcfg CLIConfig
		err := viper.Unmarshal(&vcfg)
		if err != nil {
			log.Fatalln(err)
		}

		if opts.Verbose {
			opts.LogLevel.Value = slog.LevelDebug
		}
		if opts.SSL.Certfile != "" {
			opts.Client.SSLCertfile = opts.SSL.Certfile
		}
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	wg := &sync.WaitGroup{}
	signal.Notify(
		signals,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGTERM,
		syscall.SIGINT,
	)

	defer func() {
		signal.Stop(signals)
		cancel()
	}()

	go func() {
		select {
		case <-signals:
			// only wait for the server to stop if it's running
			// this avoids blocking the client
			wg.Add(1)
			defer wg.Done()
			cancel()
			if cliOpts.grpcServer != nil {
				cliOpts.grpcServer.GracefulStop()
			}
			if cliOpts.ServerBackup != "" && cliOpts.server != nil {
				err := cliOpts.server.Dump(cliOpts.ServerBackup)
				if err != nil {
					panic(fmt.Sprintf("error dumping data: %s", err.Error()))
				}
			}
		case <-ctx.Done():
		}
	}()
	err := rootCmd.ExecuteContext(ctx)
	wg.Wait()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(
		&cliOpts.configFile,
		"config",
		"",
		"config file (default is $HOME/.gokv.yaml)",
	)
	rootCmd.PersistentFlags().StringVarP(
		&cliOpts.Address,
		"address",
		"a",
		"tcp://localhost:33969",
		"Address to connect to/listen from",
	)
	rootCmd.PersistentFlags().BoolVarP(
		&cliOpts.Verbose,
		"verbose",
		"v",
		false,
		"verbose output",
	)
	rootCmd.PersistentFlags().StringVar(
		&cliOpts.SSL.Certfile,
		"ssl-certfile",
		"",
		"SSL certificate file",
	)
	_ = serverCmd.MarkFlagFilename("ssl-certfile")
	rootCmd.PersistentFlags().StringVar(
		&cliOpts.SSL.Keyfile,
		"ssl-keyfile",
		"",
		"SSL key file",
	)
	_ = serverCmd.MarkFlagFilename("ssl-keyfile")
	rootCmd.PersistentFlags().Var(
		&cliOpts.LogLevel,
		"log-level",
		"Log level (debug, info, warn, error)",
	)
	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	var err error

	_ = viper.BindPFlag("address", serverCmd.Flags().Lookup("address"))
	viper.SetDefault(
		"address",
		fmt.Sprintf(":%d", defaultPort),
	)

	err = viper.BindPFlag(
		"ssl.certfile",
		rootCmd.Flags().Lookup("ssl-certfile"),
	)
	if err != nil {
		log.Fatalf("error binding flag: %s", err.Error())
	}

	err = viper.BindPFlag(
		"ssl.keyfile",
		rootCmd.Flags().Lookup("ssl-keyfile"),
	)
	if err != nil {
		log.Fatalf("error binding flag: %s", err.Error())
	}

	_ = viper.BindPFlag(
		"server.max_value_size",
		serverCmd.Flags().Lookup("max-value-size"),
	)
	viper.SetDefault("server.max_value_size", server.DefaultMaxValueSize)

	_ = viper.BindPFlag(
		"server.max_key_size",
		serverCmd.Flags().Lookup("max-key-size"),
	)
	viper.SetDefault("server.max_key_size", server.DefaultMaxKeySize)

	_ = viper.BindPFlag(
		"server.history.enabled",
		serverCmd.Flags().Lookup("history"),
	)
	viper.SetDefault("server.history.enabled", false)

	_ = viper.BindPFlag(
		"server.history.revision_limit",
		serverCmd.Flags().Lookup("history-revision-limit"),
	)
	viper.SetDefault("server.history.revision_limit", 5)

	_ = viper.BindPFlag(
		"server.backup",
		serverCmd.Flags().Lookup("backup"),
	)

	_ = viper.BindPFlag(
		"log_level",
		serverCmd.Flags().Lookup("log-level"),
	)

	viper.SetEnvPrefix("GOKV")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv() // read in environment variables that match

	if cliOpts.configFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cliOpts.configFile)
		if err = viper.SafeWriteConfigAs(cliOpts.configFile); err != nil {
			if _, ok := err.(viper.ConfigFileAlreadyExistsError); !ok {
				log.Fatalf("error writing config file: %s", err.Error())
			}
		}
	} else {
		// Find home directory.
		home, e := os.UserHomeDir()
		cobra.CheckErr(e)

		// Search config in home directory with name ".gokv" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".gokv")
		if err = viper.SafeWriteConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileAlreadyExistsError); !ok {
				log.Fatalf("error writing config file: %s", err.Error())
			}
		}
	}

	// If a config file is found, read it in.
	if err = viper.ReadInConfig(); err == nil {
		_, err = fmt.Fprintln(
			os.Stderr,
			"Using config file:",
			viper.ConfigFileUsed(),
		)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	err = viper.Unmarshal(&cliOpts)
	if err != nil {
		log.Fatalln(err)
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
