package cmd

import (
	"context"
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/durationpb"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"
)

type setResult struct {
	Key    string
	Result *pb.SetResponse
	Error  error
}

var bulkSetCmd = &cobra.Command{
	Use:   "load",
	Short: "Loads key-value pairs from stdin, creates them",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		if len(args) == 0 {
			values, err := readStdin()
			if err != nil {
				printError(fmt.Errorf("failed to read from stdin: %w", err))
			}
			args = append(args, values...)
		}

		if len(args) == 0 {
			printError(fmt.Errorf("no values provided"))
		}

		opts := &cliOpts
		cfg := opts.clientOpts
		pending := make([]*pb.KeyValue, 0, len(args))
		doneChannel := make(chan setResult)

		var lockDuration *durationpb.Duration
		if cfg.LockTimeout > 0 {
			lockDuration = durationpb.New(cfg.LockTimeout)
		}

		var expireAfter *durationpb.Duration
		if cfg.KeyLifespan.Seconds() > 0 {
			expireAfter = durationpb.New(cfg.KeyLifespan)
		}

		for _, v := range args {
			if ctx.Err() != nil {
				printError(fmt.Errorf("cancelled: %w", ctx.Err()))
			}
			key, value, _ := strings.Cut(v, "=")
			pending = append(
				pending,
				&pb.KeyValue{
					Key:          key,
					Value:        []byte(value),
					LockDuration: lockDuration,
					Lifespan:     expireAfter,
				},
			)
		}

		workers := runtime.GOMAXPROCS(0)
		sendChannel := make(chan *pb.KeyValue)
		wg := sync.WaitGroup{}
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for pk := range sendChannel {
					if ctx.Err() != nil {
						return
					}
					rv := setResult{Key: pk.Key}
					res, err := opts.client.Set(ctx, pk)
					rv.Result = res
					rv.Error = err
					doneChannel <- rv
				}
			}()
		}

		start := time.Now()

		go func() {
			for _, k := range pending {
				if ctx.Err() != nil {
					return
				}
				k := k
				sendChannel <- k
			}
			close(sendChannel)
		}()

		var secs float64
		go func() {
			wg.Wait()
			close(doneChannel)
			secs = time.Since(start).Seconds()
		}()

		for result := range doneChannel {
			switch {
			case result.Error == nil:
				fmt.Printf("%s: %+v\n", result.Key, result.Result)
			default:
				fmt.Printf("error: %s: %s\n", result.Key, result.Error)
			}
		}
		defaultLogger.Info(
			"finished processing",
			slog.Int("processed", len(args)),
			slog.Float64("seconds", secs),
		)
		return nil
	},
}

func init() {
	clientCmd.AddCommand(bulkSetCmd)
	bulkSetCmd.Flags().DurationVar(
		&cliOpts.clientOpts.KeyLifespan,
		"lifespan",
		0,
		"Expire key in specified duration (e.g. 1h30m)",
	)
	bulkSetCmd.Flags().DurationVar(
		&cliOpts.clientOpts.LockTimeout,
		"lock",
		0,
		"Lock duration (ex: 15m)",
	)

}
