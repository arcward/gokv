package cmd

import (
	"context"
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/spf13/cobra"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"
)

type setResult struct {
	Key    string
	Result *pb.SetResponse
}

var bulkSetCmd = &cobra.Command{
	Use:   "load",
	Short: "Loads key-value pairs from stdin, creates them",
	Args:  cobra.ExactArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		vals, err := readStdin()
		if err != nil {
			log.Fatalf("failed to read stdin: %s", err.Error())
		}
		opts := &cliOpts
		cfg := &cliOpts.clientOpts
		pending := make([]*pb.KeyValue, 0, len(vals))
		doneChannel := make(chan setResult)
		lockDuration := uint32(cfg.LockTimeout.Seconds())
		expires := uint32(cfg.ExpireKeyIn.Seconds())
		for _, v := range vals {
			if ctx.Err() != nil {
				log.Fatalf("cancelled: %s", ctx.Err().Error())
			}
			key, value, _ := strings.Cut(v, "=")
			pending = append(
				pending,
				&pb.KeyValue{
					Key:          key,
					Value:        []byte(value),
					Lock:         cfg.Lock,
					LockDuration: lockDuration,
					ExpireIn:     expires,
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
					if err != nil {
						log.Printf("failed to set key: %s", err.Error())
					}
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
			fmt.Printf("%s: %+v\n", result.Key, result.Result)
		}
		fmt.Printf("processed %d in %f secs\n", len(vals), secs)
		return nil
	},
}

func init() {
	clientCmd.AddCommand(bulkSetCmd)
	bulkSetCmd.Flags().DurationVar(
		&cliOpts.clientOpts.ExpireKeyIn,
		"expires",
		0,
		"Expire key in specified duration (e.g. 1h30m)",
	)
	bulkSetCmd.Flags().BoolVar(
		&cliOpts.clientOpts.Lock,
		"lock",
		false,
		"Lock the key",
	)
	bulkSetCmd.Flags().DurationVar(
		&cliOpts.clientOpts.LockTimeout,
		"lock-timeout",
		0,
		"Lock timeout",
	)

}
