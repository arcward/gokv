package main

import (
	"context"
	"github.com/arcward/gokv/cmd"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
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
		case sig := <-signals:
			log.Printf("received signal: %s", sig)
			cancel()
		}
	}()
	cmd.Execute(ctx)
}
