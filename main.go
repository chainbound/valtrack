package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/chainbound/valtrack/discovery"
)

func main() {
	disc, err := discovery.NewDiscovery()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	go func() {
		if err := disc.Start(ctx); err != nil {
			panic(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit

	cancel()
}
