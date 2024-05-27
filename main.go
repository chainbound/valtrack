package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/chainbound/valtrack/discovery"
	"github.com/rs/zerolog"
)

var (
	logLevel = flag.String("log-level", "info", "log level")
)

func main() {
	flag.Parse()

	disc, err := discovery.NewDiscovery()
	if err != nil {
		panic(err)
	}

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	level, _ := zerolog.ParseLevel(*logLevel)
	zerolog.SetGlobalLevel(level)

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
}
