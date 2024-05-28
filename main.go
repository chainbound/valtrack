package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/chainbound/valtrack/discovery"
	"github.com/rs/zerolog"

	"github.com/urfave/cli/v2"
)

type Config struct {
	logLevel string
	natsURL  string
}

func main() {
	cfg := new(Config)

	app := &cli.App{
		Name:  "valtrack",
		Usage: "Ethereum consensus validator tracking tool",
		Commands: []*cli.Command{
			{
				Name:  "sentry",
				Usage: "run the sentry node",
				Action: func(*cli.Context) error {
					level, _ := zerolog.ParseLevel(cfg.logLevel)
					zerolog.SetGlobalLevel(level)

					runSentry(cfg.natsURL)
					return nil
				},
			},
			{
				Name:  "consumer",
				Usage: "run the consumer",
				Action: func(*cli.Context) error {
					level, _ := zerolog.ParseLevel(cfg.logLevel)
					zerolog.SetGlobalLevel(level)

					// TODO: implement consumer logic
					return nil
				},
			},
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "log-level",
				Usage:       "log level",
				Aliases:     []string{"l"},
				Value:       "info",
				Destination: &cfg.logLevel,
			},
			&cli.StringFlag{
				Name:        "nats",
				Usage:       "natsJS server url",
				Aliases:     []string{"n"},
				Value:       os.Getenv("NATS_URL"), // If nil, sentry will run without NATS
				Destination: &cfg.natsURL,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}

func runSentry(natsURL string) {
	disc, err := discovery.NewDiscovery(natsURL)
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
}
