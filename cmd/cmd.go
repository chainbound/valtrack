package cmd

import (
	"github.com/chainbound/valtrack/clickhouse"
	"github.com/chainbound/valtrack/consumer"

	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

var ConsumerCommand = &cli.Command{
	Name:   "consumer",
	Usage:  "run the consumer",
	Action: LaunchConsumer,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "log-level",
			Usage:   "Log level",
			Aliases: []string{"l"},
			Value:   "info",
		},
		&cli.StringFlag{
			Name:    "nats-url",
			Usage:   "NATS server URL (needs JetStream)",
			Aliases: []string{"n"},
			Value:   "nats://localhost:4222",
		},
		&cli.StringFlag{
			Name:  "name",
			Usage: "Consumer name",
			Value: "valtrack-consumer",
		},
		&cli.StringFlag{
			Name:  "endpoint",
			Usage: "Clickhouse server endpoint",
			Value: "", // If empty URL, run the consumer without Clickhouse
		},
		&cli.StringFlag{
			Name:  "db",
			Usage: "Clickhouse database name",
			Value: "default",
		},
		&cli.StringFlag{
			Name:  "username",
			Usage: "Clickhouse username",
			Value: "default",
		},
		&cli.StringFlag{
			Name:  "password",
			Usage: "Clickhouse password",
			Value: "",
		},
		&cli.Uint64Flag{
			Name:  "batch-size",
			Usage: "Clickhouse max validator batch size",
			Value: 128,
		},
	},
}

func LaunchConsumer(c *cli.Context) error {
	cfg := consumer.ConsumerConfig{
		LogLevel: c.String("log-level"),
		NatsURL:  c.String("nats-url"),
		Name:     c.String("name"),
		ChCfg: clickhouse.ClickhouseConfig{
			Endpoint:              c.String("endpoint"),
			DB:                    c.String("db"),
			Username:              c.String("username"),
			Password:              c.String("password"),
			MaxValidatorBatchSize: c.Uint64("batch-size"),
		},
	}

	level, _ := zerolog.ParseLevel(cfg.LogLevel)
	zerolog.SetGlobalLevel(level)

	consumer.RunConsumer(&cfg)
	return nil
}
