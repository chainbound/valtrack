package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/chainbound/valtrack/log"
	"github.com/rs/zerolog"
)

type ClickhouseConfig struct {
	Endpoint string
	DB       string
	Username string
	Password string
}

type ClickhouseClient struct {
	cfg *ClickhouseConfig
	log zerolog.Logger

	chConn driver.Conn
}

func NewClickhouseClient(cfg *ClickhouseConfig) (*ClickhouseClient, error) {
	log := log.NewLogger("clickhouse")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{cfg.Endpoint},
		DialTimeout: time.Second * 60,
		Auth: clickhouse.Auth{
			Database: "default",
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Debugf: func(format string, v ...interface{}) {
			log.Debug().Str("module", "clickhouse").Msgf(format, v)
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	})

	if err != nil {
		return nil, err
	}

	return &ClickhouseClient{
		cfg:    cfg,
		log:    log,
		chConn: conn,
	}, nil
}

func (c *ClickhouseClient) Start() error {
	c.log.Info().Str("endpoint", c.cfg.Endpoint).Msg("Setting up Clickhouse database")
	if err := c.chConn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.cfg.DB)); err != nil {
		return err
	}
	c.log.Info().Str("db", c.cfg.DB).Msg("Database created")

	return nil
}
