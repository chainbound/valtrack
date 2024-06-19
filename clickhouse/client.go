package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/chainbound/valtrack/log"
	"github.com/chainbound/valtrack/types"
	"github.com/rs/zerolog"
)

func ValidatorMetadataDDL(db string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.validator_metadata (
		enr String,
		id String,
		multiaddr String,
		epoch Int32,
		seq_number Int64,
		syncnets String,
		attnets String,
		long_lived_subnets Array(Int64),
		subscribed_subnets Array(Int64),
		client_version String,
		crawler_id String,
		crawler_location String,
		timestamp Int64,
	) ENGINE = MergeTree()
PRIMARY KEY (id, timestamp)`, db)
}

type ClickhouseConfig struct {
	Endpoint string
	DB       string
	Username string
	Password string

	MaxValidatorBatchSize uint64
}

type ClickhouseClient struct {
	cfg *ClickhouseConfig
	log zerolog.Logger

	chConn driver.Conn

	ValidatorEventChan chan *types.ValidatorEvent
}

func NewClickhouseClient(cfg *ClickhouseConfig) (*ClickhouseClient, error) {
	log := log.NewLogger("clickhouse")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{cfg.Endpoint},
		DialTimeout: time.Second * 60,
		Auth: clickhouse.Auth{
			Database: cfg.DB,
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

		ValidatorEventChan: make(chan *types.ValidatorEvent, 16384),
	}, nil
}

func (c *ClickhouseClient) Start() error {
	c.log.Info().Str("endpoint", c.cfg.Endpoint).Msg("Setting up Clickhouse database")
	if err := c.chConn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.cfg.DB)); err != nil {
		c.log.Error().Err(err).Msg("creating database")
		return err
	}
	c.log.Info().Str("db", c.cfg.DB).Msg("Database created")

	if err := c.chConn.Exec(context.Background(), ValidatorMetadataDDL(c.cfg.DB)); err != nil {
		c.log.Error().Err(err).Msg("creating validator_metadata table")
		return err
	}

	go c.validatorEventBatcher()

	return nil
}

func (c *ClickhouseClient) validatorEventBatcher() {
	var (
		err   error
		batch driver.Batch
	)

	count := uint64(0)

	for {
		batch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.validator_metadata", c.cfg.DB))
		if err != nil {
			c.log.Error().Err(err).Msg("preparing validator_metadata batch failed, retrying...")
		} else {
			break
		}
	}

	for row := range c.ValidatorEventChan {
		if err := batch.AppendStruct(row); err != nil {
			c.log.Error().Err(err).Msg("appending struct to validator_metadata batch")
		}

		count++
		c.log.Debug().Uint64("count", count).Msg("appended struct to validator_metadata batch")

		if count >= c.cfg.MaxValidatorBatchSize {
			// Reset counter
			count = 0

			start := time.Now()
			// Infinite retries for now
			for {
				if batch.IsSent() {
					break
				}

				if err := batch.Send(); err != nil {
					c.log.Error().Err(err).Msg("sending validator_metadata batch failed, retrying...")
				} else {
					break
				}
			}

			c.log.Info().Str("took", time.Since(start).String()).Int("channel_len", len(c.ValidatorEventChan)).Msg("Inserted validator_metadata batch")

			// Reset batch
			for {
				batch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.validator_metadata", c.cfg.DB))
				if err != nil {
					c.log.Error().Err(err).Msg("preparing validator_metadata batch (reset) failed, retrying")
				} else {
					break
				}
			}
		}
	}
}
