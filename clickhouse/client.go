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

	MaxPeerDiscoveredBatchSize   uint64
	MaxMetadataReceivedBatchSize uint64
}

type ClickhouseClient struct {
	cfg *ClickhouseConfig
	log zerolog.Logger

	chConn driver.Conn

	peerDiscovered   chan *ClickHousePeerDiscoveredEvent
	metadataReceived chan *ClickHouseMetadataReceivedEvent
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

	if err := c.chConn.Exec(context.Background(), PeerDiscoveredDDL(c.cfg.DB)); err != nil {
		return err
	}

	if err := c.chConn.Exec(context.Background(), MetadataReceivedDDL(c.cfg.DB)); err != nil {
		return err
	}

	go c.peerDiscoveredBatcher()
	go c.metadataReceivedBatcher()

	return nil
}

func (c *ClickhouseClient) peerDiscoveredBatcher() {
	var (
		err   error
		batch driver.Batch
	)

	count := uint64(0)

	for {
		batch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.peer_discovered", c.cfg.DB))
		if err != nil {
			c.log.Error().Err(err).Msg("preparing transaction_observations batch failed, retrying...")
		} else {
			break
		}
	}

	for row := range c.peerDiscovered {
		if err := batch.AppendStruct(row); err != nil {
			c.log.Error().Err(err).Msg("appending struct to peer discovered batch")
		}

		count++

		if count >= c.cfg.MaxPeerDiscoveredBatchSize {
			// Reset counter
			count = 0

			start := time.Now()
			// Infinite retries for now
			for {
				if batch.IsSent() {
					break
				}

				if err := batch.Send(); err != nil {
					c.log.Error().Err(err).Msg("sending peer discovered batch failed, retrying...")
				} else {
					break
				}
			}

			c.log.Debug().Str("took", time.Since(start).String()).Int("channel_len", len(c.peerDiscovered)).Msg("Inserted peer discovered batch")

			// Reset batch
			for {
				batch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.peer_discovered", c.cfg.DB))
				if err != nil {
					c.log.Error().Err(err).Msg("preparing peer_discovered batch (reset) failed, retrying")
				} else {
					break
				}
			}
		}
	}
}

func (c *ClickhouseClient) metadataReceivedBatcher() {
	var (
		err   error
		batch driver.Batch
	)

	count := uint64(0)

	for {
		batch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.metadata_received", c.cfg.DB))
		if err != nil {
			c.log.Error().Err(err).Msg("preparing metadata_received batch failed, retrying...")
		} else {
			break
		}
	}

	for row := range c.metadataReceived {
		if err := batch.AppendStruct(row); err != nil {
			c.log.Error().Err(err).Msg("appending struct to metadata_received batch")
		}

		count++

		if count >= c.cfg.MaxMetadataReceivedBatchSize {
			// Reset counter
			count = 0

			start := time.Now()
			// Infinite retries for now
			for {
				if batch.IsSent() {
					break
				}

				if err := batch.Send(); err != nil {
					c.log.Error().Err(err).Msg("sending metadata_received batch failed, retrying...")
				} else {
					break
				}
			}

			c.log.Debug().Str("took", time.Since(start).String()).Int("channel_len", len(c.metadataReceived)).Msg("Inserted metadata_received batch")

			// Reset batch
			for {
				batch, err = c.chConn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s.metadata_received", c.cfg.DB))
				if err != nil {
					c.log.Error().Err(err).Msg("preparing metadata_received batch (reset) failed, retrying")
				} else {
					break
				}
			}
		}
	}
}
