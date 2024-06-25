package consumer

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	ch "github.com/chainbound/valtrack/clickhouse"
	"github.com/chainbound/valtrack/log"
	"github.com/chainbound/valtrack/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

const BATCH_SIZE = 1024

type ConsumerConfig struct {
	LogLevel string
	NatsURL  string
	Name     string
	ChCfg    ch.ClickhouseConfig
}

type Consumer struct {
	log             zerolog.Logger
	discoveryWriter *writer.ParquetWriter
	metadataWriter  *writer.ParquetWriter
	validatorWriter *writer.ParquetWriter
	js              jetstream.JetStream

	validatorMetadataChan chan *types.MetadataReceivedEvent

	chClient *ch.ClickhouseClient
	db       *sql.DB
}

func RunConsumer(cfg *ConsumerConfig) {
	// Set up logging
	log := log.NewLogger("consumer")

	// Set up the sqlite database
	db, err := sql.Open("sqlite3", "./validator_tracker.sqlite")
	if err != nil {
		log.Error().Err(err).Msg("Error opening database")
	}
	defer db.Close()

	err = setupDatabase(db)
	if err != nil {
		log.Error().Err(err).Msg("Error setting up database")
	}

	err = loadIPMetadataFromCSV(db, "ip_metadata.csv")
	if err != nil {
		log.Error().Err(err).Msg("Error setting up database")
	}

	log.Info().Msg("Sqlite DB setup complete")

	// Set up NATS
	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		log.Error().Err(err).Msg("Error connecting to NATS")
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Error().Err(err).Msg("Error creating JetStream context")
	}

	// Create Parquet writer files
	w_discovery, err := local.NewLocalFileWriter("discovery_events.parquet")
	if err != nil {
		log.Error().Err(err).Msg("Error creating discovery events parquet file")
	}
	defer w_discovery.Close()

	w_metadata, err := local.NewLocalFileWriter("metadata_events.parquet")
	if err != nil {
		log.Error().Err(err).Msg("Error creating metadata events parquet file")
	}
	defer w_metadata.Close()

	w_validator, err := local.NewLocalFileWriter("validator_metadata_events.parquet")
	if err != nil {
		log.Error().Err(err).Msg("Error creating validator parquet file")
	}
	defer w_validator.Close()

	// Set up Parquet writers
	discoveryWriter, err := writer.NewParquetWriter(w_discovery, new(types.PeerDiscoveredEvent), 4)
	if err != nil {
		log.Error().Err(err).Msg("Error creating Peer discovered Parquet writer")
	}
	defer func() {
		discoveryWriter.WriteStop()
		log.Info().Msg("Stopped Discovery Parquet writer")
	}()

	metadataWriter, err := writer.NewParquetWriter(w_metadata, new(types.MetadataReceivedEvent), 4)
	if err != nil {
		log.Error().Err(err).Msg("Error creating Metadata Parquet writer")
	}
	defer func() {
		metadataWriter.WriteStop()
		log.Info().Msg("Stopped Metadata Parquet writer")
	}()

	validatorWriter, err := writer.NewParquetWriter(w_validator, new(types.ValidatorEvent), 4)
	if err != nil {
		log.Error().Err(err).Msg("Error creating Validator Parquet writer")
	}
	defer func() {
		validatorWriter.WriteStop()
		log.Info().Msg("Stopped Validator Parquet writer")
	}()

	// Set up Clickhouse client
	chCfg := ch.ClickhouseConfig{
		Endpoint: cfg.ChCfg.Endpoint,
		DB:       cfg.ChCfg.DB,
		Username: cfg.ChCfg.Username,
		Password: cfg.ChCfg.Password,

		MaxValidatorBatchSize: cfg.ChCfg.MaxValidatorBatchSize,
	}

	var chClient *ch.ClickhouseClient
	if chCfg.Endpoint != "" {
		chClient, err = ch.NewClickhouseClient(&chCfg)
		if err != nil {
			log.Error().Err(err).Msg("Error creating Clickhouse client")
		}

		err = chClient.Start()
		if err != nil {
			log.Error().Err(err).Msg("Error starting Clickhouse client")
		}
	}

	consumer := Consumer{
		log:             log,
		discoveryWriter: discoveryWriter,
		metadataWriter:  metadataWriter,
		validatorWriter: validatorWriter,
		js:              js,

		validatorMetadataChan: make(chan *types.MetadataReceivedEvent, 16384),

		chClient: chClient,
		db:       db,
	}

	// Start the consumer
	go func() {
		if err := consumer.Start(cfg.Name); err != nil {
			log.Error().Err(err).Msg("Error in consumer")
		}
	}()

	ipInfoToken := os.Getenv("IPINFO_TOKEN")
	if ipInfoToken == "" {
		log.Error().Msg("IPINFO_TOKEN environment variable is required")
	}

	go consumer.runValidatorMetadataEventHandler(ipInfoToken)

	// Set up HTTP server
	server := &http.Server{Addr: ":8080", Handler: nil}
	http.HandleFunc("/validators", createGetValidatorsHandler(db))

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Error starting HTTP server")
		}
	}()
	defer func() {
		server.Shutdown(context.Background())
	}()

	// Start publishing to Dune periodically
	log.Info().Msg("Starting to publish to Dune")
	go func() {
		if err := consumer.publishToDune(); err != nil {
			log.Error().Err(err).Msg("Error publishing to Dune")
		}

		ticker := time.NewTicker(24 * time.Hour) // Adjust the publishing time interval
		defer ticker.Stop()

		for range ticker.C {
			if err := consumer.publishToDune(); err != nil {
				log.Error().Err(err).Msg("Error publishing to Dune")
			}
			log.Info().Msg("Published to Dune")
		}
	}()

	// Gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-quit
}

func (c *Consumer) Start(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Set up a consumer
	consumerCfg := jetstream.ConsumerConfig{
		Name:        name,
		Durable:     name,
		Description: "Consumes valtrack events",
		AckPolicy:   jetstream.AckExplicitPolicy,
	}

	// TODO: Change the stream name to 'valtrack'
	stream, err := c.js.Stream(ctx, "EVENTS")
	if err != nil {
		c.log.Error().Err(err).Msg("Error opening valtrack jetstream")
		return err
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		c.log.Error().Err(err).Msg("Error creating consumer")
		return err
	}

	go func() {
		for {
			batch, err := consumer.FetchNoWait(BATCH_SIZE)
			if err != nil {
				c.log.Error().Err(err).Msg("Error fetching batch of messages")
				return
			}
			if err = batch.Error(); err != nil {
				c.log.Error().Err(err).Msg("Error in messages batch")
				return
			}

			for msg := range batch.Messages() {
				handleMessage(c, msg)
			}

		}
	}()

	return nil
}

func handleMessage(c *Consumer, msg jetstream.Msg) {
	md, _ := msg.Metadata()
	progress := float64(md.Sequence.Stream) / (float64(md.NumPending) + float64(md.Sequence.Stream)) * 100

	switch msg.Subject() {
	case "events.peer_discovered":
		var event types.PeerDiscoveredEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			c.log.Err(err).Msg("Error unmarshaling PeerDiscoveredEvent")
			msg.Term()
			return
		}

		c.log.Info().Time("timestamp", md.Timestamp).Uint64("pending", md.NumPending).Str("progress", fmt.Sprintf("%.2f%%", progress)).Msg("peer_discovered")
		c.storeDiscoveryEvent(event)

	case "events.metadata_received":
		var event types.MetadataReceivedEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			c.log.Err(err).Msg("Error unmarshaling MetadataReceivedEvent")
			msg.Term()
			return
		}

		c.log.Info().Time("timestamp", md.Timestamp).Uint64("pending", md.NumPending).Str("progress", fmt.Sprintf("%.2f%%", progress)).Msg("metadata_received")
		c.handleMetadataEvent(event)
		c.storeMetadataEvent(event)

	default:
		c.log.Warn().Str("subject", msg.Subject()).Msg("Unknown event type")
	}

	if err := msg.Ack(); err != nil {
		c.log.Err(err).Msg("Error acknowledging message")
	}
}

func (c *Consumer) handleMetadataEvent(event types.MetadataReceivedEvent) {
	// Extract the long lived subnets from the metadata
	longLived := indexesFromBitfield(event.MetaData.Attnets)

	c.log.Info().Str("peer", event.ID).Any("long_lived_subnets", longLived).Any("subscribed_subnets", event.SubscribedSubnets).Msg("Checking for validator")

	if len(extractShortLivedSubnets(event.SubscribedSubnets, longLived)) == 0 || len(longLived) != 2 {
		// If the subscribed subnets and the longLived subnets are the same,
		// then there's probably no validator OR
		// If the longLived subnets are not equal to 2
		return
	}

	c.validatorMetadataChan <- &event

	validatorEvent := types.ValidatorEvent{
		ENR:               event.ENR,
		ID:                event.ID,
		Multiaddr:         event.Multiaddr,
		Epoch:             event.Epoch,
		SeqNumber:         event.MetaData.SeqNumber,
		Attnets:           hex.EncodeToString(event.MetaData.Attnets),
		Syncnets:          hex.EncodeToString(event.MetaData.Syncnets),
		ClientVersion:     event.ClientVersion,
		CrawlerID:         event.CrawlerID,
		CrawlerLoc:        event.CrawlerLoc,
		Timestamp:         event.Timestamp,
		LongLivedSubnets:  longLived,
		SubscribedSubnets: event.SubscribedSubnets,
	}

	if c.chClient != nil {
		c.chClient.ValidatorEventChan <- &validatorEvent
		c.log.Info().Any("validator_event", validatorEvent).Msg("Inserted validator event")
	}

	if err := c.validatorWriter.Write(validatorEvent); err != nil {
		c.log.Err(err).Msg("Failed to write validator event to Parquet file")
	} else {
		c.log.Trace().Msg("Wrote validator event to Parquet file")
	}
}

func (c *Consumer) storeDiscoveryEvent(event types.PeerDiscoveredEvent) {
	if err := c.discoveryWriter.Write(event); err != nil {
		c.log.Err(err).Msg("Failed to write discovery event to Parquet file")
	} else {
		c.log.Trace().Msg("Wrote discovery event to Parquet file")
	}
}

func (c *Consumer) storeMetadataEvent(event types.MetadataReceivedEvent) {
	if err := c.metadataWriter.Write(event); err != nil {
		c.log.Err(err).Msg("Failed to write metadata event to Parquet file")
	} else {
		c.log.Trace().Msg("Wrote metadata event to Parquet file")
	}
}
