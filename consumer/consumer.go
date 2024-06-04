package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/chainbound/valtrack/clickhouse"
	ch "github.com/chainbound/valtrack/clickhouse"
	"github.com/chainbound/valtrack/log"
	"github.com/chainbound/valtrack/types"
	_ "github.com/mattn/go-sqlite3"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type ConsumerConfig struct {
	LogLevel string
	NatsURL  string
	Name     string
	ChCfg    clickhouse.ClickhouseConfig
}

type Consumer struct {
	log                    zerolog.Logger
	peerDiscoveredWriter   *writer.ParquetWriter
	metadataReceivedWriter *writer.ParquetWriter
	validatorWriter        *writer.ParquetWriter
	js                     jetstream.JetStream

	validatorMetadataChan chan *types.MetadataReceivedEvent

	chClient *ch.ClickhouseClient
	db       *sql.DB
}

func setupDatabase(db *sql.DB) error {
	createTableQuery := `
    CREATE TABLE IF NOT EXISTS validator_tracker (
        PeerID TEXT PRIMARY KEY,
        ENR TEXT,
        Multiaddr TEXT,
        IP TEXT,
        Port INTEGER,
        LastSeen TEXT,
        LastEpoch INTEGER,
        PossibleValidator BOOLEAN,
        AverageValidatorCount INTEGER,
        NumObservations INTEGER
    );
    `
	_, err := db.Exec(createTableQuery)
	if err != nil {
		return err
	}
	return nil
}

func RunConsumer(cfg *ConsumerConfig) {
	log := log.NewLogger("consumer")

	db, err := sql.Open("sqlite3", "./validator_tracker.sqlite")
	if err != nil {
		log.Fatal().Err(err).Msg("Error opening database")
	}
	defer db.Close()

	err = setupDatabase(db)
	if err != nil {
		log.Fatal().Err(err).Msg("Error setting up database")
	}
	log.Info().Msg("Sqlite Database setup complete")

	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Error connecting to NATS")
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating JetStream context")
	}

	w_peer, err := local.NewLocalFileWriter("discovery_events.parquet")
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating peer_discovered parquet file")
	}
	defer w_peer.Close()

	w_metadata, err := local.NewLocalFileWriter("metadata_events.parquet")
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating metadata_received parquet file")
	}
	defer w_metadata.Close()

	w_validator, err := local.NewLocalFileWriter("validator_metadata_events.parquet")
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating validator parquet file")
	}
	defer w_validator.Close()

	metadataReceivedWriter, err := writer.NewParquetWriter(w_metadata, new(types.MetadataReceivedEvent), 4)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating Metadata Parquet writer")
	}
	defer func() {
		if err := metadataReceivedWriter.WriteStop(); err != nil {
			fmt.Printf("Error stopping Metadata Parquet writer: %v\n", err)
		} else {
			fmt.Println("Stopped Metadata Parquet writer")
		}
	}()

	peerDiscoveredWriter, err := writer.NewParquetWriter(w_peer, new(types.PeerDiscoveredEvent), 4)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating Peer discovered Parquet writer")
	}
	defer func() {
		if err := peerDiscoveredWriter.WriteStop(); err != nil {
			fmt.Printf("Error stopping Peer discovered Parquet writer: %v\n", err)
		} else {
			fmt.Println("Stopped Peer discovered Parquet writer")
		}
	}()

	validatorWriter, err := writer.NewParquetWriter(w_validator, new(types.ValidatorEvent), 4)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating Validator Parquet writer")
	}
	defer func() {
		if err := validatorWriter.WriteStop(); err != nil {
			fmt.Printf("Error stopping Validator Parquet writer: %v\n", err)
		} else {
			fmt.Println("Stopped Validator Parquet writer")
		}
	}()

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
			log.Fatal().Err(err).Msg("Error creating Clickhouse client")
		}

		err = chClient.Start()
		if err != nil {
			log.Fatal().Err(err).Msg("Error starting Clickhouse client")
		}
	}

	consumer := Consumer{
		log:                    log,
		peerDiscoveredWriter:   peerDiscoveredWriter,
		metadataReceivedWriter: metadataReceivedWriter,
		validatorWriter:        validatorWriter,
		js:                     js,

		validatorMetadataChan: make(chan *types.MetadataReceivedEvent, 16384),

		chClient: chClient,
		db:       db,
	}

	go func() {
		if err := consumer.Start(cfg.Name); err != nil {
			log.Fatal().Err(err).Msg("Error in consumer")
		}
	}()

	go consumer.HandleValidatorMetadataEvent()

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
			select {
			default:
				batch, err := consumer.FetchNoWait(100)
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
		}
	}()

	return nil
}

func handleMessage(c *Consumer, msg jetstream.Msg) {
	MsgMetadata, _ := msg.Metadata()
	switch msg.Subject() {
	case "events.peer_discovered":
		var event types.PeerDiscoveredEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			c.log.Err(err).Msg("Error unmarshaling PeerDiscoveredEvent")
			msg.Term()
			return
		}
		c.log.Info().Any("seq", MsgMetadata.Sequence).Any("event", event).Msg("peer_discovered")
		c.storePeerDiscoveredEvent(event)

	case "events.metadata_received":
		var event types.MetadataReceivedEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			c.log.Err(err).Msg("Error unmarshaling MetadataReceivedEvent")
			msg.Term()
			return
		}
		c.log.Info().Any("seq", MsgMetadata.Sequence).Any("event", event).Msg("metadata_received")
		c.validatorMetadataChan <- &event
		c.storeValidatorEvent(event)
		c.storeMetadataReceivedEvent(event)

	default:
		c.log.Warn().Str("subject", msg.Subject()).Msg("Unknown event type")
	}

	if err := msg.Ack(); err != nil {
		c.log.Err(err).Msg("Error acknowledging message")
	}
}

func (c *Consumer) storeValidatorEvent(event types.MetadataReceivedEvent) {
	// Extract the long lived subnets from the metadata
	longLived := indexesFromBitfield(event.MetaData.Attnets)

	c.log.Info().Any("long_lived_subnets", longLived).Any("subscribed_subnets", event.SubscribedSubnets).Msg("Checking for validator")

	if len(extractShortLivedSubnets(event.SubscribedSubnets, longLived)) == 0 {
		// If the subscribed subnets and the longLived subnets are the same,
		// then there's probably no validator
		return
	}

	validatorEvent := types.ValidatorEvent{
		ENR:       event.ENR,
		ID:        event.ID,
		Multiaddr: event.Multiaddr,
		Epoch:     event.Epoch,
		// MetaData:          event.MetaData,
		SeqNumber:         event.MetaData.SeqNumber,
		Attnets:           event.MetaData.Attnets,
		Syncnets:          event.MetaData.Syncnets,
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

func (c *Consumer) storePeerDiscoveredEvent(event types.PeerDiscoveredEvent) {
	if err := c.peerDiscoveredWriter.Write(event); err != nil {
		c.log.Err(err).Msg("Failed to write peer_discovered event to Parquet file")
	} else {
		c.log.Trace().Msg("Wrote peer_discovered event to Parquet file")
	}
}

func (c *Consumer) storeMetadataReceivedEvent(event types.MetadataReceivedEvent) {
	if err := c.metadataReceivedWriter.Write(event); err != nil {
		c.log.Err(err).Msg("Failed to write metadata_received event to Parquet file")
	} else {
		c.log.Trace().Msg("Wrote metadata_received event to Parquet file")
	}
}

func (c *Consumer) HandleValidatorMetadataEvent() error {
	for {
		select {
		case event := <-c.validatorMetadataChan:
			c.log.Trace().Any("event", event).Msg("Received validator event")

			maddr, err := ma.NewMultiaddr(event.Multiaddr)
			if err != nil {
				c.log.Error().Err(err).Msg("Invalid multiaddr")
				continue
			}

			ip, err := maddr.ValueForProtocol(ma.P_IP4)
			if err != nil {
				ip, err = maddr.ValueForProtocol(ma.P_IP6)
				if err != nil {
					c.log.Error().Err(err).Msg("Invalid IP in multiaddr")
					continue
				}
			}

			portStr, err := maddr.ValueForProtocol(ma.P_TCP)
			if err != nil {
				c.log.Error().Err(err).Msg("Invalid port in multiaddr")
				continue
			}

			port, err := strconv.Atoi(portStr)
			if err != nil {
				c.log.Error().Err(err).Msg("Invalid port number")
				continue
			}

			isValidator := true
			longLived := indexesFromBitfield(event.MetaData.Attnets)
			shortLived := extractShortLivedSubnets(event.SubscribedSubnets, longLived)
			// If there are no short lived subnets, then the peer is not a validator
			if len(shortLived) == 0 {
				isValidator = false
			}

			prevNumObservations := uint64(0)
			prevAvgValidatorCount := int32(0)
			err = c.db.QueryRow("SELECT NumObservations, AverageValidatorCount FROM validator_tracker WHERE PeerID = ?", event.ID).Scan(&prevNumObservations, &prevAvgValidatorCount)

			currValidatorCount := 1 + (len(shortLived)-1)/2
			// If there are no short lived subnets, then the validator count is 0
			if len(shortLived) == 0 {
				currValidatorCount = 0
			}
			currAvgValidatorCount := ComputeNewAvg(prevAvgValidatorCount, prevNumObservations, currValidatorCount)

			if err == sql.ErrNoRows {
				// Insert new row
				insertQuery := `
				INSERT INTO validator_tracker (PeerID, ENR, Multiaddr, IP, Port, LastSeen, LastEpoch, PossibleValidator, AverageValidatorCount, NumObservations)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				`
				_, err = c.db.Exec(insertQuery, event.ID, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, isValidator, currAvgValidatorCount, prevNumObservations+1)
				if err != nil {
					c.log.Error().Err(err).Msg("Error inserting row")
				}
				c.log.Trace().Str("PeerID", event.ID).Msg("Inserted new row")
			} else if err != nil {
				c.log.Error().Err(err).Msg("Error querying database")
			} else {
				// Update existing row
				updateQuery := `
				UPDATE validator_tracker
				SET ENR = ?, Multiaddr = ?, IP = ?, Port = ?, LastSeen = ?, LastEpoch = ?, PossibleValidator = ?, AverageValidatorCount = ?, NumObservations = ?
				WHERE PeerID = ?
				`
				_, err = c.db.Exec(updateQuery, event.ENR, event.Multiaddr, ip, port, event.Timestamp, event.Epoch, isValidator, currAvgValidatorCount, prevNumObservations+1, event.ID)
				if err != nil {
					c.log.Error().Err(err).Msg("Error updating row")
				}
				c.log.Trace().Str("PeerID", event.ID).Msg("Updated row")
			}
		default:
			c.log.Debug().Msg("No validator metadata event")
			time.Sleep(1 * time.Second) // Prevents busy waiting
		}
	}
}
