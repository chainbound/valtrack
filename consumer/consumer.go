package consumer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chainbound/valtrack/log"
	"github.com/chainbound/valtrack/pkg/ethereum"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/rs/zerolog"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type Consumer struct {
	log                    zerolog.Logger
	peerDiscoveredWriter   *writer.ParquetWriter
	metadataReceivedWriter *writer.ParquetWriter
	validatorWriter        *writer.ParquetWriter
	js                     jetstream.JetStream

	peerDiscoveredChan   chan *ethereum.PeerDiscoveredEvent
	metadataReceivedChan chan *ethereum.MetadataReceivedEvent
}

type ParquetPeerDiscoveredEvent struct {
	ENR        string `parquet:"name=enr, type=BYTE_ARRAY, convertedtype=UTF8"`
	ID         string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
	IP         string `parquet:"name=ip, type=BYTE_ARRAY, convertedtype=UTF8"`
	Port       int    `parquet:"name=port, type=INT32"`
	CrawlerID  string `parquet:"name=crawler_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	CrawlerLoc string `parquet:"name=crawler_location, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp  int64  `parquet:"name=timestamp, type=INT64"`
}

type ParquetMetadataReceivedEvent struct {
	ENR           string         `parquet:"name=enr, type=BYTE_ARRAY, convertedtype=UTF8"`
	ID            string         `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Multiaddr     string         `parquet:"name=multiaddr, type=BYTE_ARRAY, convertedtype=UTF8"`
	Epoch         int            `parquet:"name=epoch, type=INT32"`
	MetaData      SimpleMetaData `parquet:"name=metadata, type=BYTE_ARRAY, convertedtype=UTF8"`
	ClientVersion string         `parquet:"name=client_version, type=BYTE_ARRAY, convertedtype=UTF8"`
	CrawlerID     string         `parquet:"name=crawler_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	CrawlerLoc    string         `parquet:"name=crawler_location, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp     int64          `parquet:"name=timestamp, type=INT64"`
}

type ParquetValidatorEvent struct {
	ENR               string         `parquet:"name=enr, type=BYTE_ARRAY, convertedtype=UTF8"`
	ID                string         `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Multiaddr         string         `parquet:"name=multiaddr, type=BYTE_ARRAY, convertedtype=UTF8"`
	Epoch             int            `parquet:"name=epoch, type=INT32"`
	MetaData          SimpleMetaData `parquet:"name=metadata, type=BYTE_ARRAY, convertedtype=UTF8"`
	LongLivedSubnets  []int64        `parquet:"name=long_lived_subnets, type=LIST, valuetype=INT64"`
	SubscribedSubnets []int64        `parquet:"name=subscribed_subnets, type=LIST, valuetype=INT64"`
	ClientVersion     string         `parquet:"name=client_version, type=BYTE_ARRAY, convertedtype=UTF8"`
	CrawlerID         string         `parquet:"name=crawler_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	CrawlerLoc        string         `parquet:"name=crawler_location, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp         int64          `parquet:"name=timestamp, type=INT64"`
}

type SimpleMetaData struct {
	SeqNumber int64  `parquet:"name=seq_number, type=INT64"`
	Attnets   string `parquet:"name=attnets, type=BYTE_ARRAY, convertedtype=UTF8"`
	Syncnets  string `parquet:"name=syncnets, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func RunConsumer(natsURL string) {
	log := log.NewLogger("consumer")

	nc, err := nats.Connect(natsURL)
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

	metadataReceivedWriter, err := writer.NewParquetWriter(w_metadata, new(ParquetMetadataReceivedEvent), 4)
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

	peerDiscoveredWriter, err := writer.NewParquetWriter(w_peer, new(ParquetPeerDiscoveredEvent), 4)
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

	validatorWriter, err := writer.NewParquetWriter(w_validator, new(ParquetValidatorEvent), 4)
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

	consumer := Consumer{
		log:                    log,
		peerDiscoveredWriter:   peerDiscoveredWriter,
		metadataReceivedWriter: metadataReceivedWriter,
		validatorWriter:        validatorWriter,
		js:                     js,

		peerDiscoveredChan:   make(chan *ethereum.PeerDiscoveredEvent, 1024),
		metadataReceivedChan: make(chan *ethereum.MetadataReceivedEvent, 1024),
	}

	go func() {
		if err := consumer.Start(); err != nil {
			log.Fatal().Err(err).Msg("Error in consumer")
		}
	}()

	// Gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-quit
}

func (c *Consumer) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uniqueID := uuid.New().String()
	c.log.Info().Str("unique_id", uniqueID).Msg("Starting consumer")

	// Set up a consumer
	consumerCfg := jetstream.ConsumerConfig{
		Name:        fmt.Sprintf("consumer-%s", uniqueID),
		Durable:     fmt.Sprintf("consumer-%s", uniqueID),
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
		var event ethereum.PeerDiscoveredEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			c.log.Err(err).Msg("Error unmarshaling PeerDiscoveredEvent")
			msg.Term()
			return
		}
		c.log.Info().Any("seq", MsgMetadata.Sequence).Any("event", event).Msg("peer_discovered")
		select {
		case c.peerDiscoveredChan <- &event:
			c.log.Debug().Msg("Sent peer_discovered event to channel")
		default:
		}
		storePeerDiscoveredEvent(c.peerDiscoveredWriter, event, c.log)

	case "events.metadata_received":
		var event ethereum.MetadataReceivedEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			c.log.Err(err).Msg("Error unmarshaling MetadataReceivedEvent")
			msg.Term()
			return
		}
		c.log.Info().Any("seq", MsgMetadata.Sequence).Any("event", event).Msg("metadata_received")
		select {
		case c.metadataReceivedChan <- &event:
			c.log.Debug().Msg("Sent metadata_received event to channel")
		default:
		}
		storeValidatorEvent(c.validatorWriter, event, c.log)
		storeMetadataReceivedEvent(c.metadataReceivedWriter, event, c.log)

	default:
		c.log.Warn().Str("subject", msg.Subject()).Msg("Unknown event type")
	}

	if err := msg.Ack(); err != nil {
		c.log.Err(err).Msg("Error acknowledging message")
	}
}

func indexesFromBitfield(bitV bitfield.Bitvector64) []int64 {
	indexes := make([]int64, 0, bitV.Len())

	for i := int64(0); i < 64; i++ {
		if bitV.BitAt(uint64(i)) {
			indexes = append(indexes, i)
		}
	}

	return indexes
}

// TODO: is this correct
func extractShortLivedSubnets(subscribed []int64, longLived []int64) []int64 {
	var shortLived []int64
	for i := 0; i < 64; i++ {
		if contains(subscribed, int64(i)) && !contains(longLived, int64(i)) {
			shortLived = append(shortLived, int64(i))
		}
	}

	return shortLived
}

func contains[T comparable](slice []T, item T) bool {
	for _, i := range slice {
		if i == item {
			return true
		}
	}
	return false
}

func storeValidatorEvent(pw *writer.ParquetWriter, event ethereum.MetadataReceivedEvent, log zerolog.Logger) {
	// Extract the long lived subnets from the metadata
	longLived := indexesFromBitfield(event.MetaData.Attnets)

	log.Info().Any("long_lived_subnets", longLived).Any("subscribed_subnets", event.SubscribedSubnets).Msg("Checking for validator")

	if len(extractShortLivedSubnets(event.SubscribedSubnets, longLived)) == 0 {
		// If the subscribed subnets and the longLived subnets are the same,
		// then there's probably no validator
		return
	}

	simpleMetaData := SimpleMetaData{
		SeqNumber: int64(event.MetaData.SeqNumber),
		Attnets:   hex.EncodeToString(event.MetaData.Attnets),
		Syncnets:  hex.EncodeToString(event.MetaData.Syncnets),
	}

	parquetEvent := ParquetValidatorEvent{
		ENR:               event.ENR,
		ID:                event.ID,
		Multiaddr:         event.Multiaddr,
		Epoch:             int(event.Epoch),
		MetaData:          simpleMetaData,
		ClientVersion:     event.ClientVersion,
		CrawlerID:         event.CrawlerID,
		CrawlerLoc:        event.CrawlerLoc,
		Timestamp:         event.Timestamp,
		LongLivedSubnets:  longLived,
		SubscribedSubnets: event.SubscribedSubnets,
	}

	if err := pw.Write(parquetEvent); err != nil {
		log.Err(err).Msg("Failed to write validator event to Parquet file")
	} else {
		log.Trace().Msg("Wrote validator event to Parquet file")
	}
}

func storePeerDiscoveredEvent(pw *writer.ParquetWriter, event ethereum.PeerDiscoveredEvent, log zerolog.Logger) {
	parquetEvent := ParquetPeerDiscoveredEvent{
		ENR:        event.ENR,
		ID:         event.ID,
		IP:         event.IP,
		Port:       int(event.Port),
		CrawlerID:  event.CrawlerID,
		CrawlerLoc: event.CrawlerLoc,
		Timestamp:  event.Timestamp,
	}

	if err := pw.Write(parquetEvent); err != nil {
		log.Err(err).Msg("Failed to write peer_discovered event to Parquet file")
	} else {
		log.Trace().Msg("Wrote peer_discovered event to Parquet file")
	}
}

func storeMetadataReceivedEvent(pw *writer.ParquetWriter, event ethereum.MetadataReceivedEvent, log zerolog.Logger) {
	simpleMetaData := SimpleMetaData{
		SeqNumber: int64(event.MetaData.SeqNumber),
		Attnets:   hex.EncodeToString(event.MetaData.Attnets),
		Syncnets:  hex.EncodeToString(event.MetaData.Syncnets),
	}

	parquetEvent := ParquetMetadataReceivedEvent{
		ID:            event.ID,
		Multiaddr:     event.Multiaddr,
		Epoch:         int(event.Epoch),
		MetaData:      simpleMetaData,
		ClientVersion: event.ClientVersion,
		CrawlerID:     event.CrawlerID,
		CrawlerLoc:    event.CrawlerLoc,
		Timestamp:     event.Timestamp,
	}

	if err := pw.Write(parquetEvent); err != nil {
		log.Err(err).Msg("Failed to write metadata_received event to Parquet file")
	} else {
		log.Trace().Msg("Wrote metadata_received event to Parquet file")
	}
}
