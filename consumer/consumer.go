package consumer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	}

	cctx, err := eventSourcingConsumer(consumer)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating consumer")
	}
	defer cctx.Stop()

	// Gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-quit
}

func eventSourcingConsumer(cons Consumer) (jetstream.ConsumeContext, error) {
	ctx := context.Background()

	uniqueID := uuid.New().String()

	// Set up a consumer
	consumerCfg := jetstream.ConsumerConfig{
		Name:        fmt.Sprintf("consumer-%s", uniqueID),
		Durable:     fmt.Sprintf("consumer-%s", uniqueID),
		Description: "Consumes valtrack events",
		AckPolicy:   jetstream.AckExplicitPolicy,
	}

	consumer, err := cons.js.CreateOrUpdateConsumer(ctx, "EVENTS", consumerCfg)
	if err != nil {
		return nil, err
	}

	return consumer.Consume(func(msg jetstream.Msg) {
		handleMessage(cons, msg)
	})
}

func handleMessage(cons Consumer, msg jetstream.Msg) {
	MsgMetadata, _ := msg.Metadata()
	switch msg.Subject() {
	case "events.peer_discovered":
		var event ethereum.PeerDiscoveredEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			cons.log.Err(err).Msg("Error unmarshaling PeerDiscoveredEvent")
			msg.Term()
			return
		}
		cons.log.Info().Any("seq", MsgMetadata.Sequence).Any("event", event).Msg("peer_discovered")
		storePeerDiscoveredEvent(cons.peerDiscoveredWriter, event, cons.log)

	case "events.metadata_received":
		var event ethereum.MetadataReceivedEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			cons.log.Err(err).Msg("Error unmarshaling MetadataReceivedEvent")
			msg.Term()
			return
		}
		cons.log.Info().Any("seq", MsgMetadata.Sequence).Any("event", event).Msg("metadata_received")
		storeValidatorEvent(cons.validatorWriter, event, cons.log)
		storeMetadataReceivedEvent(cons.metadataReceivedWriter, event, cons.log)

	default:
		cons.log.Warn().Str("subject", msg.Subject()).Msg("Unknown event type")
	}

	if err := msg.Ack(); err != nil {
		cons.log.Err(err).Msg("Error acknowledging message")
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

// func enodeFromPeerID(pid peer.ID) (enode.ID, error) {
// 	pubkey, err := pid.ExtractPublicKey()
// 	if err != nil {
// 		return enode.ID{}, errors.Wrap(err, "error extracting public key from peer ID")
// 	}

// 	// This encodes the public key in serialized, compressed secpk256k1 format
// 	comp, err := pubkey.Raw()
// 	if err != nil {
// 		return enode.ID{}, errors.Wrap(err, "error converting pubkey to raw bytes")
// 	}

// 	decomp, err := gcrypto.DecompressPubkey(comp)
// 	if err != nil {
// 		return enode.ID{}, errors.Wrap(err, "error decompressing pubkey")

// 	}

// 	return enode.PubkeyToIDV4(decomp), nil
// }

func storeValidatorEvent(pw *writer.ParquetWriter, event ethereum.MetadataReceivedEvent, log zerolog.Logger) {
	// pid, err := peer.Decode(event.ID)
	// if err != nil {
	// 	log.Err(err).Str("peer", event.ID).Msg("Error converting peer ID")
	// 	return
	// }

	// nodeID, err := enodeFromPeerID(pid)
	// if err != nil {
	// 	log.Err(err).Str("peer", event.ID).Msg("Error converting peer ID to enode ID")
	// 	return
	// }

	// Get the subscribed subnets from the metadata attnets

	// Get the long-lived subnets from epoch & nodeID
	// data, err := p2p.ComputeSubscribedSubnets(nodeID, primitives.Epoch(event.Epoch))
	// computedLongLived := convertUint64ToInt64(data)
	// if err != nil {
	// 	log.Err(err).Msg("Error computing subscribed subnets")
	// }

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

// func convertUint64ToInt64(uintSlice []uint64) []int64 {
// 	intSlice := make([]int64, len(uintSlice))
// 	for i, v := range uintSlice {
// 		intSlice[i] = int64(v)
// 	}
// 	return intSlice
// }
