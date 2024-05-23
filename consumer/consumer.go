package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/chainbound/valtrack/log"
	"github.com/chainbound/valtrack/pkg/ethereum"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
)

func main() {
	log := log.NewLogger("consumer")
	// Set up NATS connection
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		fmt.Printf("Error connecting to NATS: %v\n", err)
		return
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Printf("Error creating JetStream context: %v\n", err)
		return
	}

	// Set up a stream
	stream, err := js.Stream(context.Background(), "EVENTS")
	if err != nil {
		fmt.Printf("Error creating stream: %v\n", err)
		return
	}
	fmt.Println("Created the stream")

	// Set up a consumer
	consumerCfg := jetstream.ConsumerConfig{
		Durable:   "event-consumer",
		AckPolicy: jetstream.AckExplicitPolicy,
	}

	consumer, err := stream.CreateOrUpdateConsumer(context.Background(), consumerCfg)
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-sigChan:
				fmt.Println("Received termination signal, exiting...")
				return
			default:
				msgs, err := consumer.Fetch(10)
				if err != nil {
					fmt.Printf("Error fetching messages: %v\n", err)
					return
				}

				for msg := range msgs.Messages() {
					handleMessage(log, msg)
				}
			}
		}
	}()

	fmt.Println("Consumer is running... Press Ctrl+C to exit.")
	select {} // Run forever
}

func handleMessage(log zerolog.Logger, msg jetstream.Msg) {
	MsgMetadata, _ := msg.Metadata()
	switch msg.Subject() {
	case "events.peer_discovered":
		var event ethereum.PeerDiscoveredEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			fmt.Printf("Error unmarshaling PeerDiscoveredEvent: %v\n", err)
			msg.Term()
			return
		}
		log.Info().Any("Seq", MsgMetadata.Sequence).Any("event", event).Msg("peer_discovered")

	case "events.metadata_received":
		var event ethereum.MetadataReceivedEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			fmt.Printf("Error unmarshaling MetadataReceivedEvent: %v\n", err)
			msg.Term()
			return
		}
		log.Info().Any("Seq", MsgMetadata.Sequence).Any("event", event).Msg("metadata_received")

	default:
		fmt.Printf("Unknown event type: %s\n", msg.Subject())
	}

	if err := msg.Ack(); err != nil {
		fmt.Printf("Error acknowledging message: %v\n", err)
	}
}
