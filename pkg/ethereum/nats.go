package ethereum

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	eth "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

type PeerDiscoveredEvent struct {
	ENR        string `json:"enr"`
	ID         string `json:"id"`
	IP         string `json:"ip"`
	Port       int    `json:"port"`
	CrawlerID  string `json:"crawler_id"`
	CrawlerLoc string `json:"crawler_location"`
	Timestamp  int64  `json:"timestamp"`
}

type MetadataReceivedEvent struct {
	ENR               string          `json:"enr"`
	ID                string          `json:"id"`
	Multiaddr         string          `json:"multiaddr"`
	Epoch             int             `json:"epoch"`
	MetaData          *eth.MetaDataV1 `json:"metadata"`
	SubscribedSubnets []int64         `json:"subscribed_subnets"`
	ClientVersion     string          `json:"client_version"`
	CrawlerID         string          `json:"crawler_id"`
	CrawlerLoc        string          `json:"crawler_location"`
	Timestamp         int64           `json:"timestamp"` // Timestamp in UNIX milliseconds
}

func createNatsStream(url string) (js jetstream.JetStream, err error) {
	// If empty URL, return nil and run without NATS
	if url == "" {
		return nil, nil
	}
	// Initialize NATS JetStream
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to connect to NATS")
	}

	js, err = jetstream.New(nc)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create JetStream context")
	}

	cfgjs := jetstream.StreamConfig{
		Name:      "EVENTS",
		Retention: jetstream.InterestPolicy,
		Subjects:  []string{"events.metadata_received", "events.peer_discovered"},
	}

	ctxJs := context.Background()

	_, err = js.CreateOrUpdateStream(ctxJs, cfgjs)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create JetStream stream")
	}
	return js, nil
}

func (n *Node) sendMetadataEvent(ctx context.Context, event *MetadataReceivedEvent) {
	event.CrawlerID = getCrawlerMachineID()
	event.CrawlerLoc = getCrawlerLocation()

	n.log.Info().Any("metadata_event", event).Msg("metadata_received event")

	if n.js == nil {
		json, err := json.Marshal(event)
		if err != nil {
			n.log.Error().Err(err).Msg("Failed to marshal metadata_received event")
			return
		}
		fmt.Fprintln(n.fileLogger, string(json))
		return
	}

	select {
	case n.metadataEventChan <- event:
		n.log.Trace().Str("peer", event.ID).Msg("Sent metadata_received event to channel")
	case <-ctx.Done():
		n.log.Warn().Msg("Context cancelled before sending metadata_received event to channel")
	}
}

func (n *Node) startMetadataPublisher() {
	go func() {
		for metadataEvent := range n.metadataEventChan {
			publishCtx, publishCancel := context.WithTimeout(context.Background(), 3*time.Second)

			eventData, err := json.Marshal(metadataEvent)
			if err != nil {
				n.log.Error().Err(err).Msg("Failed to marshal metadata_received event")
				publishCancel()
				continue
			}

			ack, err := n.js.Publish(publishCtx, "events.metadata_received", eventData)
			if err != nil {
				n.log.Error().Err(err).Msg("Failed to publish metadata_received event")
				publishCancel()
				continue
			}
			n.log.Debug().Msgf("Published metadata_received event with seq: %v", ack.Sequence)
			publishCancel()
		}
	}()
}

func (d *DiscoveryV5) sendPeerEvent(ctx context.Context, node *enode.Node, hInfo *HostInfo) {
	peerEvent := &PeerDiscoveredEvent{
		ENR:        node.String(),
		ID:         hInfo.ID.String(),
		IP:         hInfo.IP,
		Port:       hInfo.Port,
		CrawlerID:  getCrawlerMachineID(),
		CrawlerLoc: getCrawlerLocation(),
		Timestamp:  time.Now().UnixMilli(),
	}

	d.log.Info().Any("peer_event", peerEvent).Msg("peer_discovered event")

	if d.js == nil {
		json, err := json.Marshal(peerEvent)
		if err != nil {
			d.log.Error().Err(err).Msg("Failed to marshal peer_discovered event")
			return
		}
		fmt.Fprintln(d.fileLogger, string(json))
		return
	}

	select {
	case d.discEventChan <- peerEvent:
		d.log.Debug().Str("peer", node.ID().String()).Msg("Sent peer_discovered event to channel")
	case <-ctx.Done():
		d.log.Warn().Msg("Context cancelled before sending peer_discovered event to channel")
	}
}

func (disc *DiscoveryV5) startDiscoveryPublisher() {
	go func() {
		for discoveryEvent := range disc.discEventChan {
			publishCtx, publishCancel := context.WithTimeout(context.Background(), 3*time.Second)

			eventData, err := json.Marshal(discoveryEvent)
			if err != nil {
				disc.log.Error().Err(err).Msg("Failed to marshal peer_discovered event")
				publishCancel()
				continue
			}

			ack, err := disc.js.Publish(publishCtx, "events.peer_discovered", eventData)
			if err != nil {
				disc.log.Error().Err(err).Msg("Failed to publish peer_discovered event")
				publishCancel()
				continue
			}
			disc.log.Debug().Msgf("Published peer_discovered event with seq: %v", ack.Sequence)
			publishCancel()
		}
	}()
}
