package ethereum

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/chainbound/valtrack/types"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

func createNatsStream(url string) (js jetstream.JetStream, err error) {
	// If empty URL and empty env variable, return nil and run without NATS
	if url == "" {
		if os.Getenv("NATS_URL") == "" {
			return nil, nil
		}
		url = os.Getenv("NATS_URL")
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

func (n *Node) sendMetadataEvent(ctx context.Context, event *types.MetadataReceivedEvent) {
	event.CrawlerID = getCrawlerMachineID()
	event.CrawlerLoc = getCrawlerLocation()

	json, _ := json.Marshal(event)
	n.log.Info().Msgf("Succesful handshake: %s", string(json))

	if n.js == nil {
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
	peerEvent := &types.PeerDiscoveredEvent{
		ENR:        node.String(),
		ID:         hInfo.ID.String(),
		IP:         hInfo.IP,
		Port:       hInfo.Port,
		CrawlerID:  getCrawlerMachineID(),
		CrawlerLoc: getCrawlerLocation(),
		Timestamp:  time.Now().UnixMilli(),
	}

	json, _ := json.Marshal(peerEvent)
	d.log.Info().Msgf("Discovered peer: %s", string(json))

	if d.js == nil {
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
