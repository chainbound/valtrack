package ethereum

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
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

func (n *Node) sendMetadataEvent(ctx context.Context, event *MetadataReceivedEvent) {
	event.CrawlerID = getCrawlerMachineID()
	event.CrawlerLoc = getCrawlerLocation()

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
