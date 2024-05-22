package ethereum

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

var _ network.Notifiee = (*Node)(nil)

type MetadataReceivedEvent struct {
	ENR        string         `json:"enr"`
	IP         string         `json:"ip"`
	Port       int            `json:"port"`
	MetaData   SimpleMetaData `json:"metadata"`
	CrawlerID  string         `json:"crawler_id"`
	CrawlerLoc string         `json:"crawler_location"`
}

type SimpleMetaData struct {
	SeqNumber uint64
	Attnets   []byte
	Syncnets  []byte
}

func (n *Node) Connected(net network.Network, c network.Conn) {
	n.log.Debug().
		Str("peer", c.RemotePeer().String()).
		Str("dir", c.Stat().Direction.String()).
		Int("total", len(n.host.Network().Peers())).
		Msg("Connected Peer")

	if c.Stat().Direction == network.DirOutbound {
		go n.handleNewConnection(c.RemotePeer())
	} else if c.Stat().Direction == network.DirInbound {
		go n.handleInboundConnection(c.RemotePeer())
	} else {
		n.log.Info().Str("peer", c.RemotePeer().String()).Msg("Unknown connection direction")
	}
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	if _, err := n.getMetadataFromCache(c.RemotePeer()); err == nil {
		n.log.Info().
			Str("peer", c.RemotePeer().String()).
			Msg("Disconnected from handshaked peer")
	}
}

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) handleNewConnection(pid peer.ID) {
	n.log.Info().Str("peer", pid.String()).Msg("Handling new outbound connection")

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	addrs := n.host.Peerstore().Addrs(pid)
	if len(addrs) == 0 {
		n.log.Fatal().Str("No addresses found for peer", pid.String())
	}

	valid := n.validatePeer(ctx, pid, peer.AddrInfo{ID: pid, Addrs: addrs[:1]})

	if !valid {
		n.log.Info().Str("peer", pid.String()).Msg("Handshake failed, disconnecting")
		n.host.Peerstore().RemovePeer(pid)
	}

	n.log.Info().Str("peer", pid.String()).Msg("Outbound connection established")

	n.reqResp.Goodbye(ctx, pid, 3) // NOTE: Figure out the correct reason code
	n.host.Network().ClosePeer(pid)
}

func (n *Node) handleInboundConnection(pid peer.ID) {
	n.log.Info().Str("peer", pid.String()).Msg("Handling new inbound connection")

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	addrs := n.host.Peerstore().Addrs(pid)
	if len(addrs) == 0 {
		n.log.Fatal().Str("No addresses found for peer", pid.String())
	}

	valid := n.validatePeer(ctx, pid, peer.AddrInfo{ID: pid, Addrs: addrs[:1]})

	if !valid {
		n.log.Info().Str("peer", pid.String()).Msg("Handshake failed, disconnecting")
		n.host.Peerstore().RemovePeer(pid)
		n.host.Network().ClosePeer(pid)
		return
	}

	n.log.Info().Str("peer", pid.String()).Msg("Inbound connection established")

	n.reqResp.Goodbye(ctx, pid, 3) // NOTE: Figure out the correct reason code
	n.host.Network().ClosePeer(pid)
}

func (n *Node) validatePeer(ctx context.Context, pid peer.ID, addrInfo peer.AddrInfo) bool {
	st, err := n.reqResp.Status(ctx, pid)
	if err != nil {
		n.log.Debug().Str("peer", pid.String()).Msg("Failed to get status from peer")
		n.addToBackoffCache(pid, addrInfo)
		return false
	}

	if err := n.reqResp.Ping(ctx, pid); err != nil {
		n.log.Debug().Str("peer", pid.String()).Msg("Failed to ping peer")
		n.addToBackoffCache(pid, addrInfo)
		return false
	}

	md, err := n.reqResp.MetaData(ctx, pid)
	if err != nil {
		n.log.Debug().Str("peer", pid.String()).Msg("Failed to get metadata from peer")
		n.addToBackoffCache(pid, addrInfo)
		return false
	}

	n.addToMetadataCache(pid, md)

	n.log.Info().
		Str("peer", pid.String()).
		Int("seq", int(md.SeqNumber)).
		Str("attnets", hex.EncodeToString(md.Attnets)).
		Msg("Performed successful handshake")

	fmt.Fprintf(n.fileLogger, "%s ID: %v, SeqNum: %v, Attnets: %s, ForkDigest: %s\n",
		time.Now().Format(time.RFC3339), pid.String(), md.SeqNumber, hex.EncodeToString(md.Attnets), hex.EncodeToString(st.ForkDigest))

	// Extract the IP and Port from the address.
	addressParts := strings.Split(addrInfo.Addrs[0].String(), "/")
	ip := addressParts[2]
	port, err := strconv.Atoi(addressParts[4])
	if err != nil {
		return false
	}
	node := n.disc.seenNodes[pid].Node

	// Publish to NATS
	metadataEvent := MetadataReceivedEvent{
		ENR:        node.String(),
		IP:         ip,
		Port:       port,
		MetaData:   SimpleMetaData{SeqNumber: md.SeqNumber, Attnets: md.Attnets, Syncnets: md.Syncnets},
		CrawlerID:  getCrawlerMachineID(),
		CrawlerLoc: getCrawlerLocation(),
	}

	eventData, err := json.Marshal(metadataEvent)
	if err != nil {
		n.log.Error().Err(err).Msg("Failed to marshal metadata event")
		return false
	}

	ack, err := n.js.Publish(ctx, "events.metadata_received", eventData)
	if err != nil {
		n.log.Error().Err(err).Msg("Failed to publish metadata event")
		return false
	}
	n.log.Debug().Msgf("Published metadata event with seq: %v", ack.Sequence)

	return true
}
