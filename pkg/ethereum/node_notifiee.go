package ethereum

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

var _ network.Notifiee = (*Node)(nil)

func (n *Node) Connected(net network.Network, c network.Conn) {
	n.log.Debug().
		Str("peer", c.RemotePeer().String()).
		Str("dir", c.Stat().Direction.String()).
		Int("total", len(n.host.Network().Peers())).
		Msg("Connected Peer")

	if c.Stat().Direction == network.DirOutbound {
		go n.handleNewConnection(c.RemotePeer())
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
	n.log.Info().Str("peer", pid.String()).Msg("Handling new connection")

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

	return true
}
