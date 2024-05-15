package ethereum

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	peerstoreKeyConnectedAt  = "connected_at"
	peerstoreKeyIsHandshaked = "is_handshaked"
)

// The Hermes Ethereum [Node] implements the [network.Notifiee] interface.
// This means it will be notified about new connections.
var _ network.Notifiee = (*Node)(nil)

func (n *Node) Connected(net network.Network, c network.Conn) {
	// slog.Debug("Connected with peer", LogAttrPeerID(c.RemotePeer()), "total", len(n.host.Network().Peers()), "dir", c.Stat().Direction)
	n.log.Debug().Str("peer", c.RemotePeer().String()).Str("dir", c.Stat().Direction.String()).Int("total", len(n.host.Network().Peers())).Msg("Connected Peer")

	if err := n.host.Peerstore().Put(c.RemotePeer(), peerstoreKeyConnectedAt, time.Now()); err != nil {
		slog.Warn("Failed to store connection timestamp in peerstore", LogAttrError(err))
	}

	if c.Stat().Direction == network.DirOutbound {
		// handle the new connection by validating the peer. Needs to happen in a
		// go routine because Connected is called synchronously.
		go n.handleNewConnection(c.RemotePeer())
	}
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	// if n.pryInfo != nil && c.RemotePeer() == n.pryInfo.ID {
	// 	slog.Warn("Beacon node disconnected")
	// }

	// if !c.Stat().Opened.IsZero() {
	// 	av := n.host.AgentVersion(c.RemotePeer())
	// 	parts := strings.Split(av, "/")
	// 	if len(parts) > 0 {
	// 		switch strings.ToLower(parts[0]) {
	// 		case "prysm", "lighthouse", "nimbus", "lodestar", "grandine", "teku", "erigon":
	// 			av = strings.ToLower(parts[0])
	// 		default:
	// 			av = "other"
	// 		}
	// 	} else {
	// 		av = "unknown"
	// 	}
	// 	// n.connAge.Record(context.TODO(), time.Since(c.Stat().Opened).Seconds(), metric.WithAttributes(attribute.String("agent", av)))
	// }

	ps := n.host.Peerstore()
	if _, err := ps.Get(c.RemotePeer(), peerstoreKeyIsHandshaked); err == nil {
		if _, err := ps.Get(c.RemotePeer(), peerstoreKeyConnectedAt); err == nil {
			slog.Info("Disconnected from handshaked peer", LogAttrPeerID(c.RemotePeer()))
			// n.connDurHist.Record(context.Background(), time.Since(val.(time.Time)).Hours())
		}
	}
}

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) handleNewConnection(pid peer.ID) {
	// slog.Info("Handling new connection with peer", LogAttrPeerID(pid))
	n.log.Info().Str("peer", pid.String()).Msg("Handling new connection")

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	valid := true

	st, err := n.reqResp.Status(ctx, pid)
	if err != nil {
		valid = false
		// slog.Info("Status check failed: %s", err)
		n.log.Debug().Err(err).Msg("Status check failed")
	} else if err := n.reqResp.Ping(ctx, pid); err != nil {
		valid = false
		// slog.Info("Ping failed: %s", err)
		n.log.Debug().Err(err).Msg("Ping failed")
	} else if md, err := n.reqResp.MetaData(ctx, pid); err != nil {
		valid = false
		// slog.Info("Metadata retrieval failed: %s", err)
		n.log.Debug().Err(err).Msg("Metadata retrieval failed")
	} else {
		// slog.Info("Performed successful handshake with peer %s: SeqNumber %d, Attnets %s, ForkDigest %s\n",
		// 	pid, md.SeqNumber, hex.EncodeToString(md.Attnets), hex.EncodeToString(st.ForkDigest))
		n.log.Info().
			Str("peer", pid.String()).
			Int("seq", int(md.SeqNumber)).
			Str("attnets", hex.EncodeToString(md.Attnets)).
			Msg("Performed successful handshake")

		// File log the handshake with time, peer ID, seq number, attnets, fork digest
		fmt.Fprintf(n.fileLogger, "%s ID: %v, SeqNum: %v, Attnets: %s, ForkDigest: %s\n", time.Now().Format(time.RFC3339), pid.String(), md.SeqNumber, hex.EncodeToString(md.Attnets), hex.EncodeToString(st.ForkDigest))
		// fmt.Fprintf(n.fileLogger, "%s ID: %v, SeqNum: %v, Attnets: %s\n", time.Now().Format(time.RFC3339), pid.String(), md.SeqNumber, md.Attnets)
	}

	if !valid {
		// slog.Info("Handshake failed with peer %s, disconnecting.\n", pid)
		n.log.Info().Str("peer", pid.String()).Msg("Handshake failed, disconnecting")
		n.host.Network().ClosePeer(pid)
	}
}
