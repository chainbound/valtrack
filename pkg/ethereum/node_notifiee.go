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

const (
	peerstoreKeyConnectedAt  = "connected_at"
	peerstoreKeyIsHandshaked = "is_handshaked"
)

// The Node implements the [network.Notifiee] interface.
// This means it will be notified about new connections.
var _ network.Notifiee = (*Node)(nil)

func (n *Node) Connected(net network.Network, c network.Conn) {
	n.log.Debug().Str("peer", c.RemotePeer().String()).Str("dir", c.Stat().Direction.String()).Int("total", len(n.host.Network().Peers())).Msg("Connected Peer")

	if err := n.host.Peerstore().Put(c.RemotePeer(), peerstoreKeyConnectedAt, time.Now()); err != nil {
		n.log.Debug().Str("peer", c.RemotePeer().String()).Msg("Failed to store connection timestamp in peerstore")
	}

	if c.Stat().Direction == network.DirOutbound {
		// handle the new connection by validating the peer. Needs to happen in a
		// go routine because Connected is called synchronously.
		go n.handleNewConnection(c.RemotePeer())
	}
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	ps := n.host.Peerstore()
	if _, err := ps.Get(c.RemotePeer(), peerstoreKeyIsHandshaked); err == nil {
		if val, err := ps.Get(c.RemotePeer(), peerstoreKeyConnectedAt); err == nil {
			n.log.Info().Str("peer", c.RemotePeer().String()).Float64("duration", time.Since(val.(time.Time)).Hours()).Msg("Disconnected from handshaked peer")
		}
	}
}

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) handleNewConnection(pid peer.ID) {
	n.log.Info().Str("peer", pid.String()).Msg("Handling new connection")

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	valid := true
	ps := n.host.Peerstore()

	st, err := n.reqResp.Status(ctx, pid)
	if err != nil {
		valid = false
	} else {
		if err := n.reqResp.Ping(ctx, pid); err != nil {
			valid = false
		} else {
			md, err := n.reqResp.MetaData(ctx, pid)
			if err != nil {
				valid = false
			} else {
				if err := ps.Put(pid, peerstoreKeyIsHandshaked, true); err != nil {
					n.log.Debug().Str("Failed to store handshaked marker in peerstore", err.Error())
				}

				n.log.Info().
					Str("peer", pid.String()).
					Int("seq", int(md.SeqNumber)).
					Str("attnets", hex.EncodeToString(md.Attnets)).
					Msg("Performed successful handshake")

				fmt.Fprintf(n.fileLogger, "%s ID: %v, SeqNum: %v, Attnets: %s, ForkDigest: %s\n", time.Now().Format(time.RFC3339), pid.String(), md.SeqNumber, hex.EncodeToString(md.Attnets), hex.EncodeToString(st.ForkDigest))
			}
		}
	}

	if !valid {
		n.log.Info().Str("peer", pid.String()).Msg("Handshake failed, disconnecting")
		ps.RemovePeer(pid)
		n.host.Network().ClosePeer(pid)
	}
}
