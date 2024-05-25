package ethereum

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

var _ network.Notifiee = (*Node)(nil)

func (n *Node) Connected(net network.Network, c network.Conn) {
	n.log.Debug().
		Str("peer", c.RemotePeer().String()).
		Str("dir", c.Stat().Direction.String()).
		Int("total", len(n.host.Network().Peers())).
		Msg("Connected Peer")

	if c.Stat().Direction == network.DirOutbound {
		go n.handleOutboundConnection(c.RemotePeer())
	} else if c.Stat().Direction == network.DirInbound {
		go n.handleInboundConnection(c.RemotePeer())
	}
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	if n.getMetadataFromCache(c.RemotePeer()) != nil {
		n.log.Info().
			Str("peer", c.RemotePeer().String()).
			Msg("Disconnected from handshaked peer")
	}
}

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) handleOutboundConnection(pid peer.ID) {
	n.log.Info().Str("peer", pid.String()).Msg("Handling new outbound connection")

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	// Cleanup function
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := n.reqResp.Goodbye(ctx, pid, 3) // NOTE: Figure out the correct reason code
		if err != nil {
			n.log.Debug().Str("peer", pid.String()).Err(err).Msg("Failed to send goodbye message")
		}

		n.host.Network().ClosePeer(pid)
	}()

	addrs := n.host.Peerstore().Addrs(pid)
	if len(addrs) == 0 {
		n.log.Fatal().Str("No addresses found for peer", pid.String())
	}

	addrInfo := peer.AddrInfo{ID: pid, Addrs: addrs[:1]}
	if err := n.validatePeer(ctx, pid, addrInfo); err != nil {
		n.log.Warn().Str("peer", pid.String()).Err(err).Msg("Handshake failed")
		n.addToBackoffCache(pid, addrInfo)

		// TODO: Should we remove peer?
		// n.host.Peerstore().RemovePeer(pid)
		return
	}

}

func (n *Node) handleInboundConnection(pid peer.ID) {
	n.log.Info().Str("peer", pid.String()).Msg("Handling new inbound connection")

	// NOTE: This timeout will be used for all the operations in this function
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	// Cleanup function
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := n.reqResp.Goodbye(ctx, pid, 3) // NOTE: Figure out the correct reason code
		if err != nil {
			n.log.Debug().Str("peer", pid.String()).Err(err).Msg("Failed to send goodbye message")
		}

		n.host.Network().ClosePeer(pid)
	}()

	addrs := n.host.Peerstore().Addrs(pid)
	if len(addrs) == 0 {
		n.log.Fatal().Str("No addresses found for peer", pid.String())
	}

	addrInfo := peer.AddrInfo{ID: pid, Addrs: addrs[:1]}
	if err := n.validatePeer(ctx, pid, addrInfo); err != nil {
		n.log.Warn().Str("peer", pid.String()).Err(err).Msg("Handshake failed")
		n.addToBackoffCache(pid, addrInfo)

		// TODO: Should we remove peer?
		// n.host.Peerstore().RemovePeer(pid)
		return
	}
}

func (n *Node) validatePeer(ctx context.Context, pid peer.ID, addrInfo peer.AddrInfo) error {
	st, err := n.reqResp.Status(ctx, pid)
	if err != nil {
		return errors.Wrap(err, "Failed to get status from peer")
	}

	// If the status head slot is higher than the current, update it
	if bytes.Equal(st.ForkDigest, n.cfg.ForkDigest[:]) {
		if st.HeadSlot > n.reqResp.status.HeadSlot {
			n.reqResp.SetStatus(st)
		}
	}

	if err := n.reqResp.Ping(ctx, pid); err != nil {
		return errors.Wrap(err, "Failed to ping peer")
	}

	md, err := n.reqResp.MetaData(ctx, pid)
	if err != nil {
		return errors.Wrap(err, "Failed to get metadata from peer")
	}

	n.sendMetadataEvent(ctx, pid, addrInfo, md)
	n.addToMetadataCache(pid, md)

	n.log.Info().
		Str("peer", pid.String()).
		Int("Seq", int(md.SeqNumber)).
		Str("Attnets", hex.EncodeToString(md.Attnets)).
		Msg("Performed successful handshake")

	fmt.Fprintf(n.fileLogger, "%s ID: %v, SeqNum: %v, Attnets: %s, ForkDigest: %s\n",
		time.Now().Format(time.RFC3339), pid.String(), md.SeqNumber, hex.EncodeToString(md.Attnets), hex.EncodeToString(st.ForkDigest))

	return nil
}
