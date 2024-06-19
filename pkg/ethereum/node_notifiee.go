package ethereum

import (
	"bytes"
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

var _ network.Notifiee = (*Node)(nil)

func (n *Node) Connected(net network.Network, c network.Conn) {
	pid := c.RemotePeer()

	if n.peerstore.State(pid) != NotConnected {
		// If we're already connecting, return
		n.log.Debug().Str("peer", pid.String()).Msg("Already connecting to peer")
		return
	}

	info := n.disc.seenNodes[pid]

	// Insert into the peerstore
	n.peerstore.Insert(pid, c.RemoteMultiaddr(), info.Node)
	n.peerstore.SetState(pid, Connecting)

	n.log.Info().
		Str("peer", pid.String()).
		Str("dir", c.Stat().Direction.String()).
		Int("total", len(n.host.Network().Peers())).
		Int("peerstore_size", n.peerstore.Size()).
		Msg("Connected Peer")

	if c.Stat().Direction == network.DirOutbound {
		go n.handleOutboundConnection(pid)
	} else if c.Stat().Direction == network.DirInbound {
		go n.handleInboundConnection(pid)
	}
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	pid := c.RemotePeer()

	n.log.Info().Str("peer", pid.String()).Msg("Peer disconnected")
}

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) handleOutboundConnection(pid peer.ID) {
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	// Cleanup function
	defer func() {
		// Mark the peer as succesfully connected, which will reset the backoff
		// and error to nil.
		n.peerstore.Reset(pid)

		// Don't do anything if we're already disconnected
		if n.host.Network().Connectedness(pid) != network.Connected {
			return
		}

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
		n.log.Error().Str("peer", pid.String()).Msg("No addresses found for peer")
		return
	}

	addrInfo := peer.AddrInfo{ID: pid, Addrs: addrs}
	if err := n.handshake(ctx, pid, addrInfo); err != nil {
		n.log.Warn().Str("peer", pid.String()).Err(err).Msg("Handshake failed")

		// If there was any issue during the handshake, we didn't get to the metadata response.
		// This means we should try again and mark the peer as backed off
		n.peerstore.SetBackoff(pid, err)

		return
	}

	// Save the client version
	if v, err := n.host.Peerstore().Get(pid, "AgentVersion"); err == nil {
		n.peerstore.SetClientVersion(pid, v.(string))
	} else {
		n.peerstore.SetClientVersion(pid, "unknown")
	}

	// Sleep 2 seconds to allow for all subnet subscriptions to be processed
	time.Sleep(2 * time.Second)

	info := n.peerstore.Get(pid)
	event := info.IntoMetadataEvent()

	n.sendMetadataEvent(ctx, event)
}

func (n *Node) handleInboundConnection(pid peer.ID) {
	n.log.Info().Str("peer", pid.String()).Msg("Handling new inbound connection")

	// Cleanup function
	defer func() {
		// Mark the peer as succesfully connected, which will reset the backoff
		// and error to nil.
		n.peerstore.Reset(pid)

		if n.host.Network().Connectedness(pid) != network.Connected {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := n.reqResp.Goodbye(ctx, pid, 3) // NOTE: Figure out the correct reason code
		if err != nil {
			n.log.Debug().Str("peer", pid.String()).Err(err).Msg("Failed to send goodbye message")
		}

		n.host.Network().ClosePeer(pid)
	}()

	// Wait max 5 seconds for the remote status to come in
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := n.waitForStatus(ctx, pid); err != nil {
		n.log.Warn().Str("peer", pid.String()).Msg("Timed out waiting for status")
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	if n.host.Network().Connectedness(pid) != network.Connected {
		n.log.Warn().Str("peer", pid.String()).Msg("Connection was closed before handshake completed")
		return
	}

	md, err := n.reqResp.MetaData(ctx, pid)
	if err != nil {
		n.log.Warn().Str("peer", pid.String()).Err(err).Msg("Failed requesting metadata")
		return
	}

	n.peerstore.SetMetadata(pid, md)

	// Save the client version
	if v, err := n.host.Peerstore().Get(pid, "AgentVersion"); err == nil {
		n.peerstore.SetClientVersion(pid, v.(string))
	}

	// Sleep 2 seconds to allow for all subnet subscriptions to be processed
	time.Sleep(2 * time.Second)

	info := n.peerstore.Get(pid)
	event := info.IntoMetadataEvent()

	n.sendMetadataEvent(ctx, event)
}

func (n *Node) waitForStatus(ctx context.Context, pid peer.ID) error {
	for {
		select {
		case <-ctx.Done():
			return errors.New("Timeout")
		default:
			if n.peerstore.Status(pid) != nil {
				return nil
			}

			time.Sleep(1 * time.Second)
		}
	}
}

func (n *Node) handshake(ctx context.Context, pid peer.ID, addrInfo peer.AddrInfo) error {
	st, err := n.reqResp.Status(ctx, pid)
	if err != nil {
		return errors.Wrap(err, "Failed to get status from peer")
	}

	// Set the status for this peer
	n.peerstore.SetStatus(pid, st)

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

	// Store the metadata for this peer
	n.peerstore.SetMetadata(pid, md)

	return nil
}
