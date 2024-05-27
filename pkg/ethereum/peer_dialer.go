package ethereum

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/rs/zerolog"
)

// PeerDialer is a suture service that reads peers from the peerChan (which
// is filled by the [Discovery] service until that peerChan channel is closed.
// When PeerDialer sees a new peer, it does a few sanity checks and tries
// to establish a connection.
type PeerDialer struct {
	host     host.Host
	peerChan <-chan peer.AddrInfo
	log      zerolog.Logger
}

func (p *PeerDialer) Serve(ctx context.Context) error {
	p.log.Trace().Msg("Started Peer Dialer Service")
	defer p.log.Warn().Msg("Stopped Peer Dialer Service")

	for {
		select {
		case <-ctx.Done():
			return nil
		case addrInfo, more := <-p.peerChan:
			if !more {
				return nil
			}

			// don't connect with ourselves
			if addrInfo.ID == p.host.ID() {
				continue
			}

			// finally, start the connection establishment.
			// The success case is handled in net_notifiee.go.
			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := p.host.Connect(timeoutCtx, addrInfo); err != nil {
				p.log.Error().Err(err).Str("peer", addrInfo.ID.String()).Msg("Failed to connect to peer")
			}

			cancel()
		}

	}
}
