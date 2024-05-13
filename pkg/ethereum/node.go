package ethereum

import (
	"context"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Config holds configuration options for the node.
type Config struct {
	DialTimeout time.Duration
}

// Node represents a node in the network with a host and configuration.
type Node struct {
	host    host.Host
	cfg     Config
	reqResp *ReqResp
}

func LogAttrPeerID(pid peer.ID) slog.Attr {
	return slog.String("AttrKeyPeerID", pid.String())
}

func LogAttrError(err error) slog.Attr {
	return slog.Attr{Key: "AttrKeyError", Value: slog.AnyValue(err)}
}

func (n *Node) handleNewConnection(pid peer.ID) {
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	valid := true

	st, err := n.reqResp.Status(ctx, pid)
	if err != nil {
		valid = false
		slog.Info("Status check failed: %s", err)
	} else if err := n.reqResp.Ping(ctx, pid); err != nil {
		valid = false
		slog.Info("Ping failed: %s", err)
	} else if md, err := n.reqResp.MetaData(ctx, pid); err != nil {
		valid = false
		slog.Info("Metadata retrieval failed: %s", err)
	} else {
		slog.Info("Performed successful handshake with peer %s: SeqNumber %d, Attnets %s, ForkDigest %s\n",
			pid, md.SeqNumber, hex.EncodeToString(md.Attnets), hex.EncodeToString(st.ForkDigest))
	}

	if !valid {
		slog.Info("Handshake failed with peer %s, disconnecting.\n", pid)
		n.host.Network().ClosePeer(pid)
	}
}

// func setupLibp2pHost(ctx context.Context) (host.Host, error) {
// 	// Generate an identity keypair using ED25519
// 	priv, _, err := crypto.GenerateEd25519Key(nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Create a new libp2p Host that listens on a random TCP port
// 	h, err := libp2p.New(ctx,
// 		libp2p.Identity(priv),
// 		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return h, nil
// }

// // handleNewConnection validates the newly established connection to the given
// // peer.
// func (n *Node) handleNewConnection(pid peer.ID) {
// 	// before we add the peer to our pool, we'll perform a handshake

// 	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
// 	defer cancel()

// 	valid := true
// 	ps := n.host.Peerstore()

// 	st, err := n.reqResp.Status(ctx, pid)
// 	if err != nil {
// 		valid = false
// 	} else {
// 		if err := n.reqResp.Ping(ctx, pid); err != nil {
// 			valid = false
// 		} else {
// 			md, err := n.reqResp.MetaData(ctx, pid)
// 			if err != nil {
// 				valid = false
// 			} else {
// 				// av := n.host.AgentVersion(pid)
// 				// if av == "" {
// 				// 	av = "n.a."
// 				// }

// 				if err := ps.Put(pid, "peerstoreKeyIsHandshaked", true); err != nil {
// 					slog.Warn("Failed to store handshaked marker in peerstore", LogAttrError(err))
// 				}

// 				// slog.Info("Performed successful handshake", tele.LogAttrPeerID(pid), "seq", md.SeqNumber, "attnets", hex.EncodeToString(md.Attnets.Bytes()), "agent", av, "fork-digest", hex.EncodeToString(st.ForkDigest))
// 				slog.Info("Performed successful handshake", LogAttrPeerID(pid), "seq", md.SeqNumber, "attnets", hex.EncodeToString(md.Attnets.Bytes()), "fork-digest", hex.EncodeToString(st.ForkDigest))
// 			}
// 		}
// 	}

// 	if !valid {
// 		// the handshake failed, we disconnect and remove it from our pool
// 		ps.RemovePeer(pid)
// 		_ = n.host.Network().ClosePeer(pid)
// 	}
// }
