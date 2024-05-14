package ethereum

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
)

// NodeConfig holds additional configuration options for the node.
type NodeConfig struct {
	PrivateKey   *ecdsa.PrivateKey
	BeaconConfig *params.BeaconChainConfig
	ForkDigest   [4]byte
	Encoder      encoder.NetworkEncoding
	DialTimeout  time.Duration
}

// Node represents a node in the network with a host and configuration.
type Node struct {
	host    host.Host
	cfg     *NodeConfig
	reqResp *ReqResp
}

// NewNode initializes a new Node using the provided configuration and options.
func NewNode(cfg *NodeConfig) (*Node, error) {
	var err error
	var privBytes []byte

	slog.Debug("Generating new private key")
	key, err := ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	privBytes = gcrypto.FromECDSA(key)
	if len(privBytes) != secp256k1.PrivKeyBytesLen {
		return nil, fmt.Errorf("expected secp256k1 data size to be %d", secp256k1.PrivKeyBytesLen)
	}

	privateKey := (*crypto.Secp256k1PrivateKey)(secp256k1.PrivKeyFromBytes(privBytes))

	// rmgr, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale()), rcmgr.WithTraceReporter(str))
	// if err != nil {
	// 	return nil, err
	// }

	// Initialize libp2p options, including security, transport, and other protocols
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), // Modify as necessary
		libp2p.Identity(privateKey),
		libp2p.UserAgent("valtrack"),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Muxer(mplex.ID, mplex.DefaultTransport),
		libp2p.DefaultMuxers,
		libp2p.Security(noise.ID, noise.New),
		libp2p.DisableRelay(),
		libp2p.Ping(false),
		// libp2p.ResourceManager(rmgr),
		libp2p.DisableMetrics(),
	}

	// Create a new libp2p Host
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	// initialize the request-response protocol handlers
	reqRespCfg := &ReqRespConfig{
		ForkDigest:   [4]byte{0x6a, 0x95, 0xa1, 0xa9},
		Encoder:      encoder.SszNetworkEncoder{},
		ReadTimeout:  cfg.BeaconConfig.TtfbTimeoutDuration(),
		WriteTimeout: cfg.BeaconConfig.RespTimeoutDuration(),
	}

	// Initialize ReqResp
	reqResp, err := NewReqResp(h, reqRespCfg) // Assume NewReqResp is a constructor for ReqResp

	if err != nil {
		return nil, fmt.Errorf("failed to create reqresp: %w", err)
	}

	// Log the node's peer ID and addresses
	slog.Info("Initialized new libp2p Host", LogAttrPeerID(h.ID()), "maddrs", h.Addrs())

	// Return the fully initialized Node
	return &Node{
		host:    h,
		cfg:     cfg,
		reqResp: reqResp,
	}, nil
}

// Start runs the operational routines of the node, such as network services and handling connections.
func (n *Node) Start(ctx context.Context) error {
	// Register stream handlers for various protocols
	// Set stream handlers on our libp2p host
	if err := n.reqResp.RegisterHandlers(ctx); err != nil {
		return fmt.Errorf("register RPC handlers: %w", err)
	}
	// Announce node's presence to the network, discover peers, etc.
	if err := n.discoverPeers(ctx); err != nil {
		return fmt.Errorf("failed to discover peers: %w", err)
	}

	// Handle incoming connections (this could be a loop or event-driven)
	n.host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(net network.Network, conn network.Conn) {
			go n.handleNewConnection(conn.RemotePeer())
		},
	})

	slog.Info("Node started and listening for connections.")
	return nil
}

// discoverPeers is a placeholder for peer discovery logic
func (n *Node) discoverPeers(ctx context.Context) error {
	// Implementation of peer discovery goes here
	return nil
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
