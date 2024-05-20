package ethereum

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/chainbound/valtrack/config"
	"github.com/chainbound/valtrack/log"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	eth "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	pb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/rs/zerolog"
	"github.com/thejerf/suture/v4"
)

type PeerMetadata struct {
	LastSeen time.Time
	Metadata *pb.MetaDataV1
}

type PeerBackoff struct {
	LastSeen       time.Time
	BackoffCounter int
	AddrInfo       peer.AddrInfo
}

// Node represents a node in the network with a host and configuration.
type Node struct {
	host    host.Host
	cfg     *config.NodeConfig
	reqResp *ReqResp
	disc    *DiscoveryV5

	// The suture supervisor that is the root of the service tree
	sup *suture.Supervisor

	log        zerolog.Logger
	fileLogger *os.File

	backoffCache  map[peer.ID]*PeerBackoff
	metadataCache map[peer.ID]*PeerMetadata
	cacheMutex    sync.Mutex
}

// NewNode initializes a new Node using the provided configuration and options.
func NewNode(cfg *config.NodeConfig) (*Node, error) {
	log := log.NewLogger("node")

	file, err := os.Create("handshakes.log")
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create log file")
	}

	data, err := cfg.PrivateKey.Raw()
	discKey, _ := gcrypto.ToECDSA(data)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to generate discv5 key")
	}
	conf := config.DefaultConfig
	disc, err := NewDiscoveryV5(discKey, &conf)

	listenMaddr, err := MaddrFrom("127.0.0.1", 0)

	opts := []libp2p.Option{
		libp2p.ListenAddrs(listenMaddr),
		libp2p.Identity(cfg.PrivateKey),
		libp2p.UserAgent("valtrack"),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Muxer(mplex.ID, mplex.DefaultTransport),
		libp2p.DefaultMuxers,
		libp2p.Security(noise.ID, noise.New),
		libp2p.DisableRelay(),
		libp2p.Ping(false),
		libp2p.DisableMetrics(),
	}

	// Create a new libp2p Host
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	reqRespCfg := &ReqRespConfig{
		ForkDigest:   cfg.ForkDigest,
		Encoder:      encoder.SszNetworkEncoder{},
		ReadTimeout:  cfg.BeaconConfig.TtfbTimeoutDuration(),
		WriteTimeout: cfg.BeaconConfig.RespTimeoutDuration(),
	}

	// Initialize ReqResp
	reqResp, err := NewReqResp(h, reqRespCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create reqresp: %w", err)
	}

	// Log the node's peer ID and addresses
	log.Info().Str("peer_id", h.ID().String()).Any("Maddr", h.Addrs()).Msg("Initialized new libp2p Host")

	// Return the fully initialized Node
	return &Node{
		host:          h,
		cfg:           cfg,
		reqResp:       reqResp,
		disc:          disc,
		sup:           suture.NewSimple("eth"),
		log:           log,
		fileLogger:    file,
		backoffCache:  make(map[peer.ID]*PeerBackoff),
		metadataCache: make(map[peer.ID]*PeerMetadata),
	}, nil
}

// Start runs the operational routines of the node, such as network services and handling connections.
func (n *Node) Start(ctx context.Context) error {
	status := &eth.Status{
		ForkDigest:     n.cfg.ForkDigest[:],
		FinalizedRoot:  make([]byte, 32),
		FinalizedEpoch: 0,
		HeadRoot:       make([]byte, 32),
		HeadSlot:       0,
	}

	n.reqResp.SetStatus(status)

	// Set stream handlers on our libp2p host
	if err := n.reqResp.RegisterHandlers(ctx); err != nil {
		return fmt.Errorf("register RPC handlers: %w", err)
	}

	// Register the node itself as the notifiee for network connection events
	n.host.Network().Notify(n)

	// Start the discovery service
	n.sup.Add(n.disc)

	log := log.NewLogger("peer_dialer")
	for i := 0; i < 16; i++ {
		cs := &PeerDialer{
			host:     n.host,
			peerChan: n.disc.out,
			maxPeers: 30,
			log:      log,
		}
		n.sup.Add(cs)
	}

	// Start the timer function to attempt reconnections every 30 seconds
	go n.startReconnectionTimer()

	n.log.Info().Msg("Starting node services")

	return n.sup.Serve(ctx)
}

func (n *Node) startReconnectionTimer() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.reconnectPeers()
		}
	}
}

func (n *Node) reconnectPeers() {
	n.cacheMutex.Lock()
	defer n.cacheMutex.Unlock()

	for pid, backoff := range n.backoffCache {
		if time.Since(backoff.LastSeen) >= 30*time.Second && backoff.BackoffCounter < 10 {
			n.log.Debug().Str("peer", pid.String()).Msg("Attempting to reconnect to peer")

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := n.host.Connect(ctx, backoff.AddrInfo)
			cancel()

			if err != nil {
				n.log.Debug().Str("peer", pid.String()).Msg("Failed to reconnect to peer")
				backoff.LastSeen = time.Now()
				backoff.BackoffCounter++
			} else {
				n.log.Info().Str("peer", pid.String()).Msg("Successfully reconnected to peer")
				delete(n.backoffCache, pid)
			}
		}
	}
}

func (n *Node) addToBackoffCache(pid peer.ID, addrInfo peer.AddrInfo) {
	n.cacheMutex.Lock()
	defer n.cacheMutex.Unlock()

	backoff, exists := n.backoffCache[pid]
	if !exists {
		backoff = &PeerBackoff{
			BackoffCounter: 0,
			AddrInfo:       addrInfo,
		}
	}

	backoff.BackoffCounter++
	backoff.LastSeen = time.Now()
	n.backoffCache[pid] = backoff

	if !exists {
		n.log.Debug().Str("peer", pid.String()).Int("backoff_counter", backoff.BackoffCounter).Msg("Added peer to backoff cache")
	} else {
		n.log.Debug().Str("peer", pid.String()).Int("backoff_counter", backoff.BackoffCounter).Msg("Updated peer in backoff cache")
	}
}

func (n *Node) addToMetadataCache(pid peer.ID, metadata *pb.MetaDataV1) {
	n.cacheMutex.Lock()
	defer n.cacheMutex.Unlock()

	n.metadataCache[pid] = &PeerMetadata{
		LastSeen: time.Now(),
		Metadata: &pb.MetaDataV1{},
	}

	n.log.Debug().Str("peer", pid.String()).Msg("Added peer to metadata cache")
}

func (n *Node) getMetadataFromCache(pid peer.ID) (*PeerMetadata, error) {
	n.cacheMutex.Lock()
	defer n.cacheMutex.Unlock()

	metadata, exists := n.metadataCache[pid]
	if !exists {
		return nil, fmt.Errorf("metadata not found for peer: %s", pid)
	}

	return metadata, nil
}

// MaddrFrom takes in an ip address string and port to produce a go multiaddr format.
func MaddrFrom(ip string, port uint) (ma.Multiaddr, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil, fmt.Errorf("invalid IP address: %s", ip)
	} else if parsed.To4() != nil {
		return ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ip, port))
	} else if parsed.To16() != nil {
		return ma.NewMultiaddr(fmt.Sprintf("/ip6/%s/tcp/%d", ip, port))
	} else {
		return nil, fmt.Errorf("invalid IP address: %s", ip)
	}
}

func LogAttrPeerID(pid peer.ID) slog.Attr {
	return slog.String("AttrKeyPeerID", pid.String())
}

func LogAttrError(err error) slog.Attr {
	return slog.Attr{Key: "AttrKeyError", Value: slog.AnyValue(err)}
}
