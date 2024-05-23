package ethereum

import (
	"context"
	"fmt"
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
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	eth "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/rs/zerolog"
)

type PeerMetadata struct {
	LastSeen time.Time
	Metadata *eth.MetaDataV1
}

type PeerBackoff struct {
	LastSeen       time.Time
	BackoffCounter int
	AddrInfo       peer.AddrInfo
}

// Node represents a node in the network with a host and configuration.
type Node struct {
	host              host.Host
	cfg               *config.NodeConfig
	reqResp           *ReqResp
	disc              *DiscoveryV5
	js                jetstream.JetStream
	log               zerolog.Logger
	fileLogger        *os.File
	backoffCache      map[peer.ID]*PeerBackoff
	metadataCache     map[peer.ID]*PeerMetadata
	cacheMutex        sync.Mutex
	metadataEventChan chan *MetadataReceivedEvent
}

// NewNode initializes a new Node using the provided configuration and options.
func NewNode(cfg *config.NodeConfig) (*Node, error) {
	log := log.NewLogger("node")

	file, err := os.Create("handshakes.log")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create log file")
	}

	data, err := cfg.PrivateKey.Raw()
	discKey, _ := gcrypto.ToECDSA(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate discv5 key")
	}
	conf := config.DefaultDiscConfig
	disc, err := NewDiscoveryV5(discKey, &conf)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create DiscoveryV5 service")
	}

	listenMaddr, err := MaddrFrom(cfg.IP, uint(cfg.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to create multiaddr: %w", err)
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrs(listenMaddr),
		libp2p.Identity(cfg.PrivateKey),
		libp2p.UserAgent("valtrack"),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Muxer(mplex.ID, mplex.DefaultTransport),
		libp2p.DefaultMuxers,
		libp2p.Security(noise.ID, noise.New),
		libp2p.DisableRelay(),
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

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	// Initialize NATS JetStream
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	cfgjs := jetstream.StreamConfig{
		Name:      "EVENTS",
		Retention: jetstream.InterestPolicy,
		Subjects:  []string{"events.metadata_received", "events.peer_discovered"},
	}

	ctxJs := context.Background()

	_, err = js.CreateOrUpdateStream(ctxJs, cfgjs)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create JetStream stream")
	}

	// Log the node's peer ID and addresses
	log.Info().Str("peer_id", h.ID().String()).Any("Maddr", h.Addrs()).Msg("Initialized new libp2p Host")

	// Return the fully initialized Node
	return &Node{
		host:              h,
		cfg:               cfg,
		reqResp:           reqResp,
		disc:              disc,
		js:                js,
		log:               log,
		fileLogger:        file,
		backoffCache:      make(map[peer.ID]*PeerBackoff),
		metadataCache:     make(map[peer.ID]*PeerMetadata),
		metadataEventChan: make(chan *MetadataReceivedEvent, 100),
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

	n.log.Info().Msg("Starting node services")

	// Register the node itself as the notifiee for network connection events
	n.host.Network().Notify(n)

	n.startMetadataPublisher()

	// Start the discovery service
	go n.runDiscovery(ctx)

	// Start the peer dialer service
	for i := 0; i < 16; i++ {
		go n.runPeerDialer(ctx)
	}

	// Start the timer function to attempt reconnections every 30 seconds
	go n.startReconnectionTimer()

	<-ctx.Done()
	n.log.Info().Msg("Shutting down node services")

	return nil
}

func (n *Node) runDiscovery(ctx context.Context) {
	if err := n.disc.Serve(ctx); err != nil && ctx.Err() == nil {
		n.log.Error().Err(err).Msg("DiscoveryV5 service stopped unexpectedly")
	}
}

func (n *Node) runPeerDialer(ctx context.Context) {
	cs := &PeerDialer{
		host:     n.host,
		peerChan: n.disc.out,
		maxPeers: n.cfg.MaxPeerCount,
		log:      log.NewLogger("peer_dialer"),
	}
	if err := cs.Serve(ctx); err != nil && ctx.Err() == nil {
		n.log.Error().Err(err).Msg("PeerDialer service stopped unexpectedly")
	}
}

func (n *Node) startReconnectionTimer() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		n.reconnectPeers()
	}
}

func (n *Node) reconnectPeers() {
	n.cacheMutex.Lock()
	defer n.cacheMutex.Unlock()

	for pid, backoff := range n.backoffCache {
		if time.Since(backoff.LastSeen) >= 30*time.Second && backoff.BackoffCounter < 10 {
			n.log.Debug().Str("peer", pid.String()).Int("backoff_counter", backoff.BackoffCounter).Msg("Attempting to reconnect to peer")

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := n.host.Connect(ctx, backoff.AddrInfo)
			cancel()

			if err != nil {
				n.log.Debug().Str("peer", pid.String()).Msg("Failed to reconnect to peer")
				backoff.LastSeen = time.Now()
				backoff.BackoffCounter++
			} else {
				n.log.Info().Str("peer", pid.String()).Msg("Successfully reconnected to peer")
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

func (n *Node) removeFromBackoffCache(pid peer.ID) {
	n.cacheMutex.Lock()
	defer n.cacheMutex.Unlock()

	delete(n.backoffCache, pid)
	n.log.Debug().Str("peer", pid.String()).Msg("Removed peer from backoff cache")
}

func (n *Node) getBackoffCounter(pid peer.ID) int {
	n.cacheMutex.Lock()
	defer n.cacheMutex.Unlock()

	backoff, exists := n.backoffCache[pid]
	if !exists {
		return 0
	}

	return backoff.BackoffCounter
}

func (n *Node) addToMetadataCache(pid peer.ID, metadata *eth.MetaDataV1) {
	n.removeFromBackoffCache(pid)

	n.cacheMutex.Lock()
	defer n.cacheMutex.Unlock()

	n.metadataCache[pid] = &PeerMetadata{
		LastSeen: time.Now(),
		Metadata: metadata,
	}

	n.log.Debug().Str("peer", pid.String()).Str("metadata", metadata.String()).Msg("Added peer to metadata cache")
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
