package ethereum

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	eth "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/chainbound/valtrack/config"
	"github.com/chainbound/valtrack/log"
	"github.com/chainbound/valtrack/types"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	gomplex "github.com/libp2p/go-mplex"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
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
	gs                *pubsub.PubSub
	cfg               *config.NodeConfig
	peerstore         *Peerstore
	reqResp           *ReqResp
	disc              *DiscoveryV5
	js                jetstream.JetStream
	log               zerolog.Logger
	fileLogger        *os.File
	metadataEventChan chan *types.MetadataReceivedEvent
	reconnectChan     chan peer.AddrInfo
}

// NewNode initializes a new Node using the provided configuration and options.
func NewNode(cfg *config.NodeConfig) (*Node, error) {
	log := log.NewLogger("node")

	file, err := os.Create(cfg.LogPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create log file")
	}

	data, err := cfg.PrivateKey.Raw()
	discKey, _ := gcrypto.ToECDSA(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate discv5 key")
	}

	peerstore := NewPeerstore(30 * time.Second)

	// TODO: read config from node config
	conf := config.NewDefaultDiscConfig()
	conf.NatsURL = cfg.NatsURL
	disc, err := NewDiscoveryV5(discKey, &conf)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create DiscoveryV5 service")
	}

	listenMaddr, err := MaddrFrom(cfg.IP, uint(cfg.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to create multiaddr: %w", err)
	}

	gomplex.ResetStreamTimeout = 5 * time.Second
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

	log.Info().Any("listen_addrs", h.Network().ListenAddresses()).Msg("Created new libp2p host")

	reqRespCfg := &ReqRespConfig{
		ForkDigest:   cfg.ForkDigest,
		Encoder:      encoder.SszNetworkEncoder{},
		ReadTimeout:  cfg.BeaconConfig.TtfbTimeoutDuration(),
		WriteTimeout: cfg.BeaconConfig.RespTimeoutDuration(),
	}

	// Initialize ReqResp
	reqResp, err := NewReqResp(h, peerstore, reqRespCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create reqresp: %w", err)
	}

	js, err := createNatsStream(cfg.NatsURL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create NATS JetStream")
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
		peerstore:         peerstore,
		metadataEventChan: make(chan *types.MetadataReceivedEvent, 100),
		reconnectChan:     make(chan peer.AddrInfo, 100),
	}, nil
}

func (n *Node) CanSubscribe(topic string) bool {
	return true
}

func (n *Node) FilterIncomingSubscriptions(pid peer.ID, opts []*pb.RPC_SubOpts) ([]*pb.RPC_SubOpts, error) {
	var attnets []int64
	for _, opt := range opts {
		topic := opt.GetTopicid()
		if strings.Contains(topic, "beacon_attestation") {
			// "/eth2/8c9f62fe/beacon_attestation_61/ssz_snappy"
			subnetIndex := strings.Split(strings.Split(topic, "/")[3], "_")[2]

			if idx, err := strconv.Atoi(subnetIndex); err == nil {
				attnets = append(attnets, int64(idx))
			}

		}
	}

	if len(attnets) > 0 {
		n.log.Debug().Str("peer", pid.String()).Any("attnets", attnets).Msg("Captured attestation subnet subscriptions")
		n.peerstore.AddSubscribedSubnets(pid, attnets...)
	}

	return nil, nil
}

// Start runs the operational routines of the node, such as network services and handling connections.
func (n *Node) Start(ctx context.Context) error {
	status := &eth.StatusV2{
		ForkDigest:            n.cfg.ForkDigest[:],
		FinalizedRoot:         make([]byte, 32),
		FinalizedEpoch:        0,
		HeadRoot:              make([]byte, 32),
		HeadSlot:              0,
		EarliestAvailableSlot: 0,
	}

	n.reqResp.SetStatus(status)

	// Set stream handlers on our libp2p host
	if err := n.reqResp.RegisterHandlers(ctx); err != nil {
		return fmt.Errorf("register RPC handlers: %w", err)
	}

	n.log.Info().Msg("Starting node services")

	// Initialize GossipSub BEFORE starting discovery/dialing so we can capture
	// subscription notifications from connected peers
	psOpts := []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		// pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
		// 	return MsgID(s.genesisValidatorsRoot, pmsg)
		// }),
		pubsub.WithSubscriptionFilter(n),
		// pubsub.WithPeerOutboundQueueSize(int(s.cfg.QueueSize)),
		// pubsub.WithMaxMessageSize(int(params.BeaconConfig().GossipMaxSize)),
		// pubsub.WithValidateQueueSize(int(s.cfg.QueueSize)),
		// pubsub.WithPeerScore(peerScoringParams()),
		// pubsub.WithPeerScoreInspect(s.peerInspector, time.Minute),
		// pubsub.WithGossipSubParams(pubsubGossipParam()),
		// pubsub.WithRawTracer(gossipTracer{host: s.host}),
	}

	gs, err := pubsub.NewGossipSub(ctx, n.host, psOpts...)
	if err != nil {
		return errors.Wrap(err, "failed to create GossipSub")
	}

	n.gs = gs

	// Register the node itself as the notifiee for network connection events
	n.host.Network().Notify(n)

	if n.js != nil {
		// Start the metadata event publisher
		n.startMetadataPublisher()
	}

	// Start the discovery service
	go n.runDiscovery(ctx)

	// Start the peer dialer service
	for i := 0; i < n.cfg.ConcurrentDialers; i++ {
		go n.runPeerDialer(ctx)
	}

	// Start the timer function to attempt reconnections every 30 seconds
	go n.startReconnectionTimer()
	n.startReconnectListener()

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
		toReconnect := n.peerstore.PeersToReconnect()

		n.log.Info().Int("amount", len(toReconnect)).Msg("Attempting to reconnect expired peers")

		for _, pid := range toReconnect {
			n.reconnectChan <- pid
		}
	}
}

func (n *Node) startReconnectListener() {
	go func() {
		for info := range n.reconnectChan {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
				err := n.host.Connect(ctx, info)
				cancel()

				if err != nil {
					counter := n.peerstore.SetBackoff(info.ID, err)

					n.log.Debug().Str("peer", info.String()).Uint32("backoff_counter", counter).Msg("Failed to reconnect to peer")
				} else {
					n.log.Info().Str("peer", info.String()).Msg("Successfully reconnected to peer")
				}
			}()
		}
	}()
}
