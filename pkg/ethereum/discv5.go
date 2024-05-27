package ethereum

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/chainbound/valtrack/config"
	"github.com/chainbound/valtrack/log"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/rs/zerolog"

	glog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

type HostInfo struct {
	sync.RWMutex

	// AddrInfo
	ID     peer.ID
	IP     string
	Port   int
	MAddrs []ma.Multiaddr

	Attr map[string]interface{}
}

type NodeInfo struct {
	Node enode.Node
	Flag bool
}

type DiscoveryV5 struct {
	Dv5Listener   *discover.UDPv5
	FilterDigest  string
	log           zerolog.Logger
	seenNodes     map[peer.ID]NodeInfo
	fileLogger    *os.File
	out           chan peer.AddrInfo
	js            jetstream.JetStream
	discEventChan chan *PeerDiscoveredEvent
}

func NewDiscoveryV5(pk *ecdsa.PrivateKey, discConfig *config.DiscConfig) (*DiscoveryV5, error) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	// Init NATS connection
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to connect to NATS")
	}

	js, _ := jetstream.New(nc)

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

	// New geth logger at debug level
	gethlog := glog.New()
	log := log.NewLogger("discv5")

	// Init the ethereum peerstore
	enodeDB, err := enode.OpenDB(discConfig.DBPath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to open the DB")
	}

	// Generate a Enode with custom ENR
	ethNode := enode.NewLocalNode(enodeDB, pk)

	// Set the enr entries
	udpEntry := enr.UDP(discConfig.UDP)
	ethNode.Set(udpEntry)

	tcpEntry := enr.TCP(discConfig.TCP)
	ethNode.Set(tcpEntry)

	ethNode.Set(attnetsEntry())

	eth2, err := discConfig.Eth2EnrEntry()
	if err != nil {
		return nil, err
	}

	ethNode.Set(eth2)

	if len(discConfig.Bootnodes) == 0 {
		return nil, errors.New("No bootnodes provided")
	}

	cfg := discover.Config{
		PrivateKey:   pk,
		NetRestrict:  nil,
		Unhandled:    nil,
		Bootnodes:    discConfig.Bootnodes,
		Log:          gethlog,
		ValidSchemes: enode.ValidSchemes,
	}

	udpAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(discConfig.IP, fmt.Sprint(discConfig.UDP)))

	_, exists := os.LookupEnv("FLY_APP_NAME")
	if exists {
		udpAddr, err = net.ResolveUDPAddr("udp", net.JoinHostPort("fly-global-services", fmt.Sprint(discConfig.UDP)))
	}

	if err != nil {
		errors.Wrap(err, "Failed to resolve UDP address:")
	}

	// start listening and create a connection object
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to listen on UDP")
	}

	listener, err := discover.ListenV5(conn, ethNode, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to start discv5 listener")
	}
	log.Info().Str("udp_addr", udpAddr.String()).Msg("Listening on UDP")

	file, err := os.Create(discConfig.LogPath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create log file")
	}

	return &DiscoveryV5{
		Dv5Listener:   listener,
		FilterDigest:  "0x" + hex.EncodeToString(discConfig.ForkDigest[:]),
		log:           log,
		seenNodes:     make(map[peer.ID]NodeInfo),
		fileLogger:    file,
		out:           make(chan peer.AddrInfo, 1024),
		js:            js,
		discEventChan: make(chan *PeerDiscoveredEvent, 1024),
	}, nil
}

func (d *DiscoveryV5) Serve(ctx context.Context) error {
	d.log.Info().Msg("Starting discv5 listener")

	// Start iterating over randomly discovered nodes
	iter := d.Dv5Listener.RandomNodes()

	d.startDiscoveryPublisher()

	defer iter.Close()
	defer close(d.out)

	go func() {
		defer d.fileLogger.Close()

		for iter.Next() {
			select {
			case <-ctx.Done():
				d.log.Info().Msg("Stopping discv5 listener")
				return
			default:
				if !iter.Next() {
					return
				}
				node := iter.Node()
				hInfo, err := d.handleENR(node)
				if err != nil {
					d.log.Error().Err(err).Msg("Error handling new ENR")
					continue
				}

				if hInfo != nil && !d.seenNodes[hInfo.ID].Flag {
					select {
					case d.out <- peer.AddrInfo{
						ID:    hInfo.ID,
						Addrs: hInfo.MAddrs,
					}:
					default:
						d.log.Debug().Msg("Disc out channel is full")
					}

					d.seenNodes[hInfo.ID] = NodeInfo{Node: *node, Flag: true}

					externalIp := d.Dv5Listener.LocalNode().Node().IP()

					d.log.Info().
						Str("id", hInfo.ID.String()).
						Str("ip", hInfo.IP).
						Int("port", hInfo.Port).
						Any("attnets", hInfo.Attr[EnrAttnetsAttribute]).
						Str("enr", node.String()).
						Str("external_ip", externalIp.String()).
						Int("total", len(d.seenNodes)).
						Msg("Discovered new node")

					// Log to file
					fmt.Fprintf(d.fileLogger, "%s ID: %s, IP: %s, Port: %d, ENR: %s, Maddr: %v, Attnets: %v, AttnetsNum: %v\n",
						time.Now().Format(time.RFC3339), hInfo.ID.String(), hInfo.IP, hInfo.Port, node.String(), hInfo.MAddrs, hInfo.Attr[EnrAttnetsAttribute], hInfo.Attr[EnrAttnetsNumAttribute])

					// Send peer event to channel
					d.sendPeerEvent(ctx, node, hInfo)
				}
			}
		}
	}()

	<-ctx.Done()
	return ctx.Err()
}

// handleENR parses and identifies all the advertised fields of a newly discovered peer
func (d *DiscoveryV5) handleENR(node *enode.Node) (*HostInfo, error) {
	// Parse ENR
	enr, err := ParseEnr(node)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse new discovered ENR")
	}

	if enr.Eth2Data.ForkDigest.String() != d.FilterDigest {
		d.log.Debug().Str("fork_digest", enr.Eth2Data.ForkDigest.String()).Msg("Fork digest does not match")
		return nil, nil
	}

	// Generate the peer ID from the pubkey
	peerID, err := enr.GetPeerID()
	if err != nil {
		return &HostInfo{}, errors.Wrap(err, "unable to convert Geth pubkey to Libp2p")
	}
	// gen the HostInfo
	hInfo := NewHostInfo(
		peerID,
		WithIPAndPorts(
			enr.IP.String(),
			enr.TCP,
		),
	)
	// Add ENR and attnets as an attribute
	hInfo.AddAtt(EnrHostInfoAttribute, enr)
	hInfo.AddAtt(EnrAttnetsAttribute, hex.EncodeToString(enr.Attnets.Raw[:]))
	hInfo.AddAtt(EnrAttnetsNumAttribute, enr.Attnets.NetNumber)
	return hInfo, nil
}

func (h *HostInfo) AddAtt(key string, attr interface{}) {
	h.Lock()
	defer h.Unlock()

	h.Attr[key] = attr
}

func WithIPAndPorts(ip string, port int) RemoteHostOptions {
	return func(h *HostInfo) error {
		h.Lock()
		defer h.Unlock()

		h.IP = ip
		h.Port = port

		// Compose Multiaddress from data
		mAddr, err := MaddrFrom(ip, uint(port))
		if err != nil {
			return err
		}
		// add single address to the HostInfo
		h.MAddrs = append(h.MAddrs, mAddr)
		return nil
	}
}

type RemoteHostOptions func(*HostInfo) error

// NewHostInfo returns a new structure of the PeerInfo field for the specific network passed as argk
func NewHostInfo(peerID peer.ID, opts ...RemoteHostOptions) *HostInfo {
	hInfo := &HostInfo{
		ID:     peerID,
		MAddrs: make([]ma.Multiaddr, 0),
		Attr:   make(map[string]interface{}),
	}

	// apply all the Options
	for _, opt := range opts {
		err := opt(hInfo)
		if err != nil {
			fmt.Println("Unable to init HostInfo with folling Option", opt)
		}
	}

	return hInfo
}

// attnetsEntry returns the fully-subscribed attnets entry for the ENR
func attnetsEntry() enr.Entry {
	bitV := bitfield.NewBitvector64()
	for i := uint64(0); i < bitV.Len(); i++ {
		bitV.SetBitAt(i, true)
	}
	return enr.WithEntry("attnets", bitV.Bytes())
}
