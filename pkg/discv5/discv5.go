package discv5

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/chainbound/valtrack/log"
	eth "github.com/chainbound/valtrack/pkg/ethereum"
	glog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/rs/zerolog"
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

type DiscoveryV5 struct {
	Dv5Listener  *discover.UDPv5
	FilterDigest string
	log          zerolog.Logger
}

func NewDiscoveryV5(
	port int,
	discKey *ecdsa.PrivateKey,
	ethNode *enode.LocalNode,
	forkDigest string,
	bootnodes []*enode.Node) (*DiscoveryV5, error) {
	// New geth logger at debug level
	gethlog := glog.New()

	log := log.NewLogger("discv5")

	if len(bootnodes) == 0 {
		return nil, errors.New("No bootnodes provided")
	}

	// udp address to listen
	udpAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: port,
	}

	cfg := discover.Config{
		PrivateKey:   discKey,
		NetRestrict:  nil,
		Unhandled:    nil,
		Bootnodes:    bootnodes,
		Log:          gethlog,
		ValidSchemes: enode.ValidSchemes,
	}

	// start listening and create a connection object
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to listen on UDP")
	}

	listener, err := discover.ListenV5(conn, ethNode, cfg)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to start discv5 listener")
	}

	return &DiscoveryV5{
		Dv5Listener:  listener,
		FilterDigest: forkDigest,
		log:          log,
	}, nil
}

// Start
func (d *DiscoveryV5) Start(ctx context.Context) (chan *HostInfo, error) {
	d.log.Info().Msg("Starting discv5 listener")

	ch := make(chan *HostInfo)

	// Start iterating over randomly discovered nodes
	iter := d.Dv5Listener.RandomNodes()

	go func() {
		for iter.Next() {
			node := iter.Node()

			hInfo, err := d.handleENR(node)
			if err != nil {
				d.log.Error().Err(err).Msg("Error handling new ENR")
				continue
			} else if hInfo != nil {
				d.log.Info().Str("ID", hInfo.ID.String()).Str("IP", hInfo.IP).Int("Port", hInfo.Port).Str("ENR", node.String()).Any("Maddr", hInfo.MAddrs).Msg("Discovered new node")
			}

			ch <- hInfo
		}
	}()

	return ch, nil
}

// handleENR parses and identifies all the advertised fields of a newly discovered peer
func (d *DiscoveryV5) handleENR(node *enode.Node) (*HostInfo, error) {
	// Parse ENR
	enr, err := eth.ParseEnr(node)
	if err != nil {
		// return nil, errors.Wrap(err, "unable to parse new discovered ENR")
	}

	if enr.Eth2Data.ForkDigest.String() != d.FilterDigest {
		d.log.Debug().Str("fork_digest", enr.Eth2Data.ForkDigest.String()).Msg("Fork digest does not match")
		return nil, nil
		// return nil, errors.New("Node is on different fork")

	}

	// Generate the peer ID from the pubkey
	peerID, err := enr.GetPeerID()
	if err != nil {
		// return &HostInfo{}, errors.Wrap(err, "unable to convert Geth pubkey to Libp2p")
		// d.log.Error().Err(err).Msg("unable to convert Geth pubkey to Libp2p")
		return &HostInfo{}, err
	}
	// gen the HostInfo
	hInfo := NewHostInfo(
		peerID,
		WithIPAndPorts(
			enr.IP.String(),
			enr.TCP,
		),
	)
	// add the enr as an attribute
	hInfo.AddAtt(eth.EnrHostInfoAttribute, enr)
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
		mAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ip, port))
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
			// log.("unable to init HostInfo with folling Option", opt)
		}
	}

	return hInfo
}

/*
func (d *DiscoveryV5) nodeIterator() {
	defer d.wg.Done()

	for {
		if d.Iterator.Next() {
			// fill the given DiscoveredPeer interface with the next found peer
			node := d.Iterator.Node()

			hInfo, err := d.handleENR(node)
			if err != nil {
				d.log.Error().Err(err).Msg("error handling new ENR")
				continue
			}

			d.log.Info().Str("enr", node.String()).Str("node_id", node.ID().String()).Msg("new ENR discovered")
			d.nodeNotC <- hInfo
		}
	}
}
*/
