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
	"github.com/rs/zerolog"

	glog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	ma "github.com/multiformats/go-multiaddr"
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

type DiscoveryV5 struct {
	Dv5Listener  *discover.UDPv5
	FilterDigest string
	log          zerolog.Logger
	seenNodes    map[peer.ID]bool
	fileLogger   *os.File
	out          chan peer.AddrInfo
}

func NewDiscoveryV5(pk *ecdsa.PrivateKey, discConfig *config.DiscConfig) (*DiscoveryV5, error) {
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

	udpAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(discConfig.IP, string(discConfig.UDP)))

	_, exists := os.LookupEnv("FLY_APP_NAME")
	if exists {
		udpAddr, err = net.ResolveUDPAddr("udp", net.JoinHostPort("fly-global-services", string(discConfig.UDP)))
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

	file, err := os.Create(discConfig.LogPath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create log file")
	}

	return &DiscoveryV5{
		Dv5Listener:  listener,
		FilterDigest: discConfig.ForkDigest,
		log:          log,
		seenNodes:    make(map[peer.ID]bool),
		fileLogger:   file,
		out:          make(chan peer.AddrInfo),
	}, nil
}

func (d *DiscoveryV5) Serve(ctx context.Context) error {
	d.log.Info().Msg("Starting discv5 listener")

	// Start iterating over randomly discovered nodes
	iter := d.Dv5Listener.RandomNodes()

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

				if hInfo != nil && !d.seenNodes[hInfo.ID] {
					d.out <- peer.AddrInfo{
						ID:    hInfo.ID,
						Addrs: hInfo.MAddrs,
					}

					d.log.Info().
						Str("ID", hInfo.ID.String()).
						Str("IP", hInfo.IP).
						Int("Port", hInfo.Port).
						Str("ENR", node.String()).
						Any("Maddr", hInfo.MAddrs).
						Any("Attnets", hInfo.Attr[EnrAttnetsAttribute]).
						Any("AttnetsNum", hInfo.Attr[EnrAttnetsNumAttribute]).
						Msg("Discovered new node")

					// Log to file
					fmt.Fprintf(d.fileLogger, "%s ID: %s, IP: %s, Port: %d, ENR: %s, Maddr: %v, Attnets: %v, AttnetsNum: %v\n",
						time.Now().Format(time.RFC3339), hInfo.ID.String(), hInfo.IP, hInfo.Port, node.String(), hInfo.MAddrs, hInfo.Attr[EnrAttnetsAttribute], hInfo.Attr[EnrAttnetsNumAttribute])

					d.seenNodes[hInfo.ID] = true
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
			fmt.Println("Unable to init HostInfo with folling Option", opt)
		}
	}

	return hInfo
}
