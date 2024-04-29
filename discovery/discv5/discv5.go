package discv5

import (
	"context"
	"net"

	"github.com/chainbound/valtrack/log"
	"github.com/rs/zerolog"

	"github.com/ethereum/go-ethereum/crypto"
	glog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

type DiscoveryV5 struct {
	listener *discover.UDPv5
	log      zerolog.Logger
}

func NewDiscoveryV5(node *enode.LocalNode, port int, bootnodes []*enode.Node) *DiscoveryV5 {
	// New geth logger at debug level
	gethlog := glog.New()

	log := log.NewLogger("discv5")

	discKey, err := crypto.GenerateKey()
	if err != nil {
		log.Panic().Err(err).Msg("Failed to generate discv5 key")
	}

	cfg := discover.Config{
		PrivateKey:   discKey,
		NetRestrict:  nil,
		Unhandled:    nil,
		Bootnodes:    bootnodes,
		Log:          gethlog,
		ValidSchemes: enode.ValidSchemes,
	}

	// udp address to listen
	udpAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: port,
	}

	// start listening and create a connection object
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to listen on UDP")
	}

	listener, err := discover.ListenV5(conn, node, cfg)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to start discv5 listener")
	}

	return &DiscoveryV5{
		listener: listener,
		log:      log,
	}
}

func (d *DiscoveryV5) Start(ctx context.Context) (<-chan *enode.Node, error) {
	d.log.Info().Msg("Starting discv5 listener")

	ch := make(chan *enode.Node)

	// Start iterating over randomly discovered nodes
	iter := d.listener.RandomNodes()

	go func() {
		for iter.Next() {
			node := iter.Node()
			d.log.Info().Str("node", node.String()).Msg("Discovered new node")

			ch <- node
		}
	}()

	return ch, nil
}
