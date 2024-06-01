package config

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/network/forks"
	pb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

// Bootnodes
var ethBootnodes []string = params.BeaconNetworkConfig().BootstrapNodes

// GetEthereumBootnodes returns the default Ethereum bootnodes in enode format.
func GetEthereumBootnodes() []*enode.Node {
	bootnodes := make([]*enode.Node, len(ethBootnodes))
	for i, enr := range ethBootnodes {
		node, err := enode.Parse(enode.ValidSchemes, enr)
		if err != nil {
			panic(err)
		}
		bootnodes[i] = node
	}
	return bootnodes
}

type DiscConfig struct {
	IP         string
	UDP        int
	TCP        int
	DBPath     string
	ForkDigest [4]byte
	LogPath    string
	Bootnodes  []*enode.Node
	NatsURL    string
}

var DefaultDiscConfig DiscConfig = DiscConfig{
	IP:         "0.0.0.0",
	UDP:        9000,
	TCP:        9000,
	DBPath:     "",
	ForkDigest: [4]byte{0x6a, 0x95, 0xa1, 0xa9},
	LogPath:    "discovery_events.log",
	Bootnodes:  GetEthereumBootnodes(),
}

func (d *DiscConfig) Eth2EnrEntry() (enr.Entry, error) {
	// currentSlot := slots.Since(genesisTime)
	// currentEpoch := slots.ToEpoch(currentSlot)

	// TODO: not hardcoded, use timestamp to calculate
	nextForkVersion, nextForkEpoch, err := forks.NextForkData(286168)
	if err != nil {
		return nil, fmt.Errorf("calculate next fork data: %w", err)
	}

	enrForkID := &pb.ENRForkID{
		CurrentForkDigest: d.ForkDigest[:],
		NextForkVersion:   nextForkVersion[:],
		NextForkEpoch:     nextForkEpoch,
	}

	enc, err := enrForkID.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("marshal enr fork id: %w", err)
	}

	return enr.WithEntry("eth2", enc), nil
}

// NodeConfig holds additional configuration options for the node.
type NodeConfig struct {
	PrivateKey        *crypto.Secp256k1PrivateKey
	BeaconConfig      *params.BeaconChainConfig
	ForkDigest        [4]byte
	Encoder           encoder.NetworkEncoding
	DialTimeout       time.Duration
	ConcurrentDialers int
	IP                string
	Port              int
	NatsURL           string
	LogPath           string
}

var DefaultNodeConfig NodeConfig = NodeConfig{
	PrivateKey:        nil,
	BeaconConfig:      nil,
	ForkDigest:        [4]byte{0x6a, 0x95, 0xa1, 0xa9},
	Encoder:           encoder.SszNetworkEncoder{},
	DialTimeout:       10 * time.Second,
	ConcurrentDialers: 64,
	IP:                "0.0.0.0",
	Port:              9000,
	LogPath:           "metadata_events.log",
}
