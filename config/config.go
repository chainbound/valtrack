package config

import (
	"fmt"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	pb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/time/slots"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/crypto"
)

// Ethereum mainnet genesis time: December 1, 2020 12:00:23 UTC
var mainnetGenesisTime = time.Unix(1606824023, 0)

// CurrentForkDigest returns the current fork digest based on the current epoch.
func CurrentForkDigest() [4]byte {
	currentEpoch := slots.EpochsSinceGenesis(mainnetGenesisTime)
	return params.ForkDigest(currentEpoch)
}

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

// NewDefaultDiscConfig returns a DiscConfig with default values and the current fork digest.
func NewDefaultDiscConfig() DiscConfig {
	return DiscConfig{
		IP:         "0.0.0.0",
		UDP:        9000,
		TCP:        9000,
		DBPath:     "",
		ForkDigest: CurrentForkDigest(),
		LogPath:    "discovery_events.log",
		Bootnodes:  GetEthereumBootnodes(),
	}
}

func (d *DiscConfig) Eth2EnrEntry() (enr.Entry, error) {
	currentEpoch := slots.EpochsSinceGenesis(mainnetGenesisTime)
	currentForkDigest := params.ForkDigest(currentEpoch)
	nextForkVersion, nextForkEpoch := params.NextForkData(currentEpoch)

	enrForkID := &pb.ENRForkID{
		CurrentForkDigest: currentForkDigest[:],
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

// NewDefaultNodeConfig returns a NodeConfig with default values and the current fork digest.
func NewDefaultNodeConfig() NodeConfig {
	return NodeConfig{
		PrivateKey:        nil,
		BeaconConfig:      nil,
		ForkDigest:        CurrentForkDigest(),
		Encoder:           encoder.SszNetworkEncoder{},
		DialTimeout:       10 * time.Second,
		ConcurrentDialers: 64,
		IP:                "0.0.0.0",
		Port:              9000,
		LogPath:           "metadata_events.log",
	}
}
