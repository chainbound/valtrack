package discovery

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/chainbound/valtrack/config"

	"github.com/chainbound/valtrack/pkg/ethereum"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
)

type Discovery struct {
	node *ethereum.Node
}

func NewDiscovery() (*Discovery, error) {
	var privBytes []byte

	key, err := ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	privBytes = gcrypto.FromECDSA(key)
	if len(privBytes) != secp256k1.PrivKeyBytesLen {
		return nil, fmt.Errorf("expected secp256k1 data size to be %d", secp256k1.PrivKeyBytesLen)
	}

	privateKey := (*crypto.Secp256k1PrivateKey)(secp256k1.PrivKeyFromBytes(privBytes))

	nodeConfig := &config.NodeConfig{
		PrivateKey:   privateKey,
		BeaconConfig: params.MainnetConfig(),
		ForkDigest:   [4]byte{0x6a, 0x95, 0xa1, 0xa9}, // Mainnet fork digest
		Encoder:      encoder.SszNetworkEncoder{},
		DialTimeout:  5 * time.Second,
	}

	n, err := ethereum.NewNode(nodeConfig)

	return &Discovery{
		node: n,
	}, err
}

func (d *Discovery) Start(ctx context.Context) error {
	return d.node.Start(ctx)
}
