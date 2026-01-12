package discovery

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"

	"github.com/chainbound/valtrack/config"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/chainbound/valtrack/pkg/ethereum"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
)

type Discovery struct {
	node *ethereum.Node
}

func NewDiscovery(natsURL string) (*Discovery, error) {
	var privBytes []byte

	key, err := ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	privBytes = gcrypto.FromECDSA(key)
	privateKey := (*crypto.Secp256k1PrivateKey)(secp256k1.PrivKeyFromBytes(privBytes))

	nodeConfig := &config.DefaultNodeConfig
	nodeConfig.PrivateKey = privateKey
	nodeConfig.BeaconConfig = params.MainnetConfig()
	nodeConfig.NatsURL = natsURL

	n, err := ethereum.NewNode(nodeConfig)

	return &Discovery{
		node: n,
	}, err
}

func (d *Discovery) Start(ctx context.Context) error {
	return d.node.Start(ctx)
}
