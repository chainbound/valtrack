package discovery

import (
	"context"
	"fmt"

	"github.com/chainbound/valtrack/config"
	"github.com/chainbound/valtrack/discovery/discv5"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

type Discovery struct {
	discv5 *discv5.DiscoveryV5
}

func NewDiscovery(port int) (*Discovery, error) {
	pk, _ := crypto.GenerateKey()
	ethDB, err := enode.OpenDB("")
	if err != nil {
		return nil, fmt.Errorf("could not create local DB %w", err)
	}

	disc := discv5.NewDiscoveryV5(enode.NewLocalNode(ethDB, pk), port, config.GetEthereumBootnodes())

	return &Discovery{
		discv5: disc,
	}, nil
}

func (d *Discovery) Start(ctx context.Context) (<-chan *enode.Node, error) {
	return d.discv5.Start(ctx)
}
