package discovery

import (
	"context"

	"github.com/chainbound/valtrack/config"
	"github.com/chainbound/valtrack/pkg/discv5"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/pkg/errors"
)

type Discovery struct {
	discv5 *discv5.DiscoveryV5
}

func NewDiscovery() (*Discovery, error) {
	conf := config.DefaultConfig

	discKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to generate discv5 key")
	}

	// Init the ethereum peerstore
	enodeDB, err := enode.OpenDB(conf.DBPath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to open the DB")
	}

	// Generate a Enode with custom ENR
	ethNode := enode.NewLocalNode(enodeDB, discKey)

	disc, err := discv5.NewDiscoveryV5(conf.UDP, discKey, ethNode, conf.ForkDigest, config.GetEthereumBootnodes())

	if err != nil {
		return nil, errors.Wrap(err, "Failed to generate the discv5 service")
	}

	return &Discovery{
		discv5: disc,
	}, nil
}

func (d *Discovery) Start(ctx context.Context) (chan *discv5.HostInfo, error) {
	return d.discv5.Start(ctx)
}
