package discovery

import (
	"context"

	"github.com/chainbound/valtrack/config"
	"github.com/chainbound/valtrack/pkg/crawler"
)

type Discovery struct {
	discv5 *crawler.Crawler
}

func NewDiscovery(ctx context.Context) error {
	conf := config.DefaultConfig

	crawlr, err := crawler.NewDiscoveryV5(ctx, conf.DBPath, conf.UDP, conf.ForkDigest, config.GetEthereumBootnodes())

	if err != nil {
		return err
	}

	return crawlr.Start()
}
