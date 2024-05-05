package cmd

import (
	"fmt"

	"github.com/chainbound/valtrack/config"
	"github.com/chainbound/valtrack/pkg/crawler"
	"github.com/urfave/cli/v2"
)

type Discovery struct {
	discv5 *crawler.Crawler
}

var Discovery5 = &cli.Command{
	Name:   "discv5",
	Usage:  "crawl Ethereum's public DHT thought the Discovery 5.1 protocol",
	Action: NewDiscovery,
}

func NewDiscovery(ctx *cli.Context) (error) {
	conf := config.DefaultConfig

	crawlr, err := crawler.NewDiscoveryV5(ctx.Context, conf.DBPath, conf.UDP, conf.ForkDigest, config.GetEthereumBootnodes())

	if err != nil {
		return err
	}
	fmt.Println(crawlr.ID())
	// return &Discovery{
	// 	discv5: valtrack,
	// }, nil
	return crawlr.Run()
}

func (d *Discovery) Start() {
	d.discv5.Run()
}
