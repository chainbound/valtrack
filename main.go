package main

import (
	"context"
	"fmt"

	cmd "github.com/chainbound/valtrack/discovery"
	cli "github.com/urfave/cli/v2"
)

func main() {
	fmt.Println("Starting Valtrack")

	// disc, err := discovery.NewDiscovery(context.Background())
	// if err != nil {
	// 	panic(err)
	// }

	// nodes, err := disc.Start()
	// if err != nil {
	// 	panic(err)
	// }

	// for range nodes {
	// }
	lightCrawler := cli.App{
		Name:      "CliName",
		Usage:     "This tool uses Ethereum's peer-discovery protocol to measure the size of the Ethereum network (testnets included)",
		UsageText: "eth-light-crawler [subcommands] [arguments]",
		Commands: []*cli.Command{
			cmd.Discovery5,
		},
	}

	err := lightCrawler.RunContext(context.Background(), []string{"eth-light-crawler", "discv5"})
	if err != nil {
		fmt.Println("error running %s - %s", "CliName", err.Error())
		// panic(err)
	}
}
