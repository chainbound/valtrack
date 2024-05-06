package main

import (
	"context"

	"github.com/chainbound/valtrack/discovery"
)

func main() {
	disc, err := discovery.NewDiscovery(context.Background())
	if err != nil {
		panic(err)
	}

	nodes, err := disc.Start()
	if err != nil {
		panic(err)
	}

	for range nodes {
	}
}
