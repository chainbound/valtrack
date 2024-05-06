package main

import (
	"context"
	"fmt"

	"github.com/chainbound/valtrack/discovery"
)

func main() {
	fmt.Println("Starting Valtrack")

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
