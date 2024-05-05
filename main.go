package main

import (
	"context"
	"fmt"

	"github.com/chainbound/valtrack/discovery"
)

func main() {
	fmt.Println("Starting Valtrack")

	err := discovery.NewDiscovery(context.Background())
	if err != nil {
		panic(err)
	}
}
