package discv5

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/chainbound/valtrack/config"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// TODO: Implement the test
func TestSingleDiscoveryV5(t *testing.T) {
	// Generate a new key
	key, err := crypto.GenerateKey()
	ethDB, err := enode.OpenDB("")
	if err != nil {
		log.Fatal(err)
	}

	// Generate a new enode
	node := enode.NewLocalNode(ethDB, key)

	// Create a new DiscoveryV5
	discv5, err := NewDiscoveryV5(context.Background(), "any", 30303, "forkDigest", config.GetEthereumBootnodes())
	if discv5 == nil {
		t.Fatal("Failed to create DiscoveryV5")
	}

	// Run the DiscoveryV5
	go discv5.Run()

	// Wait for 5 seconds
	time.Sleep(5 * time.Second)

	// Stop the DiscoveryV5
	discv5.Stop()
}
