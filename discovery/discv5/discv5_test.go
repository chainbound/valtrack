package discv5

import (
	"context"
	"log"
	"testing"

	"github.com/chainbound/valtrack/config"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func TestDiscovery(t *testing.T) {
	pk, _ := crypto.GenerateKey()
	ethDB, err := enode.OpenDB("")
	if err != nil {
		log.Panicf("Could not create local DB %s", err)
	}

	disc := NewDiscoveryV5(enode.NewLocalNode(ethDB, pk), 30303, config.GetEthereumBootnodes())

	nodes, _ := disc.Start(context.Background())

	for node := range nodes {
		t.Log(node)
	}
}
