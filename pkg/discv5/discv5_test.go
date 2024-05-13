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

func TestSingleDiscoveryV5(t *testing.T) {
	pk, _ := crypto.GenerateKey()
	ethDB, err := enode.OpenDB("")
	if err != nil {
		log.Panicf("Could not create local DB %s", err)
	}

	disc, err := NewDiscoveryV5(30303, pk, enode.NewLocalNode(ethDB, pk), "0x6a95a1a9", "nodes.log", config.GetEthereumBootnodes())

	if err != nil {
		t.FailNow()
	}

	nodes, _ := disc.Start(context.Background())

	timeout := time.After(10 * time.Second)

	select {
	case <-timeout:
		t.FailNow()
	case <-nodes:
		return
	}
}
