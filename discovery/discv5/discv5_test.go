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

	disc := NewDiscoveryV5(enode.NewLocalNode(ethDB, pk), 30303, config.GetEthereumBootnodes())

	nodes, _ := disc.Start(context.Background())

	timeout := time.After(10 * time.Second)

	select {
	case <-timeout:
		t.FailNow()
	case <-nodes:
		return
	}
}
