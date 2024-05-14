package discv5

import (
	"context"
	"testing"
	"time"

	"github.com/chainbound/valtrack/config"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestSingleDiscoveryV5(t *testing.T) {
	pk, _ := crypto.GenerateKey()

	disc, err := NewDiscoveryV5(pk, &config.DefaultConfig)

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
