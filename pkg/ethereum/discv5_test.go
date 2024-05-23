package ethereum

import (
	"context"
	"testing"
	"time"

	"github.com/chainbound/valtrack/config"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestSingleDiscoveryV5(t *testing.T) {
	pk, _ := crypto.GenerateKey()

	disc, err := NewDiscoveryV5(pk, &config.DefaultDiscConfig)

	if err != nil {
		t.FailNow()
	}

	_ = disc.Serve(context.Background())

	timeout := time.After(10 * time.Second)

	<-timeout
	t.FailNow()
}
