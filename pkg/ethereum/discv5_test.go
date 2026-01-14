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

	cfg := config.NewDefaultDiscConfig()
	disc, err := NewDiscoveryV5(pk, &cfg)

	if err != nil {
		t.FailNow()
	}

	_ = disc.Serve(context.Background())

	timeout := time.After(10 * time.Second)

	<-timeout
	t.FailNow()
}
