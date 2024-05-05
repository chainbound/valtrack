package utils

import (
	"crypto/ecdsa"
	"crypto/rand"

	gcrypto "github.com/ethereum/go-ethereum/crypto"
)

func GenNewPrivKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
}
