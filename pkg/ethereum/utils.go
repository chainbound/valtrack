package ethereum

import (
	"fmt"
	"net"
	"os"

	ma "github.com/multiformats/go-multiaddr"
)

func getCrawlerLocation() string {
	if region := os.Getenv("FLY_REGION"); region != "" {
		return region
	}
	return "FLY_REGION"
}

func getCrawlerMachineID() string {
	if id := os.Getenv("FLY_MACHINE_ID"); id != "" {
		return id
	}
	return "FLY_MACHINE_ID"
}

// MaddrFrom takes in an ip address string and port to produce a go multiaddr format.
func MaddrFrom(ip string, port uint) (ma.Multiaddr, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil, fmt.Errorf("invalid IP address: %s", ip)
	} else if parsed.To4() != nil {
		return ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ip, port))
	} else if parsed.To16() != nil {
		return ma.NewMultiaddr(fmt.Sprintf("/ip6/%s/tcp/%d", ip, port))
	} else {
		return nil, fmt.Errorf("invalid IP address: %s", ip)
	}
}
