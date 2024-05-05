package discv5

import (
	"crypto/ecdsa"
	"encoding/binary"
	"math/bits"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/migalabs/armiarma/src/utils"
	"github.com/protolambda/zrnt/eth2/beacon/common"
)

type EnrNode struct {
	Timestamp time.Time
	ID        enode.ID
	IP        net.IP
	Seq       uint64
	UDP       int
	TCP       int
	Pubkey    *ecdsa.PublicKey
	Eth2Data  *common.Eth2Data
	Attnets   *Attnets
}

func NewEnrNode(nodeID enode.ID) *EnrNode {

	return &EnrNode{
		Timestamp: time.Now(),
		ID:        nodeID,
		Pubkey:    new(ecdsa.PublicKey),
		Eth2Data:  new(common.Eth2Data),
		Attnets:   new(Attnets),
	}
}

type Attnets struct {
	Raw       utils.AttnetsENREntry
	NetNumber int
}

func ParseAttnets(node enode.Node) (attnets *Attnets, exists bool, err error) {
	att := new(Attnets)

	attEntry := new(utils.AttnetsENREntry)

	err = node.Load(attEntry)
	if err != nil {
		return att, false, nil
	}
	att.Raw = *attEntry

	// count the number of bits in the Attnets
	att.NetNumber = CountBits(att.Raw[:])
	return att, true, nil
}

func CountBits(byteArr []byte) int {
	rawInt := binary.BigEndian.Uint64(byteArr)
	return bits.OnesCount64(rawInt)
}
