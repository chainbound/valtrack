package crawler

import (
	"context"
	"encoding/hex"

	"github.com/chainbound/valtrack/log"
	"github.com/pkg/errors"
	"github.com/protolambda/zrnt/eth2/beacon/common"
	"github.com/rs/zerolog"

	"github.com/chainbound/valtrack/pkg/discv5"
	ut "github.com/chainbound/valtrack/pkg/utils"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/migalabs/armiarma/src/utils"
)

type DiscoveryV5 struct {
	listener *discover.UDPv5
	log      zerolog.Logger
}

type Crawler struct {
	ctx context.Context

	ethNode       *enode.LocalNode
	discv5Service *discv5.Discv5Service
}
func NewDiscoveryV5(ctx context.Context, dbPath string, port int, forkDigest string, bootnodes []*enode.Node) (*Crawler, error) {
	log := log.NewLogger("discv5")

	discKey, err := ut.GenNewPrivKey()
	if err != nil {
		log.Panic().Err(err).Msg("Failed to generate discv5 key")
	}

	// Init the ethereum peerstore
	enodeDB, err := enode.OpenDB(dbPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open the enode db")
	}

	// Generate a Enode with custom ENR
	node := enode.NewLocalNode(enodeDB, discKey)

	// define the Handler for when we discover a new ENR
	enrHandler := func(node *enode.Node) {
		// check if the node is valid
		err := node.ValidateComplete()
		if err != nil {
			log.Warn().Err(err).Msg("error validating the ENR - ")
		}
		// extract the information from the enode
		id := node.ID()
		seq := node.Seq()
		ip := node.IP()
		udp := node.UDP()
		tcp := node.TCP()
		pubkey := node.Pubkey()

		// Retrieve the Fork Digest and the attestnets
		eth2Data, ok, err := utils.ParseNodeEth2Data(*node)
		if !ok {
			eth2Data = new(common.Eth2Data)
		} else {
			if err != nil {
				log.Error().Err(err).Msg("eth2 data parsing error")
				// eth2Data = new(common.Eth2Data)
			}
		}

		attnets, ok, err := discv5.ParseAttnets(*node)
		if !ok {
			attnets = new(discv5.Attnets)
		} else {
			if err != nil {
				log.Error().Err(err).Msg("attnets parsing err")
				// attnets = new(discv5.Attnets)
			}
		}
		// create a new ENR node
		enrNode := discv5.NewEnrNode(id)

		// add all the fields from the CL network
		enrNode.Seq = seq
		enrNode.IP = ip
		enrNode.TCP = tcp
		enrNode.UDP = udp
		enrNode.Pubkey = pubkey
		enrNode.Eth2Data = eth2Data
		enrNode.Attnets = attnets

		// check if there is any fork digest filter only if the flag All is not set

		if eth2Data.ForkDigest.String() != forkDigest {
			log.Info().Str("fork_digest", eth2Data.ForkDigest.String()).Msg("Fork digest does not match")
		} else {
			log.Info().Str("node_id", id.String()).Str("ip", ip.String()).Int("udp", udp).Int("tcp", tcp).Str("fork_digest", eth2Data.ForkDigest.String()).Str("fork_epoch", eth2Data.NextForkEpoch.String()).Str("attnets", hex.EncodeToString(attnets.Raw[:])).Int("att_number", attnets.NetNumber).Str("enr", node.String()).Msg("Eth node found")
		}

		// logrus.WithFields(logrus.Fields{
		// 	"node_id":     id,
		// 	"ip":          ip,
		// 	"udp":         udp,
		// 	"tcp":         tcp,
		// 	"fork_digest": eth2Data.ForkDigest,
		// 	"fork_epoch":  eth2Data.NextForkEpoch,
		// 	"attnets":     hex.EncodeToString(attnets.Raw[:]),
		// 	"att_number":  attnets.NetNumber,
		// 	"enr":         node.String(),
		// }).Info("Eth node found")
	}

	discv5Serv, err := discv5.NewService(ctx, port, discKey, node, bootnodes, enrHandler)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate the discv5 service")
	}

	return &Crawler{
		ctx:           ctx,
		ethNode:       node,
		discv5Service: discv5Serv,
	}, nil
}

func (c *Crawler) Run() error {
	// if duration has not been set, run until Crtl+C
	c.discv5Service.Run()
	// otherwise, run it for X time

	return nil
}

func (c *Crawler) ID() string {
	return c.ethNode.ID().String()
}
