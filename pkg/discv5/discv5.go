package discv5

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"net"

	glog "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

type Discv5Service struct {
	ctx context.Context

	ethNode     *enode.LocalNode
	dv5Listener *discover.UDPv5
	iterator    enode.Iterator
	enrHandler  func(*enode.Node)
}

func NewService(
	ctx context.Context,
	port int,
	privkey *ecdsa.PrivateKey,
	ethNode *enode.LocalNode,
	bootnodes []*enode.Node,
	enrHandler func(*enode.Node)) (*Discv5Service, error) {

	if len(bootnodes) == 0 {
		return nil, errors.New("unable to start dv5 peer discovery, no bootnodes provided")
	}

	// udp address to listen
	udpAddr := &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: port,
	}

	// start listening and create a connection object
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	// Set custom logger for the discovery5 service (Debug)
	gethLog := glog.New()

	// configuration of the discovery5
	cfg := discover.Config{
		PrivateKey:   privkey,
		NetRestrict:  nil,
		Bootnodes:    bootnodes,
		Unhandled:    nil, // Not used in dv5
		Log:          gethLog,
		ValidSchemes: enode.ValidSchemes,
	}

	// start the discovery5 service and listen using the given connection
	dv5Listener, err := discover.ListenV5(conn, ethNode, cfg)
	if err != nil {
		return nil, err
	}

	iterator := dv5Listener.RandomNodes()

	return &Discv5Service{
		ctx:         ctx,
		ethNode:     ethNode,
		dv5Listener: dv5Listener,
		iterator:    iterator,
		enrHandler:  enrHandler,
	}, nil
}

func (dv5 *Discv5Service) Run() {
	// convert syncronous RandomNodes into asyc

	for {
		// check if the context is still up
		if err := dv5.ctx.Err(); err != nil {
			break
		}

		if dv5.iterator.Next() {
			newNode := dv5.iterator.Node()

			dv5.enrHandler(newNode)
		}

	}
}
