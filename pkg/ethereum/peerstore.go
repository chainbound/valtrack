package ethereum

import (
	"sync"
	"time"

	"github.com/chainbound/valtrack/types"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	eth "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

// ConnectionState signals the capacity for a connection with a given node.
// It is used to signal to services and other peers whether a node is reachable.
type ConnectionState int

const (
	// NotConnected means no connection to peer, and no extra information (default)
	NotConnected ConnectionState = iota

	// Connecting means we are in the process of connecting to this peer
	Connecting
)

const EPOCH_DURATION = 12 * 32 * time.Second

// PeerInfo contains information about a peer
type PeerInfo struct {
	id         peer.ID
	enode      enode.Node
	lastSeen   time.Time
	remoteAddr multiaddr.Multiaddr

	status            *eth.Status
	metadata          *eth.MetaDataV1 // Only interested in metadataV1
	subscribedSubnets []int64
	clientVersion     string

	state          ConnectionState
	lastErr        error
	backoffCounter uint32
}

func (p *PeerInfo) IntoMetadataEvent() *types.MetadataReceivedEvent {
	simpleMetadata := &types.SimpleMetaData{
		SeqNumber: int64(p.metadata.SeqNumber),
		Attnets:   p.metadata.Attnets,
		Syncnets:  p.metadata.Syncnets,
	}

	return &types.MetadataReceivedEvent{
		ENR:           p.enode.String(),
		ID:            p.id.String(),
		Multiaddr:     p.remoteAddr.String(),
		ClientVersion: p.clientVersion,
		MetaData:      simpleMetadata,
		// `epoch = slot // SLOTS_PER_EPOCH`
		Epoch: int(p.status.HeadSlot) / 32,
		// These should be set later
		CrawlerID:         "",
		CrawlerLoc:        "",
		SubscribedSubnets: p.subscribedSubnets,
		Timestamp:         p.lastSeen.UnixMilli(),
	}
}

type Peerstore struct {
	sync.RWMutex

	peers          map[peer.ID]*PeerInfo
	defaultBackoff time.Duration
}

// NewPeerstore creates a new peerstore
func NewPeerstore(defaultBackoff time.Duration) *Peerstore {
	return &Peerstore{
		peers:          make(map[peer.ID]*PeerInfo),
		defaultBackoff: defaultBackoff,
	}
}

func (p *Peerstore) Get(id peer.ID) *PeerInfo {
	p.RLock()
	defer p.RUnlock()

	return p.peers[id]
}

// Insert inserts a peer into the peerstore in the `NotConnected` state.
func (p *Peerstore) Insert(id peer.ID, addr multiaddr.Multiaddr, enode enode.Node) {
	p.Lock()
	defer p.Unlock()

	p.peers[id] = &PeerInfo{
		enode:      enode,
		id:         id,
		remoteAddr: addr,
		lastSeen:   time.Now(),
	}
}

func (p *Peerstore) SetState(id peer.ID, state ConnectionState) {
	p.Lock()
	defer p.Unlock()

	if info, ok := p.peers[id]; ok {
		info.state = state
		info.lastSeen = time.Now()
	} else {
		panic("peerstore: SetState: peer not found")
	}
}

func (p *Peerstore) State(id peer.ID) ConnectionState {
	p.RLock()
	defer p.RUnlock()

	if p.peers[id] == nil {
		return NotConnected
	}

	return p.peers[id].state
}

// SetBackoff marks the peer as backed off, increments the backoff counter
// and records the last error. This should only be used on outbound connections.
func (p *Peerstore) SetBackoff(id peer.ID, err error) uint32 {
	p.Lock()
	defer p.Unlock()

	if info, ok := p.peers[id]; ok {
		info.lastErr = err
		info.backoffCounter++
		info.lastSeen = time.Now()

		return info.backoffCounter
	} else {
		panic("peerstore: SetErr: peer not found")
	}
}

func (p *Peerstore) IsBackedOff(id peer.ID) bool {
	p.RLock()
	defer p.RUnlock()

	if info, ok := p.peers[id]; ok {
		return info.backoffCounter > 0 && time.Since(info.lastSeen) < p.defaultBackoff
	}

	return false
}

// Reset MUST be called every time we've had a succesful handshake & metadata exchange with a peer.
// It will reset the backoff counter and the last error, and remove the last status & metadata
func (p *Peerstore) Reset(id peer.ID) {
	p.Lock()
	defer p.Unlock()

	if info, ok := p.peers[id]; ok {
		info.backoffCounter = 0
		info.lastSeen = time.Now()
		info.lastErr = nil
		info.state = NotConnected

		// Remove status!
		info.status = nil
		info.metadata = nil
		info.subscribedSubnets = []int64{}
	} else {
		panic("peerstore: ResetBackoff: peer not found")
	}
}

func (p *Peerstore) AddSubscribedSubnets(id peer.ID, subnet ...int64) {
	p.Lock()
	defer p.Unlock()

	if info, ok := p.peers[id]; ok {
		info.subscribedSubnets = append(info.subscribedSubnets, subnet...)
		info.lastSeen = time.Now()
	} else {
		panic("peerstore: AddSubscribedSubnet: peer not found")
	}
}

func (p *Peerstore) SetStatus(id peer.ID, status *eth.Status) {
	p.Lock()
	defer p.Unlock()

	if info, ok := p.peers[id]; ok {
		info.status = status
		info.lastSeen = time.Now()
	} else {
		panic("peerstore: SetStatus: peer not found")
	}
}

func (p *Peerstore) Status(id peer.ID) *eth.Status {
	p.RLock()
	defer p.RUnlock()

	if p.peers[id] == nil {
		return nil
	}

	return p.peers[id].status
}

func (p *Peerstore) SetMetadata(id peer.ID, metadata *eth.MetaDataV1) {
	p.Lock()
	defer p.Unlock()

	if info, ok := p.peers[id]; ok {
		info.metadata = metadata
		info.lastSeen = time.Now()
	} else {
		panic("peerstore: SetMetadata: peer not found")
	}
}

func (p *Peerstore) SetClientVersion(id peer.ID, version string) {
	p.Lock()
	defer p.Unlock()

	if info, ok := p.peers[id]; ok {
		info.clientVersion = version
	} else {
		panic("peerstore: SetClientVersion: peer not found")
	}
}

func (p *Peerstore) LastErr(id peer.ID) error {
	p.RLock()
	defer p.RUnlock()

	if p.peers[id] == nil {
		return nil
	}

	return p.peers[id].lastErr
}

func (p *Peerstore) Size() int {
	p.RLock()
	defer p.RUnlock()

	return len(p.peers)
}

// PeersToReconnect returns the peers that we need to reconnect to. This includes
// the not connected peers that have an expired backoff, but also the succesfully connected
// peers that have not been seen for 1 epoch.
func (p *Peerstore) PeersToReconnect() []peer.AddrInfo {
	var peers []peer.AddrInfo

	p.RLock()
	defer p.RUnlock()

	for id, info := range p.peers {
		if info.state == NotConnected {
			// If the backoff expired, reconnect
			if info.backoffCounter > 0 && time.Since(info.lastSeen) > p.defaultBackoff*(time.Second*time.Duration(info.backoffCounter)) {
				peers = append(peers, peer.AddrInfo{ID: id, Addrs: []multiaddr.Multiaddr{info.remoteAddr}})
			}

			// If the last error was nil and we haven't seen the peer for an epoch, reconnect
			if info.lastErr == nil && time.Since(info.lastSeen) > EPOCH_DURATION {
				peers = append(peers, peer.AddrInfo{ID: id, Addrs: []multiaddr.Multiaddr{info.remoteAddr}})
			}

		}
	}

	return peers
}
