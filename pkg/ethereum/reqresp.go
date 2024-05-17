package ethereum

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/chainbound/valtrack/log"
	ssz "github.com/ferranbt/fastssz"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/types"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	pb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

type ReqRespConfig struct {
	ForkDigest [4]byte
	Encoder    encoder.NetworkEncoding

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// ReqResp handles request-response operations for the node.
type ReqResp struct {
	host host.Host
	cfg  *ReqRespConfig

	// Node's delegate peer ID. NOT IN USE
	delegate peer.ID

	metaData   *pb.MetaDataV1
	metaDataMu sync.RWMutex

	status    *pb.Status
	statusMu  sync.RWMutex
	statusLim *rate.Limiter

	log zerolog.Logger
}

type ContextStreamHandler func(context.Context, network.Stream) error

func NewReqResp(h host.Host, cfg *ReqRespConfig) (*ReqResp, error) {
	if cfg == nil {
		return nil, fmt.Errorf("req resp server config must not be nil")
	}

	md := &pb.MetaDataV1{
		SeqNumber: 0,
		Attnets:   bitfield.NewBitvector64(),
		Syncnets:  bitfield.Bitvector4{byte(0x00)},
	}

	// fake to support all attnets
	for i := uint64(0); i < md.Attnets.Len(); i++ {
		md.Attnets.SetBitAt(i, true)
	}

	p := &ReqResp{
		host:      h,
		cfg:       cfg,
		metaData:  md,
		statusLim: rate.NewLimiter(1, 5),
		log:       log.NewLogger("reqresp"),
	}

	return p, nil
}

func (r *ReqResp) cpyStatus() *pb.Status {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()

	if r.status == nil {
		return nil
	}

	return &pb.Status{
		ForkDigest:     bytes.Clone(r.status.ForkDigest),
		FinalizedRoot:  bytes.Clone(r.status.FinalizedRoot),
		FinalizedEpoch: r.status.FinalizedEpoch,
		HeadRoot:       bytes.Clone(r.status.HeadRoot),
		HeadSlot:       r.status.HeadSlot,
	}
}

func (r *ReqResp) SetStatus(status *pb.Status) {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()

	// if the ForkDigest is not the same, we should drop updating the local status
	// TODO: this might be re-checked for hardforks (make the client resilient to them)
	if r.status != nil && !bytes.Equal(r.status.ForkDigest, status.ForkDigest) {
		return
	}

	// check if anything has changed. Prevents the below log message to pollute
	// the log output.
	if r.status != nil && bytes.Equal(r.status.ForkDigest, status.ForkDigest) &&
		bytes.Equal(r.status.FinalizedRoot, status.FinalizedRoot) &&
		r.status.FinalizedEpoch == status.FinalizedEpoch &&
		bytes.Equal(r.status.HeadRoot, status.HeadRoot) &&
		r.status.HeadSlot == status.HeadSlot {
		// nothing has changed -> return
		return
	}

	r.log.Info().
		Str("ForkDigest", hex.EncodeToString(status.ForkDigest)).
		Str("FinalizedRoot", hex.EncodeToString(status.FinalizedRoot)).
		Uint64("FinalizedEpoch", uint64(status.FinalizedEpoch)).
		Str("HeadRoot", hex.EncodeToString(status.HeadRoot)).
		Uint64("HeadSlot", uint64(status.HeadSlot)).
		Msg("Status updated")

	r.status = status
}

// RegisterHandlers registers all RPC handlers. It verifies that initial status and metadata are valid.
func (r *ReqResp) RegisterHandlers(ctx context.Context) error {
	fmt.Println("Registering RPC handlers")

	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	if r.status == nil {
		return fmt.Errorf("chain status is nil")
	}

	r.metaDataMu.RLock()
	defer r.metaDataMu.RUnlock()
	if r.metaData == nil {
		return fmt.Errorf("chain metadata is nil")
	}

	handlers := map[string]ContextStreamHandler{
		p2p.RPCPingTopicV1:     r.pingHandler,
		p2p.RPCGoodByeTopicV1:  r.goodbyeHandler,
		p2p.RPCStatusTopicV1:   r.statusHandler,
		p2p.RPCMetaDataTopicV1: r.metadataV1Handler,
		p2p.RPCMetaDataTopicV2: r.metadataV2Handler,
	}

	for id, handler := range handlers {
		protocolID := r.protocolID(id)
		r.log.Info().Str("protocol", string(protocolID)).Msg("Registering protocol handler")
		r.host.SetStreamHandler(protocolID, r.wrapStreamHandler(ctx, string(protocolID), handler))
	}

	return nil
}

func (r *ReqResp) protocolID(topic string) protocol.ID {
	return protocol.ID(topic + r.cfg.Encoder.ProtocolSuffix())
}

func (r *ReqResp) wrapStreamHandler(ctx context.Context, name string, handler ContextStreamHandler) network.StreamHandler {

	return func(s network.Stream) {
		// Extract agent version from peerstore, defaulting to "n.a." if not present.
		agentVersion, err := r.getAgentVersion(s.Conn().RemotePeer())
		if err != nil {
			agentVersion = "n.a."
		}

		r.log.Debug().Any("protocol", s.Protocol()).Str("peer", s.Conn().RemotePeer().String()).Msg("Stream Opened")

		// Ensure the stream is reset on handler exit, which is a no-op if the stream is already closed.
		defer s.Reset()

		// Execute the handler and measure execution time.
		start := time.Now()
		err = handler(ctx, s)
		if err != nil {
			r.log.Debug().Any("protocol", s.Protocol()).Str("peer", s.Conn().RemotePeer().String()).Str("agent", agentVersion).Err(err).Msg("failed handling rpc")
		}
		duration := time.Since(start)

		r.log.Debug().Any("protocol", s.Protocol()).Str("peer", s.Conn().RemotePeer().String()).Dur("duration", time.Duration(duration.Seconds())).Msg("Stream Closed")
	}
}

func (r *ReqResp) goodbyeHandler(ctx context.Context, stream network.Stream) error {
	req := primitives.SSZUint64(0)
	if err := r.readRequest(ctx, stream, &req); err != nil {
		return fmt.Errorf("read goodbye sequence number: %w", err)
	}

	msg, found := types.GoodbyeCodeMessages[req]
	if found {
		if _, err := r.host.Peerstore().Get(stream.Conn().RemotePeer(), peerstoreKeyMetadata); err == nil {
			r.log.Info().Str("peer", stream.Conn().RemotePeer().String()).Str("msg", msg).Msg("Received goodbye message")
		} else {
			r.log.Debug().Str("peer", stream.Conn().RemotePeer().String()).Str("msg", msg).Msg("Received goodbye message")
		}
	}

	return stream.Close()
}

func (r *ReqResp) statusHandler(ctx context.Context, stream network.Stream) error {
	// First, read the incoming status request.
	req := &pb.Status{}
	if err := r.readRequest(ctx, stream, req); err != nil {
		return fmt.Errorf("read status request: %w", err)
	}

	// Optionally, update local status if the request comes from a trusted source.
	if stream.Conn().RemotePeer() == r.delegate {
		r.SetStatus(req) // Assuming SetStatus safely updates the status considering thread safety.
	}

	// Fetch a copy of the local status to respond with.
	resp := r.cpyStatus()
	if resp == nil {
		return fmt.Errorf("local status is nil")
	}

	// NOTE: Hermes - ask the delegate node for the latest status
	// ask our delegate node for the latest status, using our known latest status
	// this is important because blindly forwarding the request from a remote peer
	// will lead to intermittent disconnects from the beacon node. The "trusted peer"
	// setting doesn't seem to apply if we send, e.g., a status payload with
	// a non-matching fork-digest or non-finalized root hash.

	// Respond with the local status.
	if err := r.writeResponse(ctx, stream, resp); err != nil {
		return fmt.Errorf("write status response: %w", err)
	}

	return stream.Close()
}

func (r *ReqResp) pingHandler(ctx context.Context, stream network.Stream) error {
	req := primitives.SSZUint64(0) // Assuming a predefined type for demonstration.
	if err := r.readRequest(ctx, stream, &req); err != nil {
		return fmt.Errorf("read ping sequence number: %w", err)
	}

	r.metaDataMu.RLock()
	seqNum := primitives.SSZUint64(r.metaData.SeqNumber)
	r.metaDataMu.RUnlock()

	if err := r.writeResponse(ctx, stream, &seqNum); err != nil {
		return fmt.Errorf("write ping response: %w", err)
	}

	return stream.Close()
}

func (r *ReqResp) metadataV1Handler(ctx context.Context, stream network.Stream) error {
	r.metaDataMu.RLock()
	metaData := &pb.MetaDataV0{
		SeqNumber: r.metaData.SeqNumber,
		Attnets:   r.metaData.Attnets,
	}
	r.metaDataMu.RUnlock()

	if err := r.writeResponse(ctx, stream, metaData); err != nil {
		return fmt.Errorf("write meta data v1: %w", err)
	}

	return stream.Close()
}

func (r *ReqResp) metadataV2Handler(ctx context.Context, stream network.Stream) error {
	r.metaDataMu.RLock()
	metaData := &pb.MetaDataV1{
		SeqNumber: r.metaData.SeqNumber,
		Attnets:   r.metaData.Attnets,
		Syncnets:  r.metaData.Syncnets,
	}
	r.metaDataMu.RUnlock()

	if err := r.writeResponse(ctx, stream, metaData); err != nil {
		return fmt.Errorf("write metadata response: %w", err)
	}

	return stream.Close()
}

// getAgentVersion retrieves the agent version from the peerstore for the given peer.
func (r *ReqResp) getAgentVersion(peerID peer.ID) (string, error) {
	rawVal, err := r.host.Peerstore().Get(peerID, "AgentVersion")
	if err != nil {
		return "", err
	}
	if av, ok := rawVal.(string); ok {
		return av, nil
	}
	return "", fmt.Errorf("agent version not found or invalid")
}

// Goodbye sends a goodbye request to the given peer.
func (r *ReqResp) Goodbye(ctx context.Context, pid peer.ID, code uint64) error {
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCGoodByeTopicV1))
	if err != nil {
		return fmt.Errorf("failed to open goodbye stream to peer %s: %w", pid, err)
	}
	defer stream.Close()

	req := primitives.SSZUint64(code)
	r.log.Debug().Str("peer", pid.String()).Any("req", req).Msg("Sending goodbye message")

	if err := r.writeRequest(ctx, stream, &req); err != nil {
		return fmt.Errorf("write goodbye request: %w", err)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close() // (both sides should actually be already closed)

	return nil
}

// Status sends a status request to the given peer.
func (r *ReqResp) Status(ctx context.Context, pid peer.ID) (status *pb.Status, err error) {
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCStatusTopicV1))
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	// actually write the data to the stream
	req := r.cpyStatus()
	if req == nil {
		return nil, fmt.Errorf("status unknown")
	}

	if err := r.writeRequest(ctx, stream, req); err != nil {
		return nil, fmt.Errorf("write status request: %w", err)
	}

	// read and decode status response
	resp := &pb.Status{}
	if err := r.readResponse(ctx, stream, resp); err != nil {
		return nil, fmt.Errorf("read status response: %w", err)
	}

	// if we requested the status from our delegate
	if stream.Conn().RemotePeer() == r.delegate {
		r.SetStatus(resp)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close() // (both sides should actually be already closed)

	return resp, nil
}

// Ping sends a ping request to the given peer.
func (r *ReqResp) Ping(ctx context.Context, pid peer.ID) error {
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCPingTopicV1))
	if err != nil {
		return fmt.Errorf("failed to open ping stream to peer %s: %w", pid, err)
	}
	defer stream.Close()

	r.metaDataMu.RLock()
	seqNum := r.metaData.SeqNumber
	r.metaDataMu.RUnlock()

	req := primitives.SSZUint64(seqNum)
	if err := r.writeRequest(ctx, stream, &req); err != nil {
		return fmt.Errorf("write ping request: %w", err)
	}

	// read and decode status response
	resp := new(primitives.SSZUint64)
	if err := r.readResponse(ctx, stream, resp); err != nil {
		return fmt.Errorf("read ping response: %w", err)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close() // (both sides should actually be already closed)

	return nil
}

// MetaData sends a metadata request to the given peer.
func (r *ReqResp) MetaData(ctx context.Context, pid peer.ID) (resp *pb.MetaDataV1, err error) {
	stream, err := r.host.NewStream(ctx, pid, r.protocolID(p2p.RPCMetaDataTopicV2))
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata stream to peer %s: %w", pid, err)
	}
	defer stream.Close()

	// read and decode status response
	resp = &pb.MetaDataV1{}
	if err := r.readResponse(ctx, stream, resp); err != nil {
		return resp, fmt.Errorf("read ping response: %w", err)
	}

	// we have the data that we want, so ignore error here
	_ = stream.Close() // (both sides should actually be already closed)

	return resp, nil
}

// readRequest reads a request from the given network stream and populates the
// data parameter with the decoded request. It also sets a read deadline on the
// stream and returns an error if it fails to do so. After reading the request,
// it closes the reading side of the stream and returns an error if it fails to
// do so.
func (r *ReqResp) readRequest(ctx context.Context, stream network.Stream, data ssz.Unmarshaler) (err error) {
	if err = stream.SetReadDeadline(time.Now().Add(r.cfg.ReadTimeout)); err != nil {
		return fmt.Errorf("failed setting read deadline on stream: %w", err)
	}

	if err = r.cfg.Encoder.DecodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read request data %T: %w", data, err)
	}

	if err = stream.CloseRead(); err != nil {
		return fmt.Errorf("failed to close reading side of stream: %w", err)
	}

	return nil
}

// readResponse differs from readRequest in first reading a single byte that
// indicates the response code before actually reading the payload data. It also
// handles the response code in case it is not 0 (which would indicate success).
func (r *ReqResp) readResponse(ctx context.Context, stream network.Stream, data ssz.Unmarshaler) (err error) {
	if err = stream.SetReadDeadline(time.Now().Add(r.cfg.ReadTimeout)); err != nil {
		return fmt.Errorf("failed setting read deadline on stream: %w", err)
	}

	code := make([]byte, 1)
	if _, err := io.ReadFull(stream, code); err != nil {
		return fmt.Errorf("failed reading response code: %w", err)
	}

	// code == 0 means success
	// code != 0 means error
	if int(code[0]) != 0 {
		errData, err := io.ReadAll(stream)
		if err != nil {
			return fmt.Errorf("failed reading error data (code %d): %w", int(code[0]), err)
		}

		return fmt.Errorf("received error response (code %d): %s", int(code[0]), string(errData))
	}

	if err = r.cfg.Encoder.DecodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read request data %T: %w", data, err)
	}

	if err = stream.CloseRead(); err != nil {
		return fmt.Errorf("failed to close reading side of stream: %w", err)
	}

	return nil
}

// writeRequest writes the given payload data to the given stream. It sets the
// appropriate timeouts and closes the stream for further writes.
func (r *ReqResp) writeRequest(ctx context.Context, stream network.Stream, data ssz.Marshaler) (err error) {
	if err = stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("failed setting write deadline on stream: %w", err)
	}

	if _, err = r.cfg.Encoder.EncodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read sequence number: %w", err)
	}

	if err = stream.CloseWrite(); err != nil {
		return fmt.Errorf("failed to close writing side of stream: %w", err)
	}

	return nil
}

// writeResponse differs from writeRequest in prefixing the payload data with
// a response code byte.
func (r *ReqResp) writeResponse(ctx context.Context, stream network.Stream, data ssz.Marshaler) (err error) {
	if err = stream.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("failed setting write deadline on stream: %w", err)
	}

	if _, err := stream.Write([]byte{0}); err != nil { // success response
		return fmt.Errorf("write success response code: %w", err)
	}

	if _, err = r.cfg.Encoder.EncodeWithMaxLength(stream, data); err != nil {
		return fmt.Errorf("read sequence number: %w", err)
	}

	if err = stream.CloseWrite(); err != nil {
		return fmt.Errorf("failed to close writing side of stream: %w", err)
	}

	return nil
}
