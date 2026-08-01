package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	streamProtocol             = protocol.ID("/msh/direct-storage/1.0.0")
	maxMessageBytes            = 2 * 1024 * 1024
	requestTimeout             = 20 * time.Second
	maxStaticRelays            = 8
	maxMultiaddrBytes          = 2048
	relayReservationTTL        = 30 * time.Minute
	relayCircuitDuration       = 10 * time.Minute
	relayCircuitDataLimit      = 64 << 20
	relayMaxReservations       = 32
	relayMaxCircuitsPerPeer    = 8
	relayMaxReservationsPerIP  = 4
	relayMaxReservationsPerASN = 16
)

type connectivityMode string

const (
	connectivityDirectOnly  connectivityMode = "direct-only"
	connectivityRelayClient connectivityMode = "relay-client"
	connectivityRelayServer connectivityMode = "relay-service"
)

type command struct {
	Command         string          `json:"command"`
	CorrelationID   string          `json:"correlation_id,omitempty"`
	TargetPeerID    string          `json:"target_peer_id,omitempty"`
	TargetMultiaddr string          `json:"target_multiaddr,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Error           string          `json:"error,omitempty"`
}

type event struct {
	Event                     string          `json:"event"`
	CorrelationID             string          `json:"correlation_id,omitempty"`
	PeerID                    string          `json:"peer_id,omitempty"`
	ListenAddrs               []string        `json:"listen_addrs,omitempty"`
	SourcePeerID              string          `json:"source_peer_id,omitempty"`
	Payload                   json.RawMessage `json:"payload,omitempty"`
	Code                      string          `json:"code,omitempty"`
	Message                   string          `json:"message,omitempty"`
	ConnectivityMode          string          `json:"connectivity_mode,omitempty"`
	StaticRelayPeerIDs        []string        `json:"static_relay_peer_ids,omitempty"`
	HolePunchingEnabled       bool            `json:"hole_punching_enabled"`
	RelayServiceEnabled       bool            `json:"relay_service_enabled"`
	PrivateReachabilityForced bool            `json:"private_reachability_forced"`
}

type wireMessage struct {
	Kind          string          `json:"kind"`
	CorrelationID string          `json:"correlation_id"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	Error         string          `json:"error,omitempty"`
}

type inboundResponse struct {
	payload json.RawMessage
	err     string
}

type hostConfig struct {
	listen                 string
	staticRelays           []peer.AddrInfo
	relayService           bool
	forcePrivate           bool
	holePunching           bool
	allowPrivateRelayAddrs bool
}

func (c hostConfig) mode() connectivityMode {
	if c.relayService {
		return connectivityRelayServer
	}
	if len(c.staticRelays) > 0 {
		return connectivityRelayClient
	}
	return connectivityDirectOnly
}

func (c hostConfig) validate() error {
	if strings.TrimSpace(c.listen) == "" {
		return errors.New("listen multiaddr is required")
	}
	if len(c.listen) > maxMultiaddrBytes {
		return errors.New("listen multiaddr exceeds size limit")
	}
	if strings.Contains(c.listen, "/p2p-circuit") {
		return errors.New("relay circuit addresses cannot be used as listen addresses")
	}
	if c.relayService && len(c.staticRelays) > 0 {
		return errors.New("relay service and relay client roles cannot be combined")
	}
	if c.relayService && c.forcePrivate {
		return errors.New("relay service cannot force private reachability")
	}
	if c.forcePrivate && len(c.staticRelays) == 0 {
		return errors.New("forced private reachability requires at least one static relay")
	}
	if c.holePunching && len(c.staticRelays) == 0 {
		return errors.New("hole punching requires at least one static relay")
	}
	if len(c.staticRelays) > maxStaticRelays {
		return fmt.Errorf("at most %d static relays are supported", maxStaticRelays)
	}
	seen := make(map[peer.ID]struct{}, len(c.staticRelays))
	for _, relay := range c.staticRelays {
		if relay.ID == "" || len(relay.Addrs) == 0 {
			return errors.New("each static relay requires a peer id and address")
		}
		if _, exists := seen[relay.ID]; exists {
			return errors.New("duplicate static relay peer id")
		}
		seen[relay.ID] = struct{}{}
		for _, address := range relay.Addrs {
			if strings.Contains(address.String(), "/p2p-circuit") {
				return errors.New("static relay bootstrap addresses must not contain p2p-circuit")
			}
		}
	}
	return nil
}

type sidecar struct {
	host      host.Host
	config    hostConfig
	output    io.Writer
	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]chan inboundResponse
}

func newHost(listen string) (host.Host, error) {
	return newHostWithConfig(hostConfig{listen: listen})
}

func newHostWithConfig(config hostConfig) (host.Host, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	options := []libp2p.Option{
		libp2p.ListenAddrStrings(config.listen),
		libp2p.DisableMetrics(),
		libp2p.Ping(false),
	}
	switch config.mode() {
	case connectivityDirectOnly:
		options = append(
			options,
			libp2p.DisableRelay(),
			libp2p.DisableIdentifyAddressDiscovery(),
		)
	case connectivityRelayClient:
		options = append(
			options,
			libp2p.EnableRelay(),
			libp2p.EnableAutoRelayWithStaticRelays(
				config.staticRelays,
				autorelay.WithBootDelay(0),
			),
		)
		if config.holePunching {
			options = append(options, libp2p.EnableHolePunching())
		}
		if config.forcePrivate {
			options = append(options, libp2p.ForceReachabilityPrivate())
		}
	case connectivityRelayServer:
		resources := boundedRelayResources()
		relayOptions := []relayv2.Option{relayv2.WithResources(resources)}
		if config.allowPrivateRelayAddrs {
			relayOptions = append(
				relayOptions,
				relayv2.WithReservationAddressFilter(func(ma.Multiaddr) bool { return true }),
			)
		}
		options = append(
			options,
			libp2p.EnableRelay(),
			libp2p.EnableRelayService(relayOptions...),
			libp2p.EnableNATService(),
			libp2p.AutoNATServiceRateLimit(30, 3, time.Minute),
			libp2p.ForceReachabilityPublic(),
		)
	default:
		return nil, errors.New("unsupported connectivity mode")
	}
	return libp2p.New(options...)
}

func boundedRelayResources() relayv2.Resources {
	resources := relayv2.DefaultResources()
	resources.ReservationTTL = relayReservationTTL
	resources.MaxReservations = relayMaxReservations
	resources.MaxCircuits = relayMaxCircuitsPerPeer
	resources.MaxReservationsPerPeer = 1
	resources.MaxReservationsPerIP = relayMaxReservationsPerIP
	resources.MaxReservationsPerASN = relayMaxReservationsPerASN
	resources.Limit = &relayv2.RelayLimit{
		Duration: relayCircuitDuration,
		Data:     relayCircuitDataLimit,
	}
	return resources
}

func parseStaticRelays(values []string) ([]peer.AddrInfo, error) {
	if len(values) > maxStaticRelays {
		return nil, fmt.Errorf("at most %d --relay values are supported", maxStaticRelays)
	}
	result := make([]peer.AddrInfo, 0, len(values))
	seen := make(map[peer.ID]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || len(value) > maxMultiaddrBytes {
			return nil, errors.New("relay multiaddr must be bounded non-empty text")
		}
		if strings.Contains(value, "/p2p-circuit") {
			return nil, errors.New("relay bootstrap multiaddr must not contain p2p-circuit")
		}
		info, err := peer.AddrInfoFromString(value)
		if err != nil {
			return nil, fmt.Errorf("parse relay multiaddr: %w", err)
		}
		if info.ID == "" || len(info.Addrs) == 0 {
			return nil, errors.New("relay multiaddr must include an address and /p2p peer id")
		}
		if _, exists := seen[info.ID]; exists {
			return nil, errors.New("duplicate relay peer id")
		}
		seen[info.ID] = struct{}{}
		result = append(result, *info)
	}
	return result, nil
}

func newSidecar(h host.Host, output io.Writer) *sidecar {
	return newSidecarWithConfig(h, output, hostConfig{listen: "/ip4/127.0.0.1/tcp/0"})
}

func newSidecarWithConfig(h host.Host, output io.Writer, config hostConfig) *sidecar {
	s := &sidecar{
		host:    h,
		config:  config,
		output:  output,
		pending: make(map[string]chan inboundResponse),
	}
	h.SetStreamHandler(streamProtocol, s.handleStream)
	return s
}

func (s *sidecar) readyEvent() event {
	peerSuffix := ma.StringCast("/p2p/" + s.host.ID().String())
	addresses := make([]string, 0, len(s.host.Addrs()))
	for _, address := range s.host.Addrs() {
		addresses = append(addresses, address.Encapsulate(peerSuffix).String())
	}
	sort.Strings(addresses)
	relayPeerIDs := make([]string, 0, len(s.config.staticRelays))
	for _, relay := range s.config.staticRelays {
		relayPeerIDs = append(relayPeerIDs, relay.ID.String())
	}
	sort.Strings(relayPeerIDs)
	return event{
		Event:                     "ready",
		PeerID:                    s.host.ID().String(),
		ListenAddrs:               addresses,
		ConnectivityMode:          string(s.config.mode()),
		StaticRelayPeerIDs:        relayPeerIDs,
		HolePunchingEnabled:       s.config.holePunching,
		RelayServiceEnabled:       s.config.relayService,
		PrivateReachabilityForced: s.config.forcePrivate,
	}
}

func (s *sidecar) write(value any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	encoder := json.NewEncoder(s.output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func (s *sidecar) handleStream(stream network.Stream) {
	defer stream.Close()
	request, err := readWireMessage(stream)
	if err != nil || request.Kind != "request" || request.CorrelationID == "" {
		_ = stream.Reset()
		return
	}
	responseChannel := make(chan inboundResponse, 1)
	s.pendingMu.Lock()
	if _, exists := s.pending[request.CorrelationID]; exists {
		s.pendingMu.Unlock()
		_ = writeWireMessage(stream, wireMessage{
			Kind:          "response",
			CorrelationID: request.CorrelationID,
			Error:         "duplicate-correlation-id",
		})
		return
	}
	s.pending[request.CorrelationID] = responseChannel
	s.pendingMu.Unlock()
	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, request.CorrelationID)
		s.pendingMu.Unlock()
	}()

	if err := s.write(event{
		Event:         "incoming_request",
		CorrelationID: request.CorrelationID,
		SourcePeerID:  stream.Conn().RemotePeer().String(),
		Payload:       request.Payload,
	}); err != nil {
		_ = stream.Reset()
		return
	}

	select {
	case response := <-responseChannel:
		_ = writeWireMessage(stream, wireMessage{
			Kind:          "response",
			CorrelationID: request.CorrelationID,
			Payload:       response.payload,
			Error:         response.err,
		})
	case <-time.After(requestTimeout):
		_ = writeWireMessage(stream, wireMessage{
			Kind:          "response",
			CorrelationID: request.CorrelationID,
			Error:         "remote-handler-timeout",
		})
	}
}

func (s *sidecar) respond(cmd command) error {
	if cmd.CorrelationID == "" {
		return errors.New("correlation_id is required")
	}
	s.pendingMu.Lock()
	responseChannel := s.pending[cmd.CorrelationID]
	s.pendingMu.Unlock()
	if responseChannel == nil {
		return errors.New("unknown inbound correlation_id")
	}
	select {
	case responseChannel <- inboundResponse{payload: cmd.Payload, err: cmd.Error}:
		return nil
	default:
		return errors.New("response already supplied")
	}
}

func (s *sidecar) request(ctx context.Context, cmd command) {
	response, err := sendRequest(
		ctx,
		s.host,
		cmd.TargetPeerID,
		cmd.TargetMultiaddr,
		cmd.CorrelationID,
		cmd.Payload,
	)
	if err != nil {
		_ = s.write(event{
			Event:         "error",
			CorrelationID: cmd.CorrelationID,
			Code:          "direct-peer-unavailable",
			Message:       "direct peer request failed",
		})
		return
	}
	if response.Error != "" {
		_ = s.write(event{
			Event:         "error",
			CorrelationID: cmd.CorrelationID,
			Code:          "remote-request-rejected",
			Message:       "remote peer rejected the request",
		})
		return
	}
	_ = s.write(event{
		Event:         "response",
		CorrelationID: cmd.CorrelationID,
		Payload:       response.Payload,
	})
}

func sendRequest(
	ctx context.Context,
	h host.Host,
	targetPeerID string,
	targetMultiaddr string,
	correlationID string,
	payload json.RawMessage,
) (wireMessage, error) {
	if correlationID == "" || targetPeerID == "" || targetMultiaddr == "" {
		return wireMessage{}, errors.New("target and correlation fields are required")
	}
	info, err := peer.AddrInfoFromString(targetMultiaddr)
	if err != nil {
		return wireMessage{}, fmt.Errorf("parse target multiaddr: %w", err)
	}
	expectedID, err := peer.Decode(targetPeerID)
	if err != nil || info.ID != expectedID {
		return wireMessage{}, errors.New("target peer id does not match the multiaddr")
	}
	requestContext, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	if err := h.Connect(requestContext, *info); err != nil {
		return wireMessage{}, fmt.Errorf("connect direct peer: %w", err)
	}
	stream, err := h.NewStream(requestContext, info.ID, streamProtocol)
	if err != nil {
		return wireMessage{}, fmt.Errorf("open direct stream: %w", err)
	}
	defer stream.Close()
	if err := stream.SetDeadline(time.Now().Add(requestTimeout)); err != nil {
		return wireMessage{}, fmt.Errorf("set stream deadline: %w", err)
	}
	if err := writeWireMessage(stream, wireMessage{
		Kind:          "request",
		CorrelationID: correlationID,
		Payload:       payload,
	}); err != nil {
		return wireMessage{}, fmt.Errorf("write direct request: %w", err)
	}
	response, err := readWireMessage(stream)
	if err != nil {
		return wireMessage{}, fmt.Errorf("read direct response: %w", err)
	}
	if response.Kind != "response" || response.CorrelationID != correlationID {
		return wireMessage{}, errors.New("direct response correlation mismatch")
	}
	return response, nil
}

func writeWireMessage(writer io.Writer, value wireMessage) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) > maxMessageBytes {
		return errors.New("direct message exceeds size limit")
	}
	encoded = append(encoded, '\n')
	_, err = writer.Write(encoded)
	return err
}

func readWireMessage(reader io.Reader) (wireMessage, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), maxMessageBytes)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return wireMessage{}, err
		}
		return wireMessage{}, io.EOF
	}
	var value wireMessage
	if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
		return wireMessage{}, err
	}
	return value, nil
}

func run(ctx context.Context, input io.Reader, output io.Writer, listen string) error {
	return runWithConfig(ctx, input, output, hostConfig{listen: listen})
}

func runWithConfig(ctx context.Context, input io.Reader, output io.Writer, config hostConfig) error {
	h, err := newHostWithConfig(config)
	if err != nil {
		return fmt.Errorf("create libp2p host: %w", err)
	}
	defer h.Close()
	s := newSidecarWithConfig(h, output, config)
	if err := s.write(s.readyEvent()); err != nil {
		return err
	}

	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), maxMessageBytes)
	for scanner.Scan() {
		var cmd command
		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			_ = s.write(event{Event: "error", Code: "invalid-command", Message: "command is not JSON"})
			continue
		}
		switch cmd.Command {
		case "request":
			if cmd.CorrelationID == "" {
				_ = s.write(event{Event: "error", Code: "invalid-command", Message: "correlation_id is required"})
				continue
			}
			go s.request(ctx, cmd)
		case "respond":
			if err := s.respond(cmd); err != nil {
				_ = s.write(event{Event: "error", CorrelationID: cmd.CorrelationID, Code: "invalid-response", Message: err.Error()})
			}
		case "shutdown":
			return nil
		default:
			_ = s.write(event{Event: "error", Code: "invalid-command", Message: "unknown command"})
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

type relayFlags []string

func (values *relayFlags) String() string {
	return strings.Join(*values, ",")
}

func (values *relayFlags) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func main() {
	listen := flag.String("listen", "/ip4/127.0.0.1/tcp/0", "explicit libp2p listen multiaddr")
	relayService := flag.Bool("relay-service", false, "run a bounded circuit-v2 relay service")
	forcePrivate := flag.Bool(
		"force-private-reachability",
		false,
		"force relay-client mode to reserve immediately instead of waiting for AutoNAT",
	)
	var relayValues relayFlags
	flag.Var(
		&relayValues,
		"relay",
		"static circuit-v2 relay bootstrap multiaddr including /p2p peer id; repeatable",
	)
	flag.Parse()

	staticRelays, err := parseStaticRelays(relayValues)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	config := hostConfig{
		listen:       *listen,
		staticRelays: staticRelays,
		relayService: *relayService,
		forcePrivate: *forcePrivate,
		holePunching: len(staticRelays) > 0,
	}
	if err := config.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := runWithConfig(context.Background(), os.Stdin, os.Stdout, config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
