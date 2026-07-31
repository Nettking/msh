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
	"strings"
	"sync"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	streamProtocol  = protocol.ID("/msh/direct-storage/1.0.0")
	maxMessageBytes = 2 * 1024 * 1024
	requestTimeout  = 20 * time.Second
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
	Event         string          `json:"event"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	PeerID        string          `json:"peer_id,omitempty"`
	ListenAddrs   []string        `json:"listen_addrs,omitempty"`
	SourcePeerID  string          `json:"source_peer_id,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	Code          string          `json:"code,omitempty"`
	Message       string          `json:"message,omitempty"`
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

type sidecar struct {
	host      host.Host
	output    io.Writer
	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]chan inboundResponse
}

func newHost(listen string) (host.Host, error) {
	return libp2p.New(
		libp2p.ListenAddrStrings(listen),
		libp2p.DisableRelay(),
		libp2p.DisableMetrics(),
		libp2p.DisableIdentifyAddressDiscovery(),
		libp2p.Ping(false),
	)
}

func newSidecar(h host.Host, output io.Writer) *sidecar {
	s := &sidecar{
		host:    h,
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
	return event{
		Event:       "ready",
		PeerID:      s.host.ID().String(),
		ListenAddrs: addresses,
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
	h, err := newHost(listen)
	if err != nil {
		return fmt.Errorf("create libp2p host: %w", err)
	}
	defer h.Close()
	s := newSidecar(h, output)
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

func main() {
	listen := flag.String("listen", "/ip4/127.0.0.1/tcp/0", "explicit directly reachable libp2p listen multiaddr")
	flag.Parse()
	if strings.Contains(*listen, "/p2p-circuit") {
		fmt.Fprintln(os.Stderr, "relay addresses are not permitted in F6.2")
		os.Exit(2)
	}
	if err := run(context.Background(), os.Stdin, os.Stdout, *listen); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
