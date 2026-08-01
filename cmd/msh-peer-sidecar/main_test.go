package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	circuitclient "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	ma "github.com/multiformats/go-multiaddr"
)

func directAddress(t *testing.T, h host.Host) string {
	t.Helper()
	addresses := h.Addrs()
	if len(addresses) == 0 {
		t.Fatal("host did not expose a direct listen address")
	}
	peerSuffix := ma.StringCast("/p2p/" + h.ID().String())
	return addresses[0].Encapsulate(peerSuffix).String()
}

func relayInfo(t *testing.T, h host.Host) peer.AddrInfo {
	t.Helper()
	info, err := peer.AddrInfoFromString(directAddress(t, h))
	if err != nil {
		t.Fatal(err)
	}
	return *info
}

func TestDirectEncryptedStreamBetweenReachablePeers(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	hostA, err := newHost("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()
	hostB, err := newHost("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()

	hostB.SetStreamHandler(streamProtocol, func(stream network.Stream) {
		defer stream.Close()
		request, readErr := readWireMessage(stream)
		if readErr != nil {
			_ = stream.Reset()
			return
		}
		_ = writeWireMessage(stream, wireMessage{
			Kind:          "response",
			CorrelationID: request.CorrelationID,
			Payload:       request.Payload,
		})
	})

	payload := json.RawMessage(`{"ciphertext":"opaque"}`)
	response, err := sendRequest(
		ctx,
		hostA,
		hostB.ID().String(),
		directAddress(t, hostB),
		"test-correlation",
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != string(payload) {
		t.Fatalf("unexpected response payload: %s", response.Payload)
	}
	if hostA.Network().Connectedness(hostB.ID()) != network.Connected {
		t.Fatal("direct libp2p connection was not established")
	}
}

func TestTargetPeerIdentityMustMatchMultiaddr(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hostA, err := newHost("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()
	hostB, err := newHost("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()

	_, err = sendRequest(
		ctx,
		hostA,
		hostA.ID().String(),
		directAddress(t, hostB),
		"identity-mismatch",
		json.RawMessage(`{}`),
	)
	if err == nil {
		t.Fatal("expected peer identity mismatch")
	}
}

func TestStaticRelayClientObtainsReservationAndCarriesStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	relayHost, err := newHostWithConfig(hostConfig{
		listen:                 "/ip4/127.0.0.1/tcp/0",
		relayService:           true,
		allowPrivateRelayAddrs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer relayHost.Close()
	staticRelay := relayInfo(t, relayHost)

	hostA, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.EnableRelay(),
		libp2p.DisableMetrics(),
		libp2p.Ping(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()
	hostB, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.EnableRelay(),
		libp2p.DisableMetrics(),
		libp2p.Ping(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()

	reservation, err := circuitclient.Reserve(ctx, hostB, staticRelay)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.LimitDuration != relayCircuitDuration || reservation.LimitData != relayCircuitDataLimit {
		t.Fatalf("unexpected reservation limits: %+v", reservation)
	}

	relayedConnection := make(chan bool, 1)
	hostB.SetStreamHandler(streamProtocol, func(stream network.Stream) {
		defer stream.Close()
		relayedConnection <- stream.Conn().Stat().Limited
		request, readErr := readWireMessage(stream)
		if readErr != nil {
			_ = stream.Reset()
			return
		}
		_ = writeWireMessage(stream, wireMessage{
			Kind:          "response",
			CorrelationID: request.CorrelationID,
			Payload:       request.Payload,
		})
	})

	relayAddress, err := ma.NewMultiaddr(directAddress(t, relayHost))
	if err != nil {
		t.Fatal(err)
	}
	targetSuffix := ma.StringCast("/p2p-circuit/p2p/" + hostB.ID().String())
	hostBRelayAddress := relayAddress.Encapsulate(targetSuffix).String()
	payload := json.RawMessage(`{"ciphertext":"through-circuit-v2"}`)
	response, err := sendRequest(
		ctx,
		hostA,
		hostB.ID().String(),
		hostBRelayAddress,
		"relay-correlation",
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != string(payload) {
		t.Fatalf("unexpected relayed response payload: %s", response.Payload)
	}
	select {
	case limited := <-relayedConnection:
		if !limited {
			t.Fatal("expected the stream to arrive over a limited circuit-v2 connection")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for relayed stream evidence")
	}
}

func TestRelayClientConfigurationEnablesHolePunching(t *testing.T) {
	relayHost, err := newHostWithConfig(hostConfig{
		listen:                 "/ip4/127.0.0.1/tcp/0",
		relayService:           true,
		allowPrivateRelayAddrs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer relayHost.Close()
	config := hostConfig{
		listen:       "/ip4/127.0.0.1/tcp/0",
		staticRelays: []peer.AddrInfo{relayInfo(t, relayHost)},
		forcePrivate: true,
		holePunching: true,
	}
	h, err := newHostWithConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	s := newSidecarWithConfig(h, nil, config)
	ready := s.readyEvent()
	if ready.ConnectivityMode != string(connectivityRelayClient) {
		t.Fatalf("unexpected connectivity mode: %s", ready.ConnectivityMode)
	}
	if !ready.HolePunchingEnabled || !ready.PrivateReachabilityForced {
		t.Fatal("ready event did not report the enabled connectivity features")
	}
	if ready.RelayServiceEnabled {
		t.Fatal("relay client must not report relay service mode")
	}
	if len(ready.StaticRelayPeerIDs) != 1 || ready.StaticRelayPeerIDs[0] != relayHost.ID().String() {
		t.Fatalf("unexpected static relay identities: %v", ready.StaticRelayPeerIDs)
	}
}

func TestRelayConfigurationValidationFailsClosed(t *testing.T) {
	if _, err := parseStaticRelays([]string{"/ip4/127.0.0.1/tcp/1/p2p-circuit"}); err == nil {
		t.Fatal("expected circuit bootstrap address rejection")
	}
	if err := (hostConfig{
		listen:       "/ip4/127.0.0.1/tcp/0",
		relayService: true,
		staticRelays: []peer.AddrInfo{{ID: "invalid"}},
	}).validate(); err == nil {
		t.Fatal("expected relay client/service role conflict")
	}
	if err := (hostConfig{
		listen:       "/ip4/127.0.0.1/tcp/0",
		forcePrivate: true,
	}).validate(); err == nil {
		t.Fatal("expected forced-private configuration without relay rejection")
	}
}

func TestRelayServiceResourcesAreBounded(t *testing.T) {
	resources := boundedRelayResources()
	if resources.Limit == nil {
		t.Fatal("relay service must enforce per-circuit limits")
	}
	if resources.MaxReservations != relayMaxReservations ||
		resources.MaxCircuits != relayMaxCircuitsPerPeer ||
		resources.ReservationTTL != relayReservationTTL ||
		resources.Limit.Duration != relayCircuitDuration ||
		resources.Limit.Data != relayCircuitDataLimit {
		t.Fatalf("unexpected relay resource limits: %+v", resources)
	}
}
