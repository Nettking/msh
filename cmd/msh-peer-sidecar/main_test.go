package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
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
