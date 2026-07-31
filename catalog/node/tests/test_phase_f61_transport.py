from __future__ import annotations

import asyncio
import base64

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from catalog.federation.adaptive_transport import (
    ADAPTIVE_TRANSPORT_STATUS_SCHEMA,
    AdaptiveStorageTransport,
    DirectTransportState,
    TransportKind,
)
from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.peer_stream import (
    PEER_STREAM_CIPHER_SUITE,
    PeerStreamFrame,
    peer_stream_nonce,
)
from catalog.federation.peer_stream_verifier import PeerStreamVerifier


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


class _Signer:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.private_key = Ed25519PrivateKey.generate()
        raw = self.private_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        self.public_key = f"ed25519:{_b64(raw)}"

    def sign(self, value: bytes) -> str:
        return _b64(self.private_key.sign(value))


def _verify(public_key: str, value: bytes, signature: str) -> bool:
    encoded_key = public_key.removeprefix("ed25519:")
    raw_key = base64.urlsafe_b64decode(
        encoded_key + "=" * (-len(encoded_key) % 4)
    )
    raw_signature = base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4))
    try:
        Ed25519PublicKey.from_public_bytes(raw_key).verify(raw_signature, value)
    except InvalidSignature:
        return False
    return True


class _Relay:
    def __init__(self, response: object = None, error: Exception | None = None) -> None:
        self.response = response if response is not None else object()
        self.error = error
        self.calls: list[tuple[str, object]] = []

    async def request(self, *, target_node_id: str, envelope: object) -> object:
        self.calls.append((target_node_id, envelope))
        if self.error is not None:
            raise self.error
        return self.response


def test_f61_adaptive_transport_remains_relay_only() -> None:
    response = object()
    relay = _Relay(response=response)
    decisions = []
    transport = AdaptiveStorageTransport(relay, decision_observer=decisions.append)
    envelope = object()

    returned = asyncio.run(
        transport.request(target_node_id="node-target", envelope=envelope)
    )

    assert returned is response
    assert relay.calls == [("node-target", envelope)]
    assert decisions[0].selected is TransportKind.RELAY
    status = transport.status().to_dict()
    assert status == {
        "schema": ADAPTIVE_TRANSPORT_STATUS_SCHEMA,
        "direct_transport_enabled": False,
        "direct_state": "disabled",
        "selected_transport": "relay",
        "request_count": 1,
        "relay_request_count": 1,
        "direct_request_count": 0,
        "fallback_count": 0,
        "last_reason": "direct-disabled-f61",
        "last_outcome": "succeeded",
    }


def test_f61_ready_diagnostic_state_cannot_activate_direct_transport() -> None:
    relay = _Relay()
    transport = AdaptiveStorageTransport(
        relay,
        direct_state=DirectTransportState.READY,
    )

    asyncio.run(transport.request(target_node_id="node-target", envelope=object()))

    status = transport.status().to_dict()
    assert status["selected_transport"] == "relay"
    assert status["direct_request_count"] == 0
    assert status["relay_request_count"] == 1
    assert status["fallback_count"] == 1
    assert status["last_reason"] == "direct-not-enabled-until-f62"


def test_f61_diagnostic_observer_cannot_block_relay() -> None:
    relay = _Relay()

    def broken_observer(_decision) -> None:
        raise RuntimeError("diagnostic sink failed")

    transport = AdaptiveStorageTransport(
        relay,
        decision_observer=broken_observer,
    )
    response = asyncio.run(
        transport.request(
            target_node_id="node-target",
            envelope=object(),
        )
    )

    assert response is relay.response
    assert transport.status().last_outcome == "succeeded"


def test_f61_relay_failure_is_not_hidden() -> None:
    transport = AdaptiveStorageTransport(_Relay(error=RuntimeError("offline")))

    with pytest.raises(RuntimeError, match="offline"):
        asyncio.run(
            transport.request(target_node_id="node-target", envelope=object())
        )

    assert transport.status().to_dict()["last_outcome"] == "failed"


def _frame(
    signer: _Signer,
    *,
    sequence: int = 0,
    request_id: str = "request-1",
) -> PeerStreamFrame:
    return PeerStreamFrame.create(
        session_id="session-f61",
        stream_id="stream-1",
        request_id=request_id,
        source_node_id=signer.node_id,
        target_node_id="node-target",
        sequence=sequence,
        key_id="ephemeral-key-1",
        nonce=peer_stream_nonce(sequence),
        ciphertext=b"ciphertext-only-payload",
        sign=signer.sign,
    )


def _verifier(signer: _Signer, **kwargs) -> PeerStreamVerifier:
    return PeerStreamVerifier(
        session_id=kwargs.pop("session_id", "session-f61"),
        local_node_id=kwargs.pop("local_node_id", "node-target"),
        resolve_public_key=(
            lambda node_id: signer.public_key
            if node_id == signer.node_id
            else None
        ),
        verify_signature=_verify,
        **kwargs,
    )


def test_f61_peer_frame_round_trip_and_identity_verification() -> None:
    signer = _Signer("node-source")
    frame = _frame(signer)
    reconstructed = PeerStreamFrame.from_dict(frame.to_dict())

    assert reconstructed == frame
    assert reconstructed.cipher_suite == PEER_STREAM_CIPHER_SUITE
    accepted = _verifier(signer).accept(
        reconstructed, expected_source_node_id="node-source"
    )
    assert accepted == frame.ciphertext


def test_f61_tampered_signed_metadata_is_rejected() -> None:
    signer = _Signer("node-source")
    value = _frame(signer).to_dict()
    value["request_id"] = "request-tampered"
    tampered = PeerStreamFrame.from_dict(value)

    with pytest.raises(FederationValidationError) as raised:
        _verifier(signer).accept(tampered)

    assert raised.value.code == "invalid-peer-frame-signature"


def test_f61_hash_and_length_mismatches_fail_before_signature_verification() -> None:
    signer = _Signer("node-source")
    value = _frame(signer).to_dict()
    value["ciphertext_hash"] = "sha256:" + "0" * 64
    with pytest.raises(FederationValidationError) as hash_error:
        PeerStreamFrame.from_dict(value)
    assert hash_error.value.code == "peer-frame-hash-mismatch"

    value = _frame(signer).to_dict()
    value["ciphertext_length"] += 1
    with pytest.raises(FederationValidationError) as length_error:
        PeerStreamFrame.from_dict(value)
    assert length_error.value.code == "peer-frame-length-mismatch"


def test_f61_wrong_session_source_and_target_fail_closed() -> None:
    signer = _Signer("node-source")
    frame = _frame(signer)

    with pytest.raises(FederationValidationError) as session_error:
        _verifier(signer, session_id="session-other").accept(frame)
    assert session_error.value.code == "peer-frame-session-mismatch"

    with pytest.raises(FederationValidationError) as target_error:
        _verifier(signer, local_node_id="node-other").accept(frame)
    assert target_error.value.code == "peer-frame-target-mismatch"

    with pytest.raises(FederationValidationError) as source_error:
        _verifier(signer).accept(frame, expected_source_node_id="node-other")
    assert source_error.value.code == "peer-frame-source-mismatch"


def test_f61_replay_gap_and_stream_limit_fail_closed() -> None:
    signer = _Signer("node-source")
    verifier = _verifier(signer, max_streams=1)
    frame0 = _frame(signer, sequence=0)
    frame1 = _frame(signer, sequence=1, request_id="request-2")

    verifier.accept(frame0)
    with pytest.raises(FederationValidationError) as replay_error:
        verifier.accept(frame0)
    assert replay_error.value.code == "peer-frame-replayed"

    verifier.accept(frame1)
    gap = _frame(signer, sequence=3, request_id="request-3")
    with pytest.raises(FederationValidationError) as gap_error:
        verifier.accept(gap)
    assert gap_error.value.code == "peer-frame-sequence-gap"

    another = PeerStreamFrame.create(
        session_id="session-f61",
        stream_id="stream-2",
        request_id="request-4",
        source_node_id=signer.node_id,
        target_node_id="node-target",
        sequence=0,
        key_id="ephemeral-key-2",
        nonce=peer_stream_nonce(0),
        ciphertext=b"payload",
        sign=signer.sign,
    )
    with pytest.raises(FederationValidationError) as limit_error:
        verifier.accept(another)
    assert limit_error.value.code == "peer-stream-limit-reached"


def test_f61_key_id_cannot_be_reused_across_streams() -> None:
    signer = _Signer("node-source")
    verifier = _verifier(signer, max_streams=2)
    verifier.accept(_frame(signer))
    reused = PeerStreamFrame.create(
        session_id="session-f61",
        stream_id="stream-2",
        request_id="request-2",
        source_node_id=signer.node_id,
        target_node_id="node-target",
        sequence=0,
        key_id="ephemeral-key-1",
        nonce=peer_stream_nonce(0),
        ciphertext=b"payload",
        sign=signer.sign,
    )

    with pytest.raises(FederationValidationError) as raised:
        verifier.accept(reused)

    assert raised.value.code == "peer-key-reused-across-streams"


def test_f61_protocol_cipher_and_nonpublic_fields_are_rejected() -> None:
    signer = _Signer("node-source")
    value = _frame(signer).to_dict()
    value["protocol_version"] = "2.0"
    with pytest.raises(ProtocolCompatibilityError) as protocol_error:
        PeerStreamFrame.from_dict(value)
    assert protocol_error.value.code == "unsupported-peer-protocol-major"

    value = _frame(signer).to_dict()
    value["cipher_suite"] = "invented-cipher"
    with pytest.raises(ProtocolCompatibilityError) as cipher_error:
        PeerStreamFrame.from_dict(value)
    assert cipher_error.value.code == "unsupported-peer-cipher-suite"

    with pytest.raises(FederationValidationError) as public_error:
        PeerStreamFrame.create(
            session_id="session-f61",
            stream_id="http://private-host:9000",
            request_id="request-1",
            source_node_id=signer.node_id,
            target_node_id="node-target",
            sequence=0,
            key_id="ephemeral-key-1",
            nonce=peer_stream_nonce(0),
            ciphertext=b"payload",
            sign=signer.sign,
        )
    assert public_error.value.code == "nonpublic-peer-frame-field"
