"""Connection-local identity and replay verification for peer stream frames."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable

from .errors import FederationValidationError
from .peer_stream import (
    DEFAULT_PEER_STREAM_LIMIT,
    PeerStreamFrame,
    validate_peer_public_text,
)


class PeerStreamVerifier:
    """Fail-closed identity, signature, and connection-local replay verifier."""

    def __init__(
        self,
        *,
        session_id: str,
        local_node_id: str,
        resolve_public_key: Callable[[str], str | None],
        verify_signature: Callable[[str, bytes, str], bool],
        max_streams: int = DEFAULT_PEER_STREAM_LIMIT,
    ) -> None:
        self.session_id = validate_peer_public_text(session_id, "session_id")
        self.local_node_id = validate_peer_public_text(local_node_id, "local_node_id")
        if (
            isinstance(max_streams, bool)
            or not isinstance(max_streams, int)
            or max_streams < 1
        ):
            raise FederationValidationError(
                "invalid-peer-stream-limit", "max_streams", "must be a positive integer"
            )
        self.resolve_public_key = resolve_public_key
        self.verify_signature = verify_signature
        self.max_streams = max_streams
        self._next_sequence: OrderedDict[tuple[str, str, str], int] = OrderedDict()
        self._key_streams: dict[tuple[str, str], str] = {}

    def accept(
        self,
        frame: PeerStreamFrame,
        *,
        expected_source_node_id: str | None = None,
    ) -> bytes:
        if frame.session_id != self.session_id:
            raise FederationValidationError(
                "peer-frame-session-mismatch",
                "session_id",
                "frame belongs to another session",
            )
        if frame.target_node_id != self.local_node_id:
            raise FederationValidationError(
                "peer-frame-target-mismatch",
                "target_node_id",
                "frame is not addressed to this node",
            )
        if (
            expected_source_node_id is not None
            and frame.source_node_id != expected_source_node_id
        ):
            raise FederationValidationError(
                "peer-frame-source-mismatch",
                "source_node_id",
                "frame does not come from the expected peer",
            )
        try:
            public_key = self.resolve_public_key(frame.source_node_id)
        except Exception as exc:
            raise FederationValidationError(
                "peer-identity-resolution-failed",
                "source_node_id",
                "source identity could not be resolved",
            ) from exc
        if not isinstance(public_key, str) or not public_key:
            raise FederationValidationError(
                "unknown-peer-identity",
                "source_node_id",
                "source node has no enrolled public identity",
            )
        try:
            signature_valid = self.verify_signature(
                public_key, frame.signing_bytes(), frame.signature
            )
        except Exception as exc:
            raise FederationValidationError(
                "peer-signature-verification-failed",
                "signature",
                "source signature could not be verified",
            ) from exc
        if signature_valid is not True:
            raise FederationValidationError(
                "invalid-peer-frame-signature",
                "signature",
                "signature does not match the enrolled source identity",
            )
        key_scope = (frame.source_node_id, frame.key_id)
        assigned_stream = self._key_streams.get(key_scope)
        if assigned_stream is not None and assigned_stream != frame.stream_id:
            raise FederationValidationError(
                "peer-key-reused-across-streams",
                "key_id",
                "one peer encryption key may protect only one stream",
            )
        stream_key = (frame.source_node_id, frame.stream_id, frame.key_id)
        expected_sequence = self._next_sequence.get(stream_key)
        if expected_sequence is None:
            if len(self._next_sequence) >= self.max_streams:
                raise FederationValidationError(
                    "peer-stream-limit-reached",
                    "stream_id",
                    "too many active peer streams",
                )
            expected_sequence = 0
        if frame.sequence < expected_sequence:
            raise FederationValidationError(
                "peer-frame-replayed",
                "sequence",
                "frame sequence was already accepted",
            )
        if frame.sequence > expected_sequence:
            raise FederationValidationError(
                "peer-frame-sequence-gap",
                "sequence",
                f"expected sequence {expected_sequence}",
            )
        self._key_streams[key_scope] = frame.stream_id
        self._next_sequence[stream_key] = expected_sequence + 1
        self._next_sequence.move_to_end(stream_key)
        return frame.ciphertext
