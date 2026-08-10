"""Durable resumable verified object transfer runtime for Phase F6.5."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import os
import secrets
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)

from .adaptive_transport import DirectTransportUnavailable
from .direct_peer import (
    DirectPacketChannel,
    NodeSigningCredentials,
    PeerStreamOpen,
)
from .errors import FederationOperationError, FederationValidationError
from .object_transfer import (
    DEFAULT_TRANSFER_CHUNK_BYTES,
    MAX_TRANSFER_OBJECT_BYTES,
    ObjectTransferChunk,
    ObjectTransferManifest,
    ObjectTransferReceiver,
    VerifiedObjectTransfer,
)
from .object_transfer_staging import FilesystemObjectTransferChunkStore
from .verified_chunk_transfer import (
    ObjectTransferAck,
    VerifiedChunkTransferEndpoint,
)

DURABLE_INCOMING_SCHEMA = "fcp.object_transfer.durable.incoming.v1"
DURABLE_OUTGOING_SCHEMA = "fcp.object_transfer.durable.outgoing.v1"
DURABLE_STATE_VERSION = 1
DEFAULT_ABANDONED_TRANSFER_AGE_SECONDS = 24 * 60 * 60
MAX_DURABLE_TRANSFERS = 4096
JSON = dict[str, Any]


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest_name(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _integer(
    value: Any,
    field: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FederationValidationError(
            "invalid-durable-transfer-integer",
            field,
            f"must be an integer greater than or equal to {minimum}",
        )
    if maximum is not None and value > maximum:
        raise FederationValidationError(
            "durable-transfer-limit-exceeded",
            field,
            f"must not exceed {maximum}",
        )
    return value


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FederationValidationError(
            "invalid-durable-transfer-number",
            field,
            "must be a finite number",
        )
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise FederationValidationError(
            "invalid-durable-transfer-number",
            field,
            "must be a finite non-negative number",
        )
    return result


def _public_key(value: str) -> X25519PublicKey:
    if not isinstance(value, str) or not value.startswith("x25519:"):
        raise FederationValidationError(
            "unsupported-object-transfer-key",
            "encryption_public_key",
            "expected an X25519 public key",
        )
    encoded = value.removeprefix("x25519:")
    try:
        raw = base64.b64decode(
            encoded + "=" * (-len(encoded) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, base64.binascii.Error) as exc:
        raise FederationValidationError(
            "invalid-object-transfer-key",
            "encryption_public_key",
            "must be canonical base64url text",
        ) from exc
    if len(raw) != 32:
        raise FederationValidationError(
            "invalid-object-transfer-key",
            "encryption_public_key",
            "must encode exactly 32 bytes",
        )
    return X25519PublicKey.from_public_bytes(raw)


def _public_key_text(public_key: X25519PublicKey) -> str:
    raw = public_key.public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return "x25519:" + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _receipt_from_dict(value: Any) -> VerifiedObjectTransfer:
    if not isinstance(value, dict) or value.get("schema") != "fcp.object_transfer.receipt.v1":
        raise FederationValidationError(
            "invalid-durable-transfer-receipt",
            "receipt",
            "must contain a verified object-transfer receipt",
        )
    if value.get("verified") is not True:
        raise FederationValidationError(
            "invalid-durable-transfer-receipt",
            "verified",
            "receipt must be verified",
        )
    return VerifiedObjectTransfer(
        transfer_id=value.get("transfer_id"),
        object_id=value.get("object_id"),
        total_size=value.get("total_size"),
        object_hash=value.get("object_hash"),
        chunk_count=value.get("chunk_count"),
        protocol_version=value.get("protocol_version"),
    )


def _receipt_for_manifest(
    value: Any,
    manifest: ObjectTransferManifest,
) -> VerifiedObjectTransfer:
    receipt = _receipt_from_dict(value)
    if (
        receipt.transfer_id,
        receipt.object_id,
        receipt.total_size,
        receipt.object_hash,
        receipt.chunk_count,
    ) != (
        manifest.transfer_id,
        manifest.object_id,
        manifest.total_size,
        manifest.object_hash,
        manifest.chunk_count,
    ):
        raise FederationValidationError(
            "durable-transfer-receipt-mismatch",
            "receipt",
            "completed durable receipt does not match its manifest",
        )
    return receipt


@dataclass(frozen=True)
class DurableTransferCleanup:
    incoming_removed: int
    outgoing_removed: int
    staged_bytes_released: int
    managed_sources_removed: int

    def to_dict(self) -> JSON:
        return {
            "incoming_removed": self.incoming_removed,
            "outgoing_removed": self.outgoing_removed,
            "staged_bytes_released": self.staged_bytes_released,
            "managed_sources_removed": self.managed_sources_removed,
        }


class DurableObjectTransferJournal:
    """Atomic single-writer metadata journal for resumable transfers."""

    def __init__(
        self,
        root: str | Path,
        *,
        clock: Callable[[], float] = time.time,
        max_transfers: int = MAX_DURABLE_TRANSFERS,
    ) -> None:
        self.root = Path(root)
        self.incoming_root = self.root / "incoming"
        self.outgoing_root = self.root / "outgoing"
        self.source_root = self.root / "managed-sources"
        self.clock = clock
        self.max_transfers = _integer(
            max_transfers,
            "max_transfers",
            minimum=1,
            maximum=MAX_DURABLE_TRANSFERS,
        )
        for directory in (self.incoming_root, self.outgoing_root, self.source_root):
            directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    @staticmethod
    def outgoing_key(
        target_node_id: str,
        request_id: str,
        object_id: str,
    ) -> str:
        return _digest_name(
            json.dumps(
                [target_node_id, request_id, object_id],
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

    def managed_source_path(self, outgoing_key: str) -> Path:
        return self.source_root / f"{outgoing_key}.source"

    def _incoming_path(self, transfer_id: str) -> Path:
        return self.incoming_root / f"{_digest_name(transfer_id)}.json"

    def _outgoing_path(self, outgoing_key: str) -> Path:
        return self.outgoing_root / f"{outgoing_key}.json"

    def _write(self, path: Path, value: JSON) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.parent / f".{path.name}.{secrets.token_hex(8)}.tmp"
        try:
            with temporary.open("xb") as handle:
                handle.write(_canonical(value))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _read(path: Path, expected_schema: str) -> JSON | None:
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "durable-transfer-state-corrupt",
                "state",
                f"invalid durable transfer state at {path.name}",
            ) from exc
        if not isinstance(value, dict) or value.get("schema") != expected_schema:
            raise FederationValidationError(
                "durable-transfer-state-corrupt",
                "schema",
                f"unexpected durable transfer schema at {path.name}",
            )
        if value.get("version") != DURABLE_STATE_VERSION:
            raise FederationValidationError(
                "durable-transfer-state-version-unsupported",
                "version",
                "unsupported durable transfer state version",
            )
        _number(value.get("updated_at"), "updated_at")
        return value

    def _ensure_capacity(self, root: Path) -> None:
        if sum(1 for _ in root.glob("*.json")) >= self.max_transfers:
            raise FederationOperationError(
                "durable-transfer-capacity-exceeded",
                "too many durable transfer records",
            )

    def open_incoming(
        self,
        source_node_id: str,
        manifest: ObjectTransferManifest,
    ) -> tuple[JSON, bool]:
        path = self._incoming_path(manifest.transfer_id)
        with self._lock:
            current = self._read(path, DURABLE_INCOMING_SCHEMA)
            if current is None:
                self._ensure_capacity(self.incoming_root)
                current = {
                    "schema": DURABLE_INCOMING_SCHEMA,
                    "version": DURABLE_STATE_VERSION,
                    "source_node_id": source_node_id,
                    "manifest": manifest.to_dict(),
                    "accepted": {},
                    "state": "active",
                    "receipt": None,
                    "updated_at": self.clock(),
                }
                self._write(path, current)
                return current, True
            persisted = ObjectTransferManifest.from_dict(current.get("manifest"))
            if current.get("source_node_id") != source_node_id:
                raise FederationValidationError(
                    "object-transfer-resume-source-node-mismatch",
                    "source_node_id",
                    "durable transfer belongs to another enrolled source node",
                )
            if persisted != manifest:
                raise FederationValidationError(
                    "object-transfer-conflicting-durable-manifest",
                    "manifest",
                    "durable transfer already has a different manifest",
                )
            if current.get("state") not in {"active", "publishing", "completed"}:
                raise FederationValidationError(
                    "durable-transfer-state-corrupt",
                    "state",
                    "incoming state must be active, publishing, or completed",
                )
            if not isinstance(current.get("accepted"), dict):
                raise FederationValidationError(
                    "durable-transfer-state-corrupt",
                    "accepted",
                    "accepted chunk cursor must be an object",
                )
            return current, False

    def load_incoming(self, transfer_id: str) -> JSON | None:
        with self._lock:
            return self._read(
                self._incoming_path(transfer_id),
                DURABLE_INCOMING_SCHEMA,
            )

    def incoming_records(self) -> tuple[JSON, ...]:
        with self._lock:
            records = []
            for path in sorted(self.incoming_root.glob("*.json")):
                value = self._read(path, DURABLE_INCOMING_SCHEMA)
                if value is not None:
                    records.append(value)
            return tuple(records)

    def record_incoming_chunk(
        self,
        manifest: ObjectTransferManifest,
        chunk: ObjectTransferChunk,
    ) -> None:
        path = self._incoming_path(manifest.transfer_id)
        with self._lock:
            current = self._read(path, DURABLE_INCOMING_SCHEMA)
            if current is None:
                raise FederationOperationError(
                    "durable-transfer-manifest-missing",
                    "incoming manifest must be durable before chunks",
                )
            if current.get("state") in {"publishing", "completed"}:
                raise FederationOperationError(
                    "object-transfer-already-verified",
                    "completion has started and the durable transfer is immutable",
                )
            accepted = current.get("accepted")
            if not isinstance(accepted, dict):
                raise FederationValidationError(
                    "durable-transfer-state-corrupt",
                    "accepted",
                    "accepted chunk cursor must be an object",
                )
            key = str(chunk.index)
            value = {
                "chunk_hash": chunk.chunk_hash,
                "chunk_length": chunk.chunk_length,
            }
            previous = accepted.get(key)
            if previous is not None and previous != value:
                raise FederationValidationError(
                    "object-transfer-conflicting-durable-chunk",
                    "index",
                    "durable chunk cursor already contains different content",
                )
            accepted[key] = value
            current["updated_at"] = self.clock()
            self._write(path, current)

    def prepare_incoming_completion(
        self,
        manifest: ObjectTransferManifest,
        receipt: VerifiedObjectTransfer,
    ) -> None:
        path = self._incoming_path(manifest.transfer_id)
        with self._lock:
            current = self._read(path, DURABLE_INCOMING_SCHEMA)
            if current is None:
                raise FederationOperationError(
                    "durable-transfer-manifest-missing",
                    "incoming manifest must be durable before completion",
                )
            current["state"] = "publishing"
            current["receipt"] = receipt.to_dict()
            current["updated_at"] = self.clock()
            self._write(path, current)

    def complete_incoming(
        self,
        manifest: ObjectTransferManifest,
        receipt: VerifiedObjectTransfer,
    ) -> None:
        path = self._incoming_path(manifest.transfer_id)
        with self._lock:
            current = self._read(path, DURABLE_INCOMING_SCHEMA)
            if current is None:
                raise FederationOperationError(
                    "durable-transfer-manifest-missing",
                    "incoming manifest must be durable before completion",
                )
            current["state"] = "completed"
            current["receipt"] = receipt.to_dict()
            current["updated_at"] = self.clock()
            self._write(path, current)

    def open_outgoing(
        self,
        *,
        outgoing_key: str,
        target_node_id: str,
        session_id: str,
        request_id: str,
        object_id: str,
        source_path: Path,
        managed_source: bool,
        candidate_manifest: ObjectTransferManifest,
    ) -> tuple[JSON, ObjectTransferManifest, bool]:
        path = self._outgoing_path(outgoing_key)
        with self._lock:
            current = self._read(path, DURABLE_OUTGOING_SCHEMA)
            if current is None:
                self._ensure_capacity(self.outgoing_root)
                current = {
                    "schema": DURABLE_OUTGOING_SCHEMA,
                    "version": DURABLE_STATE_VERSION,
                    "outgoing_key": outgoing_key,
                    "target_node_id": target_node_id,
                    "session_id": session_id,
                    "request_id": request_id,
                    "object_id": object_id,
                    "source_path": str(source_path),
                    "managed_source": managed_source,
                    "manifest": candidate_manifest.to_dict(),
                    "next_index": 0,
                    "state": "active",
                    "receipt": None,
                    "updated_at": self.clock(),
                }
                self._write(path, current)
                return current, candidate_manifest, True
            persisted = ObjectTransferManifest.from_dict(current.get("manifest"))
            expected = (
                target_node_id,
                request_id,
                object_id,
                candidate_manifest.total_size,
                candidate_manifest.object_hash,
                candidate_manifest.chunk_size,
                candidate_manifest.chunk_count,
            )
            actual = (
                current.get("target_node_id"),
                current.get("request_id"),
                current.get("object_id"),
                persisted.total_size,
                persisted.object_hash,
                persisted.chunk_size,
                persisted.chunk_count,
            )
            if actual != expected:
                raise FederationValidationError(
                    "object-transfer-resume-source-mismatch",
                    "source_path",
                    "source content or transfer identity changed after interruption",
                )
            if current.get("state") not in {"active", "completed"}:
                raise FederationValidationError(
                    "durable-transfer-state-corrupt",
                    "state",
                    "outgoing state must be active or completed",
                )
            _integer(
                current.get("next_index"),
                "next_index",
                maximum=persisted.chunk_count,
            )
            current["session_id"] = session_id
            current["source_path"] = str(source_path)
            current["managed_source"] = (
                current.get("managed_source") is True or managed_source
            )
            current["updated_at"] = self.clock()
            self._write(path, current)
            return current, persisted, False

    def load_outgoing(self, outgoing_key: str) -> JSON | None:
        with self._lock:
            return self._read(
                self._outgoing_path(outgoing_key),
                DURABLE_OUTGOING_SCHEMA,
            )

    def record_outgoing_cursor(
        self,
        outgoing_key: str,
        next_index: int,
        chunk_count: int,
    ) -> None:
        path = self._outgoing_path(outgoing_key)
        with self._lock:
            current = self._read(path, DURABLE_OUTGOING_SCHEMA)
            if current is None:
                raise FederationOperationError(
                    "durable-transfer-outgoing-missing",
                    "outgoing transfer must be durable before cursor updates",
                )
            current["next_index"] = _integer(
                next_index,
                "next_index",
                maximum=chunk_count,
            )
            current["updated_at"] = self.clock()
            self._write(path, current)

    def complete_outgoing(
        self,
        outgoing_key: str,
        receipt: VerifiedObjectTransfer,
    ) -> None:
        path = self._outgoing_path(outgoing_key)
        with self._lock:
            current = self._read(path, DURABLE_OUTGOING_SCHEMA)
            if current is None:
                raise FederationOperationError(
                    "durable-transfer-outgoing-missing",
                    "outgoing transfer must be durable before completion",
                )
            current["state"] = "completed"
            current["next_index"] = receipt.chunk_count
            current["receipt"] = receipt.to_dict()
            current["updated_at"] = self.clock()
            self._write(path, current)

    def restore_receiver(
        self,
        manifest: ObjectTransferManifest,
        store: FilesystemObjectTransferChunkStore,
    ) -> ObjectTransferReceiver:
        current = self.load_incoming(manifest.transfer_id)
        if current is None:
            raise FederationOperationError(
                "durable-transfer-manifest-missing",
                "incoming transfer state is missing",
            )
        receiver = ObjectTransferReceiver(manifest, store)
        completed = current.get("state") == "completed"
        accepted_value = current.get("accepted")
        if not isinstance(accepted_value, dict):
            raise FederationValidationError(
                "durable-transfer-state-corrupt",
                "accepted",
                "accepted chunk cursor must be an object",
            )
        accepted: dict[int, tuple[str, int]] = {}
        for raw_index, metadata in accepted_value.items():
            try:
                index = int(raw_index)
            except (TypeError, ValueError) as exc:
                raise FederationValidationError(
                    "durable-transfer-state-corrupt",
                    "accepted",
                    "chunk indexes must be decimal integers",
                ) from exc
            if (
                not isinstance(metadata, dict)
                or not isinstance(metadata.get("chunk_hash"), str)
                or isinstance(metadata.get("chunk_length"), bool)
                or not isinstance(metadata.get("chunk_length"), int)
            ):
                raise FederationValidationError(
                    "durable-transfer-state-corrupt",
                    "accepted",
                    "chunk metadata is invalid",
                )
            expected_length = manifest.expected_chunk_length(index)
            if metadata["chunk_length"] != expected_length:
                raise FederationValidationError(
                    "object-transfer-durable-chunk-mismatch",
                    "accepted",
                    f"durable chunk {index} length does not match its cursor",
                )
            if not completed:
                data = store.get(manifest.transfer_id, index)
                if len(data) != expected_length or metadata["chunk_hash"] != _sha256(data):
                    raise FederationValidationError(
                        "object-transfer-durable-chunk-mismatch",
                        "accepted",
                        f"durable chunk {index} does not match its cursor",
                    )
            accepted[index] = (metadata["chunk_hash"], metadata["chunk_length"])
        expected_prefix = set(range(len(accepted)))
        if set(accepted) != expected_prefix:
            raise FederationValidationError(
                "object-transfer-durable-cursor-gap",
                "accepted",
                "durable sender cursor must be a contiguous prefix",
            )
        receiver.__dict__["_accepted"] = accepted
        if completed:
            receiver.__dict__["_verified_receipt"] = _receipt_for_manifest(
                current.get("receipt"),
                manifest,
            )
        return receiver

    def cleanup_abandoned(
        self,
        store: FilesystemObjectTransferChunkStore,
        *,
        older_than_seconds: float,
    ) -> DurableTransferCleanup:
        age = _number(older_than_seconds, "older_than_seconds")
        now = self.clock()
        incoming_removed = 0
        outgoing_removed = 0
        released = 0
        managed_sources_removed = 0
        with self._lock:
            for path in sorted(self.incoming_root.glob("*.json")):
                current = self._read(path, DURABLE_INCOMING_SCHEMA)
                if (
                    current is None
                    or current.get("state") not in {"active", "publishing"}
                    or now - _number(current.get("updated_at"), "updated_at") < age
                ):
                    continue
                manifest = ObjectTransferManifest.from_dict(current.get("manifest"))
                before = store.stored_bytes
                store.delete(manifest.transfer_id)
                released += max(0, before - store.stored_bytes)
                path.unlink(missing_ok=True)
                incoming_removed += 1
            for path in sorted(self.outgoing_root.glob("*.json")):
                current = self._read(path, DURABLE_OUTGOING_SCHEMA)
                if (
                    current is None
                    or current.get("state") != "active"
                    or now - _number(current.get("updated_at"), "updated_at") < age
                ):
                    continue
                if current.get("managed_source") is True:
                    source = Path(str(current.get("source_path", "")))
                    try:
                        source.relative_to(self.source_root)
                    except ValueError:
                        pass
                    else:
                        if source.exists():
                            source.unlink(missing_ok=True)
                            managed_sources_removed += 1
                path.unlink(missing_ok=True)
                outgoing_removed += 1
        return DurableTransferCleanup(
            incoming_removed=incoming_removed,
            outgoing_removed=outgoing_removed,
            staged_bytes_released=released,
            managed_sources_removed=managed_sources_removed,
        )


class DurableFilesystemObjectTransferChunkStore(
    FilesystemObjectTransferChunkStore
):
    """F6.4.2 staging store with indexes reconstructed from durable cursors."""

    def __init__(
        self,
        staging_root: str | Path,
        publication_root: str | Path,
        journal: DurableObjectTransferJournal,
        *,
        max_bytes: int = MAX_TRANSFER_OBJECT_BYTES,
    ) -> None:
        super().__init__(staging_root, publication_root, max_bytes=max_bytes)
        self.journal = journal
        self._recover_indexes()

    def _recover_indexes(self) -> None:
        for current in self.journal.incoming_records():
            manifest = ObjectTransferManifest.from_dict(current.get("manifest"))
            accepted = current.get("accepted")
            if not isinstance(accepted, dict):
                continue
            for raw_index in accepted:
                try:
                    index = int(raw_index)
                except (TypeError, ValueError):
                    continue
                path = self._chunk_path(manifest.transfer_id, index)
                if not path.is_file():
                    continue
                size = path.stat().st_size
                self._lengths[(manifest.transfer_id, index)] = size
                self._stored_bytes += size
        if self._stored_bytes > self.max_bytes:
            raise FederationOperationError(
                "object-transfer-staging-capacity-exceeded",
                "recovered durable staging exceeds configured capacity",
            )

    def published_path(self, object_id: str) -> Path | None:
        destination = self.publication_root / f"{_digest_name(object_id)}.object"
        return destination if destination.is_file() else None

    def published_matches(self, manifest: ObjectTransferManifest) -> bool:
        published = self.published_path(manifest.object_id)
        return (
            published is not None
            and self._file_identity(published)
            == (manifest.total_size, manifest.object_hash)
        )


class ResumableChunkTransferEndpoint(VerifiedChunkTransferEndpoint):
    """F6.4.2 encrypted transfer with durable F6.5 restart negotiation."""

    def __init__(
        self,
        channel: DirectPacketChannel,
        credentials: NodeSigningCredentials,
        *,
        resolve_public_key: Callable[[str], str | None],
        verify_signature: Callable[[str, bytes, str], bool],
        publication_root: str | Path,
        durable_root: str | Path,
        max_staged_bytes: int = MAX_TRANSFER_OBJECT_BYTES,
        max_attempts: int = 4,
        max_sequence_gap: int = 64,
        max_inbound_transfers: int = 128,
        clock: Callable[[], float] = time.time,
    ) -> None:
        durable_root = Path(durable_root)
        durable_root.mkdir(parents=True, exist_ok=True)
        super().__init__(
            channel,
            credentials,
            resolve_public_key=resolve_public_key,
            verify_signature=verify_signature,
            publication_root=publication_root,
            staging_parent=durable_root / "_unused-ephemeral",
            max_staged_bytes=max_staged_bytes,
            max_attempts=max_attempts,
            max_sequence_gap=max_sequence_gap,
            max_inbound_transfers=max_inbound_transfers,
        )
        self._temporary.cleanup()
        self.journal = DurableObjectTransferJournal(
            durable_root / "journal",
            clock=clock,
        )
        self.store = DurableFilesystemObjectTransferChunkStore(
            durable_root / "chunks",
            publication_root,
            self.journal,
            max_bytes=max_staged_bytes,
        )
        self.durable_root = durable_root

    async def close(self) -> None:
        self._inbound.clear()
        await self.channel.close()
        self._started = False

    async def send_bytes(
        self,
        *,
        target_node_id: str,
        session_id: str,
        request_id: str,
        object_id: str,
        content: bytes,
        chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
    ) -> VerifiedObjectTransfer:
        if not isinstance(content, bytes):
            raise FederationValidationError(
                "invalid-object-transfer-content",
                "content",
                "must be bytes",
            )
        outgoing_key = self.journal.outgoing_key(
            target_node_id,
            request_id,
            object_id,
        )
        source = self.journal.managed_source_path(outgoing_key)
        await asyncio.to_thread(self._write_managed_source, source, content)
        receipt = await self.send_file(
            target_node_id=target_node_id,
            session_id=session_id,
            request_id=request_id,
            object_id=object_id,
            source_path=source,
            chunk_size=chunk_size,
            _managed_source=True,
        )
        await asyncio.to_thread(source.unlink, missing_ok=True)
        return receipt

    @staticmethod
    def _write_managed_source(path: Path, content: bytes) -> None:
        if path.exists():
            existing = path.read_bytes()
            if existing != content:
                raise FederationValidationError(
                    "object-transfer-resume-source-mismatch",
                    "content",
                    "managed source content changed after interruption",
                )
            return
        temporary = path.parent / f".{path.name}.{secrets.token_hex(8)}.tmp"
        try:
            with temporary.open("xb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    async def send_file(
        self,
        *,
        target_node_id: str,
        session_id: str,
        request_id: str,
        object_id: str,
        source_path: str | Path,
        chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
        _managed_source: bool = False,
    ) -> VerifiedObjectTransfer:
        await self.start()
        target = self._peers.get(target_node_id)
        if target is None:
            raise DirectTransportUnavailable(
                "object-transfer-route-missing",
                f"no explicit direct transfer route for {target_node_id}",
            )
        source = Path(source_path)
        candidate = await asyncio.to_thread(
            self._file_manifest,
            source,
            object_id,
            chunk_size,
        )
        outgoing_key = self.journal.outgoing_key(
            target_node_id,
            request_id,
            object_id,
        )
        current, manifest, _ = self.journal.open_outgoing(
            outgoing_key=outgoing_key,
            target_node_id=target_node_id,
            session_id=session_id,
            request_id=request_id,
            object_id=object_id,
            source_path=source,
            managed_source=_managed_source,
            candidate_manifest=candidate,
        )
        if current.get("state") == "completed":
            return _receipt_for_manifest(current.get("receipt"), manifest)

        ephemeral = X25519PrivateKey.generate()
        opening = PeerStreamOpen.create(
            session_id=session_id,
            stream_id=(
                f"object-{manifest.transfer_id}-resume-{secrets.token_urlsafe(10)}"
            ),
            request_id=request_id,
            source_node_id=self.node_id,
            target_node_id=target.node_id,
            key_id=f"key-{secrets.token_urlsafe(18)}",
            ephemeral_public_key=_public_key_text(ephemeral.public_key()),
            sign=self.credentials.sign,
        )
        shared_secret = ephemeral.exchange(_public_key(target.encryption_public_key))
        sequence = 0
        acknowledgement, sequence = await self._send(
            target,
            opening,
            shared_secret,
            sequence,
            manifest,
            "manifest",
            {"manifest": manifest.to_dict()},
        )
        next_index = self._negotiated_index(acknowledgement, manifest)
        self.journal.record_outgoing_cursor(
            outgoing_key,
            next_index,
            manifest.chunk_count,
        )
        for index in range(next_index, manifest.chunk_count):
            expected_length = manifest.expected_chunk_length(index)
            data = await asyncio.to_thread(
                self._read_source_chunk,
                source,
                index * manifest.chunk_size,
                expected_length,
            )
            if len(data) != expected_length:
                raise FederationValidationError(
                    "object-transfer-source-size-changed",
                    "source_path",
                    "source file size changed during resumable transfer",
                )
            chunk = ObjectTransferChunk.create(
                manifest=manifest,
                index=index,
                data=data,
            )
            acknowledgement, sequence = await self._send(
                target,
                opening,
                shared_secret,
                sequence,
                manifest,
                "chunk",
                {"chunk": chunk.to_dict()},
                chunk_index=index,
            )
            remote_next = self._negotiated_index(acknowledgement, manifest)
            if remote_next != index + 1:
                raise FederationValidationError(
                    "object-transfer-resume-cursor-mismatch",
                    "next_missing_index",
                    "remote durable cursor did not advance by one chunk",
                )
            self.journal.record_outgoing_cursor(
                outgoing_key,
                remote_next,
                manifest.chunk_count,
            )
        source_size = (await asyncio.to_thread(source.stat)).st_size
        if source_size != manifest.total_size:
            raise FederationValidationError(
                "object-transfer-source-size-changed",
                "source_path",
                "source file size changed during resumable transfer",
            )
        acknowledgement, _ = await self._send(
            target,
            opening,
            shared_secret,
            sequence,
            manifest,
            "complete",
            {},
        )
        receipt = self._receipt(acknowledgement, manifest)
        self.journal.complete_outgoing(outgoing_key, receipt)
        return receipt

    @staticmethod
    def _negotiated_index(
        acknowledgement: ObjectTransferAck,
        manifest: ObjectTransferManifest,
    ) -> int:
        accepted = acknowledgement.accepted_chunks
        remaining = acknowledgement.remaining_chunks
        if accepted > manifest.chunk_count or remaining != manifest.chunk_count - accepted:
            raise FederationValidationError(
                "object-transfer-resume-count-mismatch",
                "accepted_chunks",
                "remote durable cursor does not match the manifest",
            )
        if remaining == 0:
            if acknowledgement.next_missing_index is not None:
                raise FederationValidationError(
                    "object-transfer-resume-cursor-mismatch",
                    "next_missing_index",
                    "complete cursor must not identify a missing chunk",
                )
            return manifest.chunk_count
        if acknowledgement.next_missing_index != accepted:
            raise FederationValidationError(
                "object-transfer-resume-cursor-gap",
                "next_missing_index",
                "resumable transfer requires a contiguous accepted prefix",
            )
        return accepted

    def _reserve_capacity(self) -> None:
        if len(self._inbound) < self.max_inbound_transfers:
            return
        self._inbound.popitem(last=False)

    def _apply_manifest(
        self,
        inbound: Any,
        value: Any,
    ) -> ObjectTransferAck:
        manifest = ObjectTransferManifest.from_dict(value)
        expected = f"object-{manifest.transfer_id}"
        if not (
            inbound.opening.stream_id == expected
            or inbound.opening.stream_id.startswith(f"{expected}-resume-")
        ):
            raise FederationValidationError(
                "object-transfer-stream-id-mismatch",
                "stream_id",
                "manifest does not match the signed resumable stream",
            )
        if inbound.manifest is not None:
            if inbound.manifest == manifest:
                return self._ack(
                    inbound,
                    "manifest",
                    accepted=True,
                    duplicate=True,
                )
            raise FederationValidationError(
                "object-transfer-conflicting-manifest",
                "manifest",
                "stream already has a different manifest",
            )
        _, created = self.journal.open_incoming(
            inbound.opening.source_node_id,
            manifest,
        )
        current = self.journal.load_incoming(manifest.transfer_id)
        if current is not None and current.get("state") == "publishing":
            if self.store.published_matches(manifest):
                receipt = _receipt_for_manifest(current.get("receipt"), manifest)
                self.journal.complete_incoming(manifest, receipt)
                self.store.delete(manifest.transfer_id)
                current = self.journal.load_incoming(manifest.transfer_id)
            elif self.store.published_path(manifest.object_id) is not None:
                raise FederationValidationError(
                    "object-transfer-durable-publication-mismatch",
                    "object_id",
                    "publishing receipt conflicts with the published object",
                )
        inbound.manifest = manifest
        inbound.receiver = self.journal.restore_receiver(manifest, self.store)
        if current is not None and current.get("state") == "completed":
            inbound.receipt = inbound.receiver.verify_complete()
            inbound.published_path = self.store.published_path(manifest.object_id)
            if inbound.published_path is None:
                raise FederationValidationError(
                    "object-transfer-durable-publication-missing",
                    "object_id",
                    "completed durable receipt has no published object",
                )
        return self._ack(
            inbound,
            "manifest",
            accepted=True,
            duplicate=not created,
        )

    def _apply_chunk(
        self,
        inbound: Any,
        value: Any,
    ) -> ObjectTransferAck:
        assert inbound.receiver is not None
        assert inbound.manifest is not None
        try:
            chunk = ObjectTransferChunk.from_dict(value)
            accepted = inbound.receiver.accept(chunk)
            self.journal.record_incoming_chunk(inbound.manifest, chunk)
        except (FederationOperationError, FederationValidationError) as exc:
            status = inbound.receiver.status()
            retryable = isinstance(exc, FederationValidationError) and exc.code in {
                "invalid-object-transfer-encoding",
                "object-transfer-chunk-length-mismatch",
                "object-transfer-chunk-hash-mismatch",
                "object-transfer-chunk-size-mismatch",
            }
            return self._ack(
                inbound,
                "chunk",
                accepted=False,
                retryable=retryable,
                error_code=exc.code,
                status=status,
            )
        return self._ack(
            inbound,
            "chunk",
            accepted=True,
            duplicate=not accepted,
            chunk_index=chunk.index,
        )

    def _apply_complete(self, inbound: Any) -> ObjectTransferAck:
        assert inbound.receiver is not None
        assert inbound.manifest is not None
        duplicate = inbound.receipt is not None
        if inbound.receipt is None:
            try:
                receipt = inbound.receiver.verify_complete()
                self.journal.prepare_incoming_completion(inbound.manifest, receipt)
                published_path = self.store.publish_verified(inbound.manifest, receipt)
                self.journal.complete_incoming(inbound.manifest, receipt)
            except (FederationOperationError, FederationValidationError) as exc:
                return self._ack(
                    inbound,
                    "complete",
                    accepted=False,
                    retryable=False,
                    error_code=exc.code,
                )
            inbound.receipt = receipt
            inbound.published_path = published_path
        return self._ack(
            inbound,
            "complete",
            accepted=True,
            duplicate=duplicate,
            receipt=inbound.receipt.to_dict(),
        )

    async def cleanup_abandoned(
        self,
        *,
        older_than_seconds: float = DEFAULT_ABANDONED_TRANSFER_AGE_SECONDS,
    ) -> DurableTransferCleanup:
        return await asyncio.to_thread(
            self.journal.cleanup_abandoned,
            self.store,
            older_than_seconds=older_than_seconds,
        )
