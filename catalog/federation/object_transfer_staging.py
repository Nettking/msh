"""Process-local disk staging for verified object transfers."""

from __future__ import annotations

import hashlib
import os
import secrets
import shutil
from pathlib import Path

from .errors import FederationOperationError, FederationValidationError
from .object_transfer import (
    MAX_TRANSFER_CHUNK_BYTES,
    MAX_TRANSFER_OBJECT_BYTES,
    ObjectTransferManifest,
    VerifiedObjectTransfer,
)
from .peer_stream import validate_peer_public_text


class FilesystemObjectTransferChunkStore:
    """Atomically stage chunks and publish only a fully verified object."""

    def __init__(
        self,
        staging_root: str | Path,
        publication_root: str | Path,
        *,
        max_bytes: int = MAX_TRANSFER_OBJECT_BYTES,
    ) -> None:
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        self.staging_root = Path(staging_root)
        self.publication_root = Path(publication_root)
        self.max_bytes = max_bytes
        self.staging_root.mkdir(parents=True, exist_ok=True)
        self.publication_root.mkdir(parents=True, exist_ok=True)
        self._lengths: dict[tuple[str, int], int] = {}
        self._stored_bytes = 0
        self._published: dict[str, tuple[str, Path]] = {}

    @property
    def stored_bytes(self) -> int:
        return self._stored_bytes

    def _directory(self, transfer_id: str) -> Path:
        transfer_id = validate_peer_public_text(transfer_id, "transfer_id")
        digest = hashlib.sha256(transfer_id.encode("utf-8")).hexdigest()
        return self.staging_root / digest

    def _chunk_path(self, transfer_id: str, index: int) -> Path:
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise FederationValidationError(
                "invalid-object-transfer-index", "index", "must be non-negative"
            )
        return self._directory(transfer_id) / f"chunk-{index:08d}.part"

    def put(self, transfer_id: str, index: int, data: bytes) -> None:
        if not isinstance(data, bytes) or not 0 < len(data) <= MAX_TRANSFER_CHUNK_BYTES:
            raise FederationValidationError(
                "invalid-object-transfer-staged-chunk",
                "data",
                f"must contain 1-{MAX_TRANSFER_CHUNK_BYTES} bytes",
            )
        destination = self._chunk_path(transfer_id, index)
        key = (transfer_id, index)
        projected = self._stored_bytes - self._lengths.get(key, 0) + len(data)
        if projected > self.max_bytes:
            raise FederationOperationError(
                "object-transfer-staging-capacity-exceeded",
                "disk staging capacity would be exceeded",
                "data",
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.parent / f".{destination.name}.{secrets.token_hex(8)}.tmp"
        try:
            with temporary.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        self._lengths[key] = len(data)
        self._stored_bytes = projected

    def get(self, transfer_id: str, index: int) -> bytes:
        try:
            data = self._chunk_path(transfer_id, index).read_bytes()
        except FileNotFoundError as exc:
            raise FederationOperationError(
                "object-transfer-chunk-not-staged",
                "accepted chunk is missing from disk staging",
                "index",
            ) from exc
        if len(data) > MAX_TRANSFER_CHUNK_BYTES:
            raise FederationValidationError(
                "object-transfer-staged-chunk-too-large",
                "index",
                "staged chunk exceeds the protocol bound",
            )
        return data

    def delete(self, transfer_id: str) -> None:
        shutil.rmtree(self._directory(transfer_id), ignore_errors=True)
        for key in [key for key in self._lengths if key[0] == transfer_id]:
            self._stored_bytes -= self._lengths.pop(key)

    def published_path(self, object_id: str) -> Path | None:
        value = self._published.get(object_id)
        return value[1] if value else None

    @staticmethod
    def _file_identity(path: Path) -> tuple[int, str]:
        digest = hashlib.sha256()
        total = 0
        with path.open("rb") as handle:
            while data := handle.read(1024 * 1024):
                digest.update(data)
                total += len(data)
        return total, f"sha256:{digest.hexdigest()}"

    def publish_verified(
        self,
        manifest: ObjectTransferManifest,
        receipt: VerifiedObjectTransfer,
    ) -> Path:
        expected = (
            manifest.transfer_id,
            manifest.object_id,
            manifest.total_size,
            manifest.object_hash,
            manifest.chunk_count,
        )
        actual = (
            receipt.transfer_id,
            receipt.object_id,
            receipt.total_size,
            receipt.object_hash,
            receipt.chunk_count,
        )
        if actual != expected:
            raise FederationValidationError(
                "object-transfer-publication-receipt-mismatch",
                "receipt",
                "verified receipt does not match the manifest",
            )
        known = self._published.get(manifest.object_id)
        if known:
            if known[0] != manifest.object_hash:
                raise FederationValidationError(
                    "object-transfer-publication-conflict",
                    "object_id",
                    "object ID is already published with different content",
                )
            return known[1]

        name = hashlib.sha256(manifest.object_id.encode("utf-8")).hexdigest() + ".object"
        destination = self.publication_root / name
        if destination.exists():
            if self._file_identity(destination) != (manifest.total_size, manifest.object_hash):
                raise FederationValidationError(
                    "object-transfer-publication-conflict",
                    "object_id",
                    "publication target already contains different content",
                )
            self._published[manifest.object_id] = (manifest.object_hash, destination)
            self.delete(manifest.transfer_id)
            return destination

        temporary = self.publication_root / f".{name}.{secrets.token_hex(8)}.tmp"
        digest = hashlib.sha256()
        total = 0
        try:
            with temporary.open("xb") as handle:
                for index in range(manifest.chunk_count):
                    data = self.get(manifest.transfer_id, index)
                    digest.update(data)
                    total += len(data)
                    handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            if (total, f"sha256:{digest.hexdigest()}") != (
                manifest.total_size,
                manifest.object_hash,
            ):
                raise FederationValidationError(
                    "object-transfer-publication-verification-failed",
                    "object_hash",
                    "staged bytes changed before atomic publication",
                )
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        self._published[manifest.object_id] = (manifest.object_hash, destination)
        self.delete(manifest.transfer_id)
        return destination
