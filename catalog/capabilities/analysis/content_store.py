"""Content-addressed local storage for analysis input and result artifacts.

The store is deliberately dumb: it validates logical object keys, refuses to
leave its root, and verifies content identity on every read. All authorization
decisions belong to the artifact authority, never to this class.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from catalog.federation.errors import FederationValidationError

from ..artifact_contracts import _logical_key
from .contracts import DEFAULT_MAX_SLICE_BYTES

DEFAULT_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class ContentIdentity:
    """Verifiable identity of one stored artifact body."""

    content_hash: str
    size_bytes: int


class LocalArtifactContentStore:
    """Store artifact bodies under one root using validated logical keys."""

    def __init__(
        self,
        root: Path | str,
        *,
        chunk_size: int = DEFAULT_CHUNK_BYTES,
        max_bytes: int = DEFAULT_MAX_SLICE_BYTES,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.chunk_size = int(chunk_size)
        self.max_bytes = int(max_bytes)

    def resolve(self, object_key: str) -> Path:
        """Return the on-disk path for one logical key, never escaping the root."""

        key = _logical_key(object_key, "object_key")
        candidate = (self.root / key).resolve()
        if candidate != self.root and self.root not in candidate.parents:
            raise FederationValidationError(
                "artifact-object-key-escape",
                "object_key",
                "resolved outside the artifact content root",
            )
        return candidate

    def exists(self, object_key: str) -> bool:
        return self.resolve(object_key).is_file()

    def _atomic_write(self, destination: Path, chunks: Iterable[bytes]) -> ContentIdentity:
        destination.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        descriptor, name = tempfile.mkstemp(dir=destination.parent, suffix=".partial")
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                for chunk in chunks:
                    if not isinstance(chunk, (bytes, bytearray)):
                        raise FederationValidationError(
                            "invalid-artifact-chunk", "chunk", "must be bytes"
                        )
                    size += len(chunk)
                    if size > self.max_bytes:
                        raise FederationValidationError(
                            "analysis-artifact-too-large",
                            "size_bytes",
                            f"must not exceed {self.max_bytes} bytes",
                        )
                    digest.update(chunk)
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return ContentIdentity("sha256:" + digest.hexdigest(), size)

    def write_bytes(self, object_key: str, payload: bytes) -> ContentIdentity:
        return self._atomic_write(self.resolve(object_key), (payload,))

    def write_chunks(self, object_key: str, chunks: Iterable[bytes]) -> ContentIdentity:
        return self._atomic_write(self.resolve(object_key), chunks)

    def identity(self, object_key: str) -> ContentIdentity:
        path = self.resolve(object_key)
        if not path.is_file():
            raise FederationValidationError(
                "analysis-artifact-missing", "object_key", "artifact body is not stored"
            )
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as handle:
            while chunk := handle.read(self.chunk_size):
                size += len(chunk)
                digest.update(chunk)
        return ContentIdentity("sha256:" + digest.hexdigest(), size)

    def stream(
        self,
        object_key: str,
        *,
        content_hash: str,
        size_bytes: int,
    ) -> Iterator[bytes]:
        """Yield the stored body, failing closed on any identity mismatch."""

        path = self.resolve(object_key)
        if not path.is_file():
            raise FederationValidationError(
                "analysis-artifact-missing", "object_key", "artifact body is not stored"
            )
        actual_size = path.stat().st_size
        if actual_size != size_bytes:
            raise FederationValidationError(
                "analysis-artifact-integrity-mismatch",
                "size_bytes",
                "stored artifact size differs from its registered identity",
            )
        if actual_size > self.max_bytes:
            raise FederationValidationError(
                "analysis-artifact-too-large",
                "size_bytes",
                f"must not exceed {self.max_bytes} bytes",
            )
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(self.chunk_size):
                digest.update(chunk)
                yield chunk
        if "sha256:" + digest.hexdigest() != content_hash:
            raise FederationValidationError(
                "analysis-artifact-integrity-mismatch",
                "content_hash",
                "stored artifact content differs from its registered identity",
            )

    def read_range(
        self,
        object_key: str,
        *,
        offset: int,
        length: int,
        content_hash: str,
        size_bytes: int,
    ) -> bytes:
        """Return one bounded slice of a stored body for F6 chunk transfer.

        Whole-object integrity is verified by the F6 receiver against the
        manifest, so this only enforces that the stored body still has the
        registered size and that the range stays inside it.
        """

        path = self.resolve(object_key)
        if not path.is_file():
            raise FederationValidationError(
                "analysis-artifact-missing", "object_key", "artifact body is not stored"
            )
        if path.stat().st_size != size_bytes:
            raise FederationValidationError(
                "analysis-artifact-integrity-mismatch",
                "size_bytes",
                "stored artifact size differs from its registered identity",
            )
        if offset < 0 or length < 0 or offset + length > size_bytes:
            raise FederationValidationError(
                "analysis-artifact-range-invalid",
                "offset",
                "requested range is outside the registered artifact",
            )
        if length > self.chunk_size and length > DEFAULT_CHUNK_BYTES:
            raise FederationValidationError(
                "analysis-artifact-range-too-large",
                "length",
                "chunk length exceeds the configured transfer chunk size",
            )
        with path.open("rb") as handle:
            handle.seek(offset)
            data = handle.read(length)
        if len(data) != length:
            raise FederationValidationError(
                "analysis-artifact-integrity-mismatch",
                "object_key",
                "stored artifact is shorter than its registered identity",
            )
        return data

    def read_bytes(
        self,
        object_key: str,
        *,
        content_hash: str,
        size_bytes: int,
        max_bytes: int | None = None,
    ) -> bytes:
        limit = self.max_bytes if max_bytes is None else min(self.max_bytes, max_bytes)
        if size_bytes > limit:
            raise FederationValidationError(
                "analysis-artifact-too-large",
                "size_bytes",
                f"must not exceed {limit} bytes",
            )
        return b"".join(
            self.stream(object_key, content_hash=content_hash, size_bytes=size_bytes)
        )


__all__ = [
    "DEFAULT_CHUNK_BYTES",
    "ContentIdentity",
    "LocalArtifactContentStore",
]
