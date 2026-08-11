"""Manifest-authoritative discovery of committed federation batches.

The catalog deliberately never scans or reads a storage provider.  A provider
may contain prepared, orphaned, or stale data; only a committed item in the
coordinator-owned manifest is federation-visible.  Cursors pin pagination to
one immutable manifest revision so a concurrent commit cannot duplicate or
skip entries between pages.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar, Self

from .errors import FederationValidationError, ProtocolCompatibilityError
from .manifest import AuthoritativeStorageManifest, ManifestItem, ManifestItemKind
from .models import CommitState

COMMITTED_BATCH_REFERENCE_SCHEMA = "fcp.committed_batch_reference.v1"
COMMITTED_BATCH_PAGE_SCHEMA = "fcp.committed_batch_page.v1"
COMMITTED_BATCH_DELTA_PAGE_SCHEMA = "fcp.committed_batch_delta_page.v1"
MAX_BATCH_PAGE_SIZE = 50

_CURSOR_SCHEMA = "fcp.manifest_batch_cursor.v1"
_DELTA_CURSOR_SCHEMA = "fcp.manifest_batch_delta_cursor.v1"
_CURSOR_MAX_BYTES = 8192
_TEXT_MAX_BYTES = 4096
_SHA256_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
_PROCESS_CURSOR_SECRET = secrets.token_bytes(32)


def _validation(code: str, field: str, message: str) -> FederationValidationError:
    return FederationValidationError(code, field, message)


def _required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise _validation(
            "invalid-id",
            field,
            f"must be non-empty bounded opaque text (at most {_TEXT_MAX_BYTES} bytes)",
        )
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise _validation(
            "invalid-id",
            field,
            "must contain valid Unicode text",
        ) from exc
    if len(encoded) > _TEXT_MAX_BYTES:
        raise _validation(
            "invalid-id",
            field,
            f"must be non-empty bounded opaque text (at most {_TEXT_MAX_BYTES} bytes)",
        )
    return value


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field)


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _validation(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


def _positive_int(value: Any, field: str) -> int:
    result = _uint(value, field)
    if result == 0:
        raise _validation(
            "invalid-positive-integer",
            field,
            "must be greater than zero",
        )
    return result


def _sha256(value: Any, field: str) -> str:
    value = _required_text(value, field)
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise _validation(
            "invalid-content-hash",
            field,
            "must be a lowercase sha256 digest",
        )
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise _validation(
                "invalid-timestamp",
                field,
                "must be an RFC 3339 timestamp",
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise _validation(
            "invalid-timestamp",
            field,
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _strict_object(
    value: Any,
    *,
    field: str,
    keys: frozenset[str],
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _validation("invalid-object", field, "must be an object")
    if any(not isinstance(key, str) for key in value):
        raise _validation(
            "invalid-field-name",
            field,
            "object field names must be strings",
        )
    actual = set(value)
    missing = sorted(keys - actual)
    if missing:
        raise _validation("missing-field", missing[0], "is required")
    unknown = sorted(actual - keys)
    if unknown:
        raise _validation("unknown-field", unknown[0], "is not supported")
    return value


def _require_schema(value: Any, expected: str) -> str:
    if value != expected:
        raise ProtocolCompatibilityError(
            "unsupported-schema",
            "schema",
            f"expected {expected}, received {value!r}",
        )
    return expected


def _cursor_error() -> FederationValidationError:
    return _validation(
        "invalid-batch-cursor",
        "cursor",
        "must be an intact coordinator-issued cursor for this listing",
    )


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    decoded = base64.b64decode(
        value + "=" * (-len(value) % 4),
        altchars=b"-_",
        validate=True,
    )
    if _b64encode(decoded) != value:
        raise ValueError("non-canonical base64url")
    return decoded


def _json_without_duplicate_keys(value: bytes) -> Any:
    def build_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    return json.loads(value.decode("utf-8"), object_pairs_hook=build_object)


@dataclass(frozen=True)
class CommittedBatchReference:
    """Portable identity and commit evidence for one manifest batch."""

    SCHEMA: ClassVar[str] = COMMITTED_BATCH_REFERENCE_SCHEMA

    session_id: str
    group_id: str
    dataset_id: str
    batch_id: str
    idempotency_key: str
    content_hash: str
    size_bytes: int
    schema_name: str
    schema_version: int
    source_id: str | None
    first_sequence: int | None
    last_sequence: int | None
    commit_state: CommitState
    acknowledged_provider_ids: tuple[str, ...]
    committed_at: datetime
    manifest_revision: int
    manifest_hash: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {
            "schema",
            "session_id",
            "group_id",
            "dataset_id",
            "batch_id",
            "idempotency_key",
            "content_hash",
            "size_bytes",
            "schema_name",
            "schema_version",
            "source_id",
            "first_sequence",
            "last_sequence",
            "commit_state",
            "acknowledged_provider_ids",
            "committed_at",
            "manifest_revision",
            "manifest_hash",
        }
    )

    def __post_init__(self) -> None:
        for field in (
            "session_id",
            "group_id",
            "dataset_id",
            "batch_id",
            "idempotency_key",
            "schema_name",
        ):
            object.__setattr__(self, field, _required_text(getattr(self, field), field))
        object.__setattr__(
            self, "content_hash", _sha256(self.content_hash, "content_hash")
        )
        object.__setattr__(self, "size_bytes", _uint(self.size_bytes, "size_bytes"))
        object.__setattr__(
            self,
            "schema_version",
            _positive_int(self.schema_version, "schema_version"),
        )
        object.__setattr__(
            self, "source_id", _optional_text(self.source_id, "source_id")
        )
        if (self.first_sequence is None) != (self.last_sequence is None):
            raise _validation(
                "invalid-sequence-range",
                "first_sequence",
                "sequence start and end must be supplied together",
            )
        if self.first_sequence is not None and self.last_sequence is not None:
            first = _uint(self.first_sequence, "first_sequence")
            last = _uint(self.last_sequence, "last_sequence")
            if last < first:
                raise _validation(
                    "invalid-sequence-range",
                    "last_sequence",
                    "must not precede first_sequence",
                )
            if self.source_id is None:
                raise _validation(
                    "missing-field",
                    "source_id",
                    "sequence-bearing batches require a source identity",
                )
            object.__setattr__(self, "first_sequence", first)
            object.__setattr__(self, "last_sequence", last)
        try:
            commit_state = CommitState(self.commit_state)
        except (TypeError, ValueError) as exc:
            raise _validation(
                "invalid-enum",
                "commit_state",
                "unknown storage commit state",
            ) from exc
        if commit_state is not CommitState.COMMITTED:
            raise _validation(
                "batch-not-committed",
                "commit_state",
                "a committed batch reference must contain committed evidence",
            )
        object.__setattr__(self, "commit_state", commit_state)
        if not isinstance(self.acknowledged_provider_ids, tuple):
            raise _validation(
                "invalid-array",
                "acknowledged_provider_ids",
                "must be a tuple of provider identities",
            )
        providers = tuple(
            sorted(
                _required_text(provider_id, "acknowledged_provider_ids")
                for provider_id in self.acknowledged_provider_ids
            )
        )
        if not providers:
            raise _validation(
                "missing-commit-evidence",
                "acknowledged_provider_ids",
                "at least one durable provider acknowledgement is required",
            )
        if len(providers) != len(set(providers)):
            raise _validation(
                "duplicate-provider",
                "acknowledged_provider_ids",
                "provider acknowledgements must be unique",
            )
        object.__setattr__(self, "acknowledged_provider_ids", providers)
        object.__setattr__(
            self, "committed_at", _utc(self.committed_at, "committed_at")
        )
        object.__setattr__(
            self,
            "manifest_revision",
            _uint(self.manifest_revision, "manifest_revision"),
        )
        object.__setattr__(
            self, "manifest_hash", _sha256(self.manifest_hash, "manifest_hash")
        )

    @classmethod
    def from_manifest_item(
        cls,
        manifest: AuthoritativeStorageManifest,
        item: ManifestItem,
    ) -> Self:
        if item.kind is not ManifestItemKind.BATCH:
            raise _validation(
                "invalid-manifest-item-kind",
                "kind",
                "only batch items can become committed batch references",
            )
        if item.commit_state is not CommitState.COMMITTED:
            raise _validation(
                "batch-not-committed",
                "commit_state",
                "only committed manifest items are federation-visible",
            )
        if item.idempotency_key is None:
            raise _validation(
                "missing-field",
                "idempotency_key",
                "batch items require an idempotency key",
            )
        return cls(
            session_id=manifest.session_id,
            group_id=manifest.group_id,
            dataset_id=item.dataset_id,
            batch_id=item.item_id,
            idempotency_key=item.idempotency_key,
            content_hash=item.content_hash,
            size_bytes=item.size_bytes,
            schema_name=item.schema_name,
            schema_version=item.schema_version,
            source_id=item.source_id,
            first_sequence=item.first_sequence,
            last_sequence=item.last_sequence,
            commit_state=item.commit_state,
            acknowledged_provider_ids=item.acknowledged_provider_ids,
            committed_at=item.committed_at,
            manifest_revision=manifest.revision,
            manifest_hash=manifest.manifest_hash,
        )

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        value = _strict_object(
            value,
            field="committed_batch_reference",
            keys=cls._KEYS,
        )
        _require_schema(value["schema"], cls.SCHEMA)
        providers = value["acknowledged_provider_ids"]
        if not isinstance(providers, list):
            raise _validation(
                "invalid-array",
                "acknowledged_provider_ids",
                "must be an array",
            )
        return cls(
            session_id=value["session_id"],
            group_id=value["group_id"],
            dataset_id=value["dataset_id"],
            batch_id=value["batch_id"],
            idempotency_key=value["idempotency_key"],
            content_hash=value["content_hash"],
            size_bytes=value["size_bytes"],
            schema_name=value["schema_name"],
            schema_version=value["schema_version"],
            source_id=value["source_id"],
            first_sequence=value["first_sequence"],
            last_sequence=value["last_sequence"],
            commit_state=value["commit_state"],
            acknowledged_provider_ids=tuple(providers),
            committed_at=value["committed_at"],
            manifest_revision=value["manifest_revision"],
            manifest_hash=value["manifest_hash"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "dataset_id": self.dataset_id,
            "batch_id": self.batch_id,
            "idempotency_key": self.idempotency_key,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "source_id": self.source_id,
            "first_sequence": self.first_sequence,
            "last_sequence": self.last_sequence,
            "commit_state": self.commit_state.value,
            "acknowledged_provider_ids": list(self.acknowledged_provider_ids),
            "committed_at": _timestamp(self.committed_at),
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
        }


@dataclass(frozen=True)
class CommittedBatchPage:
    """One bounded page from an immutable authoritative manifest revision."""

    SCHEMA: ClassVar[str] = COMMITTED_BATCH_PAGE_SCHEMA

    session_id: str
    group_id: str
    dataset_id: str | None
    manifest_revision: int
    manifest_hash: str
    batches: tuple[CommittedBatchReference, ...]
    next_cursor: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {
            "schema",
            "session_id",
            "group_id",
            "dataset_id",
            "manifest_revision",
            "manifest_hash",
            "batches",
            "next_cursor",
        }
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "session_id", _required_text(self.session_id, "session_id")
        )
        object.__setattr__(self, "group_id", _required_text(self.group_id, "group_id"))
        object.__setattr__(
            self, "dataset_id", _optional_text(self.dataset_id, "dataset_id")
        )
        object.__setattr__(
            self,
            "manifest_revision",
            _uint(self.manifest_revision, "manifest_revision"),
        )
        object.__setattr__(
            self, "manifest_hash", _sha256(self.manifest_hash, "manifest_hash")
        )
        if not isinstance(self.batches, tuple):
            raise _validation("invalid-array", "batches", "must be a tuple")
        if len(self.batches) > MAX_BATCH_PAGE_SIZE:
            raise _validation(
                "page-too-large",
                "batches",
                f"must contain at most {MAX_BATCH_PAGE_SIZE} references",
            )
        previous_identity: tuple[str, str] | None = None
        seen: set[tuple[str, str]] = set()
        for batch in self.batches:
            if not isinstance(batch, CommittedBatchReference):
                raise _validation(
                    "invalid-batch-reference",
                    "batches",
                    "must contain committed batch references",
                )
            if (
                batch.session_id != self.session_id
                or batch.group_id != self.group_id
                or batch.manifest_revision != self.manifest_revision
                or batch.manifest_hash != self.manifest_hash
            ):
                raise _validation(
                    "batch-page-scope-mismatch",
                    "batches",
                    "every reference must belong to the page manifest",
                )
            if self.dataset_id is not None and batch.dataset_id != self.dataset_id:
                raise _validation(
                    "batch-page-dataset-mismatch",
                    "batches",
                    "every reference must match the page dataset filter",
                )
            identity = (batch.dataset_id, batch.batch_id)
            if identity in seen:
                raise _validation(
                    "duplicate-batch-reference",
                    "batches",
                    "batch references must be unique within a page",
                )
            if previous_identity is not None and identity < previous_identity:
                raise _validation(
                    "non-canonical-batch-order",
                    "batches",
                    "batch references must retain authoritative manifest order",
                )
            seen.add(identity)
            previous_identity = identity
        if self.next_cursor is not None and (
            not isinstance(self.next_cursor, str)
            or not self.next_cursor
            or not self.next_cursor.isascii()
            or len(self.next_cursor.encode("ascii")) > _CURSOR_MAX_BYTES
        ):
            raise _validation(
                "invalid-batch-cursor",
                "next_cursor",
                "must be a bounded opaque ASCII cursor",
            )

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        value = _strict_object(value, field="committed_batch_page", keys=cls._KEYS)
        _require_schema(value["schema"], cls.SCHEMA)
        batches = value["batches"]
        if not isinstance(batches, list):
            raise _validation("invalid-array", "batches", "must be an array")
        if len(batches) > MAX_BATCH_PAGE_SIZE:
            raise _validation(
                "page-too-large",
                "batches",
                f"must contain at most {MAX_BATCH_PAGE_SIZE} references",
            )
        return cls(
            session_id=value["session_id"],
            group_id=value["group_id"],
            dataset_id=value["dataset_id"],
            manifest_revision=value["manifest_revision"],
            manifest_hash=value["manifest_hash"],
            batches=tuple(CommittedBatchReference.from_dict(item) for item in batches),
            next_cursor=value["next_cursor"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "dataset_id": self.dataset_id,
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
            "batches": [batch.to_dict() for batch in self.batches],
            "next_cursor": self.next_cursor,
        }


@dataclass(frozen=True)
class CommittedBatchDeltaPage:
    """Committed batches added after one exact authoritative manifest."""

    SCHEMA: ClassVar[str] = COMMITTED_BATCH_DELTA_PAGE_SCHEMA

    session_id: str
    group_id: str
    dataset_id: str | None
    baseline_manifest_revision: int
    baseline_manifest_hash: str
    manifest_revision: int
    manifest_hash: str
    batches: tuple[CommittedBatchReference, ...]
    next_cursor: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {
            "schema",
            "session_id",
            "group_id",
            "dataset_id",
            "baseline_manifest_revision",
            "baseline_manifest_hash",
            "manifest_revision",
            "manifest_hash",
            "batches",
            "next_cursor",
        }
    )

    def __post_init__(self) -> None:
        baseline_revision = _uint(
            self.baseline_manifest_revision,
            "baseline_manifest_revision",
        )
        baseline_hash = _sha256(
            self.baseline_manifest_hash,
            "baseline_manifest_hash",
        )
        # Reuse the full-page contract for scope, ordering, bounds, and cursor
        # validation. Delta references are still pinned to the current head.
        page = CommittedBatchPage(
            session_id=self.session_id,
            group_id=self.group_id,
            dataset_id=self.dataset_id,
            manifest_revision=self.manifest_revision,
            manifest_hash=self.manifest_hash,
            batches=self.batches,
            next_cursor=self.next_cursor,
        )
        if baseline_revision > page.manifest_revision:
            raise _validation(
                "invalid-manifest-baseline",
                "baseline_manifest_revision",
                "must not be newer than the listed manifest",
            )
        if (
            baseline_revision == page.manifest_revision
            and baseline_hash != page.manifest_hash
        ):
            raise _validation(
                "manifest-baseline-mismatch",
                "baseline_manifest_hash",
                "must equal the listed manifest hash at the same revision",
            )
        object.__setattr__(self, "session_id", page.session_id)
        object.__setattr__(self, "group_id", page.group_id)
        object.__setattr__(self, "dataset_id", page.dataset_id)
        object.__setattr__(
            self,
            "baseline_manifest_revision",
            baseline_revision,
        )
        object.__setattr__(self, "baseline_manifest_hash", baseline_hash)
        object.__setattr__(self, "manifest_revision", page.manifest_revision)
        object.__setattr__(self, "manifest_hash", page.manifest_hash)
        object.__setattr__(self, "batches", page.batches)
        object.__setattr__(self, "next_cursor", page.next_cursor)

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        value = _strict_object(
            value,
            field="committed_batch_delta_page",
            keys=cls._KEYS,
        )
        _require_schema(value["schema"], cls.SCHEMA)
        batches = value["batches"]
        if not isinstance(batches, list):
            raise _validation("invalid-array", "batches", "must be an array")
        if len(batches) > MAX_BATCH_PAGE_SIZE:
            raise _validation(
                "page-too-large",
                "batches",
                f"must contain at most {MAX_BATCH_PAGE_SIZE} references",
            )
        return cls(
            session_id=value["session_id"],
            group_id=value["group_id"],
            dataset_id=value["dataset_id"],
            baseline_manifest_revision=value["baseline_manifest_revision"],
            baseline_manifest_hash=value["baseline_manifest_hash"],
            manifest_revision=value["manifest_revision"],
            manifest_hash=value["manifest_hash"],
            batches=tuple(CommittedBatchReference.from_dict(item) for item in batches),
            next_cursor=value["next_cursor"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "dataset_id": self.dataset_id,
            "baseline_manifest_revision": self.baseline_manifest_revision,
            "baseline_manifest_hash": self.baseline_manifest_hash,
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
            "batches": [batch.to_dict() for batch in self.batches],
            "next_cursor": self.next_cursor,
        }


class ManifestBatchCatalog:
    """Read-only batch discovery backed by coordinator manifest authority."""

    _CURSOR_KEYS = frozenset(
        {
            "schema",
            "session_id",
            "group_id",
            "dataset_id",
            "manifest_revision",
            "manifest_hash",
            "offset",
        }
    )
    _DELTA_CURSOR_KEYS = frozenset(
        {
            "schema",
            "session_id",
            "group_id",
            "dataset_id",
            "baseline_manifest_revision",
            "baseline_manifest_hash",
            "manifest_revision",
            "manifest_hash",
            "offset",
        }
    )

    def __init__(
        self,
        control_plane: Any,
        session_id: str,
        *,
        cursor_secret: bytes | None = None,
    ) -> None:
        self.control_plane = control_plane
        self.session_id = _required_text(session_id, "session_id")
        secret = _PROCESS_CURSOR_SECRET if cursor_secret is None else cursor_secret
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise _validation(
                "invalid-cursor-secret",
                "cursor_secret",
                "must contain at least 32 bytes",
            )
        self._cursor_key = hmac.new(
            secret,
            b"fcp-manifest-batch-catalog\x00" + self.session_id.encode("utf-8"),
            hashlib.sha256,
        ).digest()

    def list_batches(
        self,
        group_id: str,
        *,
        dataset_id: str | None = None,
        limit: int = MAX_BATCH_PAGE_SIZE,
        cursor: str | None = None,
    ) -> CommittedBatchPage:
        """List committed batches without consulting provider-local catalogues."""

        group_id = _required_text(group_id, "group_id")
        dataset_id = _optional_text(dataset_id, "dataset_id")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise _validation(
                "invalid-page-limit",
                "limit",
                "must be a positive integer",
            )
        page_limit = min(limit, MAX_BATCH_PAGE_SIZE)

        # This validates current group membership before a signed cursor is
        # allowed to address historical manifest state.
        current_manifest = self.control_plane.manifest(self.session_id, group_id)
        self._validate_manifest_scope(current_manifest, group_id)
        if cursor is None:
            manifest = current_manifest
            offset = 0
        else:
            payload = self._decode_cursor(cursor)
            if (
                payload["session_id"] != self.session_id
                or payload["group_id"] != group_id
                or payload["dataset_id"] != dataset_id
            ):
                raise _cursor_error()
            manifest = self.control_plane.manifests.at_revision(
                self.session_id,
                group_id,
                payload["manifest_revision"],
            )
            if (
                not isinstance(manifest, AuthoritativeStorageManifest)
                or manifest.session_id != self.session_id
                or manifest.group_id != group_id
                or manifest.revision != payload["manifest_revision"]
                or manifest.manifest_hash != payload["manifest_hash"]
                or manifest.commit_state is not CommitState.COMMITTED
            ):
                raise _cursor_error()
            offset = payload["offset"]

        items = tuple(
            item
            for item in manifest.items
            if item.kind is ManifestItemKind.BATCH
            and item.commit_state is CommitState.COMMITTED
            and (dataset_id is None or item.dataset_id == dataset_id)
        )
        # No cursor is emitted for an exhausted or empty page, so these
        # offsets can only originate in a forged or obsolete cursor.
        if cursor is not None and (offset < 0 or offset >= len(items)):
            raise _cursor_error()
        page_items = items[offset : offset + page_limit]
        next_offset = offset + len(page_items)
        next_cursor = (
            self._encode_cursor(
                group_id=group_id,
                dataset_id=dataset_id,
                manifest=manifest,
                offset=next_offset,
            )
            if next_offset < len(items)
            else None
        )
        return CommittedBatchPage(
            session_id=self.session_id,
            group_id=group_id,
            dataset_id=dataset_id,
            manifest_revision=manifest.revision,
            manifest_hash=manifest.manifest_hash,
            batches=tuple(
                CommittedBatchReference.from_manifest_item(manifest, item)
                for item in page_items
            ),
            next_cursor=next_cursor,
        )

    def list_batch_delta(
        self,
        group_id: str,
        *,
        baseline_manifest_revision: int,
        baseline_manifest_hash: str,
        dataset_id: str | None = None,
        limit: int = MAX_BATCH_PAGE_SIZE,
        cursor: str | None = None,
    ) -> CommittedBatchDeltaPage:
        """List batches added after an exact committed manifest baseline.

        The first page snapshots the current manifest head. Subsequent pages
        are pinned to both that head and the exact baseline by an HMAC cursor,
        so concurrent commits cannot introduce skips or duplicates.
        """

        group_id = _required_text(group_id, "group_id")
        dataset_id = _optional_text(dataset_id, "dataset_id")
        baseline_manifest_revision = _uint(
            baseline_manifest_revision,
            "baseline_manifest_revision",
        )
        baseline_manifest_hash = _sha256(
            baseline_manifest_hash,
            "baseline_manifest_hash",
        )
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise _validation(
                "invalid-page-limit",
                "limit",
                "must be a positive integer",
            )
        page_limit = min(limit, MAX_BATCH_PAGE_SIZE)

        # Always consult the current head first. Besides selecting the first
        # page, this preserves the same current-membership check as full lists.
        current_manifest = self.control_plane.manifest(self.session_id, group_id)
        self._validate_manifest_scope(current_manifest, group_id)
        if cursor is None:
            manifest = current_manifest
            offset = 0
        else:
            payload = self._decode_delta_cursor(cursor)
            if (
                payload["session_id"] != self.session_id
                or payload["group_id"] != group_id
                or payload["dataset_id"] != dataset_id
                or payload["baseline_manifest_revision"] != baseline_manifest_revision
                or payload["baseline_manifest_hash"] != baseline_manifest_hash
            ):
                raise _cursor_error()
            manifest = self._manifest_at_exact_revision(
                group_id,
                revision=payload["manifest_revision"],
                manifest_hash=payload["manifest_hash"],
                missing_code="manifest-delta-head-not-found",
                mismatch_code="manifest-delta-head-mismatch",
                revision_field="manifest_revision",
                hash_field="manifest_hash",
            )
            offset = payload["offset"]

        if baseline_manifest_revision > manifest.revision:
            raise _validation(
                "invalid-manifest-baseline",
                "baseline_manifest_revision",
                "must not be newer than the listed manifest",
            )
        baseline = self._manifest_at_exact_revision(
            group_id,
            revision=baseline_manifest_revision,
            manifest_hash=baseline_manifest_hash,
            missing_code="manifest-baseline-not-found",
            mismatch_code="manifest-baseline-mismatch",
            revision_field="baseline_manifest_revision",
            hash_field="baseline_manifest_hash",
        )

        baseline_batches = {
            (item.dataset_id, item.item_id): item
            for item in baseline.items
            if item.kind is ManifestItemKind.BATCH
            and item.commit_state is CommitState.COMMITTED
        }
        current_batches = {
            (item.dataset_id, item.item_id): item
            for item in manifest.items
            if item.kind is ManifestItemKind.BATCH
            and item.commit_state is CommitState.COMMITTED
        }
        # Manifest revisions are append-only. Refuse a delta if historical
        # identity disappeared or changed instead of silently treating it as a
        # new batch and hiding divergence from the caller.
        if any(
            current_batches.get(identity) != item
            for identity, item in baseline_batches.items()
        ):
            raise _validation(
                "manifest-history-diverged",
                "baseline_manifest_revision",
                "baseline committed batches are not an immutable subset of the listed manifest",
            )
        items = tuple(
            item
            for item in manifest.items
            if item.kind is ManifestItemKind.BATCH
            and item.commit_state is CommitState.COMMITTED
            and (item.dataset_id, item.item_id) not in baseline_batches
            and (dataset_id is None or item.dataset_id == dataset_id)
        )
        if cursor is not None and (offset < 0 or offset >= len(items)):
            raise _cursor_error()
        page_items = items[offset : offset + page_limit]
        next_offset = offset + len(page_items)
        next_cursor = (
            self._encode_delta_cursor(
                group_id=group_id,
                dataset_id=dataset_id,
                baseline=baseline,
                manifest=manifest,
                offset=next_offset,
            )
            if next_offset < len(items)
            else None
        )
        return CommittedBatchDeltaPage(
            session_id=self.session_id,
            group_id=group_id,
            dataset_id=dataset_id,
            baseline_manifest_revision=baseline.revision,
            baseline_manifest_hash=baseline.manifest_hash,
            manifest_revision=manifest.revision,
            manifest_hash=manifest.manifest_hash,
            batches=tuple(
                CommittedBatchReference.from_manifest_item(manifest, item)
                for item in page_items
            ),
            next_cursor=next_cursor,
        )

    def _manifest_at_exact_revision(
        self,
        group_id: str,
        *,
        revision: int,
        manifest_hash: str,
        missing_code: str,
        mismatch_code: str,
        revision_field: str,
        hash_field: str,
    ) -> AuthoritativeStorageManifest:
        try:
            manifest = self.control_plane.manifests.at_revision(
                self.session_id,
                group_id,
                revision,
            )
        except (
            FederationValidationError,
            IndexError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise _validation(
                missing_code,
                revision_field,
                "does not identify an available committed authoritative manifest",
            ) from exc
        self._validate_manifest_scope(manifest, group_id)
        if manifest.revision != revision or manifest.manifest_hash != manifest_hash:
            raise _validation(
                mismatch_code,
                hash_field,
                "does not match the exact authoritative manifest revision",
            )
        return manifest

    def resolve_batch(
        self,
        group_id: str,
        batch_id: str,
        *,
        dataset_id: str | None = None,
        manifest_revision: int | None = None,
        manifest_hash: str | None = None,
    ) -> CommittedBatchReference:
        """Resolve commit evidence only; callers perform any provider read."""

        group_id = _required_text(group_id, "group_id")
        batch_id = _required_text(batch_id, "batch_id")
        dataset_id = _optional_text(dataset_id, "dataset_id")
        current = self.control_plane.manifest(self.session_id, group_id)
        self._validate_manifest_scope(current, group_id)
        if manifest_revision is None:
            if manifest_hash is not None:
                raise _validation(
                    "missing-field",
                    "manifest_revision",
                    "is required when manifest_hash is supplied",
                )
            manifest = current
        else:
            manifest_revision = _uint(manifest_revision, "manifest_revision")
            manifest = self.control_plane.manifests.at_revision(
                self.session_id,
                group_id,
                manifest_revision,
            )
            self._validate_manifest_scope(manifest, group_id)
            if manifest_hash is not None and manifest.manifest_hash != _sha256(
                manifest_hash,
                "manifest_hash",
            ):
                raise _validation(
                    "manifest-reference-mismatch",
                    "manifest_hash",
                    "does not identify the requested authoritative revision",
                )
        for item in manifest.items:
            if (
                item.kind is ManifestItemKind.BATCH
                and item.commit_state is CommitState.COMMITTED
                and item.item_id == batch_id
                and (dataset_id is None or item.dataset_id == dataset_id)
            ):
                return CommittedBatchReference.from_manifest_item(manifest, item)
        raise _validation(
            "batch-not-found",
            "batch_id",
            "batch is not committed in the authoritative manifest",
        )

    def resolve_reference(
        self,
        reference: CommittedBatchReference | dict[str, Any],
    ) -> CommittedBatchReference:
        """Revalidate a serialized reference against its pinned manifest."""

        if isinstance(reference, dict):
            reference = CommittedBatchReference.from_dict(reference)
        if not isinstance(reference, CommittedBatchReference):
            raise _validation(
                "invalid-batch-reference",
                "reference",
                "must be a committed batch reference",
            )
        if reference.session_id != self.session_id:
            raise _validation(
                "batch-reference-scope-mismatch",
                "session_id",
                "reference belongs to another federation session",
            )
        resolved = self.resolve_batch(
            reference.group_id,
            reference.batch_id,
            dataset_id=reference.dataset_id,
            manifest_revision=reference.manifest_revision,
            manifest_hash=reference.manifest_hash,
        )
        if resolved != reference:
            raise _validation(
                "batch-reference-mismatch",
                "reference",
                "reference does not match authoritative commit evidence",
            )
        return resolved

    def _validate_manifest_scope(
        self,
        manifest: Any,
        group_id: str,
    ) -> None:
        if not isinstance(manifest, AuthoritativeStorageManifest):
            raise _validation(
                "invalid-authoritative-manifest",
                "manifest",
                "control plane returned an invalid authoritative manifest",
            )
        if manifest.session_id != self.session_id or manifest.group_id != group_id:
            raise _validation(
                "manifest-scope-mismatch",
                "manifest",
                "authoritative manifest belongs to another storage scope",
            )
        if manifest.commit_state is not CommitState.COMMITTED:
            raise _validation(
                "manifest-not-committed",
                "manifest",
                "only a committed authoritative manifest may be listed",
            )

    def _encode_cursor(
        self,
        *,
        group_id: str,
        dataset_id: str | None,
        manifest: AuthoritativeStorageManifest,
        offset: int,
    ) -> str:
        payload = {
            "schema": _CURSOR_SCHEMA,
            "session_id": self.session_id,
            "group_id": group_id,
            "dataset_id": dataset_id,
            "manifest_revision": manifest.revision,
            "manifest_hash": manifest.manifest_hash,
            "offset": offset,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        signature = hmac.digest(self._cursor_key, encoded, hashlib.sha256)
        cursor = f"{_b64encode(encoded)}.{_b64encode(signature)}"
        if len(cursor.encode("ascii")) > _CURSOR_MAX_BYTES:
            raise _validation(
                "cursor-too-large",
                "cursor",
                "listing scope cannot be represented by a bounded cursor",
            )
        return cursor

    def _encode_delta_cursor(
        self,
        *,
        group_id: str,
        dataset_id: str | None,
        baseline: AuthoritativeStorageManifest,
        manifest: AuthoritativeStorageManifest,
        offset: int,
    ) -> str:
        payload = {
            "schema": _DELTA_CURSOR_SCHEMA,
            "session_id": self.session_id,
            "group_id": group_id,
            "dataset_id": dataset_id,
            "baseline_manifest_revision": baseline.revision,
            "baseline_manifest_hash": baseline.manifest_hash,
            "manifest_revision": manifest.revision,
            "manifest_hash": manifest.manifest_hash,
            "offset": offset,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        signature = hmac.digest(self._cursor_key, encoded, hashlib.sha256)
        cursor = f"{_b64encode(encoded)}.{_b64encode(signature)}"
        if len(cursor.encode("ascii")) > _CURSOR_MAX_BYTES:
            raise _validation(
                "cursor-too-large",
                "cursor",
                "delta listing scope cannot be represented by a bounded cursor",
            )
        return cursor

    def _decode_cursor(self, cursor: Any) -> dict[str, Any]:
        if (
            not isinstance(cursor, str)
            or not cursor
            or not cursor.isascii()
            or len(cursor.encode("ascii")) > _CURSOR_MAX_BYTES
            or cursor.count(".") != 1
        ):
            raise _cursor_error()
        payload_part, signature_part = cursor.split(".", 1)
        try:
            encoded = _b64decode(payload_part)
            signature = _b64decode(signature_part)
        except (ValueError, binascii.Error) as exc:
            raise _cursor_error() from exc
        expected = hmac.digest(self._cursor_key, encoded, hashlib.sha256)
        if len(signature) != len(expected) or not hmac.compare_digest(
            signature,
            expected,
        ):
            raise _cursor_error()
        try:
            payload = _json_without_duplicate_keys(encoded)
            payload = _strict_object(
                payload,
                field="cursor",
                keys=self._CURSOR_KEYS,
            )
            _require_schema(payload["schema"], _CURSOR_SCHEMA)
            _required_text(payload["session_id"], "session_id")
            _required_text(payload["group_id"], "group_id")
            _optional_text(payload["dataset_id"], "dataset_id")
            _uint(payload["manifest_revision"], "manifest_revision")
            _sha256(payload["manifest_hash"], "manifest_hash")
            offset = _positive_int(payload["offset"], "offset")
        except (
            FederationValidationError,
            ProtocolCompatibilityError,
            ValueError,
        ) as exc:
            raise _cursor_error() from exc
        payload["offset"] = offset
        return payload

    def _decode_delta_cursor(self, cursor: Any) -> dict[str, Any]:
        if (
            not isinstance(cursor, str)
            or not cursor
            or not cursor.isascii()
            or len(cursor.encode("ascii")) > _CURSOR_MAX_BYTES
            or cursor.count(".") != 1
        ):
            raise _cursor_error()
        payload_part, signature_part = cursor.split(".", 1)
        try:
            encoded = _b64decode(payload_part)
            signature = _b64decode(signature_part)
        except (ValueError, binascii.Error) as exc:
            raise _cursor_error() from exc
        expected = hmac.digest(self._cursor_key, encoded, hashlib.sha256)
        if len(signature) != len(expected) or not hmac.compare_digest(
            signature, expected
        ):
            raise _cursor_error()
        try:
            payload = _json_without_duplicate_keys(encoded)
            payload = _strict_object(
                payload,
                field="cursor",
                keys=self._DELTA_CURSOR_KEYS,
            )
            _require_schema(payload["schema"], _DELTA_CURSOR_SCHEMA)
            _required_text(payload["session_id"], "session_id")
            _required_text(payload["group_id"], "group_id")
            _optional_text(payload["dataset_id"], "dataset_id")
            _uint(
                payload["baseline_manifest_revision"],
                "baseline_manifest_revision",
            )
            _sha256(payload["baseline_manifest_hash"], "baseline_manifest_hash")
            _uint(payload["manifest_revision"], "manifest_revision")
            _sha256(payload["manifest_hash"], "manifest_hash")
            offset = _positive_int(payload["offset"], "offset")
        except (
            FederationValidationError,
            ProtocolCompatibilityError,
            ValueError,
        ) as exc:
            raise _cursor_error() from exc
        payload["offset"] = offset
        return payload


__all__ = [
    "COMMITTED_BATCH_DELTA_PAGE_SCHEMA",
    "COMMITTED_BATCH_PAGE_SCHEMA",
    "COMMITTED_BATCH_REFERENCE_SCHEMA",
    "MAX_BATCH_PAGE_SIZE",
    "CommittedBatchDeltaPage",
    "CommittedBatchPage",
    "CommittedBatchReference",
    "ManifestBatchCatalog",
]
