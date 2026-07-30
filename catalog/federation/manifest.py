"""Pure Phase E authoritative storage-manifest contracts.

The coordinator owns one immutable, hashed manifest chain per storage group.
Provider catalogues and reports are evidence about that authority; they do not
advance it. Sequence ranges are inclusive. Range arithmetic and durable
persistence are introduced by later Phase E checkpoints.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Self

from .errors import FederationValidationError, ProtocolCompatibilityError
from .models import CommitState

MANIFEST_SCHEMA = "msh.authoritative_storage_manifest.v1"
_SHA256_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")


def _required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-id",
            field,
            "must be non-empty opaque text",
        )
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


def _positive_int(value: Any, field: str) -> int:
    result = _uint(value, field)
    if result == 0:
        raise FederationValidationError(
            "invalid-positive-integer",
            field,
            "must be greater than zero",
        )
    return result


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp",
                field,
                "must be RFC 3339",
            ) from exc
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise FederationValidationError(
            "invalid-timestamp",
            field,
            "must be timezone-aware",
        )
    if value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-timestamp",
            field,
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _sha256(value: Any, field: str) -> str:
    value = _required_text(value, field)
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise FederationValidationError(
            "invalid-content-hash",
            field,
            "must be a lowercase sha256 digest",
        )
    return value


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _schema(value: Any) -> str:
    if value != MANIFEST_SCHEMA:
        raise ProtocolCompatibilityError(
            "unsupported-schema",
            "schema",
            f"expected {MANIFEST_SCHEMA}, received {value!r}",
        )
    return MANIFEST_SCHEMA


def _sequence_range(value: SequenceRange | dict[str, int] | tuple[int, int]) -> SequenceRange:
    if isinstance(value, SequenceRange):
        return value
    if isinstance(value, tuple) and len(value) == 2:
        return SequenceRange(start=value[0], end=value[1])
    return SequenceRange.from_dict(value)


@dataclass(frozen=True, order=True)
class SequenceRange:
    """An inclusive source-sequence interval."""

    start: int
    end: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "start", _uint(self.start, "start"))
        object.__setattr__(self, "end", _uint(self.end, "end"))
        if self.end < self.start:
            raise FederationValidationError(
                "invalid-range",
                "end",
                "inclusive range end must not precede start",
            )

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                "sequence_range",
                "must be an object",
            )
        if "start" not in value or "end" not in value:
            raise FederationValidationError(
                "missing-field",
                "sequence_range",
                "start and end are required",
            )
        return cls(start=value["start"], end=value["end"])

    def to_dict(self) -> dict[str, int]:
        return {"start": self.start, "end": self.end}


def normalize_sequence_ranges(
    ranges: tuple[SequenceRange | dict[str, int], ...],
) -> tuple[SequenceRange, ...]:
    """Return sorted, merged, non-overlapping inclusive ranges."""

    normalized = sorted(
        _sequence_range(value)
        for value in ranges
    )
    if not normalized:
        return ()
    merged: list[SequenceRange] = [normalized[0]]
    for current in normalized[1:]:
        previous = merged[-1]
        if current.start <= previous.end + 1:
            merged[-1] = SequenceRange(previous.start, max(previous.end, current.end))
        else:
            merged.append(current)
    return tuple(merged)


def subtract_sequence_ranges(
    source: tuple[SequenceRange, ...],
    removed: tuple[SequenceRange | dict[str, int] | tuple[int, int], ...],
) -> tuple[SequenceRange, ...]:
    """Subtract inclusive ranges deterministically from an ordered coverage set."""

    remaining = list(normalize_sequence_ranges(source))
    for gap in normalize_sequence_ranges(removed):
        next_remaining: list[SequenceRange] = []
        for current in remaining:
            if gap.end < current.start or gap.start > current.end:
                next_remaining.append(current)
                continue
            if gap.start > current.start:
                next_remaining.append(SequenceRange(current.start, gap.start - 1))
            if gap.end < current.end:
                next_remaining.append(SequenceRange(gap.end + 1, current.end))
        remaining = next_remaining
    return tuple(normalize_sequence_ranges(tuple(remaining)))


def _coverage_from_sequences(
    committed: tuple[SequenceRange | dict[str, int], ...],
) -> tuple[int | None, tuple[SequenceRange, ...]]:
    normalized = normalize_sequence_ranges(committed)
    if not normalized:
        return None, ()
    watermark = normalized[0].end
    missing: list[SequenceRange] = []
    previous_end = normalized[0].end
    for current in normalized[1:]:
        if current.start > previous_end + 1:
            missing.append(SequenceRange(previous_end + 1, current.start - 1))
        previous_end = current.end
    return watermark, tuple(missing)


class ManifestItemKind(str, Enum):
    BATCH = "batch"
    OBJECT = "object"


@dataclass(frozen=True)
class ManifestItem:
    """One immutable item included in authoritative committed state."""

    item_id: str
    kind: ManifestItemKind
    dataset_id: str
    idempotency_key: str | None
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

    def __post_init__(self) -> None:
        for field in ("item_id", "dataset_id", "schema_name"):
            object.__setattr__(
                self,
                field,
                _required_text(getattr(self, field), field),
            )
        try:
            object.__setattr__(self, "kind", ManifestItemKind(self.kind))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-enum",
                "kind",
                "unknown manifest item kind",
            ) from exc
        if self.idempotency_key is not None:
            object.__setattr__(
                self,
                "idempotency_key",
                _required_text(self.idempotency_key, "idempotency_key"),
            )
        if self.kind is ManifestItemKind.BATCH and self.idempotency_key is None:
            raise FederationValidationError(
                "missing-field",
                "idempotency_key",
                "batch items require an idempotency key",
            )
        object.__setattr__(
            self,
            "content_hash",
            _sha256(self.content_hash, "content_hash"),
        )
        object.__setattr__(self, "size_bytes", _uint(self.size_bytes, "size_bytes"))
        object.__setattr__(
            self,
            "schema_version",
            _positive_int(self.schema_version, "schema_version"),
        )
        if self.source_id is not None:
            object.__setattr__(
                self,
                "source_id",
                _required_text(self.source_id, "source_id"),
            )
        if (self.first_sequence is None) != (self.last_sequence is None):
            raise FederationValidationError(
                "invalid-sequence-range",
                "first_sequence",
                "sequence start and end must be supplied together",
            )
        if self.first_sequence is not None and self.last_sequence is not None:
            start = _uint(self.first_sequence, "first_sequence")
            end = _uint(self.last_sequence, "last_sequence")
            if end < start:
                raise FederationValidationError(
                    "invalid-sequence-range",
                    "last_sequence",
                    "must not precede first_sequence",
                )
            if self.source_id is None:
                raise FederationValidationError(
                    "missing-field",
                    "source_id",
                    "sequence-bearing items require a source identity",
                )
            object.__setattr__(self, "first_sequence", start)
            object.__setattr__(self, "last_sequence", end)
        try:
            object.__setattr__(self, "commit_state", CommitState(self.commit_state))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-enum",
                "commit_state",
                "unknown storage commit state",
            ) from exc
        providers = tuple(
            sorted(
                _required_text(provider_id, "acknowledged_provider_ids")
                for provider_id in self.acknowledged_provider_ids
            )
        )
        if not providers:
            raise FederationValidationError(
                "missing-commit-evidence",
                "acknowledged_provider_ids",
                "at least one durable provider acknowledgement is required",
            )
        if len(providers) != len(set(providers)):
            raise FederationValidationError(
                "duplicate-provider",
                "acknowledged_provider_ids",
                "provider acknowledgements must be unique",
            )
        object.__setattr__(self, "acknowledged_provider_ids", providers)
        object.__setattr__(
            self,
            "committed_at",
            _utc(self.committed_at, "committed_at"),
        )

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                "manifest_item",
                "must be an object",
            )
        required = {
            "item_id",
            "kind",
            "dataset_id",
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
        }
        missing = sorted(required - value.keys())
        if missing:
            raise FederationValidationError(
                "missing-field",
                missing[0],
                "is required",
            )
        return cls(
            item_id=value["item_id"],
            kind=value["kind"],
            dataset_id=value["dataset_id"],
            idempotency_key=value["idempotency_key"],
            content_hash=value["content_hash"],
            size_bytes=value["size_bytes"],
            schema_name=value["schema_name"],
            schema_version=value["schema_version"],
            source_id=value["source_id"],
            first_sequence=value["first_sequence"],
            last_sequence=value["last_sequence"],
            commit_state=value["commit_state"],
            acknowledged_provider_ids=tuple(value["acknowledged_provider_ids"]),
            committed_at=value["committed_at"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "kind": self.kind.value,
            "dataset_id": self.dataset_id,
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
        }


@dataclass(frozen=True)
class DatasetManifest:
    """Authoritative coverage target for one dataset and sequence namespace."""

    dataset_id: str
    schema_name: str
    schema_version: int
    required: bool
    source_id: str | None
    last_contiguous_sequence: int | None
    missing_ranges: tuple[SequenceRange, ...]

    def __post_init__(self) -> None:
        for field in ("dataset_id", "schema_name"):
            object.__setattr__(
                self,
                field,
                _required_text(getattr(self, field), field),
            )
        object.__setattr__(
            self,
            "schema_version",
            _positive_int(self.schema_version, "schema_version"),
        )
        if not isinstance(self.required, bool):
            raise FederationValidationError(
                "invalid-boolean",
                "required",
                "must be boolean",
            )
        if self.source_id is not None:
            object.__setattr__(
                self,
                "source_id",
                _required_text(self.source_id, "source_id"),
            )
        if self.last_contiguous_sequence is not None:
            object.__setattr__(
                self,
                "last_contiguous_sequence",
                _uint(self.last_contiguous_sequence, "last_contiguous_sequence"),
            )
            if self.source_id is None:
                raise FederationValidationError(
                    "missing-field",
                    "source_id",
                    "watermarked datasets require a source identity",
                )
        ranges = tuple(
            sorted(
                value
                if isinstance(value, SequenceRange)
                else SequenceRange.from_dict(value)
                for value in self.missing_ranges
            )
        )
        previous: SequenceRange | None = None
        for value in ranges:
            if self.source_id is None:
                raise FederationValidationError(
                    "missing-field",
                    "source_id",
                    "missing ranges require a source identity",
                )
            if (
                self.last_contiguous_sequence is not None
                and value.start <= self.last_contiguous_sequence
            ):
                raise FederationValidationError(
                    "range-before-watermark",
                    "missing_ranges",
                    "missing ranges must begin after the contiguous watermark",
                )
            if previous is not None and value.start <= previous.end + 1:
                raise FederationValidationError(
                    "non-normalized-range",
                    "missing_ranges",
                    "ranges must be disjoint, sorted, and non-adjacent",
                )
            previous = value
        object.__setattr__(self, "missing_ranges", ranges)

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                "dataset_manifest",
                "must be an object",
            )
        required = {
            "dataset_id",
            "schema_name",
            "schema_version",
            "required",
            "source_id",
            "last_contiguous_sequence",
            "missing_ranges",
        }
        missing = sorted(required - value.keys())
        if missing:
            raise FederationValidationError(
                "missing-field",
                missing[0],
                "is required",
            )
        return cls(
            dataset_id=value["dataset_id"],
            schema_name=value["schema_name"],
            schema_version=value["schema_version"],
            required=value["required"],
            source_id=value["source_id"],
            last_contiguous_sequence=value["last_contiguous_sequence"],
            missing_ranges=tuple(
                SequenceRange.from_dict(item) for item in value["missing_ranges"]
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "required": self.required,
            "source_id": self.source_id,
            "last_contiguous_sequence": self.last_contiguous_sequence,
            "missing_ranges": [value.to_dict() for value in self.missing_ranges],
        }

    def with_committed_sequences(
        self,
        committed_ranges: tuple[
            SequenceRange | dict[str, int] | tuple[int, int], ...
        ],
    ) -> Self:
        """Return a new dataset manifest with deterministic coverage evidence."""

        last_contiguous_sequence, missing_ranges = _coverage_from_sequences(
            committed_ranges,
        )
        return DatasetManifest(
            dataset_id=self.dataset_id,
            schema_name=self.schema_name,
            schema_version=self.schema_version,
            required=self.required,
            source_id=self.source_id,
            last_contiguous_sequence=last_contiguous_sequence,
            missing_ranges=missing_ranges,
        )

    def fill_missing_ranges(
        self,
        filled_ranges: tuple[
            SequenceRange | dict[str, int] | tuple[int, int], ...
        ],
    ) -> Self:
        """Return a new dataset manifest after deterministically filling gaps."""

        if self.last_contiguous_sequence is None:
            return self.with_committed_sequences(filled_ranges)
        normalized = subtract_sequence_ranges(self.missing_ranges, filled_ranges)
        if normalized == self.missing_ranges:
            return self
        return DatasetManifest(
            dataset_id=self.dataset_id,
            schema_name=self.schema_name,
            schema_version=self.schema_version,
            required=self.required,
            source_id=self.source_id,
            last_contiguous_sequence=self.last_contiguous_sequence,
            missing_ranges=normalized,
        )


@dataclass(frozen=True)
class AuthoritativeStorageManifest:
    """One validated head in a coordinator-owned per-group manifest chain."""

    session_id: str
    group_id: str
    revision: int
    term: int
    previous_manifest_hash: str | None
    commit_state: CommitState
    datasets: tuple[DatasetManifest, ...]
    items: tuple[ManifestItem, ...]
    manifest_hash: str
    updated_at: datetime

    def __post_init__(self) -> None:
        for field in ("session_id", "group_id"):
            object.__setattr__(
                self,
                field,
                _required_text(getattr(self, field), field),
            )
        object.__setattr__(self, "revision", _uint(self.revision, "revision"))
        object.__setattr__(self, "term", _uint(self.term, "term"))
        if self.previous_manifest_hash is not None:
            object.__setattr__(
                self,
                "previous_manifest_hash",
                _sha256(self.previous_manifest_hash, "previous_manifest_hash"),
            )
        if self.revision == 0 and self.previous_manifest_hash is not None:
            raise FederationValidationError(
                "invalid-manifest-chain",
                "previous_manifest_hash",
                "genesis revision cannot have a predecessor",
            )
        if self.revision > 0 and self.previous_manifest_hash is None:
            raise FederationValidationError(
                "invalid-manifest-chain",
                "previous_manifest_hash",
                "non-genesis revision requires its predecessor hash",
            )
        try:
            object.__setattr__(self, "commit_state", CommitState(self.commit_state))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-enum",
                "commit_state",
                "unknown storage commit state",
            ) from exc
        if self.commit_state is not CommitState.COMMITTED:
            raise FederationValidationError(
                "manifest-not-committed",
                "commit_state",
                "an authoritative manifest head must be committed",
            )
        datasets = tuple(sorted(self.datasets, key=lambda value: value.dataset_id))
        items = tuple(
            sorted(
                self.items,
                key=lambda value: (
                    value.dataset_id,
                    value.kind.value,
                    value.item_id,
                ),
            )
        )
        dataset_ids = [value.dataset_id for value in datasets]
        if len(dataset_ids) != len(set(dataset_ids)):
            raise FederationValidationError(
                "duplicate-dataset",
                "datasets",
                "dataset identities must be unique within a storage group",
            )
        datasets_by_id = {value.dataset_id: value for value in datasets}
        item_ids: set[str] = set()
        idempotency_keys: set[str] = set()
        for item in items:
            if item.commit_state is not CommitState.COMMITTED:
                raise FederationValidationError(
                    "item-not-committed",
                    "items",
                    "authoritative manifests contain only committed items",
                )
            if item.item_id in item_ids:
                raise FederationValidationError(
                    "duplicate-item",
                    "items",
                    "item identities must be unique within a storage group",
                )
            item_ids.add(item.item_id)
            if item.idempotency_key is not None:
                if item.idempotency_key in idempotency_keys:
                    raise FederationValidationError(
                        "duplicate-idempotency-key",
                        "items",
                        "idempotency identities must be unique within a storage group",
                    )
                idempotency_keys.add(item.idempotency_key)
            dataset = datasets_by_id.get(item.dataset_id)
            if dataset is None:
                raise FederationValidationError(
                    "unknown-dataset",
                    "items",
                    "every item must reference a declared dataset",
                )
            if (
                item.schema_name != dataset.schema_name
                or item.schema_version != dataset.schema_version
            ):
                raise FederationValidationError(
                    "item-schema-mismatch",
                    "items",
                    "item and dataset schema identities must match",
                )
            if item.source_id != dataset.source_id:
                raise FederationValidationError(
                    "item-source-mismatch",
                    "items",
                    "item and dataset sequence-source identities must match",
                )
        object.__setattr__(self, "datasets", datasets)
        object.__setattr__(self, "items", items)
        object.__setattr__(
            self,
            "manifest_hash",
            _sha256(self.manifest_hash, "manifest_hash"),
        )
        object.__setattr__(self, "updated_at", _utc(self.updated_at, "updated_at"))
        if self.manifest_hash != self.calculated_hash():
            raise FederationValidationError(
                "manifest-hash-mismatch",
                "manifest_hash",
                "does not match the canonical authoritative manifest",
            )

    @classmethod
    def build(
        cls,
        *,
        session_id: str,
        group_id: str,
        revision: int,
        term: int,
        previous_manifest_hash: str | None,
        datasets: tuple[DatasetManifest, ...],
        items: tuple[ManifestItem, ...],
        updated_at: datetime,
    ) -> Self:
        datasets = tuple(sorted(datasets, key=lambda value: value.dataset_id))
        items = tuple(
            sorted(
                items,
                key=lambda value: (
                    value.dataset_id,
                    value.kind.value,
                    value.item_id,
                ),
            )
        )
        canonical = cls._canonical_value(
            session_id=session_id,
            group_id=group_id,
            revision=revision,
            term=term,
            previous_manifest_hash=previous_manifest_hash,
            commit_state=CommitState.COMMITTED,
            datasets=datasets,
            items=items,
        )
        digest = hashlib.sha256(
            json.dumps(
                canonical,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        return cls(
            session_id=session_id,
            group_id=group_id,
            revision=revision,
            term=term,
            previous_manifest_hash=previous_manifest_hash,
            commit_state=CommitState.COMMITTED,
            datasets=datasets,
            items=items,
            manifest_hash=f"sha256:{digest}",
            updated_at=updated_at,
        )

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                "storage_manifest",
                "must be an object",
            )
        _schema(value.get("schema"))
        required = {
            "session_id",
            "group_id",
            "revision",
            "term",
            "previous_manifest_hash",
            "commit_state",
            "datasets",
            "items",
            "manifest_hash",
            "updated_at",
        }
        missing = sorted(required - value.keys())
        if missing:
            raise FederationValidationError(
                "missing-field",
                missing[0],
                "is required",
            )
        return cls(
            session_id=value["session_id"],
            group_id=value["group_id"],
            revision=value["revision"],
            term=value["term"],
            previous_manifest_hash=value["previous_manifest_hash"],
            commit_state=value["commit_state"],
            datasets=tuple(
                DatasetManifest.from_dict(item) for item in value["datasets"]
            ),
            items=tuple(ManifestItem.from_dict(item) for item in value["items"]),
            manifest_hash=value["manifest_hash"],
            updated_at=value["updated_at"],
        )

    @staticmethod
    def _canonical_value(
        *,
        session_id: str,
        group_id: str,
        revision: int,
        term: int,
        previous_manifest_hash: str | None,
        commit_state: CommitState,
        datasets: tuple[DatasetManifest, ...],
        items: tuple[ManifestItem, ...],
    ) -> dict[str, Any]:
        return {
            "schema": MANIFEST_SCHEMA,
            "session_id": session_id,
            "group_id": group_id,
            "revision": revision,
            "term": term,
            "previous_manifest_hash": previous_manifest_hash,
            "commit_state": commit_state.value,
            "datasets": [value.to_dict() for value in datasets],
            "items": [value.to_dict() for value in items],
        }

    def calculated_hash(self) -> str:
        canonical = self._canonical_value(
            session_id=self.session_id,
            group_id=self.group_id,
            revision=self.revision,
            term=self.term,
            previous_manifest_hash=self.previous_manifest_hash,
            commit_state=self.commit_state,
            datasets=self.datasets,
            items=self.items,
        )
        digest = hashlib.sha256(
            json.dumps(
                canonical,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        return f"sha256:{digest}"

    def to_dict(self) -> dict[str, Any]:
        return {
            **self._canonical_value(
                session_id=self.session_id,
                group_id=self.group_id,
                revision=self.revision,
                term=self.term,
                previous_manifest_hash=self.previous_manifest_hash,
                commit_state=self.commit_state,
                datasets=self.datasets,
                items=self.items,
            ),
            "manifest_hash": self.manifest_hash,
            "updated_at": _timestamp(self.updated_at),
        }

    def dataset_hash(self, dataset_id: str) -> str:
        """Return the canonical committed identity digest for one dataset."""

        dataset = next(
            (value for value in self.datasets if value.dataset_id == dataset_id),
            None,
        )
        if dataset is None:
            raise FederationValidationError(
                "unknown-dataset",
                "dataset_id",
                "dataset is not present in this manifest",
            )
        value = {
            "dataset": dataset.to_dict(),
            "items": [
                item.to_dict()
                for item in self.items
                if item.dataset_id == dataset_id
            ],
        }
        digest = hashlib.sha256(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        return f"sha256:{digest}"
