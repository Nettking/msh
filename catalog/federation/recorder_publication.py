"""Publish locally committed MTConnect recorder data to Federation storage.

This module deliberately does not participate in MTConnect capture.  It watches
the recorder's existing durable checkpoint, reconciles only checkpoint-covered
archives into an idempotent outbox, and retries logical Federation delivery
independently of recorder availability.
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.mtconnect_recorder.model import RawBatchRef, SourceCheckpoint
from catalog.mtconnect_recorder.schema_compat import (
    SUPPORTED_CHECKPOINT_SCHEMAS,
    is_supported_checkpoint_schema,
)
from catalog.mtconnect_recorder.storage import DurableRecorderStore

from .errors import FederationValidationError
from .outbox import MAX_PAYLOAD_BYTES, RetiredSummary
from .recorder_delivery import (
    RECORDER_STORAGE_SCHEMA,
    DurableRecorderDeliveryQueue,
    RecorderDeliveryProgressObserver,
    RecorderDeliveryRunResult,
)

RECORDER_TELEMETRY_SCHEMA = "fcp.mtconnect.observations.v1"
RECORDER_DATASET_SCHEMA_NAME = "fcp.mtconnect.observations"
RECORDER_DATASET_SCHEMA_VERSION = 1
MAX_SAFE_CONTENT_BYTES = min(900_000, MAX_PAYLOAD_BYTES - 64 * 1024)
DEFAULT_MAX_CONTENT_BYTES = MAX_SAFE_CONTENT_BYTES


def _required_text(value: object, field: str, *, max_bytes: int = 512) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FederationValidationError(
            "invalid-recorder-publication", field, "must be non-empty text"
        )
    normalized = value.strip()
    if (
        len(normalized.encode("utf-8")) > max_bytes
        or any(ord(character) < 32 for character in normalized)
    ):
        raise FederationValidationError(
            "invalid-recorder-publication",
            field,
            f"must be printable text no longer than {max_bytes} bytes",
        )
    return normalized


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return cleaned or "unknown"


def _hash(value: str, *, field: str = "sha256") -> str:
    normalized = value.strip().lower().removeprefix("sha256:")
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise FederationValidationError(
            "invalid-recorder-publication",
            field,
            "must contain 64 lowercase hexadecimal characters",
        )
    return f"sha256:{normalized}"


def _optional_hash(value: object, *, field: str) -> str | None:
    if value is None or not str(value).strip():
        return None
    return _hash(str(value), field=field)


def _parse_utc(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise FederationValidationError(
            "invalid-recorder-publication", field, "must be RFC 3339 text"
        )
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise FederationValidationError(
            "invalid-recorder-publication", field, "must be RFC 3339 text"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FederationValidationError(
            "invalid-recorder-publication", field, "must be timezone-aware"
        )
    return parsed.astimezone(timezone.utc)


def _canonical_size(value: object) -> int:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-recorder-publication",
            "content",
            "must be JSON-compatible",
        ) from exc
    return len(encoded)


@dataclass(frozen=True)
class RecorderPublicationTarget:
    """Logical destination selected by local contribution policy."""

    session_id: str
    group_id: str
    recorder_node_id: str
    dataset_schema_name: str = RECORDER_DATASET_SCHEMA_NAME
    dataset_schema_version: int = RECORDER_DATASET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field in (
            "session_id",
            "group_id",
            "recorder_node_id",
            "dataset_schema_name",
        ):
            object.__setattr__(
                self,
                field,
                _required_text(getattr(self, field), field),
            )
        if (
            isinstance(self.dataset_schema_version, bool)
            or not isinstance(self.dataset_schema_version, int)
            or self.dataset_schema_version <= 0
        ):
            raise FederationValidationError(
                "invalid-recorder-publication",
                "dataset_schema_version",
                "must be a positive integer",
            )

    def dataset_id(self, source_name: str) -> str:
        return f"mtconnect:{self.recorder_node_id}:{_slug(source_name)}"


@dataclass(frozen=True)
class QuarantinedSource:
    """One source whose archive reconciliation stopped on an unusable item.

    The item itself is named only by its bounded raw identity, never by a path:
    this record travels into an operator health surface.
    """

    source_name: str
    error_code: str
    batch_identity: str


@dataclass(frozen=True)
class QuarantineSummary:
    """A bounded view of sources fenced behind an unusable archive item.

    ``sources`` is truncated to a fixed sample while ``total`` counts every
    quarantined source, so a health surface can report the condition truthfully
    without materialising an unbounded result set.
    """

    total: int = 0
    sources: tuple[QuarantinedSource, ...] = ()
    truncated: bool = False


#: Item-level archive faults that fence one source rather than the whole cycle.
#
# Each names an archive item this reconciler cannot turn into a publication:
# unreadable, malformed, empty, discontinuous, or larger than the bounded
# publication size. None of them is a reason the *other* sources cannot
# publish, and none of them can be fixed by trying the same item again.
QUARANTINE_CODES = frozenset(
    {
        "malformed-recorder-observation",
        "recorder-observation-too-large",
        "recorder-observations-empty",
        "recorder-observations-missing",
        "recorder-observations-unavailable",
        "recorder-receipt-time-missing",
        "recorder-sequence-mismatch",
    }
)

#: Bounded sample of quarantined sources carried in one reconcile result.
MAX_REPORTED_QUARANTINED_SOURCES = 8

#: Bound on the item locator carried into an operator health surface.
MAX_QUARANTINE_IDENTITY_CHARS = 64


@dataclass(frozen=True)
class RecorderReconcileResult:
    scanned_batches: int
    eligible_batches: int
    publication_chunks: int
    enqueued: int
    already_enqueued: int
    quarantine: QuarantineSummary = QuarantineSummary()


@dataclass(frozen=True)
class RecorderPublisherRunResult:
    reconcile: RecorderReconcileResult
    delivery: RecorderDeliveryRunResult


class RecorderArchiveReconciler:
    """Turn locally committed MTConnect archives into Federation outbox entries.

    The recorder checkpoint is the local commit boundary. Raw manifests beyond
    that checkpoint are ignored. Re-running reconciliation is safe because batch
    and idempotency identities are derived only from durable recorder facts.
    """

    def __init__(
        self,
        *,
        store: DurableRecorderStore,
        checkpoint_file: Path | str,
        queue: DurableRecorderDeliveryQueue,
        target: RecorderPublicationTarget,
        max_content_bytes: int = DEFAULT_MAX_CONTENT_BYTES,
    ) -> None:
        if (
            isinstance(max_content_bytes, bool)
            or not isinstance(max_content_bytes, int)
            or max_content_bytes <= 0
            or max_content_bytes > MAX_SAFE_CONTENT_BYTES
        ):
            raise FederationValidationError(
                "invalid-recorder-publication",
                "max_content_bytes",
                (
                    "must be positive and leave reserved headroom for the "
                    "durable outbox envelope"
                ),
            )
        self.store = store
        self.checkpoint_file = Path(checkpoint_file)
        self.queue = queue
        self.target = target
        if self.queue.session_id != self.target.session_id:
            raise FederationValidationError(
                "recorder-session-mismatch",
                "session_id",
                "publication target must match the queue's authenticated session",
            )
        self.max_content_bytes = max_content_bytes

    def _checkpoints(self) -> dict[str, SourceCheckpoint]:
        if not self.checkpoint_file.exists():
            return {}
        try:
            payload = json.loads(self.checkpoint_file.read_text(encoding="utf-8"))
        except OSError as exc:
            raise FederationValidationError(
                "recorder-state-unavailable",
                "checkpoint_file",
                "could not read recorder checkpoint state",
            ) from exc
        except json.JSONDecodeError as exc:
            raise FederationValidationError(
                "malformed-recorder-state",
                "checkpoint_file",
                "recorder checkpoint state is not valid JSON",
            ) from exc
        # ``RecorderRuntime.load_state`` restores a pre-rename checkpoint and
        # deliberately leaves the file untouched until the next successful
        # commit. Publication reads the same file, so it has to accept the same
        # schemas or an upgraded installation cannot publish anything it has
        # already recorded during that window. Both forms carry an identical
        # payload shape and go through identical validation below.
        if not isinstance(payload, dict) or not is_supported_checkpoint_schema(
            payload.get("schema")
        ):
            raise FederationValidationError(
                "unsupported-recorder-state",
                "schema",
                "expected one of " + ", ".join(sorted(SUPPORTED_CHECKPOINT_SCHEMAS)),
            )
        sources = payload.get("sources")
        if not isinstance(sources, dict):
            raise FederationValidationError(
                "malformed-recorder-state",
                "sources",
                "must be an object",
            )
        checkpoints: dict[str, SourceCheckpoint] = {}
        for source_name, value in sources.items():
            if not isinstance(source_name, str) or not isinstance(value, dict):
                raise FederationValidationError(
                    "malformed-recorder-state",
                    "sources",
                    "contains an invalid source checkpoint",
                )
            try:
                checkpoints[source_name] = SourceCheckpoint.from_dict(
                    source_name,
                    value,
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise FederationValidationError(
                    "malformed-recorder-state",
                    f"sources.{source_name}",
                    "contains an invalid source checkpoint",
                ) from exc
        return checkpoints

    def _observation_path(
        self,
        *,
        source_name: str,
        archive_source_name: str,
        instance_id: int,
        first_sequence: int,
        last_sequence: int,
        next_sequence: int,
        raw_sha256: str,
        day: str,
    ) -> Path:
        legacy_filename = (
            f"seq-{first_sequence}-{last_sequence}-next-{next_sequence}.ndjson"
        )
        digest = _hash(raw_sha256, field="raw_sha256")[7:]
        full_hashed_filename = (
            legacy_filename.removesuffix(".ndjson") + f"-{digest}.ndjson"
        )
        short_hashed_filename = (
            legacy_filename.removesuffix(".ndjson") + f"-{digest[:12]}.ndjson"
        )
        candidates = (
            self.store.observation_root
            / _slug(source_name)
            / str(instance_id)
            / day
            / full_hashed_filename,
            self.store.observation_root
            / _slug(archive_source_name)
            / str(instance_id)
            / day
            / full_hashed_filename,
            # Transitional builds used a 12-hex prefix. Retain read-only
            # compatibility while all new writes use the collision-safe digest.
            self.store.observation_root
            / _slug(source_name)
            / str(instance_id)
            / day
            / short_hashed_filename,
            self.store.observation_root
            / _slug(archive_source_name)
            / str(instance_id)
            / day
            / short_hashed_filename,
            # Legacy recorder archives predate raw-hash-bound derived names.
            self.store.observation_root
            / _slug(source_name)
            / str(instance_id)
            / day
            / legacy_filename,
            self.store.observation_root
            / _slug(archive_source_name)
            / str(instance_id)
            / day
            / legacy_filename,
        )
        for candidate in candidates:
            if candidate.exists():
                return candidate
        raise FederationValidationError(
            "recorder-observations-missing",
            "observation_file",
            (
                "no detailed observation archive exists for sequences "
                f"{first_sequence}-{last_sequence}"
            ),
        )

    @staticmethod
    def _read_observations(path: Path) -> list[dict[str, Any]]:
        observations: list[dict[str, Any]] = []
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise FederationValidationError(
                "recorder-observations-unavailable",
                "observation_file",
                "could not read detailed observation archive",
            ) from exc
        for index, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise FederationValidationError(
                    "malformed-recorder-observation",
                    f"observation_file:{index}",
                    "line is not valid JSON",
                ) from exc
            if not isinstance(value, dict):
                raise FederationValidationError(
                    "malformed-recorder-observation",
                    f"observation_file:{index}",
                    "line must contain a JSON object",
                )
            sequence = value.get("sequence")
            if isinstance(sequence, bool) or not isinstance(sequence, int):
                raise FederationValidationError(
                    "malformed-recorder-observation",
                    f"observation_file:{index}.sequence",
                    "must be an integer",
                )
            observations.append(value)
        if not observations:
            raise FederationValidationError(
                "recorder-observations-empty",
                "observation_file",
                "committed batch has no detailed observations",
            )
        observations.sort(key=lambda record: int(record["sequence"]))
        return observations

    @staticmethod
    def _received_at(
        manifest_path: Path,
        observations: list[dict[str, Any]],
    ) -> datetime:
        manifest: dict[str, Any] = {}
        try:
            value = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                manifest = value
        except (OSError, json.JSONDecodeError):
            pass
        candidates = (
            manifest.get("received_at"),
            observations[0].get("received_at"),
            observations[0].get("timestamp"),
        )
        for value in candidates:
            if not isinstance(value, str) or not value.strip():
                continue
            try:
                return _parse_utc(value, "created_at")
            except FederationValidationError:
                # A stamp that is present but unparseable is not better
                # evidence than the next candidate. Falling through also keeps
                # this failure inside the item-level code below, rather than
                # leaving it as the shared construction-validation code, which
                # a per-item fence must not be widened to cover.
                continue
        raise FederationValidationError(
            "recorder-receipt-time-missing",
            "created_at",
            "committed batch has no usable durable receipt timestamp",
        )

    def _content(
        self,
        *,
        source_name: str,
        checkpoint: SourceCheckpoint,
        raw_sha256: str,
        raw_first_sequence: int,
        raw_last_sequence: int,
        observations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "schema": RECORDER_TELEMETRY_SCHEMA,
            "source_name": source_name,
            "machine_id": checkpoint.machine_id,
            "agent_instance_id": checkpoint.agent_instance_id,
            "first_sequence": observations[0]["sequence"],
            "last_sequence": observations[-1]["sequence"],
            "raw_batch_first_sequence": raw_first_sequence,
            "raw_batch_last_sequence": raw_last_sequence,
            "raw_sha256": _hash(raw_sha256, field="raw_sha256"),
            "probe_sha256": _optional_hash(
                checkpoint.probe_sha256,
                field="probe_sha256",
            ),
            "observation_count": len(observations),
            "observations": observations,
        }

    def _chunks(
        self,
        *,
        source_name: str,
        checkpoint: SourceCheckpoint,
        raw_sha256: str,
        raw_first_sequence: int,
        raw_last_sequence: int,
        observations: list[dict[str, Any]],
    ) -> list[list[dict[str, Any]]]:
        chunks: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        for observation in observations:
            candidate = [*current, observation]
            content = self._content(
                source_name=source_name,
                checkpoint=checkpoint,
                raw_sha256=raw_sha256,
                raw_first_sequence=raw_first_sequence,
                raw_last_sequence=raw_last_sequence,
                observations=candidate,
            )
            if _canonical_size(content) <= self.max_content_bytes:
                current = candidate
                continue
            if not current:
                raise FederationValidationError(
                    "recorder-observation-too-large",
                    "observation",
                    (
                        "one observation exceeds the bounded Federation "
                        "publication size"
                    ),
                )
            chunks.append(current)
            current = [observation]
            single = self._content(
                source_name=source_name,
                checkpoint=checkpoint,
                raw_sha256=raw_sha256,
                raw_first_sequence=raw_first_sequence,
                raw_last_sequence=raw_last_sequence,
                observations=current,
            )
            if _canonical_size(single) > self.max_content_bytes:
                raise FederationValidationError(
                    "recorder-observation-too-large",
                    "observation",
                    (
                        "one observation exceeds the bounded Federation "
                        "publication size"
                    ),
                )
        if current:
            chunks.append(current)
        return chunks

    @staticmethod
    def _quarantine_identity(exc: FederationValidationError) -> str:
        """Name the unusable item without publishing a local path.

        ``field`` already carries the bounded locator this reconciler raises
        with -- ``observation_file``, ``observation_file:<line>``,
        ``observation``. It is the only part of the failure safe to put on an
        operator health surface, so it is bounded and passed through as-is
        rather than joined with a filesystem location.
        """

        field = getattr(exc, "field", None)
        if not isinstance(field, str) or not field:
            return "unknown"
        return field[:MAX_QUARANTINE_IDENTITY_CHARS]

    def _reconcile_source(
        self,
        source_name: str,
        checkpoint: SourceCheckpoint,
        counters: dict[str, int],
    ) -> None:
        """Enqueue one source's eligible archive, in strict sequence order."""

        archive_names = tuple(
            dict.fromkeys([source_name, *checkpoint.storage_aliases])
        )
        archive_refs: dict[
            tuple[int, int, int], tuple[str, RawBatchRef]
        ] = {}
        for archive_source_name in archive_names:
            for ref in self.store.iter_raw_batches(
                source_name=archive_source_name,
                instance_id=checkpoint.agent_instance_id,
            ):
                identity = (
                    ref.first_sequence,
                    ref.last_sequence,
                    ref.next_sequence,
                )
                candidate = (archive_source_name, ref)
                current_variant = archive_refs.get(identity)
                if current_variant is None or (
                    ref.raw_sha256,
                    ref.manifest_path.as_posix(),
                ) > (
                    current_variant[1].raw_sha256,
                    current_variant[1].manifest_path.as_posix(),
                ):
                    # A crash can leave more than one raw envelope for the
                    # same sequence range. Recovery deterministically picks
                    # the greatest raw hash; publication must select that
                    # same variant rather than binding both manifests to one
                    # overwritten derived observation file.
                    archive_refs[identity] = candidate

        ordered_refs = sorted(
            archive_refs.values(),
            key=lambda item: (
                item[1].first_sequence,
                item[1].last_sequence,
                item[1].next_sequence,
                item[1].raw_sha256,
                item[1].manifest_path.as_posix(),
            ),
        )
        for archive_source_name, ref in ordered_refs:
            counters["scanned"] += 1
            if ref.next_sequence > checkpoint.next_sequence:
                continue
            counters["eligible"] += 1
            day = ref.manifest_path.parent.name
            observation_path = self._observation_path(
                source_name=source_name,
                archive_source_name=archive_source_name,
                instance_id=checkpoint.agent_instance_id,
                first_sequence=ref.first_sequence,
                last_sequence=ref.last_sequence,
                next_sequence=ref.next_sequence,
                raw_sha256=ref.raw_sha256,
                day=day,
            )
            observations = self._read_observations(observation_path)
            sequences = [int(item["sequence"]) for item in observations]
            if (
                sequences[0] != ref.first_sequence
                or sequences[-1] != ref.last_sequence
                or len(observations) != ref.observation_count
                or sequences
                != list(range(ref.first_sequence, ref.last_sequence + 1))
            ):
                raise FederationValidationError(
                    "recorder-sequence-mismatch",
                    "observation_file",
                    (
                        "detailed observation archive does not match "
                        "its raw manifest or contains a sequence "
                        "discontinuity"
                    ),
                )
            created_at = self._received_at(
                ref.manifest_path,
                observations,
            )
            chunks = self._chunks(
                source_name=source_name,
                checkpoint=checkpoint,
                raw_sha256=ref.raw_sha256,
                raw_first_sequence=ref.first_sequence,
                raw_last_sequence=ref.last_sequence,
                observations=observations,
            )
            dataset_id = self.target.dataset_id(source_name)
            raw_hash = _hash(
                ref.raw_sha256,
                field="raw_sha256",
            )[7:]
            for chunk in chunks:
                counters["chunks"] += 1
                first_sequence = int(chunk[0]["sequence"])
                last_sequence = int(chunk[-1]["sequence"])
                batch_id = (
                    f"{_slug(source_name)}:"
                    f"{checkpoint.agent_instance_id}:"
                    f"{first_sequence}:{last_sequence}:{raw_hash}"
                )
                idempotency_key = (
                    f"{self.target.session_id}:{dataset_id}:{batch_id}"
                )
                content = self._content(
                    source_name=source_name,
                    checkpoint=checkpoint,
                    raw_sha256=ref.raw_sha256,
                    raw_first_sequence=ref.first_sequence,
                    raw_last_sequence=ref.last_sequence,
                    observations=chunk,
                )
                _entry, created = self.queue.enqueue(
                    session_id=self.target.session_id,
                    group_id=self.target.group_id,
                    dataset_id=dataset_id,
                    dataset_schema_name=self.target.dataset_schema_name,
                    dataset_schema_version=(
                        self.target.dataset_schema_version
                    ),
                    batch_id=batch_id,
                    idempotency_key=idempotency_key,
                    content=content,
                    created_at=created_at,
                )
                if created:
                    counters["enqueued"] += 1
                else:
                    counters["existing"] += 1

    def reconcile(self) -> RecorderReconcileResult:
        checkpoints = self._checkpoints()
        counters = {
            "scanned": 0,
            "eligible": 0,
            "chunks": 0,
            "enqueued": 0,
            "existing": 0,
        }
        quarantined: list[QuarantinedSource] = []
        for source_name in sorted(checkpoints):
            try:
                self._reconcile_source(
                    source_name,
                    checkpoints[source_name],
                    counters,
                )
            except FederationValidationError as exc:
                if exc.code not in QUARANTINE_CODES:
                    raise
                # One unusable archive item used to end the whole cycle, and
                # the next cycle failed on the same item again. Nothing after
                # it was ever published -- not the rest of this source's
                # archive, and not any other source, because the loop above
                # never got that far. Capture kept recording, so the durable
                # work was not lost, it was stranded, permanently and silently
                # apart from a repeating cycle failure.
                #
                # This source stops here instead. Recorder sequence order is
                # preserved per dataset by the delivery queue and must not be
                # broken by publishing across the gap, so the rest of *this*
                # source waits behind the item an operator has to resolve --
                # but every other source now progresses, and the condition is
                # named rather than inferred from a loop that keeps failing.
                #
                # Nothing is deleted. The raw archive is primary evidence and
                # stays exactly where it is.
                quarantined.append(
                    QuarantinedSource(
                        source_name=source_name,
                        error_code=exc.code,
                        batch_identity=self._quarantine_identity(exc),
                    )
                )

        return RecorderReconcileResult(
            scanned_batches=counters["scanned"],
            eligible_batches=counters["eligible"],
            publication_chunks=counters["chunks"],
            enqueued=counters["enqueued"],
            already_enqueued=counters["existing"],
            quarantine=QuarantineSummary(
                total=len(quarantined),
                sources=tuple(quarantined[:MAX_REPORTED_QUARANTINED_SOURCES]),
                truncated=len(quarantined) > MAX_REPORTED_QUARANTINED_SOURCES,
            ),
        )



class RecorderFederationPublisher:
    """Reconcile and attempt due delivery without touching capture state."""

    def __init__(
        self,
        *,
        reconciler: RecorderArchiveReconciler,
        queue: DurableRecorderDeliveryQueue,
    ) -> None:
        self.reconciler = reconciler
        self.queue = queue

    async def run_once(self, *, limit: int = 100) -> RecorderPublisherRunResult:
        reconcile = await asyncio.to_thread(self.reconciler.reconcile)
        delivery = await self.queue.run_once(limit=limit)
        return RecorderPublisherRunResult(
            reconcile=reconcile,
            delivery=delivery,
        )


#: No tombstones at all, for a cycle that could not read the outbox.
_NO_RETIREMENT = RetiredSummary(total=0, datasets=(), truncated=False)

#: Publication health, worst first. ``failing`` means the worker cycle itself
#: could not complete; ``degraded`` means delivery works but some evidence has
#: been permanently withdrawn and will not arrive without operator action;
#: ``blocked`` means an ordered dataset is fenced behind a row that is still
#: being retried; ``publishing`` means ordinary progress.
PUBLICATION_HEALTH_STATES = ("failing", "degraded", "blocked", "publishing")


@dataclass(frozen=True)
class RecorderWorkerCycleResult:
    checkpoint_changed: bool
    reconcile: RecorderReconcileResult | None
    delivery: RecorderDeliveryRunResult
    # Durable tombstones as they exist right now, re-read from the outbox on
    # every cycle rather than accumulated in memory. That is the whole reason
    # degraded health cannot be forgotten by a restart or left stale by a
    # repair: it is never remembered in the first place, only observed.
    retirement: RetiredSummary = _NO_RETIREMENT


#: Failures one bounded publication cycle must survive rather than escape on.
#
# The outbox and the delivery queue behind this loop are SQLite, so
# ``sqlite3.Error`` -- a locked database, a transient disk I/O error, a store
# that cannot be opened -- is an ordinary condition for this driver, exactly as
# it is for the analysis lifecycle driver. Leaving it out did not make it fatal,
# but it did make it escape: the supervisor above rebuilt the whole worker
# instead, which discards this loop's own consecutive-failure count and its
# poll interval, so a store that had been unreadable for hours was presented as
# a first retry.
_CYCLE_RETRY_ERRORS = (
    FederationValidationError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
    sqlite3.Error,
)


@dataclass(frozen=True)
class RecorderPublicationCycleReport:
    """What one bounded worker cycle proved, whether or not it succeeded.

    The required publication loop used to absorb every cycle failure silently,
    so a recorder whose outbox had become unreadable looked exactly like a
    recorder with nothing to do. This report is what the loop hands to whoever
    owns the thread, so a failure has somewhere to be seen.
    """

    result: RecorderWorkerCycleResult | None = None
    error_code: str | None = None
    consecutive_failures: int = 0
    # Cycle outcomes that could not be handed to the owner because the health
    # sink itself raised. Carried into the first report that gets through.
    dropped_reports: int = 0

    @property
    def state(self) -> str:
        if self.error_code is not None or self.result is None:
            return "failing"
        if self.result.retirement.total:
            return "degraded"
        if self.quarantined_sources:
            # Delivery works and every other source is publishing, but one
            # source's archive is fenced behind an item no retry can fix. That
            # is not a failing cycle and it is not ordinary pending work: it
            # needs an operator, so it is reported as degraded rather than
            # hidden behind a healthy-looking cycle.
            return "degraded"
        if self.result.delivery.blocked_datasets:
            return "blocked"
        return "publishing"

    @property
    def quarantined_sources(self) -> int:
        reconcile = None if self.result is None else self.result.reconcile
        return 0 if reconcile is None else reconcile.quarantine.total

    @property
    def healthy(self) -> bool:
        return self.state == "publishing"


class RecorderFederationDeliveryWorker:
    """Near-live publisher driven by the recorder's atomic checkpoint.

    The recorder remains unaware of Federation delivery. Its checkpoint is
    written only after raw, detailed-observation, and compatibility-JSONL
    persistence succeeds, so a checkpoint modification is already the precise
    wake signal required by the delivery side. Periodic delivery attempts also
    let an offline outbox drain when Federation connectivity returns.
    """

    def __init__(
        self,
        *,
        reconciler: RecorderArchiveReconciler,
        queue: DurableRecorderDeliveryQueue,
        poll_interval_seconds: float = 0.2,
        delivery_limit: int = 100,
        cycle_observer: Callable[[RecorderPublicationCycleReport], None]
        | None = None,
    ) -> None:
        if (
            isinstance(poll_interval_seconds, bool)
            or not isinstance(poll_interval_seconds, (int, float))
            or not 0 < float(poll_interval_seconds) <= 60.0
        ):
            raise FederationValidationError(
                "invalid-recorder-publication",
                "poll_interval_seconds",
                "must be greater than zero and at most 60 seconds",
            )
        if (
            isinstance(delivery_limit, bool)
            or not isinstance(delivery_limit, int)
            or delivery_limit <= 0
        ):
            raise FederationValidationError(
                "invalid-recorder-publication",
                "delivery_limit",
                "must be a positive integer",
            )
        if cycle_observer is not None and not callable(cycle_observer):
            raise FederationValidationError(
                "invalid-recorder-publication",
                "cycle_observer",
                "must be callable when supplied",
            )
        self.reconciler = reconciler
        self.queue = queue
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.delivery_limit = delivery_limit
        self.cycle_observer = cycle_observer
        self._last_checkpoint_stamp: tuple[int, int] | None = None
        self._reconciled_once = False
        self._consecutive_failures = 0
        self._dropped_reports = 0
        self.last_cycle_stage: str | None = None

    def _checkpoint_stamp(self) -> tuple[int, int] | None:
        try:
            stat = self.reconciler.checkpoint_file.stat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise FederationValidationError(
                "recorder-state-unavailable",
                "checkpoint_file",
                "could not stat recorder checkpoint state",
            ) from exc
        return stat.st_mtime_ns, stat.st_size

    @staticmethod
    def _belongs_to_queue(entry: object, queue: DurableRecorderDeliveryQueue) -> bool:
        return (
            getattr(entry, "schema_id", None) == RECORDER_STORAGE_SCHEMA
            and getattr(entry, "session_id", None) == queue.session_id
            and (
                queue.destination_id is None
                or getattr(entry, "destination_id", None) == queue.destination_id
            )
        )

    async def run_cycle(
        self,
        *,
        force_reconcile: bool = False,
        progress_observer: RecorderDeliveryProgressObserver | None = None,
    ) -> RecorderWorkerCycleResult:
        self.last_cycle_stage = "validate"
        if progress_observer is not None and not callable(progress_observer):
            raise FederationValidationError(
                "invalid-recorder-publication",
                "progress_observer",
                "must be callable when supplied",
            )
        self.last_cycle_stage = "checkpoint"
        stamp = self._checkpoint_stamp()
        changed = (
            force_reconcile
            or not self._reconciled_once
            or stamp != self._last_checkpoint_stamp
        )
        reconcile: RecorderReconcileResult | None = None
        startup_probe_pending = self.queue.startup_probe_pending

        # On restart an offline recorder can already have thousands of durable
        # batches waiting in its outbox. Give that backlog one bounded startup
        # route probe before scanning checkpoint-covered archives. Do not wait
        # for the entire due backlog to drain: one failed ordered head can fence
        # many still-due rows in the same dataset, making an "any due row"
        # check stay true forever while newer local captures never enter the
        # outbox.
        current_backlog = False
        if changed and not force_reconcile and startup_probe_pending:
            # This check applies only to the first cycle. Include deferred rows
            # because run_once will give each ordered dataset one bounded route
            # probe even when its durable retry time is still in the future.
            # That proves a recovered route before the expensive archive scan.
            # Later checkpoint changes always reconcile; a due row can remain
            # visible forever behind a failed ordered head.
            self.last_cycle_stage = "backlog-probe"
            has_pending = getattr(self.queue.outbox, "has_pending", None)
            if callable(has_pending):
                current_backlog = await asyncio.to_thread(
                    has_pending,
                    session_id=self.queue.session_id,
                    destination_id=self.queue.destination_id,
                    schema_id=RECORDER_STORAGE_SCHEMA,
                    now=None,
                )
            else:
                # Compatibility for test/durable-store adapters that expose
                # only the original outbox protocol. The installed SQLite
                # outbox uses the bounded existence query above.
                pending_snapshot = await asyncio.to_thread(
                    self.queue.outbox.pending, now=None
                )
                current_backlog = any(
                    self._belongs_to_queue(entry, self.queue)
                    for entry in pending_snapshot
                )

        if changed and (
            force_reconcile
            or not current_backlog
            or not startup_probe_pending
        ):
            self.last_cycle_stage = "reconcile"
            reconcile = await asyncio.to_thread(self.reconciler.reconcile)
            # Record the stamp observed before reconciliation. If capture commits
            # again during the scan, the next cycle sees the newer stamp and
            # reconciles again rather than losing that wakeup.
            self._last_checkpoint_stamp = stamp
            self._reconciled_once = True

        self.last_cycle_stage = "delivery"
        if progress_observer is None:
            delivery = await self.queue.run_once(limit=self.delivery_limit)
        else:
            delivery = await self.queue.run_once(
                limit=self.delivery_limit,
                progress_observer=progress_observer,
            )

        # Read the tombstones back from the database rather than reporting what
        # this cycle happened to retire. A cycle that retires nothing because
        # the poisoned row was withdrawn days ago must still report degraded,
        # and a cycle that runs after an operator repaired one must stop
        # reporting it. Only durable truth answers both.
        self.last_cycle_stage = "retirement"
        retirement = await asyncio.to_thread(
            self.queue.outbox.retired_summary,
            session_id=self.queue.session_id,
            destination_id=self.queue.destination_id,
            schema_id=RECORDER_STORAGE_SCHEMA,
        )
        self.last_cycle_stage = None
        return RecorderWorkerCycleResult(
            checkpoint_changed=changed,
            reconcile=reconcile,
            delivery=delivery,
            retirement=retirement,
        )

    def _report(self, report: RecorderPublicationCycleReport) -> None:
        """Hand one cycle outcome to the owner of this required thread."""

        observer = self.cycle_observer
        if observer is None:
            return
        if self._dropped_reports:
            report = replace(report, dropped_reports=self._dropped_reports)
        try:
            observer(report)
        except Exception:  # noqa: BLE001 - a health sink cannot kill the loop
            # A broken health sink must never be able to strand durable
            # delivery work, so the hand-off failure is absorbed -- but not
            # discarded. It is counted and carried into the first report that
            # gets through, because a delivery about not swallowing failures
            # has no business swallowing this one.
            self._dropped_reports += 1
            return
        self._dropped_reports = 0

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        if not isinstance(stop_event, asyncio.Event):
            raise FederationValidationError(
                "invalid-recorder-publication",
                "stop_event",
                "must be an asyncio.Event",
            )
        while not stop_event.is_set():
            try:
                result = await self.run_cycle()
            except _CYCLE_RETRY_ERRORS as exc:
                # Capture remains independent and the next bounded cycle
                # retries reconciliation/delivery from durable local state --
                # but the failure is no longer invisible. Absorbing it silently
                # let an unreadable outbox or an unstattable checkpoint spin
                # this required loop forever while every health surface still
                # reported an ordinary running publisher.
                self._consecutive_failures += 1
                self._report(
                    RecorderPublicationCycleReport(
                        result=None,
                        error_code=str(getattr(exc, "code", type(exc).__name__)),
                        consecutive_failures=self._consecutive_failures,
                    )
                )
            else:
                self._consecutive_failures = 0
                self._report(
                    RecorderPublicationCycleReport(result=result)
                )
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.poll_interval_seconds,
                )
            except TimeoutError:
                continue


__all__ = [
    "DEFAULT_MAX_CONTENT_BYTES",
    "MAX_QUARANTINE_IDENTITY_CHARS",
    "MAX_REPORTED_QUARANTINED_SOURCES",
    "MAX_SAFE_CONTENT_BYTES",
    "PUBLICATION_HEALTH_STATES",
    "QUARANTINE_CODES",
    "RECORDER_DATASET_SCHEMA_NAME",
    "RECORDER_DATASET_SCHEMA_VERSION",
    "RECORDER_TELEMETRY_SCHEMA",
    "QuarantineSummary",
    "QuarantinedSource",
    "RecorderArchiveReconciler",
    "RecorderFederationDeliveryWorker",
    "RecorderFederationPublisher",
    "RecorderPublicationCycleReport",
    "RecorderPublicationTarget",
    "RecorderPublisherRunResult",
    "RecorderReconcileResult",
    "RecorderWorkerCycleResult",
]
