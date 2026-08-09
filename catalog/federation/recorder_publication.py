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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.mtconnect_recorder.model import SourceCheckpoint
from catalog.mtconnect_recorder.storage import DurableRecorderStore

from .errors import FederationValidationError
from .outbox import MAX_PAYLOAD_BYTES
from .recorder_delivery import DurableRecorderDeliveryQueue, RecorderDeliveryRunResult

RECORDER_TELEMETRY_SCHEMA = "msh.mtconnect.observations.v1"
RECORDER_DATASET_SCHEMA_NAME = "msh.mtconnect.observations"
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
class RecorderReconcileResult:
    scanned_batches: int
    eligible_batches: int
    publication_chunks: int
    enqueued: int
    already_enqueued: int


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
        if (
            not isinstance(payload, dict)
            or payload.get("schema") != "msh.mtconnect_recorder.checkpoints.v3"
        ):
            raise FederationValidationError(
                "unsupported-recorder-state",
                "schema",
                "expected msh.mtconnect_recorder.checkpoints.v3",
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
        day: str,
    ) -> Path:
        filename = (
            f"seq-{first_sequence}-{last_sequence}-next-{next_sequence}.ndjson"
        )
        candidates = (
            self.store.observation_root
            / _slug(source_name)
            / str(instance_id)
            / day
            / filename,
            self.store.observation_root
            / _slug(archive_source_name)
            / str(instance_id)
            / day
            / filename,
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
            if isinstance(value, str) and value.strip():
                return _parse_utc(value, "created_at")
        raise FederationValidationError(
            "recorder-receipt-time-missing",
            "created_at",
            "committed batch has no durable receipt timestamp",
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

    def reconcile(self) -> RecorderReconcileResult:
        checkpoints = self._checkpoints()
        scanned = 0
        eligible = 0
        publication_chunks = 0
        enqueued = 0
        existing = 0
        seen_manifests: set[Path] = set()

        for source_name in sorted(checkpoints):
            checkpoint = checkpoints[source_name]
            archive_names = tuple(
                dict.fromkeys([source_name, *checkpoint.storage_aliases])
            )
            for archive_source_name in archive_names:
                refs = self.store.iter_raw_batches(
                    source_name=archive_source_name,
                    instance_id=checkpoint.agent_instance_id,
                )
                for ref in refs:
                    if ref.manifest_path in seen_manifests:
                        continue
                    seen_manifests.add(ref.manifest_path)
                    scanned += 1
                    if ref.next_sequence > checkpoint.next_sequence:
                        continue
                    eligible += 1
                    day = ref.manifest_path.parent.name
                    observation_path = self._observation_path(
                        source_name=source_name,
                        archive_source_name=archive_source_name,
                        instance_id=checkpoint.agent_instance_id,
                        first_sequence=ref.first_sequence,
                        last_sequence=ref.last_sequence,
                        next_sequence=ref.next_sequence,
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
                        publication_chunks += 1
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
                            enqueued += 1
                        else:
                            existing += 1

        return RecorderReconcileResult(
            scanned_batches=scanned,
            eligible_batches=eligible,
            publication_chunks=publication_chunks,
            enqueued=enqueued,
            already_enqueued=existing,
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
        reconcile = self.reconciler.reconcile()
        delivery = await self.queue.run_once(limit=limit)
        return RecorderPublisherRunResult(
            reconcile=reconcile,
            delivery=delivery,
        )


@dataclass(frozen=True)
class RecorderWorkerCycleResult:
    checkpoint_changed: bool
    reconcile: RecorderReconcileResult | None
    delivery: RecorderDeliveryRunResult


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
        self.reconciler = reconciler
        self.queue = queue
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.delivery_limit = delivery_limit
        self._last_checkpoint_stamp: tuple[int, int] | None = None
        self._reconciled_once = False

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

    async def run_cycle(
        self,
        *,
        force_reconcile: bool = False,
    ) -> RecorderWorkerCycleResult:
        stamp = self._checkpoint_stamp()
        changed = (
            force_reconcile
            or not self._reconciled_once
            or stamp != self._last_checkpoint_stamp
        )
        reconcile: RecorderReconcileResult | None = None
        if changed:
            reconcile = self.reconciler.reconcile()
            # Record the stamp observed before reconciliation. If capture commits
            # again during the scan, the next cycle sees the newer stamp and
            # reconciles again rather than losing that wakeup.
            self._last_checkpoint_stamp = stamp
            self._reconciled_once = True
        delivery = await self.queue.run_once(limit=self.delivery_limit)
        return RecorderWorkerCycleResult(
            checkpoint_changed=changed,
            reconcile=reconcile,
            delivery=delivery,
        )

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        if not isinstance(stop_event, asyncio.Event):
            raise FederationValidationError(
                "invalid-recorder-publication",
                "stop_event",
                "must be an asyncio.Event",
            )
        while not stop_event.is_set():
            try:
                await self.run_cycle()
            except (
                FederationValidationError,
                OSError,
                RuntimeError,
                TypeError,
                ValueError,
            ):
                # Capture remains independent. The next bounded cycle retries
                # reconciliation/delivery from durable local state.
                pass
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.poll_interval_seconds,
                )
            except TimeoutError:
                continue


__all__ = [
    "DEFAULT_MAX_CONTENT_BYTES",
    "MAX_SAFE_CONTENT_BYTES",
    "RECORDER_DATASET_SCHEMA_NAME",
    "RECORDER_DATASET_SCHEMA_VERSION",
    "RECORDER_TELEMETRY_SCHEMA",
    "RecorderArchiveReconciler",
    "RecorderFederationDeliveryWorker",
    "RecorderFederationPublisher",
    "RecorderPublicationTarget",
    "RecorderPublisherRunResult",
    "RecorderReconcileResult",
    "RecorderWorkerCycleResult",
]
