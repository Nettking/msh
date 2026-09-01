"""Incremental recorder archive reconciliation backed by durable discovery.

This class deliberately reuses the existing recorder publication validation,
chunking, quarantine, and durable outbox contracts. Only archive discovery is
changed: upgraded sources pay one explicit legacy scan, then ordinary cycles
enumerate only pending frontier records and retire each record after the outbox
has durably accepted the corresponding batch identity.
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

from catalog.mtconnect_recorder.model import (
    MtconnectProtocolError,
    RawBatchRef,
    SourceCheckpoint,
)
from catalog.mtconnect_recorder.publication_frontier import (
    PUBLICATION_FRONTIER_RECORD_INODES,
    PUBLICATION_FRONTIER_RECORD_MAX_BYTES,
    PUBLICATION_FRONTIER_STATE_INODES,
    PUBLICATION_FRONTIER_STATE_MAX_BYTES,
    PendingPublication,
    RecorderPublicationFrontier,
)
from catalog.mtconnect_recorder.schema_compat import (
    SUPPORTED_RAW_BATCH_MANIFEST_SCHEMAS,
)

from .process_resource_admission import PROCESS_RESOURCE_ADMISSION
from .recorder_publication import (
    MAX_REPORTED_QUARANTINED_SOURCES,
    QuarantinedSource,
    QuarantineSummary,
    RecorderArchiveReconciler,
    RecorderReconcileResult,
    _hash,
    _slug,
)

MAX_PENDING_PUBLICATIONS_PER_RECONCILIATION = 64


@contextmanager
def _admit_frontier_write(
    path: object,
    *,
    bytes_required: int,
    inodes_required: int,
) -> Iterator[None]:
    """Admit publisher-owned migration metadata through the shared controller."""

    with PROCESS_RESOURCE_ADMISSION.reserve(
        path,
        bytes_required=bytes_required,
        inodes_required=inodes_required,
    ):
        yield


class IncrementalRecorderArchiveReconciler(RecorderArchiveReconciler):
    """Reconcile only unreconciled recorder batches after one legacy migration."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.frontier = RecorderPublicationFrontier(self.store)
        self._blocked_migrations: dict[str, str] = {}

    def reconcile(self) -> RecorderReconcileResult:
        self._blocked_migrations = {}
        result = super().reconcile()
        if not self._blocked_migrations:
            return result
        migration_sources = tuple(
            QuarantinedSource(
                source_name=source_name,
                error_code="recorder-migration-blocked",
                batch_identity=f"legacy:{archive_source_name}",
            )
            for source_name, archive_source_name in sorted(
                self._blocked_migrations.items()
            )
        )
        quarantined = result.quarantine
        sources = (*quarantined.sources, *migration_sources)
        return replace(
            result,
            quarantine=QuarantineSummary(
                total=quarantined.total + len(migration_sources),
                sources=sources[:MAX_REPORTED_QUARANTINED_SOURCES],
                truncated=(
                    quarantined.truncated
                    or len(sources) > MAX_REPORTED_QUARANTINED_SOURCES
                ),
            ),
        )

    def _seed_legacy_archive(
        self,
        *,
        source_name: str,
        archive_source_name: str,
        checkpoint: SourceCheckpoint,
    ) -> None:
        """Pay one lifetime scan for one archive identity, then mark it migrated."""

        instance_id = checkpoint.agent_instance_id
        if self.frontier.migration_state(
            source_name=archive_source_name,
            instance_id=instance_id,
        ) in {"initialized", "blocked"}:
            return

        # This is the one explicit compatibility scan. If admission refuses a
        # record write or the process crashes half-way through, the initialized
        # marker is not written. A later cycle repeats the scan and idempotently
        # overwrites the already-seeded records rather than skipping evidence.
        scan = self.store.scan_raw_batches(
            source_name=archive_source_name,
            instance_id=instance_id,
        )
        migration_issues = [
            (Path(issue.manifest_path), issue.detail)
            for issue in scan.issues
        ]
        for ref in scan.refs:
            try:
                with _admit_frontier_write(
                    self.store.data_dir,
                    bytes_required=PUBLICATION_FRONTIER_RECORD_MAX_BYTES,
                    inodes_required=PUBLICATION_FRONTIER_RECORD_INODES,
                ):
                    self.frontier.mark_pending(
                        source_name=source_name,
                        archive_source_name=archive_source_name,
                        instance_id=instance_id,
                        ref=ref,
                    )
            except MtconnectProtocolError as exc:
                # A structurally readable manifest can still fail the stricter
                # frontier identity/path contract. Treat that as a completed
                # migration issue too, so it cannot force the same lifetime
                # scan forever while remaining operator-visible.
                migration_issues.append((ref.manifest_path, str(exc)))

        if migration_issues:
            issue_path, _detail = min(
                migration_issues,
                key=lambda item: item[0].as_posix(),
            )
            issue_sample = f"{issue_path.parent.name}/{issue_path.name}"
            with _admit_frontier_write(
                self.store.data_dir,
                bytes_required=PUBLICATION_FRONTIER_STATE_MAX_BYTES,
                inodes_required=PUBLICATION_FRONTIER_STATE_INODES,
            ):
                self.frontier.mark_blocked(
                    source_name=archive_source_name,
                    instance_id=instance_id,
                    issue_count=len(migration_issues),
                    issue_sample=issue_sample,
                )
            return

        with _admit_frontier_write(
            self.store.data_dir,
            bytes_required=PUBLICATION_FRONTIER_STATE_MAX_BYTES,
            inodes_required=PUBLICATION_FRONTIER_STATE_INODES,
        ):
            self.frontier.mark_initialized(
                source_name=archive_source_name,
                instance_id=instance_id,
            )

    def _pending_for_source(
        self,
        source_name: str,
        checkpoint: SourceCheckpoint,
        *,
        limit: int,
    ) -> tuple[PendingPublication, ...]:
        archive_names = tuple(dict.fromkeys([source_name, *checkpoint.storage_aliases]))
        for archive_source_name in archive_names:
            self._seed_legacy_archive(
                source_name=source_name,
                archive_source_name=archive_source_name,
                checkpoint=checkpoint,
            )
            if self.frontier.migration_state(
                source_name=archive_source_name,
                instance_id=checkpoint.agent_instance_id,
            ) == "blocked":
                self._blocked_migrations.setdefault(
                    source_name,
                    archive_source_name,
                )
        return self.frontier.pending(
            source_name=source_name,
            instance_id=checkpoint.agent_instance_id,
            limit=limit,
        )

    @staticmethod
    def _manifest_integer(payload: dict[str, object], field: str) -> int:
        value = payload.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(field)
        return value

    @staticmethod
    def _pending_identity_error(detail: str):
        from .errors import FederationValidationError

        return FederationValidationError(
            "malformed-recorder-observation",
            "manifest",
            detail,
        )

    def _validate_pending_evidence(
        self,
        *,
        pending: PendingPublication,
        checkpoint: SourceCheckpoint,
    ) -> None:
        """Re-prove a pending pointer before using its derived observation file."""

        ref = pending.ref
        try:
            manifest = json.loads(ref.manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise self._pending_identity_error(
                "pending pointer names an unreadable raw manifest"
            ) from exc
        if not isinstance(manifest, dict):
            raise self._pending_identity_error(
                "pending pointer names a non-object raw manifest"
            )

        try:
            manifest_schema = manifest["schema"]
            manifest_source = manifest["source_name"]
            manifest_instance = self._manifest_integer(
                manifest, "agent_instance_id"
            )
            manifest_requested = self._manifest_integer(
                manifest, "requested_from"
            )
            manifest_first = self._manifest_integer(
                manifest, "first_observation_sequence"
            )
            manifest_last = self._manifest_integer(
                manifest, "last_observation_sequence"
            )
            manifest_next = self._manifest_integer(manifest, "next_sequence")
            manifest_count = self._manifest_integer(
                manifest, "observation_count"
            )
            manifest_digest = manifest["raw_sha256"]
            manifest_raw_file = manifest["raw_file"]
        except (KeyError, TypeError, ValueError) as exc:
            raise self._pending_identity_error(
                "pending pointer names a structurally invalid raw manifest"
            ) from exc

        if (
            not isinstance(manifest_schema, str)
            or manifest_schema not in SUPPORTED_RAW_BATCH_MANIFEST_SCHEMAS
            or manifest_schema != ref.manifest_schema
            or manifest_source != pending.archive_source_name
            or manifest_instance != checkpoint.agent_instance_id
            or manifest_requested != ref.requested_from
            or manifest_first != ref.first_sequence
            or manifest_last != ref.last_sequence
            or manifest_next != ref.next_sequence
            or manifest_count != ref.observation_count
            or manifest_digest != ref.raw_sha256
            or not isinstance(manifest_raw_file, str)
            or Path(manifest_raw_file).name != ref.raw_path.name
        ):
            raise self._pending_identity_error(
                "pending pointer does not match raw manifest identity"
            )

    def _validate_observation_identity(
        self,
        *,
        observations: list[dict[str, object]],
        source_name: str,
        instance_id: int,
    ) -> None:
        for record in observations:
            if (
                record.get("source_name") != source_name
                or record.get("agent_instance_id") != instance_id
            ):
                raise self._pending_identity_error(
                    "pending pointer names observations from another source or instance"
                )

    def _reconcile_source(
        self,
        source_name: str,
        checkpoint: SourceCheckpoint,
        counters: dict[str, int],
        *,
        pending_limit: int = MAX_PENDING_PUBLICATIONS_PER_RECONCILIATION,
    ) -> None:
        """Enqueue one source from bounded pending discovery, in sequence order."""

        variants: dict[tuple[int, int, int], PendingPublication] = {}
        superseded: list[PendingPublication] = []
        for pending in self._pending_for_source(
            source_name,
            checkpoint,
            limit=pending_limit,
        ):
            ref = pending.ref
            identity = (ref.first_sequence, ref.last_sequence, ref.next_sequence)
            current = variants.get(identity)
            if current is None:
                variants[identity] = pending
                continue
            if (
                ref.raw_sha256,
                ref.manifest_path.as_posix(),
            ) > (
                current.ref.raw_sha256,
                current.ref.manifest_path.as_posix(),
            ):
                superseded.append(current)
                variants[identity] = pending
            else:
                superseded.append(pending)

        # Recovery already defines the greatest raw hash/path as the winner for
        # duplicate envelopes of one sequence identity. Retiring only the
        # duplicate discovery pointer cannot remove recorder evidence and keeps
        # a crash-created loser from becoming permanent publication backlog.
        for pending in superseded:
            self.frontier.retire(pending)

        ordered = sorted(
            variants.values(),
            key=lambda item: (
                item.ref.first_sequence,
                item.ref.last_sequence,
                item.ref.next_sequence,
                item.ref.raw_sha256,
                item.ref.manifest_path.as_posix(),
            ),
        )

        for pending in ordered:
            ref: RawBatchRef = pending.ref
            archive_source_name = pending.archive_source_name
            counters["scanned"] += 1
            if ref.next_sequence > checkpoint.next_sequence:
                # Raw/derived evidence can become discoverable before the
                # checkpoint commit. Keep the pointer until that commit makes
                # the batch locally publishable.
                continue
            counters["eligible"] += 1
            self._validate_pending_evidence(
                pending=pending,
                checkpoint=checkpoint,
            )
            if self.frontier.migration_state(
                source_name=archive_source_name,
                instance_id=checkpoint.agent_instance_id,
            ) == "blocked":
                self._blocked_migrations.setdefault(
                    source_name,
                    archive_source_name,
                )
            day = ref.manifest_path.parent.name
            observation_path = self._observation_path(
                source_name=source_name,
                archive_source_name=pending.archive_source_name,
                instance_id=checkpoint.agent_instance_id,
                first_sequence=ref.first_sequence,
                last_sequence=ref.last_sequence,
                next_sequence=ref.next_sequence,
                raw_sha256=ref.raw_sha256,
                day=day,
            )
            observations = self._read_observations(observation_path)
            self._validate_observation_identity(
                observations=observations,
                source_name=source_name,
                instance_id=checkpoint.agent_instance_id,
            )
            sequences = [int(item["sequence"]) for item in observations]
            if (
                sequences[0] != ref.first_sequence
                or sequences[-1] != ref.last_sequence
                or len(observations) != ref.observation_count
                or sequences != list(range(ref.first_sequence, ref.last_sequence + 1))
            ):
                from .errors import FederationValidationError

                raise FederationValidationError(
                    "recorder-sequence-mismatch",
                    "observation_file",
                    (
                        "detailed observation archive does not match its raw "
                        "manifest or contains a sequence discontinuity"
                    ),
                )

            created_at = self._received_at(ref.manifest_path, observations)
            chunks = self._chunks(
                source_name=source_name,
                checkpoint=checkpoint,
                raw_sha256=ref.raw_sha256,
                raw_first_sequence=ref.first_sequence,
                raw_last_sequence=ref.last_sequence,
                observations=observations,
            )
            dataset_id = self.target.dataset_id(source_name)
            raw_hash = _hash(ref.raw_sha256, field="raw_sha256")[7:]
            for chunk in chunks:
                counters["chunks"] += 1
                first_sequence = int(chunk[0]["sequence"])
                last_sequence = int(chunk[-1]["sequence"])
                batch_id = (
                    f"{_slug(source_name)}:{checkpoint.agent_instance_id}:"
                    f"{first_sequence}:{last_sequence}:{raw_hash}"
                )
                idempotency_key = f"{self.target.session_id}:{dataset_id}:{batch_id}"
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
                    dataset_schema_version=self.target.dataset_schema_version,
                    batch_id=batch_id,
                    idempotency_key=idempotency_key,
                    content=content,
                    created_at=created_at,
                )
                if created:
                    counters["enqueued"] += 1
                else:
                    counters["existing"] += 1

            # All chunks now have a durable outbox representation (new or
            # pre-existing). A crash before this unlink merely causes harmless
            # duplicate reconciliation; a crash before the outbox write leaves
            # the pointer intact because control never reaches this line.
            self.frontier.retire(pending)


__all__ = [
    "MAX_PENDING_PUBLICATIONS_PER_RECONCILIATION",
    "IncrementalRecorderArchiveReconciler",
]
