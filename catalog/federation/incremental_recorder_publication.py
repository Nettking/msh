"""Incremental recorder archive reconciliation backed by durable discovery.

This class deliberately reuses the existing recorder publication validation,
chunking, quarantine, and durable outbox contracts. Only archive discovery is
changed: upgraded sources pay one explicit legacy scan, then ordinary cycles
enumerate only pending frontier records and retire each record after the outbox
has durably accepted the corresponding batch identity.
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from catalog.mtconnect_recorder.model import RawBatchRef, SourceCheckpoint
from catalog.mtconnect_recorder.publication_frontier import (
    PUBLICATION_FRONTIER_RECORD_INODES,
    PUBLICATION_FRONTIER_RECORD_MAX_BYTES,
    PUBLICATION_FRONTIER_STATE_INODES,
    PUBLICATION_FRONTIER_STATE_MAX_BYTES,
    PendingPublication,
    RecorderPublicationFrontier,
)

from .process_resource_admission import PROCESS_RESOURCE_ADMISSION
from .recorder_publication import RecorderArchiveReconciler, _hash, _slug


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

    def _seed_legacy_archive(
        self,
        *,
        source_name: str,
        archive_source_name: str,
        checkpoint: SourceCheckpoint,
    ) -> None:
        """Pay one lifetime scan for one archive identity, then mark it migrated."""

        instance_id = checkpoint.agent_instance_id
        if self.frontier.initialized(
            source_name=archive_source_name,
            instance_id=instance_id,
        ):
            return

        # This is the one explicit compatibility scan. If admission refuses a
        # record write or the process crashes half-way through, the initialized
        # marker is not written. A later cycle repeats the scan and idempotently
        # overwrites the already-seeded records rather than skipping evidence.
        for ref in self.store.iter_raw_batches(
            source_name=archive_source_name,
            instance_id=instance_id,
        ):
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
    ) -> tuple[PendingPublication, ...]:
        archive_names = tuple(dict.fromkeys([source_name, *checkpoint.storage_aliases]))
        for archive_source_name in archive_names:
            self._seed_legacy_archive(
                source_name=source_name,
                archive_source_name=archive_source_name,
                checkpoint=checkpoint,
            )
        return self.frontier.pending(
            source_name=source_name,
            instance_id=checkpoint.agent_instance_id,
        )

    def _reconcile_source(
        self,
        source_name: str,
        checkpoint: SourceCheckpoint,
        counters: dict[str, int],
    ) -> None:
        """Enqueue one source from bounded pending discovery, in sequence order."""

        variants: dict[tuple[int, int, int], PendingPublication] = {}
        superseded: list[PendingPublication] = []
        for pending in self._pending_for_source(source_name, checkpoint):
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
            counters["scanned"] += 1
            if ref.next_sequence > checkpoint.next_sequence:
                # Raw/derived evidence can become discoverable before the
                # checkpoint commit. Keep the pointer until that commit makes
                # the batch locally publishable.
                continue
            counters["eligible"] += 1
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


__all__ = ["IncrementalRecorderArchiveReconciler"]
