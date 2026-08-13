"""Turn verified raw recorder batches into canonical observations.

The projection is a plain synchronous component with one entry point per Agent
instance. It owns no scheduler, no worker pool, no transport, and no job
lifecycle: those already exist in this repository and a future caller can drive
:meth:`CanonicalObservationProjection.project_instance` from a durable job
without this module knowing anything about federation.

Properties it guarantees, and how:

Deterministic
    Every field is a pure function of the immutable inputs — the compressed XML,
    its manifest, and the archived ``/probe`` for the same instance. No wall
    clock is read while deriving an observation; a document that omits a
    timestamp falls back to the manifest's recorded ``received_at``.
Idempotent
    Observation identity is a natural key, and the batch ledger is keyed by the
    digest of the uncompressed payload. Replaying a batch finds its ledger row
    and does nothing; replaying an overlapping batch finds the observation
    identities already present.
Rebuildable
    Delete the SQLite file and run again. The raw capture is untouched and is
    never written to, so nothing about a rebuild depends on mutable state.
Incremental
    Batches whose digest is already recorded as projected are skipped without
    being decompressed or parsed, so a new batch costs one batch of work.
Ordered
    Observations carry both their Agent sequence and an exact microsecond
    timestamp, and the store exposes a total order over both.
Auditable
    Every observation row carries the ``raw_sha256`` of the batch it came from,
    and the ledger maps that digest to the manifest and payload paths on disk.
Failure safe
    Each batch is validated and committed in its own transaction. A batch that
    fails is recorded as rejected with a reason and leaves no rows behind; every
    other batch still projects.
"""

from __future__ import annotations

import gzip
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from .. import parsing as recorder_parsing
from ..model import (
    RAW_BATCH_ISSUE_MALFORMED,
    RAW_BATCH_ISSUE_RAW_MISSING,
    RAW_BATCH_ISSUE_UNSUPPORTED_SCHEMA,
    MtconnectProtocolError,
    ProbeModel,
    RawBatchIssue,
    RawBatchRef,
    _slug,
)
from ..storage import DurableRecorderStore
from .observation import CanonicalObservation, CanonicalObservationError
from .store import (
    BATCH_STATUS_PROJECTED,
    BATCH_STATUS_REJECTED,
    CanonicalObservationStore,
    ProjectedBatchRecord,
)

#: The compressed payload is missing, truncated, or not gzip.
REJECT_PAYLOAD_UNREADABLE = "raw-payload-unreadable"

#: ``raw_sha256`` is over the *uncompressed* XML. A mismatch means the evidence
#: does not match its manifest, so nothing from it may enter the projection.
REJECT_DIGEST_MISMATCH = "raw-digest-mismatch"

#: The payload is not a parseable MTConnect streams document.
REJECT_XML_INVALID = "raw-xml-invalid"

#: The document's ``Header/@instanceId`` disagrees with the manifest, so the
#: observations cannot be attributed to an Agent instance with confidence.
REJECT_INSTANCE_MISMATCH = "agent-instance-mismatch"

#: One observation could not be canonicalised (no sequence, no timestamp, an
#: unknown category). The batch is rejected whole rather than partially stored.
REJECT_NOT_CANONICALISABLE = "observation-not-canonicalisable"

#: Two observations in one batch claim the same sequence number. MTConnect
#: assigns a distinct sequence to every observation, so this breaks the identity
#: invariant and is surfaced loudly rather than silently deduplicated.
REJECT_DUPLICATE_SEQUENCE = "duplicate-sequence-in-batch"

#: An identity already stored carries different content in this batch. Genuine
#: overlapping replays repeat identical observations; disagreement means one of
#: the two batches is not what it claims to be.
REJECT_OVERLAP_CONFLICT = "overlapping-observation-conflict"

#: The manifest's ``source_name`` does not slug to the directory the batch was
#: found in, so the recorded identity and the storage partition disagree.
REJECT_SOURCE_KEY_MISMATCH = "source-key-mismatch"

#: A reference reached the projection with no recorded source name. Manifests
#: missing one are already reported as malformed by the reader, so this covers a
#: directly constructed reference — and rejects rather than substituting a name,
#: because any substitution would be the fallback this design removes.
REJECT_MISSING_SOURCE_NAME = "manifest-source-name-missing"

#: Advisory, not a rejection: the manifest's declared observation count differs
#: from the number of observations actually parsed from the verified payload.
NOTE_COUNT_MISMATCH = "manifest-observation-count-mismatch"


@dataclass(frozen=True)
class BatchRejection:
    raw_sha256: str
    manifest_path: str
    reason: str
    detail: str


@dataclass(frozen=True)
class ObservedRange:
    """What the digest-verified payload actually contained.

    The manifest's declared range and count are metadata: the digest covers the
    XML, not the sidecar, so a corrupted sidecar can survive verification. The
    ledger therefore records the range derived from the parsed observations and
    keeps the manifest's count separately, under a name that says where it came
    from, rather than asserting unverified numbers as fact.
    """

    first_sequence: int
    last_sequence: int
    count: int
    declared_count: int

    @property
    def advisory(self) -> str | None:
        if self.count == self.declared_count:
            return None
        return (
            f"{NOTE_COUNT_MISMATCH}: manifest declares {self.declared_count} "
            f"observations, payload contains {self.count}"
        )


@dataclass(frozen=True)
class ProjectionReport:
    """What one projection run did, including everything it refused to do."""

    source_name: str
    source_key: str
    agent_instance_id: int
    scanned_batches: int = 0
    projected_batches: int = 0
    already_projected_batches: int = 0
    rejected_batches: int = 0
    observations_written: int = 0
    observations_already_present: int = 0
    unsupported_manifests: tuple[RawBatchIssue, ...] = ()
    malformed_manifests: tuple[RawBatchIssue, ...] = ()
    missing_payloads: tuple[RawBatchIssue, ...] = ()
    rejections: tuple[BatchRejection, ...] = ()
    probe_available: bool = False

    @property
    def skipped_manifests(self) -> int:
        return (
            len(self.unsupported_manifests)
            + len(self.malformed_manifests)
            + len(self.missing_payloads)
        )

    @property
    def ok(self) -> bool:
        return not self.rejections and self.skipped_manifests == 0


def _utc_stamp(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


class CanonicalObservationProjection:
    """Project one recorder archive into the canonical observation store.

    The recorder's own :class:`DurableRecorderStore` supplies and verifies the
    raw batches, and the recorder's own parser interprets them, so this class
    cannot drift from what the recorder writes. It adds canonicalisation,
    identity, and the durable ledger — nothing else.
    """

    def __init__(
        self,
        *,
        store: DurableRecorderStore,
        database: CanonicalObservationStore,
        clock: Any = None,
    ) -> None:
        self.store = store
        self.database = database
        # Only the ledger's ``projected_at`` uses this. No observation field
        # depends on it, so determinism does not depend on the clock.
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    # -- discovery ---------------------------------------------------------

    def available_instances(self, source_name: str) -> tuple[int, ...]:
        """Return every Agent instance recorded for a source, oldest label first."""

        root = self.store.raw_root / _slug(source_name)
        if not root.exists():
            return ()
        instances: list[int] = []
        for child in root.iterdir():
            if not child.is_dir():
                continue
            try:
                instances.append(int(child.name))
            except ValueError:
                continue
        return tuple(sorted(instances))

    def project_source(self, source_name: str) -> tuple[ProjectionReport, ...]:
        """Project every Agent instance recorded under one source."""

        return tuple(
            self.project_instance(source_name=source_name, agent_instance_id=instance)
            for instance in self.available_instances(source_name)
        )

    # -- projection --------------------------------------------------------

    def project_instance(
        self,
        *,
        source_name: str,
        agent_instance_id: int,
    ) -> ProjectionReport:
        source_key = _slug(source_name)
        scan = self.store.scan_raw_batches(
            source_name=source_name, instance_id=agent_instance_id
        )
        probe = self._load_probe(
            source_name=source_name, agent_instance_id=agent_instance_id
        )
        already = self.database.projected_digests(source_key=source_key)

        projected = 0
        skipped = 0
        rejected: list[BatchRejection] = []
        written = 0
        already_present = 0

        for ref in scan.refs:
            # Only a successful projection suppresses work. A previously
            # rejected batch is retried, so restoring a truncated payload from
            # backup is enough to bring it in without touching the database.
            if already.get(ref.raw_sha256) == BATCH_STATUS_PROJECTED:
                skipped += 1
                continue
            # The manifest is the only evidence of the original source name;
            # the directory is a lossy slug of it. There is deliberately no
            # fallback to the caller's name — falling back would make the
            # canonical identity depend on how the projection was invoked,
            # which is exactly the configured-versus-archive divergence the
            # recorded name exists to prevent. A manifest without one never
            # reaches here: scan_raw_batches reports it as malformed.
            recorded_name = ref.source_name
            outcome = self._project_batch(
                ref=ref,
                source_name=recorded_name,
                source_key=source_key,
                agent_instance_id=agent_instance_id,
                probe=probe,
            )
            if isinstance(outcome, BatchRejection):
                rejected.append(outcome)
                self._record_rejection(
                    ref=ref,
                    source_name=recorded_name,
                    source_key=source_key,
                    agent_instance_id=agent_instance_id,
                    rejection=outcome,
                )
                continue
            new_rows, repeated, observed = outcome
            self.database.record_batch(
                observations=new_rows,
                batch=self._ledger_row(
                    ref=ref,
                    source_name=recorded_name,
                    source_key=source_key,
                    agent_instance_id=agent_instance_id,
                    projected_count=len(new_rows) + repeated,
                    status=BATCH_STATUS_PROJECTED,
                    reason=None,
                    detail=observed.advisory,
                    observed=observed,
                ),
            )
            projected += 1
            written += len(new_rows)
            already_present += repeated

        return ProjectionReport(
            source_name=source_name,
            source_key=source_key,
            agent_instance_id=agent_instance_id,
            scanned_batches=len(scan.refs),
            projected_batches=projected,
            already_projected_batches=skipped,
            rejected_batches=len(rejected),
            observations_written=written,
            observations_already_present=already_present,
            unsupported_manifests=tuple(
                issue
                for issue in scan.issues
                if issue.reason == RAW_BATCH_ISSUE_UNSUPPORTED_SCHEMA
            ),
            malformed_manifests=tuple(
                issue
                for issue in scan.issues
                if issue.reason == RAW_BATCH_ISSUE_MALFORMED
            ),
            missing_payloads=tuple(
                issue
                for issue in scan.issues
                if issue.reason == RAW_BATCH_ISSUE_RAW_MISSING
            ),
            rejections=tuple(rejected),
            probe_available=probe is not None,
        )

    # -- one batch ---------------------------------------------------------

    def _project_batch(
        self,
        *,
        ref: RawBatchRef,
        source_name: str,
        source_key: str,
        agent_instance_id: int,
        probe: ProbeModel | None,
    ) -> tuple[list[CanonicalObservation], int, ObservedRange] | BatchRejection:
        def reject(reason: str, detail: str) -> BatchRejection:
            return BatchRejection(
                raw_sha256=ref.raw_sha256,
                manifest_path=str(ref.manifest_path),
                reason=reason,
                detail=detail,
            )

        if not source_name:
            return reject(
                REJECT_MISSING_SOURCE_NAME,
                "manifest records no source name, so the original identity "
                "cannot be reconstructed from the archive",
            )
        # The recorded name must belong to the partition the batch was found
        # in, or the identity written into every row would disagree with where
        # the evidence lives.
        if _slug(source_name) != source_key:
            return reject(
                REJECT_SOURCE_KEY_MISMATCH,
                f"manifest source_name {source_name!r} belongs to partition "
                f"{_slug(source_name)!r}, not {source_key!r}",
            )

        # read_raw_batch decompresses and re-hashes, so digest verification
        # keeps exactly the semantics the recorder already relies on.
        try:
            xml_text = self.store.read_raw_batch(ref)
        except MtconnectProtocolError as exc:
            return reject(REJECT_DIGEST_MISMATCH, str(exc))
        except (OSError, gzip.BadGzipFile, EOFError) as exc:
            return reject(REJECT_PAYLOAD_UNREADABLE, f"{type(exc).__name__}: {exc}")
        except UnicodeDecodeError as exc:
            return reject(REJECT_PAYLOAD_UNREADABLE, f"payload is not UTF-8: {exc}")

        try:
            batch = recorder_parsing.parse_streams(
                xml_text,
                source_name=source_name,
                probe=probe,
                # Never let the parser reach for a wall clock: an absent
                # timestamp must resolve to recorded data or to nothing.
                received_at=ref.received_at or "",
            )
        except (MtconnectProtocolError, ET.ParseError) as exc:
            return reject(REJECT_XML_INVALID, f"{type(exc).__name__}: {exc}")

        if int(batch.header.instance_id) != int(agent_instance_id):
            return reject(
                REJECT_INSTANCE_MISMATCH,
                f"document declares instance {batch.header.instance_id}, "
                f"archive path declares {agent_instance_id}",
            )

        rows: list[CanonicalObservation] = []
        try:
            for record in batch.observations:
                rows.append(
                    CanonicalObservation.from_recorder_record(
                        record,
                        source_name=source_name,
                        agent_instance_id=agent_instance_id,
                        raw_batch_sha256=ref.raw_sha256,
                        fallback_timestamp=ref.received_at,
                    )
                )
        except CanonicalObservationError as exc:
            return reject(REJECT_NOT_CANONICALISABLE, str(exc))

        rows.sort(key=lambda item: item.sequence)
        sequences = [item.sequence for item in rows]
        if not sequences:
            # The recorder refuses to store an empty batch, so a payload with no
            # observations is evidence that does not match its own manifest.
            return reject(
                REJECT_XML_INVALID,
                "verified payload contains no observations while its manifest "
                f"declares {ref.observation_count}",
            )
        if len(set(sequences)) != len(sequences):
            repeated = sorted(
                {value for value in sequences if sequences.count(value) > 1}
            )[:10]
            return reject(
                REJECT_DUPLICATE_SEQUENCE,
                "one Agent instance repeated sequence number(s) "
                + ", ".join(str(value) for value in repeated),
            )

        stored = self.database.existing_observations(
            source_key=source_key,
            agent_instance_id=agent_instance_id,
            sequences=sequences,
        )
        fresh: list[CanonicalObservation] = []
        repeated_rows = 0
        for row in rows:
            existing = stored.get(row.sequence)
            if existing is None:
                fresh.append(row)
                continue
            # An overlapping batch legitimately repeats observations. It must
            # repeat them identically; only the batch that first carried the
            # observation is recorded as its provenance.
            if replace(row, raw_batch_sha256=existing.raw_batch_sha256) != existing:
                return reject(
                    REJECT_OVERLAP_CONFLICT,
                    f"sequence {row.sequence} is already stored from batch "
                    f"{existing.raw_batch_sha256} with different content",
                )
            repeated_rows += 1
        return (
            fresh,
            repeated_rows,
            ObservedRange(
                first_sequence=sequences[0],
                last_sequence=sequences[-1],
                count=len(rows),
                declared_count=ref.observation_count,
            ),
        )

    # -- ledger ------------------------------------------------------------

    def _ledger_row(
        self,
        *,
        ref: RawBatchRef,
        source_name: str,
        source_key: str,
        agent_instance_id: int,
        projected_count: int,
        status: str,
        reason: str | None,
        detail: str | None,
        observed: ObservedRange | None = None,
    ) -> ProjectedBatchRecord:
        # A projected row reports the range derived from the digest-verified
        # payload. Only a rejected row falls back to the manifest's declared
        # range, and it is not claiming that range is correct.
        first_sequence = (
            observed.first_sequence if observed is not None else ref.first_sequence
        )
        last_sequence = (
            observed.last_sequence if observed is not None else ref.last_sequence
        )
        return ProjectedBatchRecord(
            source_key=source_key,
            raw_sha256=ref.raw_sha256,
            source_name=source_name,
            agent_instance_id=agent_instance_id,
            manifest_path=str(ref.manifest_path),
            raw_path=str(ref.raw_path),
            manifest_schema=ref.manifest_schema,
            first_sequence=first_sequence,
            last_sequence=last_sequence,
            manifest_observation_count=ref.observation_count,
            projected_count=projected_count,
            status=status,
            reason=reason,
            detail=detail,
            projected_at=_utc_stamp(self._clock()),
        )

    def _record_rejection(
        self,
        *,
        ref: RawBatchRef,
        source_name: str,
        source_key: str,
        agent_instance_id: int,
        rejection: BatchRejection,
    ) -> None:
        self.database.record_batch(
            observations=(),
            batch=self._ledger_row(
                ref=ref,
                source_name=source_name,
                source_key=source_key,
                agent_instance_id=agent_instance_id,
                projected_count=0,
                status=BATCH_STATUS_REJECTED,
                reason=rejection.reason,
                detail=rejection.detail[:2000],
            ),
        )

    # -- probe -------------------------------------------------------------

    def _load_probe(
        self, *, source_name: str, agent_instance_id: int
    ) -> ProbeModel | None:
        """Return the archived ``/probe`` for this instance, if one was kept.

        The probe supplies data item units, MTConnect type, sub type and name.
        A capture without one still projects; those columns are simply null,
        which is why none of them participate in identity.
        """

        try:
            return self.store.load_archived_probe(
                source_name=source_name, instance_id=agent_instance_id
            )
        except (MtconnectProtocolError, ET.ParseError, OSError, gzip.BadGzipFile):
            return None


def rebuild_from_recorder_archive(
    *,
    data_dir: Path | str,
    database_path: Path | str,
    sources: Sequence[str] | None = None,
) -> tuple[ProjectionReport, ...]:
    """Rebuild the projection from a recorder data directory.

    The raw capture is opened read-only. Deleting ``database_path`` first is a
    complete reset: nothing about the reconstruction depends on state that lives
    only in the projection.
    """

    store = DurableRecorderStore(Path(data_dir))
    database = CanonicalObservationStore(database_path)
    projection = CanonicalObservationProjection(store=store, database=database)
    names = list(sources) if sources is not None else _discover_sources(store)
    reports: list[ProjectionReport] = []
    for source_name in names:
        reports.extend(projection.project_source(source_name))
    return tuple(reports)


def _discover_sources(store: DurableRecorderStore) -> list[str]:
    """Return the source directory names present in a recorder archive.

    Directories are named ``_slug(source_name)``, so these are partition keys
    rather than source names. They are used only to locate archives: the
    canonical ``source_name`` written into every observation comes from each
    batch's manifest, so discovering an archive this way records exactly the
    same identity as projecting it from a configured source list.
    """

    if not store.raw_root.exists():
        return []
    return sorted(child.name for child in store.raw_root.iterdir() if child.is_dir())


__all__ = [
    "REJECT_DIGEST_MISMATCH",
    "REJECT_DUPLICATE_SEQUENCE",
    "REJECT_INSTANCE_MISMATCH",
    "REJECT_NOT_CANONICALISABLE",
    "REJECT_OVERLAP_CONFLICT",
    "REJECT_PAYLOAD_UNREADABLE",
    "REJECT_XML_INVALID",
    "BatchRejection",
    "CanonicalObservationProjection",
    "ProjectionReport",
    "rebuild_from_recorder_archive",
]
