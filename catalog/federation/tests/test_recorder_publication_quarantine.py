"""B03: one unusable archive item must not strand every other source forever.

Archive reconciliation turns locally committed MTConnect batches into durable
outbox entries. Every item-level fault it can meet -- an observation file that
cannot be read, a malformed line, an empty batch, a sequence discontinuity, or
a single observation larger than the bounded publication size -- was raised out
of the whole reconcile pass.

The worker loop above it treats that as an ordinary cycle failure and retries on
the next poll, which is right for a transient fault and useless for this one: the
same item fails the same way forever. Nothing after it was ever published --
neither the rest of that source's archive nor *any other source*, because the
loop over sources never got past the bad one. Capture kept recording, so the
durable work was not lost. It was stranded, permanently, behind a repeating
cycle failure that named no item and no source.

These cases pin the isolation: the faulted source stops, every other source
publishes, the condition is named on the operator health surface, and nothing is
deleted. Recorder sequence order per dataset is preserved -- the rest of the
faulted source waits behind the item rather than publishing across the gap.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.outbox import RetiredSummary, SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import (
    DurableRecorderDeliveryQueue,
    RecorderDeliveryRunResult,
)
from catalog.federation.recorder_publication import (
    MAX_REPORTED_QUARANTINED_SOURCES,
    QUARANTINE_CODES,
    RecorderArchiveReconciler,
    RecorderPublicationCycleReport,
    RecorderPublicationTarget,
    RecorderWorkerCycleResult,
)
from catalog.mtconnect_recorder import (
    DurableRecorderStore,
    SourceCheckpoint,
    parse_probe,
    parse_streams,
)

from .test_recorder_publication import PROBE_XML, SAMPLE_XML

NOW = datetime(2026, 8, 9, 3, 0, 4, tzinfo=timezone.utc)

#: The ordered dataset each source publishes into, as the target derives it.
DATASET = {
    name: RecorderPublicationTarget(
        session_id="session-1",
        group_id="telemetry-storage",
        recorder_node_id="node-recorder-1",
    ).dataset_id(name)
    for name in ("Mazak", "Okuma")
}


class _Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def ingest_batch(self, **kwargs: Any) -> PhaseDIngestOutcome:
        self.calls.append(dict(kwargs))
        return PhaseDIngestOutcome(committed=True)


def _checkpoint(source_name: str, *, probe_sha256: str) -> SourceCheckpoint:
    return SourceCheckpoint(
        source_name=source_name,
        base_url="http://agent:5000",
        machine_id="MAZAK-001",
        agent_instance_id=77,
        next_sequence=13,
        probe_sha256=probe_sha256,
        storage_aliases=[],
    )


def _two_source_recorder(tmp_path: Path, *, max_content_bytes: int = 900_000):
    """Two sources, each with one committed, eligible archive batch."""

    data_dir = tmp_path / "data"
    checkpoint_file = data_dir / "source_state" / "mtconnect_recorder_state.json"
    store = DurableRecorderStore(data_dir)
    probe = parse_probe(PROBE_XML)

    stored: dict[str, Any] = {}
    for source_name in ("Mazak", "Okuma"):
        batch = parse_streams(SAMPLE_XML, source_name=source_name, probe=probe)
        stored[source_name] = store.store_batch(
            source_name=source_name,
            requested_from=int(batch.first_observation_sequence or 0),
            xml_text=SAMPLE_XML,
            batch=batch,
        )

    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_file.write_text(
        json.dumps(
            {
                "schema": "fcp.mtconnect_recorder.checkpoints.v3",
                "updated_at": "2026-08-09T03:00:04Z",
                "sources": {
                    name: _checkpoint(name, probe_sha256=probe.sha256).to_dict()
                    for name in ("Mazak", "Okuma")
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    outbox = SQLiteOutbox(tmp_path / "publisher" / "outbox.sqlite3")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_Client(),
        session_id="session-1",
    )
    reconciler = RecorderArchiveReconciler(
        store=store,
        checkpoint_file=checkpoint_file,
        queue=queue,
        target=RecorderPublicationTarget(
            session_id="session-1",
            group_id="telemetry-storage",
            recorder_node_id="node-recorder-1",
        ),
        max_content_bytes=max_content_bytes,
    )
    return reconciler, outbox, stored


def _datasets(outbox: SQLiteOutbox) -> set[str]:
    return {
        str(entry.payload["dataset_id"])
        for entry in outbox.pending()
        if isinstance(entry.payload, dict)
    }


def test_a_malformed_archive_item_fences_only_its_own_source(tmp_path: Path) -> None:
    """The consequence: every other source used to be stranded with it."""

    reconciler, outbox, stored = _two_source_recorder(tmp_path)
    bad = Path(stored["Mazak"].observation_path)
    original = bad.read_bytes()
    bad.write_text('{"sequence": 10, "value"\n', encoding="utf-8")

    result = reconciler.reconcile()

    # Sorted source order puts the faulted source first, which is exactly the
    # case that used to stop the pass before the healthy source was reached.
    assert result.quarantine.total == 1
    quarantined = result.quarantine.sources[0]
    assert quarantined.source_name == "Mazak"
    assert quarantined.error_code == "malformed-recorder-observation"
    assert quarantined.error_code in QUARANTINE_CODES
    assert result.quarantine.truncated is False

    assert result.enqueued >= 1
    assert _datasets(outbox) == {DATASET["Okuma"]}

    # Primary evidence is never removed to make publication proceed.
    assert bad.exists()
    assert Path(stored["Mazak"].raw_path).exists()
    assert bad.read_bytes() != original  # the test corrupted it, nothing else did


@pytest.mark.parametrize(
    ("corrupt", "expected_code"),
    [
        (lambda path: path.unlink(), "recorder-observations-missing"),
        (lambda path: path.write_text("", encoding="utf-8"), "recorder-observations-empty"),
        (
            lambda path: path.write_text(
                '{"sequence": 10, "data_item_id": "xp", "value": "1"}\n',
                encoding="utf-8",
            ),
            "recorder-sequence-mismatch",
        ),
    ],
)
def test_every_item_fault_fences_one_source_rather_than_the_cycle(
    tmp_path: Path,
    corrupt,
    expected_code: str,
) -> None:
    reconciler, outbox, stored = _two_source_recorder(tmp_path)
    corrupt(Path(stored["Mazak"].observation_path))

    result = reconciler.reconcile()

    assert [item.error_code for item in result.quarantine.sources] == [expected_code]
    assert _datasets(outbox) == {DATASET["Okuma"]}


def test_an_unreadable_archive_item_fences_one_source_rather_than_the_cycle(
    tmp_path: Path,
) -> None:
    """Present but unreadable is a different fault from absent, and as fatal."""

    reconciler, outbox, stored = _two_source_recorder(tmp_path)
    observation_path = Path(stored["Mazak"].observation_path)
    observation_path.unlink()
    observation_path.mkdir()

    result = reconciler.reconcile()

    assert [item.error_code for item in result.quarantine.sources] == [
        "recorder-observations-unavailable"
    ]
    assert _datasets(outbox) == {DATASET["Okuma"]}


def test_the_fenced_codes_are_item_faults_and_nothing_else() -> None:
    """The fence must not widen to a checkpoint or contract failure.

    Those describe state this reconciler cannot interpret at all, or a
    publication target that does not match its authenticated queue. Skipping a
    source on one of those would hide a genuine contract violation behind a
    quietly degraded surface, so they stay fatal.
    """

    assert QUARANTINE_CODES == {
        "malformed-recorder-observation",
        "recorder-observation-too-large",
        "recorder-observations-empty",
        "recorder-observations-missing",
        "recorder-observations-unavailable",
        "recorder-receipt-time-missing",
        "recorder-sequence-mismatch",
    }
    for contract_code in (
        "invalid-recorder-publication",
        "malformed-recorder-state",
        "recorder-session-mismatch",
        "recorder-state-unavailable",
        "unsupported-recorder-state",
    ):
        assert contract_code not in QUARANTINE_CODES


def test_an_unparseable_receipt_stamp_falls_through_to_the_item_fault(
    tmp_path: Path,
) -> None:
    """A present-but-unparseable stamp is not better evidence than the next one.

    It also must not surface as the shared construction-validation code, which
    the per-source fence deliberately does not cover.
    """

    reconciler, outbox, stored = _two_source_recorder(tmp_path)
    raw_path = Path(stored["Mazak"].raw_path)
    manifest_path = raw_path.with_suffix(".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["received_at"] = "not-a-timestamp"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    observation_path = Path(stored["Mazak"].observation_path)
    observation_path.write_text(
        "\n".join(
            json.dumps(
                {
                    "sequence": sequence,
                    "data_item_id": "xp",
                    "value": "1",
                    "received_at": "also-not-a-timestamp",
                    "timestamp": "still-not-a-timestamp",
                }
            )
            for sequence in (10, 11, 12)
        )
        + "\n",
        encoding="utf-8",
    )

    result = reconciler.reconcile()

    assert [item.error_code for item in result.quarantine.sources] == [
        "recorder-receipt-time-missing"
    ]
    assert _datasets(outbox) == {DATASET["Okuma"]}


def test_an_oversized_observation_fences_one_source_rather_than_the_cycle(
    tmp_path: Path,
) -> None:
    """The bounded publication size is a ceiling, not a reason to stop."""

    reconciler, outbox, stored = _two_source_recorder(tmp_path)
    oversized = json.loads(
        Path(stored["Mazak"].observation_path)
        .read_text(encoding="utf-8")
        .splitlines()[0]
    )
    oversized["value"] = "x" * 4096
    Path(stored["Mazak"].observation_path).write_text(
        "\n".join(
            json.dumps({**oversized, "sequence": sequence})
            for sequence in (10, 11, 12)
        )
        + "\n",
        encoding="utf-8",
    )
    reconciler.max_content_bytes = 2_048

    result = reconciler.reconcile()

    assert [item.error_code for item in result.quarantine.sources] == [
        "recorder-observation-too-large"
    ]
    assert _datasets(outbox) == {DATASET["Okuma"]}


def test_a_repaired_archive_publishes_on_the_next_pass(tmp_path: Path) -> None:
    """Quarantine is a fence an operator can clear, not a permanent decision."""

    reconciler, outbox, stored = _two_source_recorder(tmp_path)
    bad = Path(stored["Mazak"].observation_path)
    original = bad.read_bytes()
    bad.write_text('{"sequence": 10, "value"\n', encoding="utf-8")

    assert reconciler.reconcile().quarantine.total == 1
    bad.write_bytes(original)

    repaired = reconciler.reconcile()

    assert repaired.quarantine.total == 0
    assert _datasets(outbox) == {
        DATASET["Mazak"],
        DATASET["Okuma"],
    }


def test_an_unexpected_reconcile_failure_is_still_raised(tmp_path: Path) -> None:
    """Only the named item faults are isolated; anything else stays fatal.

    Widening the containment to every validation error would turn a genuine
    contract violation -- a checkpoint this reconciler cannot parse, a session
    mismatch -- into a quietly skipped source.
    """

    reconciler, _outbox, _stored = _two_source_recorder(tmp_path)

    def _unexpected(*_args: object, **_kwargs: object) -> None:
        raise FederationValidationError(
            "recorder-session-mismatch",
            "session_id",
            "publication target must match the queue's authenticated session",
        )

    reconciler._reconcile_source = _unexpected  # type: ignore[method-assign]

    with pytest.raises(FederationValidationError) as refused:
        reconciler.reconcile()

    assert refused.value.code == "recorder-session-mismatch"
    assert refused.value.code not in QUARANTINE_CODES


def test_a_quarantined_cycle_is_reported_as_degraded(tmp_path: Path) -> None:
    """A fenced source needs an operator, so it must not read as publishing."""

    reconciler, _outbox, stored = _two_source_recorder(tmp_path)
    Path(stored["Mazak"].observation_path).write_text(
        '{"sequence": 10, "value"\n', encoding="utf-8"
    )
    reconcile = reconciler.reconcile()

    report = RecorderPublicationCycleReport(
        result=RecorderWorkerCycleResult(
            checkpoint_changed=True,
            reconcile=reconcile,
            delivery=RecorderDeliveryRunResult(
                attempted=1,
                committed=1,
                pending=0,
                blocked_datasets=(),
            ),
            retirement=RetiredSummary(total=0, datasets=(), truncated=False),
        )
    )

    assert report.quarantined_sources == 1
    assert report.state == "degraded"
    assert report.healthy is False


def test_the_reported_quarantine_sample_is_bounded() -> None:
    """A health surface must never be handed an unbounded result set."""

    assert MAX_REPORTED_QUARANTINED_SOURCES > 0
    from catalog.federation.recorder_publication import (
        MAX_QUARANTINE_IDENTITY_CHARS,
        QuarantinedSource,
        QuarantineSummary,
    )

    many = tuple(
        QuarantinedSource(
            source_name=f"source-{index}",
            error_code="malformed-recorder-observation",
            batch_identity="observation_file:1",
        )
        for index in range(MAX_REPORTED_QUARANTINED_SOURCES + 5)
    )
    summary = QuarantineSummary(
        total=len(many),
        sources=many[:MAX_REPORTED_QUARANTINED_SOURCES],
        truncated=True,
    )

    assert len(summary.sources) == MAX_REPORTED_QUARANTINED_SOURCES
    assert summary.total > len(summary.sources)
    assert summary.truncated is True
    assert MAX_QUARANTINE_IDENTITY_CHARS > 0


def test_the_reported_item_identity_carries_no_local_path(tmp_path: Path) -> None:
    """This record reaches an operator surface, so it stays path-free."""

    reconciler, _outbox, stored = _two_source_recorder(tmp_path)
    observation_path = Path(stored["Mazak"].observation_path)
    observation_path.write_text('{"sequence": 10, "value"\n', encoding="utf-8")

    result = reconciler.reconcile()
    identity = result.quarantine.sources[0].batch_identity

    assert identity == "observation_file:1"
    assert str(tmp_path) not in identity
    assert observation_path.name not in identity
