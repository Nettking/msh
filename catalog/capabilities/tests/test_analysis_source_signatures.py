"""Job identity follows the source data, and nothing else.

These tests use the repository's real source-signature contract
(:mod:`catalog.runner.data_filtering`) against a real data directory, and the
real durable submission path. Nothing about deduplication is simulated: the
same logical work must collapse onto one durable job, and materially different
source data must become different durable work.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from catalog.capabilities.analysis import (
    AnalysisJobRegistry,
    AnalysisWorkService,
    AnalysisWorkSlice,
)
from catalog.capabilities.analysis import contracts as analysis_contracts
from catalog.capabilities.analysis.contracts import (
    ORIGIN_AUTOMATIC_DISCOVERY,
    SLICE_KIND_DATE,
)
from catalog.capabilities.tests.analysis_harness import build_stack
from catalog.runner.data_filtering import (
    date_range_source_signature,
    source_date_signatures,
)

DAY = "2026-08-13"


@pytest.fixture(autouse=True)
def _isolated_data_index(tmp_path: Path, monkeypatch) -> None:
    """Keep the runner's size/mtime index inside the test's own directory."""

    monkeypatch.setattr(
        "catalog.runner.data_filtering.DATA_INDEX_FILE",
        tmp_path / "index" / "data_index.json",
    )


def _write(data_dir: Path, name: str, *, machine: str = "A", day: str = DAY) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / name
    path.write_text(
        f'{{"timestamp":"{day}T10:00:00Z","machine":"{machine}"}}\n',
        encoding="utf-8",
    )
    return path


def _signature(data_dir: Path, day: str = DAY) -> str:
    """Compute the day's source signature exactly as discovery does."""

    target = date.fromisoformat(day)
    return date_range_source_signature(source_date_signatures(data_dir), target, target)


def _work(session_id: str, signature: str, *, day: str = DAY) -> AnalysisWorkSlice:
    return AnalysisWorkSlice(
        session_id=session_id,
        slice_kind=SLICE_KIND_DATE,
        slice_key=day,
        target_dates=(day,),
        script_keys=("data_pr_day",),
        runtime_namespace="default",
        source_signature=signature,
        origin=ORIGIN_AUTOMATIC_DISCOVERY,
    )


def _submit(stack, data_dir: Path, signature: str):
    return stack.service.submit_analysis_work(
        _work(stack.session_id, signature),
        slice_files=tuple(sorted(data_dir.glob("*.jsonl"))),
        slice_root=data_dir,
    )


def test_rediscovering_unchanged_jsonl_is_the_same_durable_job(tmp_path: Path) -> None:
    stack = build_stack(tmp_path)
    data_dir = tmp_path / "source"
    _write(data_dir, f"{DAY}.jsonl")

    first_signature = _signature(data_dir)
    first = _submit(stack, data_dir, first_signature)
    second_signature = _signature(data_dir)
    second = _submit(stack, data_dir, second_signature)

    assert second_signature == first_signature
    assert second.job_id == first.job_id
    assert second.idempotency_key == first.idempotency_key
    assert first.created is True
    assert second.created is False
    # One durable job, not two: the second submission recognized the first.
    assert stack.store.snapshot(first.job_id).job.job_id == first.job_id


def test_modified_jsonl_content_is_new_durable_work(tmp_path: Path) -> None:
    stack = build_stack(tmp_path)
    data_dir = tmp_path / "source"
    path = _write(data_dir, f"{DAY}.jsonl")

    first_signature = _signature(data_dir)
    first = _submit(stack, data_dir, first_signature)

    # Rewrite the same day with different content, and make the change visible
    # to the repository's conservative size/mtime freshness contract.
    path.write_text(
        f'{{"timestamp":"{DAY}T10:00:00Z","machine":"B","extra":"changed"}}\n',
        encoding="utf-8",
    )
    changed_signature = _signature(data_dir)
    changed = _submit(stack, data_dir, changed_signature)

    assert changed_signature != first_signature
    assert changed.job_id != first.job_id
    assert changed.idempotency_key != first.idempotency_key
    assert changed.created is True


def test_a_second_file_for_an_already_processed_date_is_new_work(
    tmp_path: Path,
) -> None:
    """A later upload adding data for a processed day must not be swallowed."""

    stack = build_stack(tmp_path)
    data_dir = tmp_path / "source"
    _write(data_dir, f"{DAY}.jsonl")

    first_signature = _signature(data_dir)
    first = _submit(stack, data_dir, first_signature)
    assert first.created is True

    # A second JSONL file arrives for the *same* date.
    _write(data_dir, f"{DAY}-late-upload.jsonl", machine="B")
    extended_signature = _signature(data_dir)
    extended = _submit(stack, data_dir, extended_signature)

    assert extended_signature != first_signature
    assert extended.job_id != first.job_id
    assert extended.created is True
    # Both files are inside the new slice, so the extra data really is analysed.
    snapshot = stack.store.snapshot(extended.job_id)
    assert snapshot.job.inputs[1].size_bytes > 0
    record = stack.service.registry.record_for(extended.job_id)
    assert record is not None
    assert record.source_signature == extended_signature


def test_the_same_source_after_a_restart_is_still_deduplicated(
    tmp_path: Path,
) -> None:
    """Identity is durable, not in-memory: a restarted device re-recognizes it."""

    stack = build_stack(tmp_path)
    data_dir = tmp_path / "source"
    _write(data_dir, f"{DAY}.jsonl")
    signature = _signature(data_dir)
    first = _submit(stack, data_dir, signature)

    # Restart: fresh service and registry objects over the same durable stores.
    restarted = AnalysisWorkService(
        scheduler=stack.scheduler,
        registry=AnalysisJobRegistry(
            tmp_path / "capabilities" / "analysis_jobs.sqlite3"
        ),
        session_id=stack.session_id,
    )
    after_restart_signature = _signature(data_dir)
    after_restart = restarted.submit_analysis_work(
        _work(stack.session_id, after_restart_signature),
        slice_files=tuple(sorted(data_dir.glob("*.jsonl"))),
        slice_root=data_dir,
    )

    assert after_restart_signature == signature
    assert after_restart.job_id == first.job_id
    assert after_restart.created is False


def test_a_new_analysis_contract_version_is_new_work(
    tmp_path: Path, monkeypatch
) -> None:
    """Identical source data under a newer analysis contract is different work."""

    stack = build_stack(tmp_path)
    data_dir = tmp_path / "source"
    _write(data_dir, f"{DAY}.jsonl")
    signature = _signature(data_dir)
    first = _submit(stack, data_dir, signature)

    monkeypatch.setattr(analysis_contracts, "ANALYSIS_CONTRACT_VERSION", "2")
    upgraded = _submit(stack, data_dir, signature)

    assert _signature(data_dir) == signature
    assert upgraded.job_id != first.job_id
    assert upgraded.idempotency_key != first.idempotency_key
    assert upgraded.idempotency_key.startswith("analysis:2:")
    assert upgraded.created is True
