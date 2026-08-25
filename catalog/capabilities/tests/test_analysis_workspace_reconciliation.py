from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.analysis.worker import FederatedAnalysisHandler
from catalog.capabilities.analysis.workspace_reconciliation import (
    WORKSPACE_MARKER,
    prepare_owned_workspace,
    reconcile_stale_workspaces,
    write_workspace_marker,
)

NOW = datetime(2026, 8, 25, 6, 30, tzinfo=timezone.utc)


def _attempt(root: Path, job: str = "job-1", attempt: str = "attempt-1") -> Path:
    path = root / job / attempt
    path.mkdir(parents=True)
    return path


def _make_stale(path: Path, *, age: timedelta = timedelta(days=2)) -> None:
    timestamp = (NOW - age).timestamp()
    # The test helper receives a freshly-created plain directory. Windows does
    # not implement os.utime(..., follow_symlinks=False), so do not request a
    # capability the fixture does not need; production cleanup still performs
    # its own lstat/reparse checks before trusting an entry.
    os.utime(path, (timestamp, timestamp))


def test_stale_marked_attempt_is_removed_and_empty_job_parent_is_retired(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspaces"
    attempt = _attempt(root)
    (attempt / "data").mkdir()
    write_workspace_marker(
        attempt,
        job_id="job-1",
        attempt_id="attempt-1",
        created_at=NOW - timedelta(days=2),
    )
    _make_stale(attempt)

    report = reconcile_stale_workspaces(root, now=NOW)

    assert report.removed_attempts == 1
    assert report.stale_candidates == 1
    assert not attempt.exists()
    assert not (root / "job-1").exists()


def test_recent_attempt_is_preserved(tmp_path: Path) -> None:
    root = tmp_path / "workspaces"
    attempt = _attempt(root)
    write_workspace_marker(
        attempt,
        job_id="job-1",
        attempt_id="attempt-1",
        created_at=NOW,
    )

    report = reconcile_stale_workspaces(root, now=NOW)

    assert report.removed_attempts == 0
    assert attempt.exists()


def test_exact_legacy_workspace_layout_is_reconciled(tmp_path: Path) -> None:
    root = tmp_path / "workspaces"
    attempt = _attempt(root, "legacy-job", "legacy-attempt")
    (attempt / "staging").mkdir()
    (attempt / "inputs").mkdir()
    (attempt / "data").mkdir()
    _make_stale(attempt)

    report = reconcile_stale_workspaces(root, now=NOW)

    assert report.removed_attempts == 1
    assert not attempt.exists()


def test_unexpected_stale_content_is_preserved_as_ambiguous(tmp_path: Path) -> None:
    root = tmp_path / "workspaces"
    attempt = _attempt(root)
    (attempt / "notes.txt").write_text("not owned by the workspace contract\n")
    _make_stale(attempt)

    report = reconcile_stale_workspaces(root, now=NOW)

    assert report.removed_attempts == 0
    assert report.preserved_ambiguous >= 1
    assert attempt.exists()


def test_mismatched_marker_with_unexpected_content_is_not_cleanup_authority(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspaces"
    attempt = _attempt(root)
    write_workspace_marker(
        attempt,
        job_id="different-job",
        attempt_id="attempt-1",
        created_at=NOW - timedelta(days=2),
    )
    (attempt / "user-data.bin").write_bytes(b"keep")
    _make_stale(attempt)

    report = reconcile_stale_workspaces(root, now=NOW)

    assert report.removed_attempts == 0
    assert attempt.exists()
    assert (attempt / "user-data.bin").read_bytes() == b"keep"


def test_reparse_or_symlink_attempt_is_never_followed(tmp_path: Path) -> None:
    root = tmp_path / "workspaces"
    job = root / "job-1"
    job.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep\n")
    link = job / "attempt-1"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("directory symlinks are unavailable on this runner")

    report = reconcile_stale_workspaces(root, now=NOW)

    assert report.removed_attempts == 0
    assert report.preserved_ambiguous >= 1
    assert (outside / "keep.txt").read_text() == "keep\n"


def test_scan_budget_is_hard_bounded(tmp_path: Path) -> None:
    root = tmp_path / "workspaces"
    for index in range(8):
        attempt = _attempt(root, f"job-{index}", "attempt")
        (attempt / "data").mkdir()
        _make_stale(attempt)

    report = reconcile_stale_workspaces(
        root,
        now=NOW,
        max_scanned_entries=3,
    )

    assert report.scanned_entries <= 3
    assert report.truncated is True
    assert report.removed_attempts <= 1


def test_handler_startup_runs_reconciliation_before_accepting_work(tmp_path: Path) -> None:
    root = tmp_path / "workspaces"
    attempt = _attempt(root)
    (attempt / "data").mkdir()
    write_workspace_marker(
        attempt,
        job_id="job-1",
        attempt_id="attempt-1",
        created_at=NOW - timedelta(days=2),
    )
    _make_stale(attempt)

    handler = FederatedAnalysisHandler(
        session_id="session-1",
        node_id="node-1",
        provider_id="provider-1",
        artifact_transport=object(),  # type: ignore[arg-type]
        executor=object(),  # type: ignore[arg-type]
        workspace_root=root,
        content_store=object(),  # type: ignore[arg-type]
        clock=lambda: NOW,
        data_owner_node_id=lambda job: "owner-1",
    )

    assert handler.workspace_reconciliation.removed_attempts == 1
    assert not attempt.exists()


def test_torn_marker_in_exact_owned_layout_is_still_recoverable(tmp_path: Path) -> None:
    root = tmp_path / "workspaces"
    attempt = _attempt(root)
    (attempt / "data").mkdir()
    (attempt / WORKSPACE_MARKER).write_text('{"schema":')
    _make_stale(attempt)

    report = reconcile_stale_workspaces(root, now=NOW)

    assert report.removed_attempts == 1
    assert not attempt.exists()


def test_retry_resets_only_a_matching_owned_attempt(tmp_path: Path) -> None:
    root = tmp_path / "workspaces"
    attempt = _attempt(root)
    (attempt / "data").mkdir()
    (attempt / "data" / "old.jsonl").write_text("old\n")
    write_workspace_marker(
        attempt,
        job_id="job-1",
        attempt_id="attempt-1",
        created_at=NOW - timedelta(minutes=5),
    )

    fresh = prepare_owned_workspace(
        root,
        job_id="job-1",
        attempt_id="attempt-1",
        created_at=NOW,
    )

    assert fresh == attempt
    assert not (fresh / "data").exists()
    assert (fresh / WORKSPACE_MARKER).is_file()


def test_retry_refuses_to_remove_ambiguous_existing_content(tmp_path: Path) -> None:
    root = tmp_path / "workspaces"
    attempt = _attempt(root)
    payload = attempt / "keep.txt"
    payload.write_text("keep\n")

    with pytest.raises(ValueError, match="not safely owned"):
        prepare_owned_workspace(
            root,
            job_id="job-1",
            attempt_id="attempt-1",
            created_at=NOW,
        )

    assert payload.read_text() == "keep\n"


@pytest.mark.parametrize(
    ("job_id", "attempt_id"),
    [
        ("../outside", "attempt-1"),
        ("job-1", "../outside"),
        ("C:", "attempt-1"),
        ("job-1", "a\\b"),
        ("", "attempt-1"),
    ],
)
def test_workspace_identity_cannot_escape_or_select_a_drive(
    tmp_path: Path,
    job_id: str,
    attempt_id: str,
) -> None:
    root = tmp_path / "workspaces"
    outside = tmp_path / "outside"
    outside.mkdir()
    keep = outside / "keep.txt"
    keep.write_text("keep\n")

    with pytest.raises(ValueError, match="portable path component"):
        prepare_owned_workspace(
            root,
            job_id=job_id,
            attempt_id=attempt_id,
            created_at=NOW,
        )

    assert keep.read_text() == "keep\n"
