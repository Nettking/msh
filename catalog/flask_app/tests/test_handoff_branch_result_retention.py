"""Branch-listing handoff results do not accumulate for the device's lifetime.

``approved_branches`` mints a fresh request id per call, the host agent writes
one result file for it, and the poll returns as soon as it reads that file. The
Federation view calls this on every render, the id is a local ``uuid4`` that is
never persisted, and nothing ever deleted the file.

Update and trial results are a different category and are asserted here to stay
untouched: their request ids *are* durable state, re-read to reconcile a pending
update or trial across calls, so retiring them would change restart recovery.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from catalog.federation.software_trial import BRANCHES_RESULT_SCHEMA
from catalog.flask_app.services import federation_update_handoff as handoff_module
from catalog.flask_app.services.federation_update_handoff import HostUpdateHandoff

# Read through getattr so this module still *collects* against a tree that has
# no retention at all. A file that fails to import there proves nothing; the
# consequence has to be an assertion about files left on disk.
MAX_RETAINED_BRANCH_RESULTS = getattr(
    handoff_module, "MAX_RETAINED_BRANCH_RESULTS", 8
)
MAX_BRANCH_RESULT_SWEEP = getattr(handoff_module, "MAX_BRANCH_RESULT_SWEEP", 32)


def _handoff(tmp_path: Path) -> HostUpdateHandoff:
    return HostUpdateHandoff(tmp_path, timeout=0.05, poll_interval=0.01)


def _digest(request_id: str) -> str:
    return hashlib.sha256(request_id.encode("utf-8")).hexdigest()


def _write_branch_result(directory: Path, request_id: str) -> Path:
    path = directory / f"branches-result-{_digest(request_id)}.json"
    path.write_text(
        json.dumps(
            {
                "schema": BRANCHES_RESULT_SCHEMA,
                "request_id": request_id,
                "branches": [],
            }
        ),
        encoding="utf-8",
    )
    return path


def _branch_results(directory: Path) -> list[Path]:
    return sorted(directory.glob("branches-result-*.json"))


def test_a_consumed_branch_result_does_not_stay_on_disk(
    tmp_path: Path, monkeypatch
) -> None:
    """The consequence: one file per Federation render, forever."""

    handoff = _handoff(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)

    # Answer each request the moment it is written, the way a host agent does.
    original_write = handoff._write_request

    def answering_write(value):
        original_write(value)
        _write_branch_result(tmp_path, str(value["request_id"]))

    monkeypatch.setattr(handoff, "_write_request", answering_write)

    for _ in range(5):
        handoff.approved_branches()
        handoff.request_file.unlink(missing_ok=True)

    assert _branch_results(tmp_path) == [], (
        "consumed branch-listing results were left on disk: "
        f"{[path.name for path in _branch_results(tmp_path)]}"
    )


def test_results_left_by_readers_that_timed_out_are_bounded(
    tmp_path: Path, monkeypatch
) -> None:
    """A reader that never saw its answer still must not leak a file forever."""

    handoff = _handoff(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    for index in range(MAX_RETAINED_BRANCH_RESULTS + 20):
        _write_branch_result(tmp_path, f"orphan-{index}")

    # One ordinary call that times out still takes a bounded cleanup pass.
    handoff.approved_branches()
    handoff.request_file.unlink(missing_ok=True)

    assert len(_branch_results(tmp_path)) <= MAX_RETAINED_BRANCH_RESULTS


def test_one_call_sweeps_at_most_its_bounded_batch(tmp_path: Path) -> None:
    """Cleanup must not become work proportional to what accumulated."""

    handoff = _handoff(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    total = MAX_RETAINED_BRANCH_RESULTS + MAX_BRANCH_RESULT_SWEEP + 25
    for index in range(total):
        _write_branch_result(tmp_path, f"orphan-{index}")

    handoff._sweep_orphaned_branch_results()

    remaining = len(_branch_results(tmp_path))
    assert remaining == total - MAX_BRANCH_RESULT_SWEEP
    # And repeated calls converge rather than stalling.
    for _ in range(5):
        handoff._sweep_orphaned_branch_results()
    assert len(_branch_results(tmp_path)) == MAX_RETAINED_BRANCH_RESULTS


def test_update_and_trial_results_are_never_swept(tmp_path: Path) -> None:
    """Their request ids are durable state re-read for recovery, so removing
    them would change what a restart can reconcile."""

    handoff = _handoff(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    update = tmp_path / f"result-{_digest('local-1')}.json"
    trial = tmp_path / f"trial-result-{_digest('trial-1')}.json"
    fixed_update = tmp_path / "result.json"
    fixed_trial = tmp_path / "trial-result.json"
    for path in (update, trial, fixed_update, fixed_trial):
        path.write_text("{}", encoding="utf-8")

    for index in range(MAX_RETAINED_BRANCH_RESULTS + 10):
        _write_branch_result(tmp_path, f"orphan-{index}")

    handoff._sweep_orphaned_branch_results()

    for path in (update, trial, fixed_update, fixed_trial):
        assert path.exists(), f"{path.name} was swept but is recovery state"


def test_a_sweep_failure_never_breaks_the_dropdown(tmp_path: Path, monkeypatch) -> None:
    """Reclaiming space is best effort; a dropdown never fails a page."""

    handoff = _handoff(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)

    def exploding(*_args, **_kwargs):
        raise OSError("read-only filesystem")

    monkeypatch.setattr(Path, "unlink", exploding)
    for index in range(MAX_RETAINED_BRANCH_RESULTS + 5):
        _write_branch_result(tmp_path, f"orphan-{index}")

    handoff._sweep_orphaned_branch_results()
    handoff._retire_branch_result("anything")
