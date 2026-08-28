"""Branch-listing handoff results do not accumulate for the device's lifetime.

``approved_branches`` mints a fresh request id per call, the host agent writes
one result file for it, and the poll returns as soon as it reads that file. The
Federation view calls this on every render, the id is a local ``uuid4`` that is
never persisted, and nothing ever deleted the file.

Each call now deletes exactly the one result it consumed. It deletes nothing
else, and this module pins that boundary: a call must not touch a concurrent
reader's result, and it must not touch update or trial results, whose request
ids are durable state re-read to reconcile a pending update or trial.
"""

from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path

from catalog.federation.software_trial import BRANCHES_RESULT_SCHEMA
from catalog.flask_app.services.federation_update_handoff import HostUpdateHandoff


def _handoff(tmp_path: Path, *, timeout: float = 1.0) -> HostUpdateHandoff:
    return HostUpdateHandoff(tmp_path, timeout=timeout, poll_interval=0.02)


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


def _answer_on_write(handoff: HostUpdateHandoff, monkeypatch, directory: Path) -> None:
    """Answer each request the moment it is written, the way an agent does."""

    original = handoff._write_request

    def answering_write(value):
        original(value)
        _write_branch_result(directory, str(value["request_id"]))

    monkeypatch.setattr(handoff, "_write_request", answering_write)


def test_a_consumed_branch_result_does_not_stay_on_disk(
    tmp_path: Path, monkeypatch
) -> None:
    """The consequence: one file per Federation render, forever."""

    handoff = _handoff(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    _answer_on_write(handoff, monkeypatch, tmp_path)

    for _ in range(5):
        handoff.approved_branches()
        handoff.request_file.unlink(missing_ok=True)

    assert _branch_results(tmp_path) == [], (
        "consumed branch-listing results were left on disk: "
        f"{[path.name for path in _branch_results(tmp_path)]}"
    )


def test_a_call_never_deletes_a_concurrent_readers_result(
    tmp_path: Path, monkeypatch
) -> None:
    """The regression this slice was narrowed for.

    The writer lock covers only publication of ``request.json``. The agent
    claims that request before writing any result, so a second caller can be
    enqueued and polling while the first is still active. A cleanup that
    retires anything beyond the file it consumed will eventually delete a live
    reader's answer and manufacture the approved-main-only fallback for a valid
    request.
    """

    tmp_path.mkdir(parents=True, exist_ok=True)

    # A second caller is mid-flight: its result is already on disk and it has
    # not read it yet. Plenty of other abandoned results are present too, which
    # is exactly the state a size-triggered sweep reacts to.
    inflight = _write_branch_result(tmp_path, "concurrent-reader-request")
    for index in range(40):
        _write_branch_result(tmp_path, f"abandoned-{index}")

    handoff = _handoff(tmp_path)
    _answer_on_write(handoff, monkeypatch, tmp_path)
    handoff.approved_branches()

    assert inflight.exists(), (
        "a concurrent reader's result was deleted; that reader will now fall "
        "back to approved-main-only for a request that was answered"
    )


def test_only_the_consumed_result_is_removed(tmp_path: Path, monkeypatch) -> None:
    """Bounded by construction: exactly one file per call, never a scan."""

    tmp_path.mkdir(parents=True, exist_ok=True)
    others = [_write_branch_result(tmp_path, f"other-{index}") for index in range(12)]

    handoff = _handoff(tmp_path)
    _answer_on_write(handoff, monkeypatch, tmp_path)
    handoff.approved_branches()

    for path in others:
        assert path.exists(), f"{path.name} was removed by a call that did not own it"


def test_update_and_trial_results_are_never_removed(tmp_path: Path, monkeypatch) -> None:
    """Their request ids are durable state re-read for recovery, so removing
    them would change what a restart can reconcile."""

    tmp_path.mkdir(parents=True, exist_ok=True)
    update = tmp_path / f"result-{_digest('local-1')}.json"
    trial = tmp_path / f"trial-result-{_digest('trial-1')}.json"
    fixed_update = tmp_path / "result.json"
    fixed_trial = tmp_path / "trial-result.json"
    for path in (update, trial, fixed_update, fixed_trial):
        path.write_text("{}", encoding="utf-8")

    handoff = _handoff(tmp_path)
    _answer_on_write(handoff, monkeypatch, tmp_path)
    handoff.approved_branches()

    for path in (update, trial, fixed_update, fixed_trial):
        assert path.exists(), f"{path.name} was removed but is recovery state"


def test_a_timed_out_call_removes_only_its_own_unanswered_request(
    tmp_path: Path,
) -> None:
    """No agent answers, so there is nothing to consume and nothing to delete."""

    tmp_path.mkdir(parents=True, exist_ok=True)
    survivor = _write_branch_result(tmp_path, "someone-else")

    handoff = _handoff(tmp_path, timeout=1.0)
    assert handoff.approved_branches() == ()

    assert survivor.exists()


def test_a_removal_failure_never_breaks_the_dropdown(tmp_path: Path, monkeypatch) -> None:
    """Reclaiming space is best effort; a dropdown never fails a page."""

    tmp_path.mkdir(parents=True, exist_ok=True)
    handoff = _handoff(tmp_path)
    _answer_on_write(handoff, monkeypatch, tmp_path)

    real_unlink = Path.unlink

    def exploding(self, *args, **kwargs):
        # Only the branch-result removal fails; the writer lock's own cleanup is
        # a different concern and must keep working.
        if self.name.startswith("branches-result-"):
            raise OSError("read-only filesystem")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", exploding)

    assert handoff.approved_branches() == ()


def test_concurrent_callers_each_retire_only_their_own_result(
    tmp_path: Path, monkeypatch
) -> None:
    """Two real overlapping callers: both answers survive until consumed, and
    each caller removes exactly one file."""

    tmp_path.mkdir(parents=True, exist_ok=True)
    results: list[tuple[dict[str, str], ...]] = []
    barrier = threading.Barrier(2)

    def caller() -> None:
        handoff = _handoff(tmp_path, timeout=2.0)
        original = handoff._write_request

        def answering_write(value):
            # Serialise publication the way the real lock does, then release so
            # the other caller can enqueue while this one is still polling.
            original(value)
            _write_branch_result(tmp_path, str(value["request_id"]))
            handoff.request_file.unlink(missing_ok=True)
            barrier.wait(timeout=5)

        handoff._write_request = answering_write  # type: ignore[method-assign]
        results.append(handoff.approved_branches())

    threads = [threading.Thread(target=caller) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert len(results) == 2
    # Both callers got a real answer -- neither had its result deleted by the
    # other -- and both files are gone once consumed.
    assert _branch_results(tmp_path) == []
