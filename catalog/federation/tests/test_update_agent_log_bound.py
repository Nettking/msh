"""The update agent's own log is a lifetime history nothing retires.

Both POSIX launchers append the agent's output to
``<data>/federation/update-agent/agent.log`` and nothing truncates or rotates
it. The agent then polls for the life of the device, and a durable request it
cannot complete is deliberately left in place and retried, so a host in that
state writes to the log every poll second.

The evidence is taken by running the real runner's ``main`` for one poll against
an oversized log, so it describes the consequence and not the shape of any new
helper. This module deliberately imports nothing that exists only on this
branch: it collects on main, where it fails because the log is left exactly as
it was found.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]

# Spelled out rather than imported, so this module still collects on a tree with
# no agent-log bound at all.
BOUND_BYTES = 10 * 1024**2
MARKER = b"the most recent line before the bound was reached\n"

_POSIX_ONLY = pytest.mark.skipif(
    importlib.util.find_spec("fcntl") is None,
    reason="the POSIX update runner imports fcntl, which Windows does not provide",
)


def _load_runner():
    spec = importlib.util.spec_from_file_location(
        "_fcp_update_agent_runner_under_test",
        ROOT / "scripts/posix/fcp_update_agent_runner.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _oversized_log(data_directory: Path) -> Path:
    directory = data_directory / "federation" / "update-agent"
    directory.mkdir(parents=True, exist_ok=True)
    log = directory / "agent.log"
    log.write_bytes(b"an update agent warning, repeated\n" * 400_000 + MARKER)
    assert log.stat().st_size > BOUND_BYTES
    return log


def _run_one_poll(runner, monkeypatch, data_directory: Path) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fcp_update_agent_runner.py",
            "--repo-root",
            str(ROOT),
            "--data-directory",
            str(data_directory),
            "--once",
        ],
    )
    assert runner.main() == 0


@_POSIX_ONLY
def test_the_update_agent_log_is_bounded_by_the_agent_itself(
    tmp_path: Path, monkeypatch
) -> None:
    """One poll of the real runner has to leave the log within its bound. The
    agent is the only process that can do it: it holds the singleton lock, and
    the launchers hand it an already-open append descriptor and walk away."""

    data_directory = tmp_path / "data"
    log = _oversized_log(data_directory)

    _run_one_poll(_load_runner(), monkeypatch, data_directory)

    assert log.stat().st_size <= BOUND_BYTES


@_POSIX_ONLY
def test_bounding_the_log_keeps_the_most_recent_history(
    tmp_path: Path, monkeypatch
) -> None:
    """A bound that discards everything is a bound, and useless. What the agent
    was saying just before it rotated is the part worth keeping."""

    data_directory = tmp_path / "data"
    _oversized_log(data_directory)

    _run_one_poll(_load_runner(), monkeypatch, data_directory)

    previous = data_directory / "federation" / "update-agent" / "agent.log.1"
    assert previous.is_file()
    assert previous.read_bytes().endswith(MARKER)
    assert previous.stat().st_size <= BOUND_BYTES


@_POSIX_ONLY
def test_a_log_within_its_bound_is_left_exactly_as_it_was(
    tmp_path: Path, monkeypatch
) -> None:
    """Rotation is driven by the bound being exceeded and by nothing else - not
    by age, not by every start."""

    data_directory = tmp_path / "data"
    directory = data_directory / "federation" / "update-agent"
    directory.mkdir(parents=True)
    log = directory / "agent.log"
    log.write_bytes(b"a quiet host\n")

    _run_one_poll(_load_runner(), monkeypatch, data_directory)

    assert log.read_bytes() == b"a quiet host\n"
    assert not (directory / "agent.log.1").exists()


@_POSIX_ONLY
def test_a_missing_log_is_not_an_error_for_the_poll_loop(
    tmp_path: Path, monkeypatch
) -> None:
    """The Windows launcher sends the agent's output to the null device, and a
    POSIX operator can delete the file at any moment. Neither may stop the
    agent from polling."""

    data_directory = tmp_path / "data"
    (data_directory / "federation" / "update-agent").mkdir(parents=True)

    _run_one_poll(_load_runner(), monkeypatch, data_directory)

    assert not (data_directory / "federation" / "update-agent" / "agent.log").exists()
