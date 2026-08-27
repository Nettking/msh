"""How the agent-log bound rotates, and why it truncates instead of renaming.

New-API coverage, not consequence evidence: the consequence - an agent log that
grows for the life of the device - is proven against the real runner in
``test_update_agent_log_bound``.
"""

from __future__ import annotations

import os
from pathlib import Path

from catalog.federation import agent_log

LINE = b"0123456789abcdef\n"


def _oversized(directory: Path) -> Path:
    log = directory / agent_log.AGENT_LOG_NAME
    count = (agent_log.MAX_AGENT_LOG_BYTES // len(LINE)) + 4096
    log.write_bytes(LINE * count)
    assert log.stat().st_size > agent_log.MAX_AGENT_LOG_BYTES
    return log


def test_a_writer_holding_an_open_append_handle_keeps_writing_to_the_live_log(
    tmp_path: Path,
) -> None:
    """The whole reason this rotates in place. The launchers hand the agent an
    already-open append descriptor, so renaming the file would leave that
    descriptor on the renamed inode and the agent would fill the "rotated" copy
    forever - a rotation that looks right and bounds nothing."""

    log = _oversized(tmp_path)
    with log.open("ab") as writer:
        assert agent_log.bound_agent_log(tmp_path) is True
        writer.write(b"written after the rotation\n")

    assert log.read_bytes() == b"written after the rotation\n"


def test_rotation_bounds_both_generations(tmp_path: Path) -> None:
    log = _oversized(tmp_path)

    assert agent_log.bound_agent_log(tmp_path) is True

    previous = tmp_path / agent_log.PREVIOUS_AGENT_LOG_NAME
    assert log.stat().st_size == 0
    assert previous.stat().st_size <= agent_log.RETAINED_TAIL_BYTES


def test_the_retained_tail_starts_on_a_line_boundary(tmp_path: Path) -> None:
    """The tail is taken by byte offset, so it lands mid-line. A retained
    generation that opens on half a message is worse than one line shorter."""

    _oversized(tmp_path)

    assert agent_log.bound_agent_log(tmp_path) is True

    retained = (tmp_path / agent_log.PREVIOUS_AGENT_LOG_NAME).read_bytes()
    assert retained
    assert retained.endswith(LINE)
    assert set(retained.split(b"\n")[:-1]) == {LINE.rstrip(b"\n")}


def test_a_second_rotation_replaces_the_previous_generation(tmp_path: Path) -> None:
    """Two generations, not a growing pile of them."""

    _oversized(tmp_path)
    assert agent_log.bound_agent_log(tmp_path) is True
    (tmp_path / agent_log.PREVIOUS_AGENT_LOG_NAME).write_bytes(b"first generation\n")

    _oversized(tmp_path)
    assert agent_log.bound_agent_log(tmp_path) is True

    assert (tmp_path / agent_log.PREVIOUS_AGENT_LOG_NAME).read_bytes() != (
        b"first generation\n"
    )
    assert sorted(entry.name for entry in tmp_path.iterdir()) == [
        agent_log.AGENT_LOG_NAME,
        agent_log.PREVIOUS_AGENT_LOG_NAME,
    ]


def test_a_log_within_its_bound_is_not_touched(tmp_path: Path) -> None:
    log = tmp_path / agent_log.AGENT_LOG_NAME
    log.write_bytes(b"quiet\n")

    assert agent_log.bound_agent_log(tmp_path) is False

    assert log.read_bytes() == b"quiet\n"
    assert not (tmp_path / agent_log.PREVIOUS_AGENT_LOG_NAME).exists()


def test_a_missing_log_is_reported_rather_than_raised(tmp_path: Path) -> None:
    assert agent_log.bound_agent_log(tmp_path) is False


def test_a_failed_rotation_leaves_the_log_alone_and_stages_nothing(
    tmp_path: Path, monkeypatch
) -> None:
    """Truncating a log whose replacement was never written would destroy the
    history it was supposed to preserve, and the poll loop must survive either
    way: an unbounded log is a diagnostics problem, a dead update agent is not."""

    log = _oversized(tmp_path)
    before = log.read_bytes()

    def refuse(*_args, **_kwargs):
        raise OSError("no space left on device")

    monkeypatch.setattr(agent_log.os, "replace", refuse)

    assert agent_log.bound_agent_log(tmp_path) is False

    assert log.read_bytes() == before
    assert not (tmp_path / agent_log.PREVIOUS_AGENT_LOG_NAME).exists()
    assert [entry.name for entry in tmp_path.iterdir()] == [agent_log.AGENT_LOG_NAME]


def test_the_bound_matches_the_container_log_policy(tmp_path: Path) -> None:
    """One operator-facing size for host and service logs alike, so the answer
    to "how large can FCP's logs get" is a single number per generation."""

    compose = (Path(__file__).resolve().parents[3] / "docker-compose.yml").read_text(
        encoding="utf-8"
    )

    assert 'max-size: "10m"' in compose
    assert agent_log.MAX_AGENT_LOG_BYTES == 10 * 1024**2
    assert agent_log.RETAINED_TAIL_BYTES < agent_log.MAX_AGENT_LOG_BYTES
    assert os.linesep  # keeps the import honest on every supported platform
