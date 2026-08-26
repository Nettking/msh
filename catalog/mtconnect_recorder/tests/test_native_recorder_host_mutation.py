from __future__ import annotations

import hashlib
from contextlib import contextmanager
from pathlib import Path

import pytest

from catalog.federation.host_mutation import (
    HostMutationLockError,
    normalize_windows_directory,
    windows_host_mutation_mutex_name,
)
from scripts import fcp_native_recorder_update_agent as cli

ROOT = Path(__file__).resolve().parents[3]


class _Journal:
    def __init__(self, active: object | None) -> None:
        self._active = active

    def active(self) -> object | None:
        return self._active


class _Trial:
    def __init__(self, active: object | None) -> None:
        self.journal = _Journal(active)


class _Agent:
    def __init__(
        self,
        *,
        update_active: object | None,
        trial_active: object | None,
        events: list[str],
    ) -> None:
        self.journal = _Journal(update_active)
        self.trial = _Trial(trial_active)
        self.events = events

    def finalize_after_exit(self) -> dict[str, object]:
        self.events.append("finalize")
        return {
            "relaunch": True,
            "mode": "update",
            "code": "source_updated",
            "target_commit": "a" * 40,
            "launch_root": None,
            "data_directory": None,
            "build_commit": None,
        }


def test_windows_mutex_identity_matches_the_supported_host_build_contract() -> None:
    root = r"C:\FCP\msh\\"
    normalized = r"C:\FCP\msh"
    digest = hashlib.sha256(normalized.lower().encode("utf-8")).hexdigest()

    assert normalize_windows_directory(root) == normalized
    assert windows_host_mutation_mutex_name(root) == (
        "Global\\FCPHostMutation-" + digest[:24].upper()
    )

    host_build = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(
        encoding="utf-8"
    )
    assert "$mutexName = 'Global\\FCPHostMutation-' + (Get-PathHash $RepoRoot)" in (
        host_build
    )
    assert ".ToLowerInvariant()" in host_build
    assert ".Substring(0, 24)" in host_build


def test_an_update_finalize_holds_the_shared_boundary_until_finalize_returns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    agent = _Agent(update_active={"stage": "stop_requested"}, trial_active=None, events=events)

    @contextmanager
    def lock(root: Path):
        assert root == tmp_path
        events.append("lock_enter")
        try:
            yield
        finally:
            events.append("lock_exit")

    monkeypatch.setattr(cli, "host_mutation_lock", lock)

    outcome = cli.finalize_with_host_mutation(agent, repo_root=tmp_path)

    assert outcome["code"] == "source_updated"
    assert events == ["lock_enter", "finalize", "lock_exit"]


def test_a_trial_finalize_never_waits_on_the_production_checkout_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    agent = _Agent(
        update_active=None,
        trial_active={"stage": "stop_requested"},
        events=events,
    )

    @contextmanager
    def forbidden_lock(_root: Path):
        raise AssertionError("branch trials must not acquire the production mutation lock")
        yield  # pragma: no cover

    monkeypatch.setattr(cli, "host_mutation_lock", forbidden_lock)

    outcome = cli.finalize_with_host_mutation(agent, repo_root=tmp_path)

    assert outcome["code"] == "source_updated"
    assert events == ["finalize"]


def test_lock_contention_relaunches_the_unchanged_recorder_without_mutating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    agent = _Agent(update_active={"stage": "stop_requested"}, trial_active=None, events=events)

    @contextmanager
    def busy_lock(_root: Path):
        raise HostMutationLockError("host_mutation_busy")
        yield  # pragma: no cover

    monkeypatch.setattr(cli, "host_mutation_lock", busy_lock)

    outcome = cli.finalize_with_host_mutation(agent, repo_root=tmp_path)

    assert events == []
    assert outcome == {
        "relaunch": True,
        "mode": "update",
        "code": "host_mutation_busy",
        "target_commit": None,
        "launch_root": None,
        "data_directory": None,
        "build_commit": None,
    }


def test_a_lock_setup_failure_is_also_recoverable_without_entering_finalize(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    agent = _Agent(update_active={"stage": "stop_requested"}, trial_active=None, events=events)

    @contextmanager
    def unavailable_lock(_root: Path):
        raise HostMutationLockError("host_mutation_lock_unavailable")
        yield  # pragma: no cover

    monkeypatch.setattr(cli, "host_mutation_lock", unavailable_lock)

    outcome = cli.finalize_with_host_mutation(agent, repo_root=tmp_path)

    assert events == []
    assert outcome["relaunch"] is True
    assert outcome["code"] == "host_mutation_lock_unavailable"
    assert outcome["target_commit"] is None
