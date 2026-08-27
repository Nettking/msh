"""A path that promises the FCP build cache is bounded must prove it.

Stopping the BuildKit writer and discarding its cache are two different claims.
Collapsing them lets a pressure or timeout path report success after proving
only the first, which is exactly the state B01 says must never be reported as a
safe pressure stop.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import host_build
from catalog.federation.host_resources import PressureLevel, ResourceAssessment


def _assessment(level: PressureLevel) -> ResourceAssessment:
    return ResourceAssessment(
        resource_id="device:docker",
        level=level,
        reasons=(),
        effective_free_bytes=12 * 1024**3,
        effective_free_inodes=100_000,
        reserved_bytes=0,
        reserved_inodes=0,
        observed_at=datetime.now(timezone.utc),
    )


def _stopped_but_undeletable(monkeypatch, name: str) -> None:
    """A writer that stops cleanly but whose builder cannot be removed."""

    monkeypatch.setattr(
        host_build,
        "_builder_inspection",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="Driver: docker-container\nStatus: stopped\n",
            stderr="",
        ),
    )
    monkeypatch.setattr(
        host_build,
        "_buildkit_container_running",
        lambda *_args, **_kwargs: False,
        raising=False,
    )

    def docker_run(_root, args, **_kwargs):
        if args[:3] == ["docker", "buildx", "stop"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if args[:3] == ["docker", "buildx", "rm"]:
            return SimpleNamespace(returncode=1, stdout="", stderr="device busy")
        if args[:3] == ["docker", "buildx", "ls"]:
            return SimpleNamespace(returncode=0, stdout=f"{name}\n", stderr="")
        return SimpleNamespace(returncode=1, stdout="", stderr="unexpected")

    monkeypatch.setattr(host_build, "_docker_run", docker_run)


def test_a_failed_cache_discard_is_not_reported_as_a_discarded_cache(
    monkeypatch, tmp_path: Path
) -> None:
    _stopped_but_undeletable(monkeypatch, "fcp-build-test")

    settled = host_build.settle_build_writer(
        tmp_path, "fcp-build-test", {}, discard_cache=True
    )

    assert settled.quiescent is True
    assert settled.cache_discarded is False


def test_preflight_refuses_when_the_cache_it_promised_to_discard_survives(
    monkeypatch, tmp_path: Path
) -> None:
    """Under pressure the preflight claims it discarded cache. If it did not,
    it must not fall through to remeasuring as though space had been freed."""

    _stopped_but_undeletable(monkeypatch, "fcp-build-test")
    monkeypatch.setattr(host_build, "builder_name", lambda _root: "fcp-build-test")
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda _root, _env, **_kwargs: (tmp_path, _assessment(PressureLevel.PRESSURE)),
    )

    with pytest.raises(RuntimeError, match="build_cache_discard_failed"):
        host_build.preflight_disk(tmp_path, {})


def test_an_unrecognised_builder_state_is_refused_rather_than_read_as_safe(
    monkeypatch, tmp_path: Path
) -> None:
    """A blacklist of running states silently accepts any state it has not been
    taught about, and cache is discarded on the strength of that reading."""

    monkeypatch.setattr(
        host_build,
        "_builder_inspection",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="Driver: docker-container\nStatus: pausing\n",
            stderr="",
        ),
    )
    monkeypatch.setattr(
        host_build,
        "_buildkit_container_running",
        lambda *_args, **_kwargs: False,
        raising=False,
    )

    assert host_build._builder_stopped(tmp_path, "fcp-build-test", {}) is False


def test_a_surviving_buildkit_container_defeats_a_stopped_builder_record(
    monkeypatch, tmp_path: Path
) -> None:
    """The builder record describes the record, not the process holding cache
    open. A live driver container means the writer is still live."""

    monkeypatch.setattr(
        host_build,
        "_builder_inspection",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="Driver: docker-container\nStatus: stopped\n",
            stderr="",
        ),
    )

    def docker_run(_root, args, **_kwargs):
        if args[:2] == ["docker", "ps"]:
            return SimpleNamespace(
                returncode=0,
                stdout="buildx_buildkit_fcp-build-test0\nunrelated\n",
                stderr="",
            )
        return SimpleNamespace(returncode=1, stdout="", stderr="unexpected")

    monkeypatch.setattr(host_build, "_docker_run", docker_run)

    assert host_build._builder_stopped(tmp_path, "fcp-build-test", {}) is False


def test_a_removed_builder_record_is_not_absence_while_its_container_runs(
    monkeypatch, tmp_path: Path
) -> None:
    """Buildx can drop its store entry while the driver container survives, so
    enumeration alone cannot establish that the writer is gone."""

    def docker_run(_root, args, **_kwargs):
        if args[:3] == ["docker", "buildx", "ls"]:
            return SimpleNamespace(returncode=0, stdout="other-builder\n", stderr="")
        if args[:2] == ["docker", "ps"]:
            return SimpleNamespace(
                returncode=0,
                stdout="buildx_buildkit_fcp-build-test0\n",
                stderr="",
            )
        return SimpleNamespace(returncode=1, stdout="", stderr="unexpected")

    monkeypatch.setattr(host_build, "_docker_run", docker_run)

    assert host_build._builder_absent(tmp_path, "fcp-build-test", {}) is False


def test_an_unreadable_running_set_is_treated_as_a_possibly_live_writer(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        host_build,
        "_docker_run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1, stdout="", stderr="daemon unavailable"
        ),
    )

    assert host_build._buildkit_container_running(tmp_path, "fcp-build-test", {}) is True


def test_a_bounded_docker_timeout_cannot_leave_a_buildx_plugin_child_running(
    monkeypatch, tmp_path: Path
) -> None:
    """A Docker parent exiting after SIGTERM is not proof its plugin child died."""

    observed: dict[str, object] = {}
    signals: list[int] = []
    state = {"parent_alive": True, "group_alive": True}

    class _Hanging:
        pid = 4321
        returncode = None

        def communicate(self, timeout=None):
            if state["parent_alive"]:
                raise host_build.subprocess.TimeoutExpired("docker", timeout or 0)
            return "", ""

        def wait(self, timeout=None):
            if state["parent_alive"]:
                raise host_build.subprocess.TimeoutExpired("docker", timeout or 0)
            self.returncode = 0
            return 0

        def poll(self):
            if state["parent_alive"]:
                return None
            self.returncode = 0
            return 0

        def kill(self):
            observed["killed"] = "direct-only"
            state["parent_alive"] = False

    def fake_popen(_args, **kwargs):
        observed.update(kwargs)
        return _Hanging()

    def fake_killpg(_pid, signal_number):
        if signal_number == 0:
            if state["group_alive"]:
                return
            raise ProcessLookupError
        signals.append(signal_number)
        if signal_number == host_build.signal.SIGTERM:
            # The Docker CLI exits, but its Buildx plugin child remains in the
            # process group. Parent exit alone must not settle the helper.
            state["parent_alive"] = False
            return
        if signal_number == host_build.signal.SIGKILL:
            state["group_alive"] = False

    monkeypatch.setattr(host_build.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(host_build.os, "killpg", fake_killpg)
    monkeypatch.setattr(host_build, "BUILD_CLIENT_SETTLE_SECONDS", 0.0)

    with pytest.raises(host_build.subprocess.TimeoutExpired):
        host_build._docker_run(
            tmp_path, ["docker", "buildx", "rm", "fcp-build-test"], env={}, timeout=1.0
        )

    # Its own session makes PGID == PID. The parent exits on SIGTERM, but the
    # still-live group forces an escalation that reaches the surviving plugin.
    assert observed["start_new_session"] is True
    assert observed.get("killed") != "direct-only"
    assert signals == [host_build.signal.SIGTERM, host_build.signal.SIGKILL]
