from __future__ import annotations

import subprocess
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
        effective_free_bytes=20 * 1024**3,
        effective_free_inodes=100_000,
        reserved_bytes=0,
        reserved_inodes=0,
        observed_at=datetime.now(timezone.utc),
    )


def test_inspect_failure_is_not_proof_that_builder_is_stopped(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        host_build,
        "_builder_inspection",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout="", stderr="daemon unavailable"),
    )
    monkeypatch.setattr(
        host_build,
        "_docker_run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout="", stderr="daemon unavailable"),
    )

    assert host_build._builder_stopped(tmp_path, "fcp-build-test", {}) is False


def test_failed_inspect_can_only_mean_absent_after_successful_enumeration(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        host_build,
        "_builder_inspection",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout="", stderr="not found"),
    )
    monkeypatch.setattr(
        host_build,
        "_docker_run",
        lambda _root, args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="other-builder\n",
            stderr="",
        )
        if args[:3] == ["docker", "buildx", "ls"]
        # Absence is only absence once the driver container is also gone, so the
        # clean host this case describes has to answer the running-set probe.
        else SimpleNamespace(returncode=0, stdout="", stderr="")
        if args[:2] == ["docker", "ps"]
        else SimpleNamespace(returncode=1, stdout="", stderr="unexpected"),
    )

    assert host_build._builder_stopped(tmp_path, "fcp-build-test", {}) is True


def test_remove_builder_fails_closed_when_post_remove_absence_cannot_be_proven(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        host_build,
        "_builder_inspection",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="Driver: docker-container\nStatus: running\n",
            stderr="",
        ),
    )

    def docker_run(_root, args, **_kwargs):
        if args[:3] == ["docker", "buildx", "rm"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if args[:3] == ["docker", "buildx", "ls"]:
            return SimpleNamespace(returncode=1, stdout="", stderr="daemon unavailable")
        raise AssertionError(args)

    monkeypatch.setattr(host_build, "_docker_run", docker_run)

    assert host_build._remove_builder(tmp_path, "fcp-build-test", {}) is False


def test_pressure_preflight_refuses_when_old_writer_cannot_be_quiesced(
    monkeypatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    backing.mkdir()
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda *_args, **_kwargs: (backing, _assessment(PressureLevel.PRESSURE)),
    )
    monkeypatch.setattr(host_build, "builder_name", lambda _root: "fcp-build-test")
    monkeypatch.setattr(host_build, "stop_build_writer", lambda *_args, **_kwargs: False)

    with pytest.raises(RuntimeError, match="build_writer_stop_unverified"):
        host_build.preflight_disk(tmp_path, {})


def test_existing_builder_must_be_reproved_quiescent_before_reuse(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(host_build, "builder_name", lambda _root: "fcp-build-test")
    monkeypatch.setattr(
        host_build,
        "_builder_inspection",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="Driver: docker-container\nStatus: running\n",
            stderr="",
        ),
    )
    monkeypatch.setattr(host_build, "_builder_stopped", lambda *_args, **_kwargs: False)
    attempts: list[str] = []
    monkeypatch.setattr(
        host_build,
        "stop_build_writer",
        lambda _root, name, _env, **_kwargs: attempts.append(name) or False,
    )

    with pytest.raises(RuntimeError, match="build_writer_stop_unverified"):
        host_build.ensure_controllable_builder(tmp_path, {})

    assert attempts == ["fcp-build-test"]


class _Admission:
    def assessment(self, _path: Path) -> ResourceAssessment:
        return _assessment(PressureLevel.PRESSURE)


class _StubbornProcess:
    pid = 4321

    def poll(self):
        return None

    def wait(self, timeout=None):
        raise subprocess.TimeoutExpired("docker compose build", timeout)

    def terminate(self):
        pass

    def kill(self):
        pass


def test_pressure_never_claims_safe_stop_when_client_process_group_survives(
    monkeypatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    backing.mkdir()
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda *_args, **_kwargs: (backing, _assessment(PressureLevel.WARNING)),
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    popen_kwargs: dict[str, object] = {}

    def fake_popen(_args, **kwargs):
        popen_kwargs.update(kwargs)
        return _StubbornProcess()

    monkeypatch.setattr(host_build.subprocess, "Popen", fake_popen)
    events: list[object] = []
    monkeypatch.setattr(
        host_build.os,
        "killpg",
        lambda _pid, sig: events.append(sig),
    )
    monkeypatch.setattr(
        host_build,
        "settle_build_writer",
        lambda *_args, **_kwargs: events.append("writer-stop")
        or host_build.BuildWriterSettlement(quiescent=True, cache_discarded=True),
    )

    with pytest.raises(RuntimeError, match="build_writer_stop_unverified"):
        host_build.controlled_core_build(
            tmp_path,
            {},
            controller=_Admission(),
            timeout_seconds=30,
            poll_seconds=0.01,
        )

    assert popen_kwargs["start_new_session"] is True
    assert events == [host_build.signal.SIGTERM, host_build.signal.SIGKILL, "writer-stop"]
