from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import builder_retirement, host_build
from catalog.federation.host_resources import PressureLevel, ResourceAssessment


def _assessment(level: PressureLevel, free_bytes: int) -> ResourceAssessment:
    return ResourceAssessment(
        resource_id="device:docker",
        level=level,
        reasons=() if level is PressureLevel.NORMAL else (f"bytes_{level.name.lower()}",),
        effective_free_bytes=free_bytes,
        effective_free_inodes=100_000,
        reserved_bytes=0,
        reserved_inodes=0,
        observed_at=datetime.now(timezone.utc),
    )


class _Admission:
    def __init__(self, assessments: list[ResourceAssessment]) -> None:
        self.assessments = list(assessments)
        self.paths: list[Path] = []

    def assessment(self, path: Path) -> ResourceAssessment:
        self.paths.append(path)
        index = min(len(self.paths) - 1, len(self.assessments) - 1)
        return self.assessments[index]


def _install_admission(
    monkeypatch,
    *,
    backing: Path | None,
    assessments: list[ResourceAssessment],
) -> _Admission:
    admission = _Admission(assessments)
    monkeypatch.setattr(
        host_build,
        "docker_backing_resource_path",
        lambda *_args, **_kwargs: backing,
    )
    monkeypatch.setattr(host_build, "ProcessResourceAdmission", lambda: admission)
    return admission


def test_build_admission_measures_docker_resource_and_reproves_old_writer(
    monkeypatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    backing.mkdir()
    admission = _install_admission(
        monkeypatch,
        backing=backing,
        assessments=[_assessment(PressureLevel.WARNING, 15 * 1024**3)],
    )
    stopped: list[tuple[str, bool]] = []
    monkeypatch.setattr(host_build, "builder_name", lambda _root: "fcp-build-test")
    monkeypatch.setattr(
        host_build,
        "stop_build_writer",
        lambda _root, name, _env, *, discard_cache=False: (
            stopped.append((name, discard_cache)) or True
        ),
    )

    host_build.preflight_disk(tmp_path, {"COMPOSE_PROJECT_NAME": "fcp"})

    assert admission.paths == [backing]
    assert stopped == [("fcp-build-test", False)]


def test_build_pressure_discards_fcp_builder_then_remeasures_same_resource(
    monkeypatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    backing.mkdir()
    admission = _install_admission(
        monkeypatch,
        backing=backing,
        assessments=[
            _assessment(PressureLevel.PRESSURE, 12 * 1024**3),
            _assessment(PressureLevel.WARNING, 15 * 1024**3),
        ],
    )
    stopped: list[tuple[str, bool]] = []
    monkeypatch.setattr(host_build, "builder_name", lambda _root: "fcp-build-test")
    monkeypatch.setattr(
        host_build,
        "settle_build_writer",
        lambda _root, name, _env, *, discard_cache=False: (
            stopped.append((name, discard_cache))
            or host_build.BuildWriterSettlement(quiescent=True, cache_discarded=True)
        ),
    )

    host_build.preflight_disk(tmp_path, {})

    assert admission.paths == [backing, backing]
    assert stopped == [("fcp-build-test", True)]


def test_build_refuses_if_docker_resource_stays_under_pressure(
    monkeypatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    backing.mkdir()
    admission = _install_admission(
        monkeypatch,
        backing=backing,
        assessments=[
            _assessment(PressureLevel.PRESSURE, 12 * 1024**3),
            _assessment(PressureLevel.CRITICAL, 10 * 1024**3),
        ],
    )
    monkeypatch.setattr(
        host_build,
        "settle_build_writer",
        lambda *_args, **_kwargs: host_build.BuildWriterSettlement(
            quiescent=True, cache_discarded=True
        ),
    )

    with pytest.raises(RuntimeError, match="insufficient_disk_for_update"):
        host_build.preflight_disk(tmp_path, {})

    assert admission.paths == [backing, backing]


def test_build_refuses_when_docker_backing_resource_cannot_be_proven(
    monkeypatch, tmp_path: Path
) -> None:
    admission = _install_admission(
        monkeypatch,
        backing=None,
        assessments=[_assessment(PressureLevel.NORMAL, 100 * 1024**3)],
    )
    stopped: list[bool] = []
    monkeypatch.setattr(
        host_build,
        "stop_build_writer",
        lambda *_args, **_kwargs: stopped.append(True) or True,
    )

    with pytest.raises(RuntimeError, match="docker_backing_resource_unproven"):
        host_build.preflight_disk(tmp_path, {})

    assert admission.paths == []
    assert stopped == []


def test_controllable_builder_rejects_wrong_driver(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(host_build, "builder_name", lambda _root: "fcp-build-test")
    monkeypatch.setattr(
        host_build,
        "_builder_inspection",
        lambda *_args: SimpleNamespace(returncode=0, stdout="Driver: docker\n"),
    )

    with pytest.raises(RuntimeError, match="controllable_builder_conflict"):
        host_build.ensure_controllable_builder(tmp_path, {})


def test_new_controllable_builder_requires_docker_container_driver(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(host_build, "builder_name", lambda _root: "fcp-build-test")
    inspections = iter(
        [
            SimpleNamespace(returncode=1, stdout=""),
            SimpleNamespace(
                returncode=0,
                stdout="Name: fcp-build-test\nDriver: docker-container\nStatus: stopped\n",
            ),
        ]
    )
    monkeypatch.setattr(host_build, "_builder_inspection", lambda *_args: next(inspections))
    calls: list[list[str]] = []

    def fake_run(_root, args, *, env, timeout=120.0):
        calls.append(list(args))
        return SimpleNamespace(returncode=0, stdout="fcp-build-test\n", stderr="")

    monkeypatch.setattr(host_build, "_docker_run", fake_run)

    assert host_build.ensure_controllable_builder(tmp_path, {}) == "fcp-build-test"
    assert calls == [
        [
            "docker",
            "buildx",
            "create",
            "--name",
            "fcp-build-test",
            "--driver",
            "docker-container",
            "--driver-opt",
            "default-load=true",
            "--driver-opt",
            builder_retirement.builder_root_driver_opt(tmp_path),
        ]
    ]


class _Process:
    def __init__(self, polls: list[int | None]) -> None:
        self._polls = list(polls)
        self.terminated = False
        self.killed = False
        self.waited = False

    def poll(self):
        if len(self._polls) > 1:
            return self._polls.pop(0)
        return self._polls[0]

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True

    def wait(self, timeout=None):
        self.waited = True
        self._polls = [-15]
        return -15


def test_active_build_stops_at_pressure_and_proves_writer_quiescent(
    monkeypatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    backing.mkdir()
    admission = _Admission(
        [
            _assessment(PressureLevel.WARNING, 15 * 1024**3),
            _assessment(PressureLevel.PRESSURE, 12 * 1024**3),
        ]
    )
    monkeypatch.setattr(
        host_build,
        "docker_backing_resource_path",
        lambda *_args, **_kwargs: backing,
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    process = _Process([None, None, None])
    monkeypatch.setattr(host_build.subprocess, "Popen", lambda *_args, **_kwargs: process)
    stopped: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        host_build,
        "settle_build_writer",
        lambda _root, name, _env, *, discard_cache=False: (
            stopped.append((name, discard_cache))
            or host_build.BuildWriterSettlement(quiescent=True, cache_discarded=True)
        ),
    )
    monkeypatch.setattr(host_build.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="build_resource_pressure"):
        host_build.controlled_core_build(
            tmp_path,
            {},
            controller=admission,
            timeout_seconds=30,
            poll_seconds=0.01,
        )

    assert process.terminated is True
    assert stopped == [("fcp-build-test", True)]
    assert admission.paths == [backing, backing]


def test_pressure_stop_failure_is_distinct_from_ordinary_resource_pressure(
    monkeypatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    backing.mkdir()
    admission = _Admission(
        [
            _assessment(PressureLevel.WARNING, 15 * 1024**3),
            _assessment(PressureLevel.PRESSURE, 12 * 1024**3),
        ]
    )
    monkeypatch.setattr(
        host_build,
        "docker_backing_resource_path",
        lambda *_args, **_kwargs: backing,
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    monkeypatch.setattr(
        host_build.subprocess,
        "Popen",
        lambda *_args, **_kwargs: _Process([None, None, None]),
    )
    monkeypatch.setattr(
        host_build,
        "settle_build_writer",
        lambda *_args, **_kwargs: host_build.BuildWriterSettlement(
            quiescent=False, cache_discarded=False
        ),
    )
    monkeypatch.setattr(host_build.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="build_writer_stop_unverified"):
        host_build.controlled_core_build(
            tmp_path,
            {},
            controller=admission,
            timeout_seconds=30,
            poll_seconds=0.01,
        )


def test_successful_build_uses_dedicated_builder_prunes_and_stops(
    monkeypatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    backing.mkdir()
    admission = _Admission([_assessment(PressureLevel.WARNING, 15 * 1024**3)])
    monkeypatch.setattr(
        host_build,
        "docker_backing_resource_path",
        lambda *_args, **_kwargs: backing,
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    captured: list[list[str]] = []

    class _Success:
        def poll(self):
            return 0

    def fake_popen(args, **_kwargs):
        captured.append(list(args))
        return _Success()

    monkeypatch.setattr(host_build.subprocess, "Popen", fake_popen)
    pruned: list[str] = []
    monkeypatch.setattr(
        host_build,
        "prune_build_cache",
        lambda _root, _env, *, name=None: pruned.append(str(name)) or True,
    )
    stopped: list[str] = []
    monkeypatch.setattr(
        host_build,
        "stop_build_writer",
        lambda _root, name, _env, **_kwargs: stopped.append(name) or True,
    )

    host_build.controlled_core_build(tmp_path, {}, controller=admission)

    assert captured == [
        [
            "docker",
            "compose",
            "build",
            "--builder",
            "fcp-build-test",
            "relay",
            "flask",
            "recorder",
        ]
    ]
    assert pruned == ["fcp-build-test"]
    assert stopped == ["fcp-build-test"]


def test_resource_refusal_happens_before_controlled_build(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: "a" * 40)
    monkeypatch.setattr(
        host_build,
        "preflight_disk",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("insufficient_disk_for_update")
        ),
    )
    started: list[bool] = []
    monkeypatch.setattr(
        host_build,
        "controlled_core_build",
        lambda *_args, **_kwargs: started.append(True),
    )

    with pytest.raises(RuntimeError, match="insufficient_disk_for_update"):
        host_build.build_core_images_locked(tmp_path, {})

    assert started == []


def test_build_acceptance_requires_exact_image_identity(
    monkeypatch, tmp_path: Path
) -> None:
    commits = iter(["a" * 40, "a" * 40])
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: next(commits))
    monkeypatch.setattr(host_build, "preflight_disk", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(host_build, "controlled_core_build", lambda *_args, **_kwargs: None)
    verified: list[str] = []
    monkeypatch.setattr(
        host_build,
        "_verify_core_image_commits",
        lambda _root, _env, commit: verified.append(commit),
    )

    result = host_build.build_core_images_locked(tmp_path, {})

    assert result == "a" * 40
    assert verified == ["a" * 40]
