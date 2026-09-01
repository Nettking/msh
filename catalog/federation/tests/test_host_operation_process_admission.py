from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation import host_build
from catalog.federation import model_resource_pull as model_pull
from catalog.federation.host_resources import PressureLevel, ResourceAssessment


def _assessment(level: PressureLevel) -> ResourceAssessment:
    return ResourceAssessment(
        resource_id="device:shared",
        level=level,
        reasons=() if level is PressureLevel.NORMAL else (f"bytes_{level.name.lower()}",),
        effective_free_bytes=12 * 1024**3,
        effective_free_inodes=100_000,
        reserved_bytes=0,
        reserved_inodes=0,
        observed_at=datetime.now(timezone.utc),
    )


class _Admission:
    def __init__(self, level: PressureLevel) -> None:
        self.level = level
        self.paths: list[Path] = []

    def assessment(self, path: Path) -> ResourceAssessment:
        self.paths.append(path)
        return _assessment(self.level)


def test_docker_resource_assessment_uses_process_wide_controller_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    admission = _Admission(PressureLevel.WARNING)
    monkeypatch.setattr(
        host_build,
        "docker_backing_resource_path",
        lambda *_args, **_kwargs: backing,
    )
    monkeypatch.setattr(host_build, "PROCESS_RESOURCE_ADMISSION", admission)

    measured, result = host_build.docker_resource_assessment(tmp_path, {})

    assert measured == backing
    assert result.level is PressureLevel.WARNING
    assert admission.paths == [backing]


def test_preflight_uses_process_wide_controller_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    admission = _Admission(PressureLevel.NORMAL)
    observed_controllers: list[object] = []

    def _resource_assessment(_root, _env, *, controller=None):
        observed_controllers.append(controller)
        return backing, _assessment(PressureLevel.NORMAL)

    monkeypatch.setattr(host_build, "PROCESS_RESOURCE_ADMISSION", admission)
    monkeypatch.setattr(host_build, "docker_resource_assessment", _resource_assessment)
    monkeypatch.setattr(host_build, "builder_name", lambda _root: "fcp-build-test")
    monkeypatch.setattr(host_build, "stop_build_writer", lambda *_args, **_kwargs: True)

    host_build.preflight_disk(tmp_path, {})

    assert observed_controllers == [admission]


def test_controlled_build_uses_process_wide_controller_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    admission = _Admission(PressureLevel.PRESSURE)
    monkeypatch.setattr(
        host_build,
        "docker_backing_resource_path",
        lambda *_args, **_kwargs: backing,
    )
    monkeypatch.setattr(host_build, "PROCESS_RESOURCE_ADMISSION", admission)

    with pytest.raises(RuntimeError, match="insufficient_disk_for_update"):
        host_build.controlled_core_build(tmp_path, {})

    assert admission.paths == [backing]


def test_model_pull_uses_process_wide_controller_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    admission = _Admission(PressureLevel.PRESSURE)
    monkeypatch.setattr(model_pull, "_model_ready", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        model_pull,
        "_docker_backing_resource_path",
        lambda *_args, **_kwargs: backing,
    )
    monkeypatch.setattr(model_pull, "PROCESS_RESOURCE_ADMISSION", admission)

    def _popen(*_args, **_kwargs):
        raise AssertionError("model pull must be refused before subprocess start")

    monkeypatch.setattr(model_pull.subprocess, "Popen", _popen)

    result = model_pull.admitted_model_pull(tmp_path, model="llama3.2:3b")

    assert result.ok is False
    assert result.code == "resource_pressure"
    assert admission.paths == [backing]
