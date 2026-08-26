from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation import host_build
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


def test_build_admission_measures_docker_resource_not_checkout(
    monkeypatch, tmp_path: Path
) -> None:
    backing = tmp_path / "docker-backing"
    backing.mkdir()
    admission = _install_admission(
        monkeypatch,
        backing=backing,
        assessments=[_assessment(PressureLevel.WARNING, 15 * 1024**3)],
    )
    prunes: list[bool] = []
    monkeypatch.setattr(
        host_build,
        "prune_build_cache",
        lambda *_args, **_kwargs: prunes.append(True) or True,
    )

    host_build.preflight_disk(tmp_path, {"COMPOSE_PROJECT_NAME": "fcp"})

    assert admission.paths == [backing]
    assert prunes == []


def test_build_pressure_prunes_then_remeasures_same_docker_resource(
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
    prunes: list[bool] = []
    monkeypatch.setattr(
        host_build,
        "prune_build_cache",
        lambda *_args, **_kwargs: prunes.append(True) or True,
    )

    host_build.preflight_disk(tmp_path, {})

    assert admission.paths == [backing, backing]
    assert prunes == [True]


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
    monkeypatch.setattr(host_build, "prune_build_cache", lambda *_args, **_kwargs: True)

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
    prunes: list[bool] = []
    monkeypatch.setattr(
        host_build,
        "prune_build_cache",
        lambda *_args, **_kwargs: prunes.append(True) or True,
    )

    with pytest.raises(RuntimeError, match="docker_backing_resource_unproven"):
        host_build.preflight_disk(tmp_path, {})

    assert admission.paths == []
    assert prunes == []


def test_resource_refusal_happens_before_compose_build(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: "a" * 40)
    monkeypatch.setattr(
        host_build,
        "preflight_disk",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("insufficient_disk_for_update")
        ),
    )
    started: list[list[str]] = []
    monkeypatch.setattr(
        host_build.subprocess,
        "run",
        lambda args, **_kwargs: started.append(list(args)),
    )

    with pytest.raises(RuntimeError, match="insufficient_disk_for_update"):
        host_build.build_core_images_locked(tmp_path, {})

    assert started == []
