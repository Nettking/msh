from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from catalog.federation import docker_resources


def _completed(stdout: str, *, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        ["docker", "info"],
        returncode,
        stdout=stdout,
        stderr="",
    )


def _use_posix_logic(monkeypatch) -> None:
    monkeypatch.setattr(docker_resources, "_HOST_OS_NAME", "posix")
    monkeypatch.setattr(docker_resources, "_HOST_PLATFORM", "linux")


def test_native_posix_uses_docker_reported_data_root(
    monkeypatch, tmp_path: Path
) -> None:
    docker_root = tmp_path / "docker-root"
    docker_root.mkdir()
    calls: list[str] = []

    def _info(_root: Path, format_string: str, **_kwargs):
        calls.append(format_string)
        return _completed(str(docker_root))

    _use_posix_logic(monkeypatch)
    monkeypatch.setattr(docker_resources, "_run_docker_info", _info)

    resolved = docker_resources.docker_backing_resource_path(tmp_path)

    assert resolved == docker_root.resolve()
    assert calls == ["{{.DockerRootDir}}"]


def test_native_posix_refuses_relative_docker_root(monkeypatch, tmp_path: Path) -> None:
    _use_posix_logic(monkeypatch)
    monkeypatch.setattr(
        docker_resources,
        "_run_docker_info",
        lambda *_args, **_kwargs: _completed("relative/docker"),
    )

    assert docker_resources.docker_backing_resource_path(tmp_path) is None


def test_native_posix_fails_closed_when_docker_info_fails(
    monkeypatch, tmp_path: Path
) -> None:
    _use_posix_logic(monkeypatch)
    monkeypatch.setattr(
        docker_resources,
        "_run_docker_info",
        lambda *_args, **_kwargs: _completed("", returncode=1),
    )

    assert docker_resources.docker_backing_resource_path(tmp_path) is None


@pytest.mark.parametrize("host_os", ["nt", "posix"])
@pytest.mark.parametrize(
    "failure",
    [
        FileNotFoundError("docker unavailable"),
        subprocess.TimeoutExpired(["docker", "info"], 30.0),
    ],
)
def test_missing_or_unresponsive_docker_has_no_proven_backing_resource(
    monkeypatch,
    tmp_path: Path,
    host_os,
    failure,
) -> None:
    monkeypatch.setattr(docker_resources, "_HOST_OS_NAME", host_os)
    monkeypatch.setattr(docker_resources, "_HOST_PLATFORM", "linux")

    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(subprocess, "run", fail)
    assert docker_resources.docker_backing_resource_path(tmp_path, env={}) is None
