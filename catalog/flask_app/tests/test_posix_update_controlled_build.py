from __future__ import annotations

import importlib.util
import subprocess
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.host_resources import PressureLevel, ResourceAssessment


def _normal_assessment() -> ResourceAssessment:
    return ResourceAssessment(
        resource_id="device:docker",
        level=PressureLevel.NORMAL,
        reasons=(),
        effective_free_bytes=100 * 1024**3,
        effective_free_inodes=1_000_000,
        reserved_bytes=0,
        reserved_inodes=0,
        observed_at=datetime.now(timezone.utc),
    )


ROOT = Path(__file__).resolve().parents[3]

_POSIX_ONLY = pytest.mark.skipif(
    importlib.util.find_spec("fcntl") is None,
    reason="the POSIX update runner imports fcntl",
)


def _load_runner():
    spec = importlib.util.spec_from_file_location(
        "_fcp_update_agent_runner_under_test",
        ROOT / "scripts/posix/fcp_update_agent_runner.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@_POSIX_ONLY
def test_controlled_proxy_intercepts_only_core_build(monkeypatch, tmp_path: Path) -> None:
    runner = _load_runner()
    controlled: list[tuple[Path, dict[str, str]]] = []
    monkeypatch.setattr(
        runner.host_build,
        "controlled_core_build",
        lambda root, env: controlled.append((root, dict(env))),
    )
    delegated: list[list[str]] = []

    def original(argv, **_kwargs):
        delegated.append(list(argv))
        return subprocess.CompletedProcess(argv, 0)

    proxy = runner._ControlledSubprocess(tmp_path, original)

    build = proxy.run(
        runner.CORE_BUILD_COMMAND,
        env={"FCP_BUILD_COMMIT": "a" * 40},
        check=True,
    )
    other = proxy.run(["docker", "compose", "ps"], check=True)

    assert build.returncode == 0
    assert controlled == [(tmp_path, {"FCP_BUILD_COMMIT": "a" * 40})]
    assert other.returncode == 0
    assert delegated == [["docker", "compose", "ps"]]


@_POSIX_ONLY
def test_serialized_apply_releases_lock_only_after_controlled_build(
    monkeypatch, tmp_path: Path
) -> None:
    runner = _load_runner()
    target = "a" * 40
    request_id = "request-1"
    events: list[str] = []

    @contextmanager
    def mutation_lock(_root):
        events.append("lock-enter")
        try:
            yield
        finally:
            events.append("lock-exit")

    monkeypatch.setattr(runner.host_build, "host_mutation_lock", mutation_lock)
    monkeypatch.setattr(
        runner.host_build,
        "controlled_core_build",
        lambda _root, _env: events.append("controlled-build"),
    )
    monkeypatch.setattr(
        runner.host_build,
        "resolve_clean_commit",
        lambda _root: target,
    )
    monkeypatch.setattr(
        runner.host_build,
        "stop_build_writer",
        lambda *_args, **_kwargs: events.append("preflight-quiescence") or True,
    )
    monkeypatch.setattr(
        runner.host_build,
        "docker_resource_assessment",
        lambda _root, _env, **_kwargs: (
            tmp_path,
            _normal_assessment(),
        ),
    )

    original_subprocess = SimpleNamespace(
        run=lambda argv, **_kwargs: (
            events.append(f"delegated:{argv[0]}")
            or subprocess.CompletedProcess(argv, 0)
        ),
        SubprocessError=subprocess.SubprocessError,
        CompletedProcess=subprocess.CompletedProcess,
    )
    engine = SimpleNamespace()
    engine.preflight_disk = lambda *_args, **_kwargs: events.append("legacy-preflight")
    engine.prune_build_cache = (
        lambda *_args, **_kwargs: events.append("legacy-prune") or True
    )
    engine.subprocess = original_subprocess

    def process_once(root, _request_file, _result_file):
        engine.preflight_disk(root, {"FCP_BUILD_COMMIT": target})
        engine.subprocess.run(
            runner.CORE_BUILD_COMMAND,
            cwd=root,
            env={"FCP_BUILD_COMMIT": target},
            shell=False,
            check=True,
            timeout=900,
        )
        engine.prune_build_cache(root, {"FCP_BUILD_COMMIT": target})
        events.append("activation-after-build")
        return True

    engine.process_once = process_once

    assert (
        runner._serialized_apply(
            engine,
            tmp_path,
            tmp_path / "request.json",
            tmp_path / "result.json",
            request_id,
            target,
        )
        is True
    )

    assert events == [
        "lock-enter",
        "preflight-quiescence",
        "controlled-build",
        "lock-exit",
        "activation-after-build",
    ]
    assert engine.subprocess is original_subprocess
