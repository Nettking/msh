from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import host_build
from catalog.federation.host_resources import PressureLevel


class _CompletedProcess:
    def __init__(self, returncode: int) -> None:
        self.returncode = returncode

    def poll(self):
        return self.returncode


def _warning_assessment(tmp_path: Path):
    return tmp_path, SimpleNamespace(level=PressureLevel.WARNING)


def test_failed_build_still_runs_cache_lifecycle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda *_args, **_kwargs: _warning_assessment(tmp_path),
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    monkeypatch.setattr(
        host_build.subprocess,
        "Popen",
        lambda *_args, **_kwargs: _CompletedProcess(1),
    )
    prunes: list[tuple[str | None, dict[str, str]]] = []
    monkeypatch.setattr(
        host_build,
        "prune_build_cache",
        lambda _root, env, *, name=None: prunes.append((name, dict(env))) or True,
    )
    stopped: list[str] = []
    monkeypatch.setattr(
        host_build,
        "stop_build_writer",
        lambda _root, name, _env, **_kwargs: stopped.append(name) or True,
    )

    with pytest.raises(RuntimeError, match="core_image_build_failed"):
        host_build.controlled_core_build(
            tmp_path,
            {"COMPOSE_PROJECT_NAME": "fcp", "FCP_BUILD_COMMIT": "a" * 40},
        )

    assert prunes == [
        (
            "fcp-build-test",
            {"COMPOSE_PROJECT_NAME": "fcp", "FCP_BUILD_COMMIT": "a" * 40},
        )
    ]
    assert stopped == ["fcp-build-test"]


def test_failed_build_and_cache_prune_failure_remain_distinct(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda *_args, **_kwargs: _warning_assessment(tmp_path),
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    monkeypatch.setattr(
        host_build.subprocess,
        "Popen",
        lambda *_args, **_kwargs: _CompletedProcess(1),
    )
    monkeypatch.setattr(host_build, "prune_build_cache", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(host_build, "stop_build_writer", lambda *_args, **_kwargs: True)

    with pytest.raises(RuntimeError, match="build_failed_and_cache_prune_failed"):
        host_build.controlled_core_build(tmp_path, {})


def test_successful_build_requires_cache_prune_success(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        host_build,
        "docker_resource_assessment",
        lambda *_args, **_kwargs: _warning_assessment(tmp_path),
    )
    monkeypatch.setattr(
        host_build,
        "ensure_controllable_builder",
        lambda *_args, **_kwargs: "fcp-build-test",
    )
    monkeypatch.setattr(
        host_build.subprocess,
        "Popen",
        lambda *_args, **_kwargs: _CompletedProcess(0),
    )
    monkeypatch.setattr(host_build, "prune_build_cache", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(host_build, "stop_build_writer", lambda *_args, **_kwargs: True)

    with pytest.raises(RuntimeError, match="build_cache_prune_failed"):
        host_build.controlled_core_build(tmp_path, {})


def test_successful_build_reproves_source_identity(tmp_path: Path, monkeypatch) -> None:
    commits = iter(["c" * 40, "d" * 40])
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: next(commits))
    monkeypatch.setattr(host_build, "preflight_disk", lambda *_args: None)
    monkeypatch.setattr(host_build, "controlled_core_build", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        host_build,
        "_verify_core_image_commits",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(RuntimeError, match="build_context_changed"):
        host_build.build_core_images_locked(tmp_path, {})


def test_expected_commit_is_checked_before_build(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: "e" * 40)
    called: list[bool] = []
    monkeypatch.setattr(
        host_build,
        "preflight_disk",
        lambda *_args: called.append(True),
    )

    with pytest.raises(RuntimeError, match="source_verification_failed"):
        host_build.build_core_images_locked(
            tmp_path,
            {},
            expected_commit="f" * 40,
        )

    assert called == []


def test_build_policy_matches_supported_update_figures() -> None:
    assert host_build.UPDATE_REQUIRED_FREE_BYTES == 10 * 1024**3
    assert host_build.BUILD_CACHE_KEEP_BYTES == 8 * 1024**3
