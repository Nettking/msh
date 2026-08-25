from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from catalog.federation import host_build


def test_failed_build_still_runs_cache_lifecycle(tmp_path: Path, monkeypatch) -> None:
    commits = iter(["a" * 40])
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: next(commits))
    monkeypatch.setattr(host_build, "preflight_disk", lambda *_args: None)
    prunes: list[dict[str, str]] = []
    monkeypatch.setattr(
        host_build,
        "prune_build_cache",
        lambda _root, env: prunes.append(dict(env)) or True,
    )

    def fail_build(*_args, **_kwargs):
        raise subprocess.CalledProcessError(1, ["docker", "compose", "build"])

    monkeypatch.setattr(host_build.subprocess, "run", fail_build)

    with pytest.raises(RuntimeError, match="core_image_build_failed"):
        host_build.build_core_images_locked(tmp_path, {"COMPOSE_PROJECT_NAME": "fcp"})

    assert len(prunes) == 1
    assert prunes[0]["FCP_BUILD_COMMIT"] == "a" * 40


def test_successful_build_requires_cache_prune_success(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: "b" * 40)
    monkeypatch.setattr(host_build, "preflight_disk", lambda *_args: None)
    monkeypatch.setattr(host_build.subprocess, "run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(host_build, "prune_build_cache", lambda *_args: False)

    with pytest.raises(RuntimeError, match="build_cache_prune_failed"):
        host_build.build_core_images_locked(tmp_path, {})


def test_successful_build_reproves_source_identity(tmp_path: Path, monkeypatch) -> None:
    commits = iter(["c" * 40, "d" * 40])
    monkeypatch.setattr(host_build, "resolve_clean_commit", lambda _root: next(commits))
    monkeypatch.setattr(host_build, "preflight_disk", lambda *_args: None)
    monkeypatch.setattr(host_build.subprocess, "run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(host_build, "prune_build_cache", lambda *_args: True)

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
