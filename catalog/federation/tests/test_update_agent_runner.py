from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]
RUNNER_PATH = ROOT / "scripts" / "posix" / "fcp_update_agent_runner.py"


@pytest.fixture
def runner():
    spec = importlib.util.spec_from_file_location("_fcp_update_runner_test", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _engine(events: list[str], *, fail_before_prune: bool = False):
    engine = SimpleNamespace()
    engine.REQUEST_SCHEMA = "fcp.host-update-request.v1"
    engine.OID_RE = re.compile(r"^[0-9a-f]{40}$")

    def preflight(_root, _env):
        events.append("legacy-preflight")

    def prune(_root, _env):
        events.append("prune")
        return True

    def process_once(root, _request, _result):
        engine.preflight_disk(root, {})
        events.append("build")
        if fail_before_prune:
            raise RuntimeError("build_failed")
        engine.prune_build_cache(root, {})
        events.append("activation")
        return True

    engine.preflight_disk = preflight
    engine.prune_build_cache = prune
    engine.process_once = process_once
    return engine


def _apply_request(path: Path, target: str) -> None:
    path.write_text(
        json.dumps(
            {
                "schema": "fcp.host-update-request.v1",
                "action": "apply",
                "target_commit": target,
            }
        ),
        encoding="utf-8",
    )


def test_apply_releases_host_lock_only_after_prune_and_source_reproof(
    runner, tmp_path: Path, monkeypatch
) -> None:
    events: list[str] = []
    target = "a" * 40
    request = tmp_path / "request.json"
    result = tmp_path / "result.json"
    _apply_request(request, target)
    engine = _engine(events)

    class Lock:
        def __enter__(self):
            events.append("lock-enter")
            return self

        def __exit__(self, *_args):
            events.append("lock-exit")

    monkeypatch.setattr(runner.host_build, "host_mutation_lock", lambda _root: Lock())
    monkeypatch.setattr(
        runner.host_build,
        "preflight_disk",
        lambda _root, _env: events.append("preflight"),
    )

    def reprove(_root):
        events.append("reproof")
        return target

    monkeypatch.setattr(runner.host_build, "resolve_clean_commit", reprove)

    assert runner.process_once(engine, tmp_path, request, result) is True
    assert events == [
        "lock-enter",
        "preflight",
        "build",
        "prune",
        "reproof",
        "lock-exit",
        "activation",
    ]


def test_failed_build_still_attempts_cache_cleanup_before_unlock(
    runner, tmp_path: Path, monkeypatch
) -> None:
    events: list[str] = []
    target = "b" * 40
    request = tmp_path / "request.json"
    result = tmp_path / "result.json"
    _apply_request(request, target)
    engine = _engine(events, fail_before_prune=True)

    class Lock:
        def __enter__(self):
            events.append("lock-enter")
            return self

        def __exit__(self, *_args):
            events.append("lock-exit")

    monkeypatch.setattr(runner.host_build, "host_mutation_lock", lambda _root: Lock())
    monkeypatch.setattr(
        runner.host_build,
        "preflight_disk",
        lambda _root, _env: events.append("preflight"),
    )

    with pytest.raises(RuntimeError, match="build_failed"):
        runner.process_once(engine, tmp_path, request, result)

    assert events == ["lock-enter", "preflight", "build", "prune", "lock-exit"]


def test_busy_host_lock_leaves_apply_request_for_retry(
    runner, tmp_path: Path, monkeypatch
) -> None:
    target = "c" * 40
    request = tmp_path / "request.json"
    result = tmp_path / "result.json"
    _apply_request(request, target)
    called: list[bool] = []
    engine = _engine([])
    engine.process_once = lambda *_args: called.append(True) or True

    class Busy:
        def __enter__(self):
            raise RuntimeError("host_mutation_busy")

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(runner.host_build, "host_mutation_lock", lambda _root: Busy())

    assert runner.process_once(engine, tmp_path, request, result) is False
    assert request.exists()
    assert called == []


def test_non_apply_request_is_not_serialized(runner, tmp_path: Path, monkeypatch) -> None:
    request = tmp_path / "request.json"
    result = tmp_path / "result.json"
    request.write_text(
        json.dumps({"schema": "fcp.host-update-request.v1", "action": "check"}),
        encoding="utf-8",
    )
    called: list[bool] = []
    engine = _engine([])
    engine.process_once = lambda *_args: called.append(True) or True

    monkeypatch.setattr(
        runner.host_build,
        "host_mutation_lock",
        lambda _root: (_ for _ in ()).throw(AssertionError("lock must not be acquired")),
    )

    assert runner.process_once(engine, tmp_path, request, result) is True
    assert called == [True]
