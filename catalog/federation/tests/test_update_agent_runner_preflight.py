from __future__ import annotations

import importlib.util
import json
import re
import subprocess
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
RUNNER_PATH = ROOT / "scripts" / "posix" / "fcp_update_agent_runner.py"


def _runner():
    spec = importlib.util.spec_from_file_location(
        "_fcp_update_runner_preflight_test", RUNNER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_low_disk_preflight_prune_keeps_lock_until_post_build_prune(
    tmp_path: Path, monkeypatch
) -> None:
    runner = _runner()
    target = "a" * 40
    request = tmp_path / "request.json"
    result = tmp_path / "result.json"
    request.write_text(
        json.dumps(
            {
                "schema": "fcp.host-update-request.v1",
                "action": "apply",
                "target_commit": target,
            }
        ),
        encoding="utf-8",
    )
    events: list[str] = []
    engine = SimpleNamespace(
        REQUEST_SCHEMA="fcp.host-update-request.v1",
        OID_RE=re.compile(r"^[0-9a-f]{40}$"),
        subprocess=subprocess,
    )

    def engine_prune(_root, _env):
        events.append("prune")
        return True

    def legacy_preflight(_root, _env):
        events.append("legacy-preflight")

    def process_once(root, _request, _result):
        engine.preflight_disk(root, {})
        events.append("build")
        engine.prune_build_cache(root, {})
        events.append("activation")
        return True

    engine.prune_build_cache = engine_prune
    engine.preflight_disk = legacy_preflight
    engine.process_once = process_once

    class Lock:
        def __enter__(self):
            events.append("lock-enter")
            return self

        def __exit__(self, *_args):
            events.append("lock-exit")

    monkeypatch.setattr(runner.host_build, "host_mutation_lock", lambda _root: Lock())

    def host_prune(_root, _env):
        events.append("preflight-prune")
        return True

    def host_preflight(root, env):
        events.append("preflight")
        assert runner.host_build.prune_build_cache(root, env) is True

    monkeypatch.setattr(runner.host_build, "prune_build_cache", host_prune)
    monkeypatch.setattr(runner.host_build, "preflight_disk", host_preflight)

    def reprove(_root):
        events.append("reproof")
        return target

    monkeypatch.setattr(runner.host_build, "resolve_clean_commit", reprove)

    assert runner.process_once(engine, tmp_path, request, result) is True
    assert events == [
        "lock-enter",
        "preflight",
        "preflight-prune",  # host cleanup: lock remains held
        "build",
        "prune",  # post-build engine cleanup: source can now be re-proved
        "reproof",
        "lock-exit",
        "activation",
    ]
