#!/usr/bin/env python3
"""Serialized production runner for the POSIX FCP host update engine.

The engine is preserved separately so its mature request/activation semantics stay
unchanged. This runner adds the B04 host-mutation boundary around update apply:
source inspection/mutation and the core image build share the same checkout lock
as ordinary launcher builds. The lock is released after successful build cache
cleanup and exact source re-proof, before optional AI/model activation work.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from catalog.federation import host_build

ENGINE_NAME = "fcp_update_engine.py"
MAX_REQUEST_BYTES = 8192


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_engine(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("_fcp_update_engine", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("update_engine_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _apply_target(path: Path, engine: ModuleType) -> str | None:
    try:
        if path.stat().st_size > MAX_REQUEST_BYTES:
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    if value.get("schema") != engine.REQUEST_SCHEMA or value.get("action") != "apply":
        return None
    target = value.get("target_commit")
    if not isinstance(target, str) or not engine.OID_RE.fullmatch(target):
        return None
    return target.lower()


def _serialized_apply(
    engine: ModuleType,
    root: Path,
    request_file: Path,
    result_file: Path,
    target: str,
) -> bool:
    lock = host_build.host_mutation_lock(root)
    lock.__enter__()
    lock_held = True
    build_phase_entered = False
    post_build_prune_called = False
    original_preflight = engine.preflight_disk
    original_prune = engine.prune_build_cache

    def release_after_build() -> None:
        nonlocal lock_held
        if lock_held:
            lock.__exit__(None, None, None)
            lock_held = False

    def guarded_post_build_prune(*args, **kwargs):
        nonlocal post_build_prune_called
        post_build_prune_called = True
        ok = bool(original_prune(*args, **kwargs))
        if not ok:
            raise RuntimeError("build_cache_prune_failed")
        proven = host_build.resolve_clean_commit(root)
        if proven != target:
            raise RuntimeError("build_context_changed")
        release_after_build()
        return True

    def guarded_preflight(*args, **kwargs):
        nonlocal build_phase_entered
        build_phase_entered = True
        # preflight_disk may prune before a build to recover space. That prune
        # must NOT end the source/build critical section. Temporarily restore the
        # engine's original prune function only for preflight, then arm the
        # post-build guard again before control returns to the engine.
        engine.prune_build_cache = original_prune
        try:
            return original_preflight(*args, **kwargs)
        finally:
            engine.prune_build_cache = guarded_post_build_prune

    engine.preflight_disk = guarded_preflight
    engine.prune_build_cache = guarded_post_build_prune
    try:
        return bool(engine.process_once(root, request_file, result_file))
    finally:
        engine.preflight_disk = original_preflight
        engine.prune_build_cache = original_prune
        if build_phase_entered and not post_build_prune_called:
            # The build/preflight raised before the ordinary post-build prune.
            # Cache is reconstructible, so make one bounded cleanup attempt even
            # though the request has already been recorded as failed.
            try:
                original_prune(root, os.environ.copy())
            except Exception as exc:  # noqa: BLE001 - host cleanup boundary
                print(
                    f"FCP update cleanup warning: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
        release_after_build()


def process_once(
    engine: ModuleType,
    root: Path,
    request_file: Path,
    result_file: Path,
) -> bool:
    target = _apply_target(request_file, engine)
    if target is None:
        return bool(engine.process_once(root, request_file, result_file))
    try:
        return _serialized_apply(engine, root, request_file, result_file, target)
    except RuntimeError as exc:
        if str(exc) == "host_mutation_busy":
            # The launcher/update transaction holding the lock owns the host.
            # Leave the durable request untouched and retry on the next poll.
            return False
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--data-directory", required=True)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    data_directory = Path(args.data_directory).resolve()
    directory = data_directory / "federation" / "update-agent"
    directory.mkdir(parents=True, exist_ok=True)
    request_file = directory / "request.json"
    result_file = directory / "result.json"
    runner_path = Path(__file__).resolve()
    engine_path = runner_path.with_name(ENGINE_NAME)
    engine = _load_engine(engine_path)
    initial_runner_digest = _digest(runner_path)
    initial_engine_digest = _digest(engine_path)

    # Preserve the existing singleton boundary. An agent process from the
    # immediately previous release self-reloads this public entrypoint after it
    # completes the update that installs the new shim/runner.
    lock_path = directory / "agent.lock"
    with lock_path.open("a+", encoding="utf-8") as singleton:
        try:
            fcntl.flock(singleton.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0

        while True:
            processed = process_once(engine, root, request_file, result_file)
            if args.once:
                return 0
            if processed and (
                _digest(runner_path) != initial_runner_digest
                or _digest(engine_path) != initial_engine_digest
            ):
                fcntl.flock(singleton.fileno(), fcntl.LOCK_UN)
                os.execv(
                    sys.executable,
                    [
                        sys.executable,
                        str(runner_path),
                        "--repo-root",
                        str(root),
                        "--data-directory",
                        str(data_directory),
                        "--poll-seconds",
                        str(args.poll_seconds),
                    ],
                )
            if not processed:
                time.sleep(max(0.1, min(args.poll_seconds, 30.0)))


if __name__ == "__main__":
    raise SystemExit(main())
