"""Factory-reset mutable FCP application state while preserving recordings.

This module is executed inside the Flask Compose service for ``start.cmd --fresh``.
A fresh reset removes mutable state beneath the mounted FCP data, results, and
relay-state roots.  The recording corpus beneath ``data/sources`` is the sole
application-data exception and is never traversed for deletion.

All configured mutable-state paths are validated before the first deletion.  A
path that escapes the mounted roots, collides with the recording corpus, or a
recording corpus with broken raw MTConnect integrity causes the reset to fail
closed before any mutable state is removed.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from catalog.common.federation_paths import (
    DEFAULT_COORDINATOR_DATABASE,
    DEFAULT_RELAY_STATE_ROOT,
)

_SQLITE_SIDECARS = ("-wal", "-shm", "-journal")
_RAW_MANIFEST_SUFFIX = ".manifest.json"


@dataclass(frozen=True)
class ResetTarget:
    """One bounded state target selected for deletion or validation."""

    label: str
    path: Path
    kind: str


@dataclass(frozen=True)
class ResolvedStatePaths:
    """State paths shared by reset and post-reset verification."""

    app_root: Path
    data_root: Path
    results_root: Path
    relay_root: Path
    recordings: Path
    identity: Path
    onboarding: Path
    transition: Path
    benchmark: Path
    contribution: Path
    coordinator: Path
    remote_pairing: Path
    capabilities: Path
    capability_config: Path
    server_setup: Path
    server_settings: Path
    auth_database: Path
    auth_secret_dir: Path
    recorder_state: Path
    recorder_control: Path
    recorder_config: Path
    recorder_status: Path
    recorder_log: Path


def _configured_path(
    environ: Mapping[str, str],
    name: str,
    default: Path,
    *,
    app_root: Path,
) -> Path:
    raw = str(environ.get(name) or "").strip()
    path = Path(raw) if raw else default
    if not path.is_absolute():
        path = app_root / path
    return path.resolve(strict=False)


def resolved_state_paths(
    *,
    environ: Mapping[str, str] | None = None,
    app_root: Path | str | None = None,
    relay_root: Path | str = DEFAULT_RELAY_STATE_ROOT,
) -> ResolvedStatePaths:
    values = os.environ if environ is None else environ
    root = Path.cwd() if app_root is None else Path(app_root)
    root = root.resolve(strict=False)
    data_root = (root / "data").resolve(strict=False)
    results_root = (root / "results").resolve(strict=False)
    relay = Path(relay_root).resolve(strict=False)

    identity = _configured_path(
        values,
        "FCP_FEDERATION_NODE_STATE_DIR",
        data_root / "federation" / "device",
        app_root=root,
    )
    onboarding = _configured_path(
        values,
        "FCP_FEDERATION_ONBOARDING_DATABASE",
        data_root / "federation" / "onboarding" / "onboarding.sqlite3",
        app_root=root,
    )
    transition = _configured_path(
        values,
        "FCP_FEDERATION_TRANSITION_DATABASE",
        onboarding,
        app_root=root,
    )
    benchmark = _configured_path(
        values,
        "FCP_FEDERATION_BENCHMARK_DATABASE",
        onboarding,
        app_root=root,
    )
    contribution = _configured_path(
        values,
        "FCP_FEDERATION_CONTRIBUTION_DATABASE",
        onboarding,
        app_root=root,
    )
    coordinator = _configured_path(
        values,
        "FCP_FEDERATION_COORDINATOR_DATABASE",
        Path(DEFAULT_COORDINATOR_DATABASE),
        app_root=root,
    )
    remote_pairing = _configured_path(
        values,
        "FCP_FEDERATION_REMOTE_PAIRING_PATH",
        onboarding.with_name("remote_pairing.json"),
        app_root=root,
    )
    capabilities = (data_root / "capabilities").resolve(strict=False)
    server_setup = (data_root / "server_setup").resolve(strict=False)

    auth_database = _configured_path(
        values,
        "FCP_AUTH_DATABASE",
        data_root / "auth" / "users.sqlite3",
        app_root=root,
    )
    auth_secret_dir = _configured_path(
        values,
        "FCP_AUTH_SECRET_DIR",
        data_root / "auth",
        app_root=root,
    )
    recorder_state = _configured_path(
        values,
        "FCP_RECORDER_STATE_FILE",
        data_root / "source_state" / "mtconnect_recorder_state.json",
        app_root=root,
    )
    recorder_control = _configured_path(
        values,
        "FCP_RECORDER_CONTROL_FILE",
        data_root / "source_state" / "mtconnect_recorder_control.json",
        app_root=root,
    )
    recorder_config = _configured_path(
        values,
        "FCP_RECORDER_CONFIG_FILE",
        data_root / "capabilities" / "config.json",
        app_root=root,
    )
    recorder_status = _configured_path(
        values,
        "FCP_RECORDER_STATUS_FILE",
        data_root / "source_state" / "mtconnect_recorder_status.json",
        app_root=root,
    )
    recorder_log = _configured_path(
        values,
        "FCP_RECORDER_LOG_FILE",
        data_root / "source_state" / "mtconnect_recorder.log",
        app_root=root,
    )

    return ResolvedStatePaths(
        app_root=root,
        data_root=data_root,
        results_root=results_root,
        relay_root=relay,
        recordings=data_root / "sources",
        identity=identity,
        onboarding=onboarding,
        transition=transition,
        benchmark=benchmark,
        contribution=contribution,
        coordinator=coordinator,
        remote_pairing=remote_pairing,
        capabilities=capabilities,
        capability_config=capabilities / "config.json",
        server_setup=server_setup,
        server_settings=server_setup / "server_settings.json",
        auth_database=auth_database,
        auth_secret_dir=auth_secret_dir,
        recorder_state=recorder_state,
        recorder_control=recorder_control,
        recorder_config=recorder_config,
        recorder_status=recorder_status,
        recorder_log=recorder_log,
    )


def _require_bounded_path(
    target: ResetTarget,
    *,
    roots: tuple[Path, ...],
) -> None:
    for root in roots:
        if target.path == root and target.kind in {"contents", "data-contents"}:
            return
        if target.path != root and root in target.path.parents:
            return
    allowed = ", ".join(str(root) for root in roots)
    raise RuntimeError(
        f"Refusing to remove {target.label} outside bounded roots "
        f"({allowed}): {target.path}"
    )


def _configured_mutable_targets(paths: ResolvedStatePaths) -> tuple[ResetTarget, ...]:
    return (
        ResetTarget("device identity and node state", paths.identity, "tree"),
        ResetTarget("onboarding and saved Federation binding", paths.onboarding, "sqlite"),
        ResetTarget("startup transition state", paths.transition, "sqlite"),
        ResetTarget("benchmark state", paths.benchmark, "sqlite"),
        ResetTarget("contribution state", paths.contribution, "sqlite"),
        ResetTarget("Federation coordinator authority", paths.coordinator, "sqlite"),
        ResetTarget("remote pairing state", paths.remote_pairing, "file"),
        ResetTarget("human authentication database", paths.auth_database, "sqlite"),
        ResetTarget("human authentication secrets", paths.auth_secret_dir, "tree"),
        ResetTarget("recorder checkpoint", paths.recorder_state, "file"),
        ResetTarget("recorder control state", paths.recorder_control, "file"),
        ResetTarget("recorder configuration", paths.recorder_config, "file"),
        ResetTarget("recorder status", paths.recorder_status, "file"),
        ResetTarget("recorder log", paths.recorder_log, "file"),
    )


def _validate_root(root: Path, *, label: str) -> None:
    if root.is_symlink():
        raise RuntimeError(f"Refusing to reset through symlinked {label} root: {root}")
    if root.exists() and not root.is_dir():
        raise RuntimeError(f"Expected {label} root to be a directory: {root}")


def _validate_recording_root(paths: ResolvedStatePaths) -> None:
    recording_root = paths.recordings
    if recording_root.is_symlink():
        raise RuntimeError(
            f"Refusing to preserve a symlinked recording corpus: {recording_root}"
        )
    if recording_root.exists() and not recording_root.is_dir():
        raise RuntimeError(f"Recording corpus is not a directory: {recording_root}")


def _overlaps_recordings(path: Path, recordings: Path) -> bool:
    return path == recordings or recordings in path.parents or path in recordings.parents


def planned_reset_targets(
    *,
    environ: Mapping[str, str] | None = None,
    app_root: Path | str | None = None,
    relay_root: Path | str = DEFAULT_RELAY_STATE_ROOT,
) -> tuple[ResetTarget, ...]:
    """Validate configured state and return the three bounded factory-reset roots."""

    paths = resolved_state_paths(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )
    bounded_roots = (paths.data_root, paths.results_root, paths.relay_root)

    for root, label in (
        (paths.data_root, "data"),
        (paths.results_root, "results"),
        (paths.relay_root, "relay-state"),
    ):
        _validate_root(root, label=label)
    _validate_recording_root(paths)

    for target in _configured_mutable_targets(paths):
        _require_bounded_path(target, roots=bounded_roots)
        if _overlaps_recordings(target.path, paths.recordings):
            raise RuntimeError(
                f"Configured mutable state overlaps preserved recordings "
                f"({target.label}): {target.path}"
            )

    return (
        ResetTarget(
            "all mutable FCP data except recordings",
            paths.data_root,
            "data-contents",
        ),
        ResetTarget("all analysis, job, and result state", paths.results_root, "contents"),
        ResetTarget("all local Federation relay authority", paths.relay_root, "contents"),
    )


def _remove_path(path: Path) -> bool:
    if path.is_symlink() or path.is_file():
        path.unlink()
        return True
    if path.exists():
        shutil.rmtree(path)
        return True
    return False


def _clear_contents(root: Path) -> tuple[Path, ...]:
    if not root.exists():
        return ()
    removed: list[Path] = []
    for child in tuple(root.iterdir()):
        if _remove_path(child):
            removed.append(child)
    return tuple(removed)


def _prune_data_except_recordings(paths: ResolvedStatePaths) -> tuple[Path, ...]:
    if not paths.data_root.exists():
        return ()
    removed: list[Path] = []
    for child in tuple(paths.data_root.iterdir()):
        if child == paths.recordings:
            continue
        if _remove_path(child):
            removed.append(child)
    return tuple(removed)


def _raw_recording_integrity_problems(recordings: Path) -> tuple[str, ...]:
    """Verify recorder raw files when their intrinsic SHA-256 manifests exist."""

    if not recordings.exists():
        return ()

    problems: list[str] = []
    raw_files = tuple(recordings.rglob("*.xml.gz"))
    for raw_path in raw_files:
        manifest_path = raw_path.with_suffix(".manifest.json")
        if not manifest_path.exists():
            problems.append(f"raw recording is missing its manifest: {raw_path}")

    for manifest_path in recordings.rglob(f"*{_RAW_MANIFEST_SUFFIX}"):
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            problems.append(
                f"recording manifest is unreadable: {manifest_path} ({type(exc).__name__})"
            )
            continue
        expected = payload.get("raw_sha256") if isinstance(payload, dict) else None
        if expected is None:
            continue
        expected = str(expected).strip().lower()
        if len(expected) != 64 or any(ch not in "0123456789abcdef" for ch in expected):
            problems.append(f"recording manifest has invalid raw_sha256: {manifest_path}")
            continue
        manifest_text = str(manifest_path)
        raw_path = Path(manifest_text[: -len(_RAW_MANIFEST_SUFFIX)] + ".gz")
        if not raw_path.exists():
            problems.append(f"recording manifest points to missing raw batch: {manifest_path}")
            continue
        digest = hashlib.sha256()
        try:
            with gzip.open(raw_path, "rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
        except (OSError, EOFError) as exc:
            problems.append(
                f"raw recording is unreadable: {raw_path} ({type(exc).__name__})"
            )
            continue
        if digest.hexdigest() != expected:
            problems.append(f"raw recording digest mismatch: {raw_path}")
    return tuple(problems)


def reset_device_state(
    *,
    environ: Mapping[str, str] | None = None,
    app_root: Path | str | None = None,
    relay_root: Path | str = DEFAULT_RELAY_STATE_ROOT,
) -> tuple[Path, ...]:
    """Factory-reset mutable FCP state while leaving recordings byte-for-byte alone."""

    paths = resolved_state_paths(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )
    targets = planned_reset_targets(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )

    # Validate recording integrity before the first deletion.  A broken recording
    # must never leave the installation half-reset simply because post-reset
    # verification discovers it too late.
    recording_problems = _raw_recording_integrity_problems(paths.recordings)
    if recording_problems:
        raise RuntimeError("; ".join(recording_problems))

    removed: list[Path] = []
    for target in targets:
        if target.kind == "data-contents":
            target_removed = _prune_data_except_recordings(paths)
        else:
            target_removed = _clear_contents(target.path)
        if target_removed:
            for path in target_removed:
                print(f"removed {target.label}: {path}")
            removed.extend(target_removed)
        else:
            print(f"already empty {target.label}: {target.path}")
    return tuple(removed)


def _unexpected_children(root: Path, *, allowed: frozenset[Path] = frozenset()) -> list[Path]:
    if not root.exists():
        return []
    return [child for child in root.iterdir() if child not in allowed]


def verify_fresh_application_state(
    *,
    environ: Mapping[str, str] | None = None,
    app_root: Path | str | None = None,
    relay_root: Path | str = DEFAULT_RELAY_STATE_ROOT,
) -> tuple[str, ...]:
    """Verify that only the immutable recording corpus survives a fresh reset."""

    paths = resolved_state_paths(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )
    # Re-run all path-boundary checks.  Verification must not silently accept a
    # custom mutable-state path that reset could not safely reach.
    planned_reset_targets(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )

    problems: list[str] = []
    for child in _unexpected_children(
        paths.data_root,
        allowed=frozenset({paths.recordings}),
    ):
        problems.append(f"mutable data still exists after fresh reset: {child}")
    for child in _unexpected_children(paths.results_root):
        problems.append(f"analysis/job/result state still exists after fresh reset: {child}")
    for child in _unexpected_children(paths.relay_root):
        problems.append(f"relay/Federation authority still exists after fresh reset: {child}")

    for target in _configured_mutable_targets(paths):
        if target.kind == "sqlite":
            candidates = (target.path,) + tuple(
                Path(f"{target.path}{suffix}") for suffix in _SQLITE_SIDECARS
            )
        else:
            candidates = (target.path,)
        for candidate in candidates:
            if candidate.exists() or candidate.is_symlink():
                problems.append(f"{target.label} still exists after fresh reset: {candidate}")

    problems.extend(_raw_recording_integrity_problems(paths.recordings))
    return tuple(dict.fromkeys(problems))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify-fresh",
        action="store_true",
        help="Verify that only immutable machine recordings remain as application state.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        if args.verify_fresh:
            problems = verify_fresh_application_state()
            if problems:
                for problem in problems:
                    print(f"fresh verification failed: {problem}")
                return 1
            print(
                "fresh verification passed: mutable application state is empty and "
                "preserved raw recording integrity is valid"
            )
            return 0

        reset_device_state()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"fresh reset failed: {exc}")
        return 1
    print(
        "fresh reset completed: mutable application state was removed; "
        "machine recordings were preserved"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
