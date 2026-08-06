"""Safely remove device-local onboarding and Federation state.

This module is executed inside the Flask Compose service for ``start.cmd --fresh``.
It resolves the same environment-configurable paths as the Flask app, removes only
explicit device/Federation/setup targets, and refuses to delete outside the mounted
MSH data and relay-state roots.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

_SQLITE_SIDECARS = ("-wal", "-shm", "-journal")


@dataclass(frozen=True)
class ResetTarget:
    """One bounded state target selected for deletion."""

    label: str
    path: Path
    kind: str


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


def _require_bounded_path(path: Path, *, roots: tuple[Path, ...], label: str) -> None:
    for root in roots:
        if path != root and root in path.parents:
            return
    allowed = ", ".join(str(root) for root in roots)
    raise RuntimeError(
        f"Refusing to remove {label} outside bounded roots ({allowed}): {path}"
    )


def planned_reset_targets(
    *,
    environ: Mapping[str, str] | None = None,
    app_root: Path | str | None = None,
    relay_root: Path | str = "/var/lib/msh-relay",
) -> tuple[ResetTarget, ...]:
    """Resolve all state paths used by the current Flask/Compose configuration."""

    values = os.environ if environ is None else environ
    root = Path.cwd() if app_root is None else Path(app_root)
    root = root.resolve(strict=False)
    data_root = (root / "data").resolve(strict=False)
    relay = Path(relay_root).resolve(strict=False)
    bounded_roots = (data_root, relay)

    identity = _configured_path(
        values,
        "MSH_FEDERATION_NODE_STATE_DIR",
        data_root / "federation" / "device",
        app_root=root,
    )
    onboarding = _configured_path(
        values,
        "MSH_FEDERATION_ONBOARDING_DATABASE",
        data_root / "federation" / "onboarding" / "onboarding.sqlite3",
        app_root=root,
    )
    transition = _configured_path(
        values,
        "MSH_FEDERATION_TRANSITION_DATABASE",
        onboarding,
        app_root=root,
    )
    benchmark = _configured_path(
        values,
        "MSH_FEDERATION_BENCHMARK_DATABASE",
        onboarding,
        app_root=root,
    )
    contribution = _configured_path(
        values,
        "MSH_FEDERATION_CONTRIBUTION_DATABASE",
        onboarding,
        app_root=root,
    )
    coordinator = _configured_path(
        values,
        "MSH_FEDERATION_COORDINATOR_DATABASE",
        relay / "control.sqlite3",
        app_root=root,
    )
    remote_pairing = _configured_path(
        values,
        "MSH_FEDERATION_REMOTE_PAIRING_PATH",
        onboarding.with_name("remote_pairing.json"),
        app_root=root,
    )
    server_settings = (data_root / "server_setup" / "server_settings.json").resolve(
        strict=False
    )

    candidates = (
        ResetTarget("device identity and node state", identity, "tree"),
        ResetTarget("onboarding and saved Federation binding", onboarding, "sqlite"),
        ResetTarget("startup transition state", transition, "sqlite"),
        ResetTarget("benchmark state", benchmark, "sqlite"),
        ResetTarget("contribution state", contribution, "sqlite"),
        ResetTarget("Federation coordinator authority", coordinator, "sqlite"),
        ResetTarget("remote pairing state", remote_pairing, "file"),
        ResetTarget("saved server and device setup", server_settings, "file"),
    )

    unique: list[ResetTarget] = []
    seen: set[tuple[Path, str]] = set()
    for target in candidates:
        _require_bounded_path(target.path, roots=bounded_roots, label=target.label)
        key = (target.path, target.kind)
        if key in seen:
            continue
        seen.add(key)
        unique.append(target)
    return tuple(unique)


def _remove_file(path: Path) -> bool:
    if not path.exists() and not path.is_symlink():
        return False
    path.unlink()
    return True


def _remove_target(target: ResetTarget) -> tuple[Path, ...]:
    removed: list[Path] = []
    if target.kind == "tree":
        if target.path.is_symlink() or target.path.is_file():
            target.path.unlink()
            removed.append(target.path)
        elif target.path.exists():
            shutil.rmtree(target.path)
            removed.append(target.path)
        return tuple(removed)

    if _remove_file(target.path):
        removed.append(target.path)
    if target.kind == "sqlite":
        for suffix in _SQLITE_SIDECARS:
            sidecar = Path(f"{target.path}{suffix}")
            if _remove_file(sidecar):
                removed.append(sidecar)
    return tuple(removed)


def reset_device_state(
    *,
    environ: Mapping[str, str] | None = None,
    app_root: Path | str | None = None,
    relay_root: Path | str = "/var/lib/msh-relay",
) -> tuple[Path, ...]:
    """Remove all configured device/Federation state and return removed paths."""

    removed: list[Path] = []
    for target in planned_reset_targets(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    ):
        target_removed = _remove_target(target)
        if target_removed:
            for path in target_removed:
                print(f"removed {target.label}: {path}")
            removed.extend(target_removed)
        else:
            print(f"already absent {target.label}: {target.path}")
    return tuple(removed)


def main() -> int:
    try:
        reset_device_state()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"fresh reset failed: {exc}")
        return 1
    print("fresh reset verified: configured identity, onboarding, and Federation state are empty")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
