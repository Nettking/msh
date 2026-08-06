"""Safely remove and verify device-local onboarding and Federation state.

This module is executed inside the Flask Compose service for ``start.cmd --fresh``.
It resolves the same environment-configurable paths as the Flask app, removes both
current and retained legacy state layouts, and refuses to delete outside the
mounted MSH data and relay-state roots.
"""

from __future__ import annotations

import argparse
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


@dataclass(frozen=True)
class ResolvedStatePaths:
    """State paths shared by reset and post-start verification."""

    app_root: Path
    data_root: Path
    relay_root: Path
    identity: Path
    onboarding: Path
    transition: Path
    benchmark: Path
    contribution: Path
    coordinator: Path
    remote_pairing: Path
    server_setup: Path
    server_settings: Path


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
    relay_root: Path | str = "/var/lib/msh-relay",
) -> ResolvedStatePaths:
    values = os.environ if environ is None else environ
    root = Path.cwd() if app_root is None else Path(app_root)
    root = root.resolve(strict=False)
    data_root = (root / "data").resolve(strict=False)
    relay = Path(relay_root).resolve(strict=False)

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
    server_setup = (data_root / "server_setup").resolve(strict=False)

    return ResolvedStatePaths(
        app_root=root,
        data_root=data_root,
        relay_root=relay,
        identity=identity,
        onboarding=onboarding,
        transition=transition,
        benchmark=benchmark,
        contribution=contribution,
        coordinator=coordinator,
        remote_pairing=remote_pairing,
        server_setup=server_setup,
        server_settings=server_setup / "server_settings.json",
    )


def _require_bounded_path(
    target: ResetTarget,
    *,
    roots: tuple[Path, ...],
) -> None:
    for root in roots:
        if target.path == root and target.kind == "contents":
            return
        if target.path != root and root in target.path.parents:
            return
    allowed = ", ".join(str(root) for root in roots)
    raise RuntimeError(
        f"Refusing to remove {target.label} outside bounded roots "
        f"({allowed}): {target.path}"
    )


def planned_reset_targets(
    *,
    environ: Mapping[str, str] | None = None,
    app_root: Path | str | None = None,
    relay_root: Path | str = "/var/lib/msh-relay",
) -> tuple[ResetTarget, ...]:
    """Resolve current configured paths plus complete retained state roots."""

    paths = resolved_state_paths(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )
    bounded_roots = (paths.data_root, paths.relay_root)

    candidates = (
        # Remove configured locations even when an operator moved them away from
        # the defaults while keeping them inside the mounted state roots.
        ResetTarget("device identity and node state", paths.identity, "tree"),
        ResetTarget(
            "onboarding and saved Federation binding",
            paths.onboarding,
            "sqlite",
        ),
        ResetTarget("startup transition state", paths.transition, "sqlite"),
        ResetTarget("benchmark state", paths.benchmark, "sqlite"),
        ResetTarget("contribution state", paths.contribution, "sqlite"),
        ResetTarget("Federation coordinator authority", paths.coordinator, "sqlite"),
        ResetTarget("remote pairing state", paths.remote_pairing, "file"),
        # Also remove the complete retained roots. Older deliveries used more
        # than one filename/layout under these directories.
        ResetTarget(
            "all default and legacy Federation device state",
            paths.data_root / "federation",
            "tree",
        ),
        ResetTarget(
            "all saved server and device setup",
            paths.server_setup,
            "tree",
        ),
        ResetTarget(
            "all local Federation relay authority",
            paths.relay_root,
            "contents",
        ),
    )

    unique: list[ResetTarget] = []
    seen: set[tuple[Path, str]] = set()
    for target in candidates:
        _require_bounded_path(target, roots=bounded_roots)
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


def _remove_path(path: Path) -> bool:
    if path.is_symlink() or path.is_file():
        path.unlink()
        return True
    if path.exists():
        shutil.rmtree(path)
        return True
    return False


def _remove_target(target: ResetTarget) -> tuple[Path, ...]:
    removed: list[Path] = []
    if target.kind == "tree":
        if _remove_path(target.path):
            removed.append(target.path)
        return tuple(removed)

    if target.kind == "contents":
        if not target.path.exists():
            return ()
        for child in tuple(target.path.iterdir()):
            if _remove_path(child):
                removed.append(child)
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
    """Remove all configured and retained device/Federation state."""

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


def verify_fresh_application_state(
    *,
    environ: Mapping[str, str] | None = None,
    app_root: Path | str | None = None,
    relay_root: Path | str = "/var/lib/msh-relay",
) -> tuple[str, ...]:
    """Verify the user-visible state sources used by the started Flask app."""

    from catalog.node.identity import IdentityStore, NodeIdentityStateError

    from .capability_inspection_service import InspectionSnapshotStore
    from .capability_onboarding_service import FederationBindingStore
    from .capability_startup_transition_service import CapabilityStartupStateStore
    from .federation_pairing_service import RemotePairingStore
    from .server_setup_service import load_settings

    paths = resolved_state_paths(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )
    problems: list[str] = []

    try:
        identity = IdentityStore(
            paths.identity,
            display_name="Fresh-state verification",
        ).load()
    except NodeIdentityStateError as exc:
        if exc.code != "identity-state-missing":
            problems.append(f"identity state is unreadable: {exc.code}")
    else:
        problems.append(f"device identity still exists: {identity.identity.node_id}")

    try:
        binding = FederationBindingStore(paths.onboarding).load()
    except Exception as exc:  # noqa: BLE001 - report bounded verification failure
        problems.append(f"saved Federation binding is unreadable: {type(exc).__name__}")
    else:
        if binding is not None:
            problems.append(
                f"saved Federation binding still exists: {binding.federation_id}"
            )

    try:
        inspection = InspectionSnapshotStore(paths.onboarding).load()
    except Exception as exc:  # noqa: BLE001
        problems.append(f"inspection state is unreadable: {type(exc).__name__}")
    else:
        if inspection is not None:
            problems.append(
                f"device inspection still exists at revision {inspection.revision}"
            )

    try:
        transition = CapabilityStartupStateStore(paths.transition).load()
    except Exception as exc:  # noqa: BLE001
        problems.append(f"startup transition is unreadable: {type(exc).__name__}")
    else:
        if transition is not None:
            problems.append("completed capability-first startup state still exists")

    try:
        remote_pairing = RemotePairingStore(paths.remote_pairing).load()
    except Exception as exc:  # noqa: BLE001
        problems.append(f"remote pairing state is unreadable: {type(exc).__name__}")
    else:
        if remote_pairing is not None:
            problems.append("saved remote Federation pairing still exists")

    try:
        settings = load_settings(paths.server_settings)
    except Exception as exc:  # noqa: BLE001
        problems.append(f"saved device setup is unreadable: {type(exc).__name__}")
    else:
        if settings.configured or settings.user_setup_complete:
            problems.append(
                f"saved device setup still exists: {settings.deployment_mode}"
            )

    return tuple(problems)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify-fresh",
        action="store_true",
        help="Verify that the started Flask container has no user-visible setup state.",
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
                "fresh verification passed: identity, onboarding, Federation, "
                "inspection, and legacy setup are empty"
            )
            return 0

        reset_device_state()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"fresh reset failed: {exc}")
        return 1
    print(
        "fresh reset completed: configured and legacy device/Federation state "
        "was removed"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
