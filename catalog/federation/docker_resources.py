"""Resolve the host resource that actually backs Docker persistent writes.

B01 must reason about the resource Docker can exhaust, not assume the checkout
filesystem is that resource. Native Docker exposes its data root directly;
Docker Desktop stores Linux-container state in a host disk image, so supported
default Desktop layouts are measured through the host directory containing that
image. Unknown/custom backing storage fails closed.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path, PureWindowsPath

# Module-local platform seams keep cross-platform contract tests from mutating
# Python's process-global os.name, which pathlib itself consults.
_HOST_OS_NAME = os.name
_HOST_PLATFORM = sys.platform


def _subprocess_env(env: Mapping[str, str] | None) -> dict[str, str]:
    environment = dict(os.environ if env is None else env)
    environment.setdefault("COMPOSE_PROJECT_NAME", "fcp")
    return environment


def _run_docker_info(
    root: Path,
    format_string: str,
    *,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            ["docker", "info", "--format", format_string],
            cwd=root,
            env=_subprocess_env(env),
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            timeout=30.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def docker_backing_resource_path(
    root: Path | str,
    *,
    env: Mapping[str, str] | None = None,
) -> Path | None:
    """Return a host path on the filesystem that backs Docker persistent writes.

    The returned path is only a measurement anchor. It does not grant cleanup
    authority over Docker data. If the active Docker backing resource cannot be
    proven from a supported host layout or Docker's own reported data root,
    return ``None`` so callers can refuse new large writes conservatively.
    """

    resolved_root = Path(root).resolve()
    environment = _subprocess_env(env)

    if _HOST_OS_NAME == "nt":
        local_app_data = str(environment.get("LOCALAPPDATA") or "").strip()
        if local_app_data:
            docker = Path(local_app_data) / "Docker" / "wsl"
            for candidate in (
                docker / "disk" / "docker_data.vhdx",
                docker / "data" / "ext4.vhdx",
            ):
                try:
                    if candidate.is_file():
                        return candidate.parent.resolve()
                except OSError:
                    continue

        info = _run_docker_info(
            resolved_root,
            "{{.OSType}}|{{.DockerRootDir}}",
            env=environment,
        )
        if info is None or info.returncode != 0:
            return None
        parts = info.stdout.strip().split("|", maxsplit=1)
        if len(parts) != 2 or parts[0].strip().casefold() != "windows":
            return None
        raw = parts[1].strip()
        candidate = PureWindowsPath(raw)
        if not candidate.is_absolute():
            return None
        host_path = Path(str(candidate))
        try:
            if not host_path.exists():
                return None
            return host_path.resolve()
        except OSError:
            return None

    if _HOST_PLATFORM == "darwin":
        home = str(environment.get("HOME") or "").strip()
        if home:
            docker_data = (
                Path(home)
                / "Library"
                / "Containers"
                / "com.docker.docker"
                / "Data"
                / "vms"
                / "0"
                / "data"
            )
            for name in ("Docker.raw", "Docker.qcow2"):
                candidate = docker_data / name
                try:
                    if candidate.is_file():
                        return candidate.parent.resolve()
                except OSError:
                    continue
        return None

    info = _run_docker_info(resolved_root, "{{.DockerRootDir}}", env=environment)
    if info is None or info.returncode != 0:
        return None
    raw = info.stdout.strip()
    if not raw:
        return None
    candidate = Path(raw)
    if not candidate.is_absolute():
        return None
    try:
        if not candidate.exists():
            return None
        return candidate.resolve()
    except OSError:
        return None
