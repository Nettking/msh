"""Capacity-safe, quiesced Federation v1 backup support.

The supported backup path proves destination capacity before quiescing FCP,
serializes against supported host mutation, copies only a stopped Compose-managed
runtime, verifies copied SQLite state, and never implicitly restarts FCP after a
success or failure.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.mtconnect_recorder import native_update

from .docker_resources import docker_backing_resource_path
from .host_mutation import HOST_MUTATION_LOCK_TIMEOUT_SECONDS, host_mutation_lock
from .host_resources import FilesystemMeasurement, measure_filesystem
from .tailnet_join_bridge import PID_RELATIVE
from .tailnet_join_responder import (
    MAX_PROCESS_RECORD_BYTES,
    MAX_PROCESS_START_TOKEN_LENGTH,
    PROCESS_RECORD_SCHEMA,
    process_start_token,
    terminate_process_if_same_instance,
)

BACKUP_SCHEMA = "fcp.federation.backup.v1"
BACKUP_INCOMPLETE_NAME = ".fcp-backup-incomplete"
BACKUP_MANIFEST_NAME = "backup-manifest.json"
SOURCE_COMMIT_NAME = "source-commit.txt"
MIN_CAPACITY_MARGIN_BYTES = 64 * 1024 * 1024
CAPACITY_MARGIN_DIVISOR = 10
CAPACITY_MARGIN_INODES = 64
MAX_COMPOSE_CONFIG_BYTES = 4 * 1024 * 1024
MAX_GIT_OUTPUT_BYTES = 1024 * 1024
RESPONDER_STOP_TIMEOUT_SECONDS = 10.0
COMMAND_TIMEOUT_SECONDS = 60.0
COPY_TIMEOUT_SECONDS = 60 * 60.0
CORE_SERVICES = ("flask", "relay", "recorder")
RELAY_TARGET = "/var/lib/fcp-relay"
DATA_TARGET = "/app/data"
RESULTS_TARGET = "/app/results"
SQLITE_HEADER = b"SQLite format 3\x00"

_RELAY_SCAN_SCRIPT = r"""
import json
import os
import stat

root = "/source"
inodes = 0
bytes_total = 0
stack = [root]
while stack:
    current = stack.pop()
    with os.scandir(current) as entries:
        for entry in entries:
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                raise RuntimeError("relay_volume_contains_symlink")
            if stat.S_ISDIR(info.st_mode):
                inodes += 1
                stack.append(entry.path)
            elif stat.S_ISREG(info.st_mode):
                inodes += 1
                bytes_total += int(info.st_size)
            else:
                raise RuntimeError("relay_volume_contains_special_file")
print(json.dumps({"bytes": bytes_total, "files": inodes}, separators=(",", ":")))
""".strip()


class BackupError(RuntimeError):
    """A supported backup invariant could not be proven."""


@dataclass(frozen=True)
class TreeEstimate:
    bytes: int
    files: int

    def __add__(self, other: "TreeEstimate") -> "TreeEstimate":
        return TreeEstimate(self.bytes + other.bytes, self.files + other.files)


@dataclass(frozen=True)
class BackupLayout:
    data_dir: Path
    results_dir: Path | None
    relay_volume: str
    relay_container: str
    relay_image: str


@dataclass(frozen=True)
class CapacityRequirement:
    bytes: int
    inodes: int


def _run(
    argv: list[str],
    *,
    root: Path,
    timeout: float = COMMAND_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            argv,
            cwd=root,
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise BackupError(f"command_unavailable:{argv[0]}") from exc


def _require_command(
    argv: list[str],
    *,
    root: Path,
    timeout: float = COMMAND_TIMEOUT_SECONDS,
    code: str,
) -> str:
    completed = _run(argv, root=root, timeout=timeout)
    if completed.returncode != 0:
        raise BackupError(code)
    return completed.stdout


def _is_reparse_point(path: Path) -> bool:
    if path.is_symlink():
        return True
    is_junction = getattr(path, "is_junction", None)
    return bool(is_junction is not None and is_junction())


def _scan_tree(root: Path) -> TreeEstimate:
    """Measure regular-file bytes and destination inode demand without links."""

    root = root.resolve()
    if not root.is_dir() or _is_reparse_point(root):
        raise BackupError("backup_source_root_unusable")
    total_bytes = 0
    inodes = 0
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            entries = list(os.scandir(current))
        except OSError as exc:
            raise BackupError("backup_source_unreadable") from exc
        for entry in entries:
            path = Path(entry.path)
            if entry.is_symlink() or _is_reparse_point(path):
                raise BackupError("backup_source_contains_link")
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise BackupError("backup_source_unreadable") from exc
            if stat.S_ISDIR(info.st_mode):
                inodes += 1
                stack.append(path)
            elif stat.S_ISREG(info.st_mode):
                inodes += 1
                total_bytes += int(info.st_size)
            else:
                raise BackupError("backup_source_contains_special_file")
    return TreeEstimate(total_bytes, inodes)


def _copy_tree(source: Path, destination: Path) -> None:
    source = source.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    stack = [(source, destination)]
    while stack:
        current_source, current_destination = stack.pop()
        try:
            entries = list(os.scandir(current_source))
        except OSError as exc:
            raise BackupError("backup_source_unreadable") from exc
        for entry in entries:
            source_path = Path(entry.path)
            destination_path = current_destination / entry.name
            if entry.is_symlink() or _is_reparse_point(source_path):
                raise BackupError("backup_source_contains_link")
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise BackupError("backup_source_unreadable") from exc
            if stat.S_ISDIR(info.st_mode):
                destination_path.mkdir()
                stack.append((source_path, destination_path))
            elif stat.S_ISREG(info.st_mode):
                shutil.copy2(source_path, destination_path, follow_symlinks=False)
            else:
                raise BackupError("backup_source_contains_special_file")


def _git_commit(root: Path) -> str:
    value = _require_command(
        ["git", "rev-parse", "--verify", "HEAD^{commit}"],
        root=root,
        code="source_commit_unavailable",
    ).strip()
    if len(value) != 40 or any(c not in "0123456789abcdefABCDEF" for c in value):
        raise BackupError("source_commit_unavailable")
    return value.lower()


def _require_clean_checkout(root: Path) -> None:
    output = _require_command(
        ["git", "status", "--porcelain"],
        root=root,
        code="source_status_unavailable",
    )
    if len(output.encode("utf-8")) > MAX_GIT_OUTPUT_BYTES:
        raise BackupError("source_status_unbounded")
    if output.strip():
        raise BackupError("source_checkout_dirty")


def _compose_config(root: Path) -> dict[str, Any]:
    output = _require_command(
        ["docker", "compose", "config", "--format", "json"],
        root=root,
        code="compose_config_unavailable",
    )
    if len(output.encode("utf-8")) > MAX_COMPOSE_CONFIG_BYTES:
        raise BackupError("compose_config_too_large")
    try:
        value = json.loads(output)
    except json.JSONDecodeError as exc:
        raise BackupError("compose_config_unreadable") from exc
    if not isinstance(value, dict):
        raise BackupError("compose_config_unreadable")
    return value


def _service_volume(
    config: dict[str, Any],
    *,
    service: str,
    target: str,
    expected_type: str,
) -> str | None:
    services = config.get("services")
    if not isinstance(services, dict):
        raise BackupError("compose_config_unreadable")
    definition = services.get(service)
    if not isinstance(definition, dict):
        raise BackupError(f"compose_service_missing:{service}")
    volumes = definition.get("volumes")
    if not isinstance(volumes, list):
        raise BackupError(f"compose_volume_missing:{service}:{target}")
    matches: list[str] = []
    for value in volumes:
        if not isinstance(value, dict):
            continue
        if value.get("target") != target or value.get("type") != expected_type:
            continue
        source = value.get("source")
        if isinstance(source, str) and source.strip():
            matches.append(source.strip())
    if len(matches) > 1:
        raise BackupError(f"compose_volume_ambiguous:{service}:{target}")
    return matches[0] if matches else None


def _container_id(root: Path, service: str) -> str:
    value = _require_command(
        ["docker", "compose", "ps", "-a", "-q", service],
        root=root,
        code=f"compose_container_unavailable:{service}",
    ).strip()
    lines = [line.strip() for line in value.splitlines() if line.strip()]
    if len(lines) != 1:
        raise BackupError(f"compose_container_unavailable:{service}")
    return lines[0]


def _relay_image(root: Path, container: str) -> str:
    value = _require_command(
        ["docker", "inspect", container, "--format", "{{.Image}}"],
        root=root,
        code="relay_image_unavailable",
    ).strip()
    if not value:
        raise BackupError("relay_image_unavailable")
    return value


def _relay_mounted_volume(root: Path, container: str) -> str:
    """Return Docker's actual retained volume name, not a Compose logical key."""

    output = _require_command(
        ["docker", "inspect", container, "--format", "{{json .Mounts}}"],
        root=root,
        code="relay_volume_mount_unavailable",
    )
    try:
        mounts = json.loads(output)
    except json.JSONDecodeError as exc:
        raise BackupError("relay_volume_mount_unavailable") from exc
    if not isinstance(mounts, list):
        raise BackupError("relay_volume_mount_unavailable")
    matches: list[str] = []
    for mount in mounts:
        if not isinstance(mount, dict):
            continue
        if mount.get("Type") != "volume" or mount.get("Destination") != RELAY_TARGET:
            continue
        name = mount.get("Name")
        if isinstance(name, str) and name.strip():
            matches.append(name.strip())
    if len(matches) != 1:
        raise BackupError("relay_volume_mount_unavailable")
    return matches[0]


def _resolve_layout(root: Path) -> BackupLayout:
    config = _compose_config(root)
    data_source = _service_volume(
        config, service="flask", target=DATA_TARGET, expected_type="bind"
    )
    results_source = _service_volume(
        config, service="flask", target=RESULTS_TARGET, expected_type="bind"
    )
    relay_declared = _service_volume(
        config, service="relay", target=RELAY_TARGET, expected_type="volume"
    )
    if data_source is None or relay_declared is None:
        raise BackupError("required_backup_volume_unresolved")
    data_dir = Path(data_source).resolve()
    results_dir = Path(results_source).resolve() if results_source else None
    if not data_dir.is_dir():
        raise BackupError("data_directory_missing")
    if results_dir is not None and not results_dir.exists():
        results_dir = None
    relay_container = _container_id(root, "relay")
    relay_volume = _relay_mounted_volume(root, relay_container)
    return BackupLayout(
        data_dir=data_dir,
        results_dir=results_dir,
        relay_volume=relay_volume,
        relay_container=relay_container,
        relay_image=_relay_image(root, relay_container),
    )


def _same_layout(first: BackupLayout, second: BackupLayout) -> bool:
    return (
        first.data_dir == second.data_dir
        and first.results_dir == second.results_dir
        and first.relay_volume == second.relay_volume
        and first.relay_container == second.relay_container
        and first.relay_image == second.relay_image
    )


def _scan_relay_volume(root: Path, layout: BackupLayout) -> TreeEstimate:
    output = _require_command(
        [
            "docker",
            "run",
            "--rm",
            "--pull=never",
            "--network=none",
            "--entrypoint",
            "python",
            "--mount",
            f"type=volume,src={layout.relay_volume},dst=/source,readonly",
            layout.relay_image,
            "-c",
            _RELAY_SCAN_SCRIPT,
        ],
        root=root,
        code="relay_volume_preflight_failed",
    )
    try:
        value = json.loads(output)
    except json.JSONDecodeError as exc:
        raise BackupError("relay_volume_preflight_failed") from exc
    if not isinstance(value, dict):
        raise BackupError("relay_volume_preflight_failed")
    bytes_value = value.get("bytes")
    files_value = value.get("files")
    if (
        isinstance(bytes_value, bool)
        or not isinstance(bytes_value, int)
        or bytes_value < 0
        or isinstance(files_value, bool)
        or not isinstance(files_value, int)
        or files_value < 0
    ):
        raise BackupError("relay_volume_preflight_failed")
    return TreeEstimate(bytes_value, files_value)


def _local_estimate(root: Path, layout: BackupLayout) -> TreeEstimate:
    total = _scan_tree(layout.data_dir)
    if layout.results_dir is not None and layout.results_dir != layout.data_dir:
        total += _scan_tree(layout.results_dir)
    env_file = root / ".env"
    if env_file.exists():
        if _is_reparse_point(env_file) or not env_file.is_file():
            raise BackupError("environment_file_unusable")
        total += TreeEstimate(env_file.stat().st_size, 1)
    return total


def _capacity_requirement(estimate: TreeEstimate) -> CapacityRequirement:
    margin = max(MIN_CAPACITY_MARGIN_BYTES, estimate.bytes // CAPACITY_MARGIN_DIVISOR)
    return CapacityRequirement(
        bytes=estimate.bytes + margin,
        inodes=estimate.files + CAPACITY_MARGIN_INODES,
    )


def _measurement(path: Path) -> FilesystemMeasurement:
    value = measure_filesystem(path)
    if not value.available or value.free_bytes is None:
        raise BackupError("filesystem_measurement_unavailable")
    return value


def _source_resource_ids(root: Path, layout: BackupLayout) -> set[str]:
    paths = [root, layout.data_dir]
    if layout.results_dir is not None:
        paths.append(layout.results_dir)
    docker_path = docker_backing_resource_path(root)
    if docker_path is None:
        raise BackupError("docker_backing_resource_unproven")
    paths.append(docker_path)
    return {_measurement(path).resource_id for path in paths}


def _path_contains(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def _preflight_destination(
    root: Path,
    destination: Path,
    layout: BackupLayout,
    estimate: TreeEstimate,
) -> CapacityRequirement:
    destination = destination.resolve(strict=False)
    if destination.exists():
        raise BackupError("backup_destination_exists")
    parent = destination.parent
    if not parent.is_dir() or _is_reparse_point(parent):
        raise BackupError("backup_destination_parent_unusable")
    for source in (root, layout.data_dir, layout.results_dir):
        if source is None:
            continue
        source = source.resolve()
        if _path_contains(source, destination) or _path_contains(destination, source):
            raise BackupError("backup_destination_overlaps_source")
    destination_measurement = _measurement(parent)
    if destination_measurement.resource_id in _source_resource_ids(root, layout):
        raise BackupError("backup_destination_not_independent")
    requirement = _capacity_requirement(estimate)
    if destination_measurement.free_bytes < requirement.bytes:
        raise BackupError("backup_destination_insufficient_bytes")
    if (
        destination_measurement.free_inodes is not None
        and destination_measurement.free_inodes < requirement.inodes
    ):
        raise BackupError("backup_destination_insufficient_inodes")
    return requirement


def _read_responder_record(path: Path) -> tuple[int, str] | None:
    if not path.exists():
        return None
    try:
        if path.stat().st_size > MAX_PROCESS_RECORD_BYTES:
            raise BackupError("responder_process_record_unreadable")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackupError("responder_process_record_unreadable") from exc
    if not isinstance(value, dict) or value.get("schema") != PROCESS_RECORD_SCHEMA:
        raise BackupError("responder_process_record_unreadable")
    pid = value.get("pid")
    token = value.get("start_token")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or not isinstance(token, str)
        or not token.strip()
        or len(token.strip()) > MAX_PROCESS_START_TOKEN_LENGTH
    ):
        raise BackupError("responder_process_record_unreadable")
    return pid, token.strip()


def _quiesce_responder(data_dir: Path) -> None:
    record = _read_responder_record(data_dir / PID_RELATIVE)
    if record is None:
        return
    pid, token = record
    if process_start_token(pid) != token:
        return
    if not terminate_process_if_same_instance(pid, token):
        raise BackupError("responder_stop_unverified")
    deadline = time.monotonic() + RESPONDER_STOP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process_start_token(pid) != token:
            return
        time.sleep(0.05)
    raise BackupError("responder_stop_unverified")


def _refuse_live_native_recorder(layout: BackupLayout) -> None:
    status_file = layout.data_dir / "source_state" / "mtconnect_recorder_status.json"
    value = native_update.read_json(status_file)
    if not isinstance(value, dict) or "native_runtime" not in value:
        return
    status = native_update.read_recorder_status(status_file)
    if not status.present:
        raise BackupError("native_recorder_status_unreadable")
    if status.is_running():
        raise BackupError("native_recorder_active")


def _container_running(root: Path, container: str) -> bool:
    output = _require_command(
        ["docker", "inspect", container, "--format", "{{.State.Running}}"],
        root=root,
        code="container_state_unavailable",
    ).strip().casefold()
    if output not in {"true", "false"}:
        raise BackupError("container_state_unavailable")
    return output == "true"


def _stop_compose_and_prove(root: Path) -> None:
    containers = {service: _container_id(root, service) for service in CORE_SERVICES}
    completed = _run(["docker", "compose", "stop"], root=root, timeout=300.0)
    if completed.returncode != 0:
        raise BackupError("compose_stop_failed")
    if any(_container_running(root, container) for container in containers.values()):
        raise BackupError("compose_quiescence_unproven")


def _copy_relay(root: Path, layout: BackupLayout, destination: Path) -> None:
    relay_destination = destination / "relay-state"
    relay_destination.mkdir()
    completed = _run(
        [
            "docker",
            "cp",
            f"{layout.relay_container}:{RELAY_TARGET}/.",
            str(relay_destination),
        ],
        root=root,
        timeout=COPY_TIMEOUT_SECONDS,
    )
    if completed.returncode != 0:
        raise BackupError("relay_copy_failed")
    _scan_tree(relay_destination)


def _write_text_atomic(path: Path, text: str) -> None:
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            descriptor = -1
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _write_json_atomic(path: Path, value: object) -> None:
    _write_text_atomic(
        path,
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
    )


def _sqlite_files(root: Path) -> list[Path]:
    matches: list[Path] = []
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            entries = list(os.scandir(current))
        except OSError as exc:
            raise BackupError("backup_copy_unreadable") from exc
        for entry in entries:
            path = Path(entry.path)
            if entry.is_symlink() or _is_reparse_point(path):
                raise BackupError("backup_copy_contains_link")
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise BackupError("backup_copy_unreadable") from exc
            if stat.S_ISDIR(info.st_mode):
                stack.append(path)
                continue
            if not stat.S_ISREG(info.st_mode):
                raise BackupError("backup_copy_contains_special_file")
            if info.st_size < len(SQLITE_HEADER):
                continue
            try:
                with path.open("rb") as stream:
                    header = stream.read(len(SQLITE_HEADER))
            except OSError as exc:
                raise BackupError("backup_copy_unreadable") from exc
            if header == SQLITE_HEADER:
                matches.append(path)
    return sorted(matches)


def _quick_check_sqlite(path: Path) -> None:
    try:
        connection = sqlite3.connect(path, timeout=5.0)
        try:
            row = connection.execute("PRAGMA quick_check").fetchone()
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise BackupError("backup_sqlite_integrity_failed") from exc
    if row != ("ok",):
        raise BackupError("backup_sqlite_integrity_failed")


def _integrity_checks(destination: Path) -> list[str]:
    checked: list[str] = []
    for path in _sqlite_files(destination):
        _quick_check_sqlite(path)
        checked.append(path.relative_to(destination).as_posix())
    return checked


def _directory_digest(root: Path) -> str:
    """Digest the verified SQLite inventory without hashing large payloads."""

    digest = hashlib.sha256()
    for path in sorted(_sqlite_files(root)):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        size = path.stat().st_size
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(size.to_bytes(8, "big"))
    return digest.hexdigest()


def _copy_local_state(root: Path, layout: BackupLayout, destination: Path) -> None:
    _copy_tree(layout.data_dir, destination / "data")
    if layout.results_dir is not None and layout.results_dir != layout.data_dir:
        _copy_tree(layout.results_dir, destination / "results")
    env_file = root / ".env"
    if env_file.exists():
        if _is_reparse_point(env_file) or not env_file.is_file():
            raise BackupError("environment_file_unusable")
        shutil.copy2(env_file, destination / ".env", follow_symlinks=False)


def _create_destination(destination: Path) -> Path:
    destination.mkdir(mode=0o700)
    marker = destination / BACKUP_INCOMPLETE_NAME
    _write_text_atomic(marker, "incomplete\n")
    return marker


def _complete_manifest(
    destination: Path,
    *,
    source_commit: str,
    layout: BackupLayout,
    estimate: TreeEstimate,
    requirement: CapacityRequirement,
    sqlite_checks: list[str],
) -> None:
    manifest = {
        "schema": BACKUP_SCHEMA,
        "complete": True,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_commit": source_commit,
        "relay_volume": layout.relay_volume,
        "estimated_source_bytes": estimate.bytes,
        "estimated_source_files": estimate.files,
        "required_destination_bytes": requirement.bytes,
        "required_destination_inodes": requirement.inodes,
        "sqlite_quick_check": sqlite_checks,
        "sqlite_inventory_sha256": _directory_digest(destination),
        "restart_required": True,
    }
    _write_text_atomic(destination / SOURCE_COMMIT_NAME, source_commit + "\n")
    _write_json_atomic(destination / BACKUP_MANIFEST_NAME, manifest)
    (destination / BACKUP_INCOMPLETE_NAME).unlink()


def _read_recorded_source_commit(root: Path) -> str:
    path = root / SOURCE_COMMIT_NAME
    try:
        value = path.read_text(encoding="utf-8").strip().lower()
    except (OSError, UnicodeDecodeError) as exc:
        raise BackupError("backup_source_commit_unreadable") from exc
    if len(value) != 40 or any(c not in "0123456789abcdef" for c in value):
        raise BackupError("backup_source_commit_unreadable")
    return value


def verify_backup(destination: Path | str) -> dict[str, Any]:
    root = Path(destination).resolve()
    if not root.is_dir() or _is_reparse_point(root):
        raise BackupError("backup_directory_unusable")
    if (root / BACKUP_INCOMPLETE_NAME).exists():
        raise BackupError("backup_incomplete")
    manifest_path = root / BACKUP_MANIFEST_NAME
    try:
        if manifest_path.stat().st_size > MAX_COMPOSE_CONFIG_BYTES:
            raise BackupError("backup_manifest_unreadable")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackupError("backup_manifest_unreadable") from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema") != BACKUP_SCHEMA
        or manifest.get("complete") is not True
    ):
        raise BackupError("backup_manifest_unreadable")
    recorded_commit = _read_recorded_source_commit(root)
    if manifest.get("source_commit") != recorded_commit:
        raise BackupError("backup_source_commit_mismatch")
    checks = _integrity_checks(root)
    expected = manifest.get("sqlite_quick_check")
    if not isinstance(expected, list) or checks != expected:
        raise BackupError("backup_sqlite_inventory_changed")
    if _directory_digest(root) != manifest.get("sqlite_inventory_sha256"):
        raise BackupError("backup_sqlite_inventory_changed")
    return manifest


def create_quiesced_backup(
    root: Path | str,
    destination: Path | str,
    *,
    lock_timeout_seconds: float = HOST_MUTATION_LOCK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    repo_root = Path(root).resolve()
    backup_root = Path(destination).resolve(strict=False)
    _require_clean_checkout(repo_root)
    source_commit = _git_commit(repo_root)
    layout = _resolve_layout(repo_root)
    _refuse_live_native_recorder(layout)

    # Capacity is proven before any production writer is stopped.
    preflight_estimate = _local_estimate(repo_root, layout) + _scan_relay_volume(
        repo_root, layout
    )
    _preflight_destination(repo_root, backup_root, layout, preflight_estimate)

    with host_mutation_lock(repo_root, timeout_seconds=lock_timeout_seconds):
        if _git_commit(repo_root) != source_commit:
            raise BackupError("source_commit_changed")
        locked_layout = _resolve_layout(repo_root)
        if not _same_layout(layout, locked_layout):
            raise BackupError("backup_layout_changed")
        _refuse_live_native_recorder(layout)
        _quiesce_responder(layout.data_dir)
        _stop_compose_and_prove(repo_root)

        final_estimate = _local_estimate(repo_root, layout) + _scan_relay_volume(
            repo_root, layout
        )
        requirement = _preflight_destination(
            repo_root, backup_root, layout, final_estimate
        )
        _create_destination(backup_root)
        _copy_local_state(repo_root, layout, backup_root)
        _copy_relay(repo_root, layout, backup_root)
        sqlite_checks = _integrity_checks(backup_root)
        _complete_manifest(
            backup_root,
            source_commit=source_commit,
            layout=layout,
            estimate=final_estimate,
            requirement=requirement,
            sqlite_checks=sqlite_checks,
        )

    # Explicit resume is intentionally outside the backup primitive.
    return verify_backup(backup_root)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FCP Federation v1 backup tooling")
    subcommands = parser.add_subparsers(dest="command", required=True)

    backup = subcommands.add_parser("backup", help="create a quiesced backup")
    backup.add_argument("--repo-root", default=".")
    backup.add_argument("--destination", required=True)
    backup.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=HOST_MUTATION_LOCK_TIMEOUT_SECONDS,
    )

    verify = subcommands.add_parser("verify", help="verify an existing backup")
    verify.add_argument("--backup", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "backup":
            manifest = create_quiesced_backup(
                arguments.repo_root,
                arguments.destination,
                lock_timeout_seconds=arguments.lock_timeout_seconds,
            )
            print(
                "FCP backup complete and verified at "
                f"{Path(arguments.destination).resolve()}. "
                "FCP remains stopped; inspect the result, then resume explicitly."
            )
        else:
            manifest = verify_backup(arguments.backup)
            print(f"FCP backup verified: {Path(arguments.backup).resolve()}")
        print(f"source commit: {manifest['source_commit']}")
        return 0
    except (BackupError, ValueError) as exc:
        print(f"FCP backup refused: {exc}", file=os.sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
