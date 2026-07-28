"""Shared models and durable helper functions for the MTConnect recorder."""
from __future__ import annotations

import json
import os
import re
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from xml.etree import ElementTree as ET

DEFAULT_SOURCES = {
    "QuickTurn": "http://192.168.200.249:5000",
    "IG500": "http://192.168.200.251:5000",
    "VTC": "http://192.168.200.252:5000",
}


class MtconnectProtocolError(RuntimeError):
    """Raised when an MTConnect Agent returns an invalid or error document."""


@dataclass(frozen=True)
class StreamHeader:
    instance_id: int
    first_sequence: int
    last_sequence: int
    next_sequence: int
    creation_time: str | None = None
    sender: str | None = None
    buffer_size: int | None = None


@dataclass(frozen=True)
class ProbeModel:
    sha256: str
    devices: dict[str, dict[str, Any]]
    data_items: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class ParsedBatch:
    header: StreamHeader
    observations: list[dict[str, Any]]
    first_observation_sequence: int | None
    last_observation_sequence: int | None


@dataclass
class SourceCheckpoint:
    source_name: str
    base_url: str
    machine_id: str
    agent_instance_id: int
    next_sequence: int
    probe_sha256: str
    storage_aliases: list[str] = field(default_factory=list)
    last_raw_file: str | None = None
    last_observation_file: str | None = None
    last_normalized_file: str | None = None
    latest_values: dict[str, dict[str, Any]] = field(default_factory=dict)
    updated_at: str = field(default_factory=lambda: _utc_now())

    @classmethod
    def from_dict(cls, source_name: str, payload: Mapping[str, Any]) -> SourceCheckpoint:
        aliases = payload.get("storage_aliases")
        storage_aliases = (
            [
                alias.strip()
                for alias in aliases
                if isinstance(alias, str)
                and alias.strip()
                and alias.strip() != source_name
            ]
            if isinstance(aliases, list)
            else []
        )
        return cls(
            source_name=source_name,
            base_url=str(payload.get("base_url") or ""),
            machine_id=str(payload.get("machine_id") or source_name),
            agent_instance_id=int(payload["agent_instance_id"]),
            next_sequence=int(payload["next_sequence"]),
            probe_sha256=str(payload.get("probe_sha256") or ""),
            storage_aliases=list(dict.fromkeys(storage_aliases)),
            last_raw_file=(str(payload["last_raw_file"]) if payload.get("last_raw_file") else None),
            last_observation_file=(
                str(payload["last_observation_file"])
                if payload.get("last_observation_file")
                else None
            ),
            last_normalized_file=(
                str(payload["last_normalized_file"])
                if payload.get("last_normalized_file")
                else None
            ),
            latest_values={
                str(machine): dict(values)
                for machine, values in (payload.get("latest_values") or {}).items()
                if isinstance(values, Mapping)
            },
            updated_at=str(payload.get("updated_at") or _utc_now()),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "machine_id": self.machine_id,
            "agent_instance_id": self.agent_instance_id,
            "next_sequence": self.next_sequence,
            "probe_sha256": self.probe_sha256,
            "storage_aliases": self.storage_aliases,
            "last_raw_file": self.last_raw_file,
            "last_observation_file": self.last_observation_file,
            "last_normalized_file": self.last_normalized_file,
            "latest_values": self.latest_values,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class RawBatchRef:
    raw_path: Path
    manifest_path: Path
    raw_sha256: str
    requested_from: int
    first_sequence: int
    last_sequence: int
    next_sequence: int
    observation_count: int


@dataclass(frozen=True)
class StoredBatch:
    raw_path: Path
    observation_path: Path
    normalized_path: Path
    raw_sha256: str
    observation_count: int
    latest_values: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class SequencePlan:
    requested_from: int
    gap_from: int | None = None
    gap_to: int | None = None
    reason: str | None = None


# ---------------------------------------------------------------------------
# Environment and filesystem helpers
# ---------------------------------------------------------------------------


def _bool_from_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _float_from_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int_from_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _fsync_directory(path: Path) -> None:
    """Best-effort directory fsync; unavailable on some platforms."""

    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except (AttributeError, OSError):
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _write_bytes_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)


def _write_text_atomic(path: Path, payload: str) -> None:
    _write_bytes_atomic(path, payload.encode("utf-8"))


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    _write_text_atomic(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return cleaned or "unknown"


def _validate_source_storage_names(sources: Mapping[str, str]) -> None:
    """Reject source names that would share a recorder directory on Windows."""

    names_by_directory: dict[str, str] = {}
    for source_name in sources:
        directory_key = _slug(source_name).casefold()
        existing_name = names_by_directory.get(directory_key)
        if existing_name is not None and existing_name != source_name:
            raise ValueError(
                "Recorder source names map to the same storage directory: "
                f"{existing_name!r} and {source_name!r}. "
                "Use distinct MTConnect UUIDs or serial numbers."
            )
        names_by_directory[directory_key] = source_name


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _find_first(root: ET.Element, local_name: str) -> ET.Element | None:
    return next((element for element in root.iter() if _local_name(element.tag) == local_name), None)


def _try_number(value: str | None) -> Any:
    if value is None:
        return None
    stripped = value.strip()
    if stripped == "":
        return ""
    try:
        return int(stripped)
    except ValueError:
        try:
            return float(stripped)
        except ValueError:
            return stripped


def normalize_agent_base_url(url: str) -> str:
    """Return an MTConnect Agent base URL from base/current/sample/probe input."""

    candidate = url.strip()
    if not candidate:
        raise ValueError("MTConnect source URL cannot be empty.")
    parts = urlsplit(candidate)
    if not parts.scheme or not parts.netloc:
        raise ValueError(f"MTConnect source must be an absolute HTTP URL: {url!r}")
    path_parts = [part for part in parts.path.split("/") if part]
    if path_parts and path_parts[-1].lower() in {"current", "sample", "probe"}:
        path_parts.pop()
    normalized_path = "/" + "/".join(path_parts) if path_parts else ""
    return urlunsplit((parts.scheme, parts.netloc, normalized_path.rstrip("/"), "", ""))


def _parse_sources_text(value: str) -> dict[str, str]:
    sources: dict[str, str] = {}
    for item in value.split(";"):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(
                "Recorder source entries must use MachineName=http://host:port format."
            )
        name, url = item.split("=", 1)
        name = name.strip()
        url = normalize_agent_base_url(url)
        if name:
            if name in sources:
                raise ValueError(
                    f"Recorder source name is duplicated: {name}. "
                    "Use the MTConnect machine UUID or another unique identity."
                )
            sources[name] = url
    _validate_source_storage_names(sources)
    return sources


def _sources_from_environment() -> dict[str, str]:
    json_text = os.getenv("MSH_RECORDER_SOURCES_JSON", "").strip()
    if json_text:
        payload = json.loads(json_text)
        if not isinstance(payload, dict):
            raise ValueError("MSH_RECORDER_SOURCES_JSON must be an object.")
        sources = {
            str(name).strip(): normalize_agent_base_url(str(url))
            for name, url in payload.items()
            if str(name).strip() and str(url).strip()
        }
        if not sources:
            raise ValueError("MSH_RECORDER_SOURCES_JSON contains no valid sources.")
        _validate_source_storage_names(sources)
        return sources

    list_text = os.getenv("MSH_RECORDER_SOURCES", "").strip()
    if list_text:
        sources = _parse_sources_text(list_text)
        if not sources:
            raise ValueError("MSH_RECORDER_SOURCES contains no valid sources.")
        return sources

    return dict(DEFAULT_SOURCES)
