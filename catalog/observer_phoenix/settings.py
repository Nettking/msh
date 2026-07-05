"""Runtime configuration helpers for the Observer Phoenix connector.

Environment variables remain the highest-precedence configuration source. The
Flask settings page writes a local runtime JSON file under data/source_config so
operators can configure a development or lab instance without committing secrets.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from catalog.observer_phoenix.client import DEFAULT_TIMEOUT_SECONDS, ObserverPhoenixConfig, ObserverPhoenixError


SOURCE_NAME = "observer_phoenix"
DEFAULT_BASE_URL = "https://localhost:14050"
CONFIG_DIR_NAME = "source_config"
CONFIG_FILE_NAME = "observer_phoenix.json"


@dataclass(frozen=True)
class SettingsSnapshot:
    active_source: str
    active_label: str
    configured: bool
    uses_default: bool
    env_status: str
    local_status: str
    config_path: str
    base_url: str
    username: str
    password_set: bool
    verify_tls: bool
    timeout_seconds: int
    last_test: dict[str, Any] | None
    message: str


def config_path(data_dir: Path | str = Path("data")) -> Path:
    return Path(data_dir) / CONFIG_DIR_NAME / CONFIG_FILE_NAME


def _bool_from_text(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ObserverPhoenixError(f"Could not read Observer Phoenix settings: {path}") from exc
    if not isinstance(payload, dict):
        raise ObserverPhoenixError(f"Observer Phoenix settings must be a JSON object: {path}")
    return payload


def load_local_settings(data_dir: Path | str = Path("data")) -> dict[str, Any]:
    return _load_json(config_path(data_dir))


def _env_payload() -> dict[str, Any]:
    return {
        "base_url": os.getenv("OBSERVER_PHOENIX_BASE_URL", "").strip(),
        "username": os.getenv("OBSERVER_PHOENIX_USERNAME", "").strip(),
        "password": os.getenv("OBSERVER_PHOENIX_PASSWORD", ""),
        "verify_tls": _bool_from_text(os.getenv("OBSERVER_PHOENIX_VERIFY_TLS"), True),
        "timeout_seconds": int(os.getenv("OBSERVER_PHOENIX_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS))),
    }


def _status(payload: dict[str, Any]) -> str:
    has_base_url = bool(str(payload.get("base_url") or "").strip())
    has_username = bool(str(payload.get("username") or "").strip())
    has_password = bool(str(payload.get("password") or ""))
    if has_base_url and has_username and has_password:
        return "complete"
    if has_base_url or has_username or has_password:
        return "partial"
    return "missing"


def _config_from_payload(payload: dict[str, Any]) -> ObserverPhoenixConfig:
    return ObserverPhoenixConfig(
        base_url=str(payload.get("base_url") or "").strip(),
        username=str(payload.get("username") or "").strip(),
        password=str(payload.get("password") or ""),
        timeout_seconds=int(payload.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS),
        verify_tls=_bool_from_text(payload.get("verify_tls"), True),
    )


def resolve_runtime_config(data_dir: Path | str = Path("data")) -> tuple[ObserverPhoenixConfig, str]:
    """Return the active connector config and its source label.

    Precedence is environment variables first, then the local runtime settings
    file. Partial settings are treated as errors because silently falling back can
    hide incorrect credentials.
    """

    env_payload = _env_payload()
    env_status = _status(env_payload)
    if env_status == "complete":
        return _config_from_payload(env_payload), "environment"
    if env_status == "partial":
        raise ObserverPhoenixError("Observer Phoenix environment variables are partially set; provide base URL, username, and password.")

    local_payload = load_local_settings(data_dir)
    local_status = _status(local_payload)
    if local_status == "complete":
        return _config_from_payload(local_payload), "local"
    if local_status == "partial":
        raise ObserverPhoenixError("Observer Phoenix local settings are partially set; provide base URL, username, and password.")

    raise ObserverPhoenixError("Observer Phoenix credentials are not configured. Set them on the source settings page or with environment variables.")


def snapshot(data_dir: Path | str = Path("data")) -> SettingsSnapshot:
    path = config_path(data_dir)
    env_payload = _env_payload()
    local_payload = load_local_settings(data_dir)
    env_status = _status(env_payload)
    local_status = _status(local_payload)
    last_test = local_payload.get("last_test") if isinstance(local_payload.get("last_test"), dict) else None

    if env_status == "complete":
        active = _config_from_payload(env_payload)
        return SettingsSnapshot(
            active_source="environment",
            active_label="Environment variables",
            configured=True,
            uses_default=False,
            env_status=env_status,
            local_status=local_status,
            config_path=path.as_posix(),
            base_url=active.base_url,
            username=active.username,
            password_set=True,
            verify_tls=active.verify_tls,
            timeout_seconds=active.timeout_seconds,
            last_test=last_test,
            message="Credentials are configured from environment variables. Local settings are ignored while environment variables are complete.",
        )

    if local_status == "complete":
        active = _config_from_payload(local_payload)
        return SettingsSnapshot(
            active_source="local",
            active_label="Local runtime settings",
            configured=True,
            uses_default=False,
            env_status=env_status,
            local_status=local_status,
            config_path=path.as_posix(),
            base_url=active.base_url,
            username=active.username,
            password_set=True,
            verify_tls=active.verify_tls,
            timeout_seconds=active.timeout_seconds,
            last_test=last_test,
            message="Credentials are configured from the local runtime settings file.",
        )

    return SettingsSnapshot(
        active_source="default",
        active_label="Default / not configured",
        configured=False,
        uses_default=True,
        env_status=env_status,
        local_status=local_status,
        config_path=path.as_posix(),
        base_url=str(local_payload.get("base_url") or DEFAULT_BASE_URL),
        username=str(local_payload.get("username") or ""),
        password_set=bool(local_payload.get("password")),
        verify_tls=_bool_from_text(local_payload.get("verify_tls"), True),
        timeout_seconds=int(local_payload.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS),
        last_test=last_test,
        message="Observer Phoenix is using default/unset settings. Save credentials before synchronizing data.",
    )


def save_local_settings(
    *,
    data_dir: Path | str = Path("data"),
    base_url: str,
    username: str,
    password: str | None,
    verify_tls: bool,
    timeout_seconds: int,
    keep_existing_password: bool = True,
) -> Path:
    path = config_path(data_dir)
    existing = load_local_settings(data_dir)
    effective_password = password if password is not None and password != "" else (existing.get("password") if keep_existing_password else "")
    payload = {
        "base_url": base_url.strip(),
        "username": username.strip(),
        "password": str(effective_password or ""),
        "verify_tls": bool(verify_tls),
        "timeout_seconds": int(timeout_seconds or DEFAULT_TIMEOUT_SECONDS),
        "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    if isinstance(existing.get("last_test"), dict):
        payload["last_test"] = existing["last_test"]
    status = _status(payload)
    if status != "complete":
        raise ObserverPhoenixError("Base URL, username, and password are required before saving Observer Phoenix credentials.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def clear_local_settings(data_dir: Path | str = Path("data")) -> None:
    path = config_path(data_dir)
    if path.exists():
        path.unlink()


def save_last_test(data_dir: Path | str, *, connected: bool, message: str, machine_count: int | None, source: str) -> Path:
    path = config_path(data_dir)
    payload = load_local_settings(data_dir)
    payload["last_test"] = {
        "connected": bool(connected),
        "message": message,
        "machine_count": machine_count,
        "source": source,
        "tested_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
