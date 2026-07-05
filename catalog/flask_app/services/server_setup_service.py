"""Browser-managed server setup for one-command MSH startup.

The intent is that a fresh checkout can be started with one command:

    docker compose up -d --build

Then /startup is used to choose the local role and AI model. Command-driven setup
is still supported by setup_msh.py for scripted deployments.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from urllib import request


SETTINGS_PATH = Path("data") / "server_setup" / "server_settings.json"
DEFAULT_OLLAMA_BASE_URL = "http://ollama:11434"

DEPLOYMENT_MODES: dict[str, dict[str, str]] = {
    "full-server": {
        "label": "Full server",
        "description": "Web workbench plus MTConnect recorder settings for a complete MSH station.",
        "runtime": "enabled",
    },
    "web-workbench": {
        "label": "Web workbench",
        "description": "Recommended. Analysis UI, playback, source settings, strategy capture, and AI.",
        "runtime": "enabled",
    },
    "web-ui-only": {
        "label": "Web UI only",
        "description": "Inspect existing files without background orchestration. Useful for debugging.",
        "runtime": "disabled",
    },
    "recorder-only": {
        "label": "Recorder only",
        "description": "Data capture role. Best used with command-driven setup when no web UI is needed.",
        "runtime": "disabled",
    },
}

AI_MODEL_CHOICES: dict[str, dict[str, str]] = {
    "edge-small": {
        "label": "Edge small",
        "model": "smollm2:360m",
        "device": "Small CPU, Raspberry Pi class, or very low memory testing.",
    },
    "laptop-standard": {
        "label": "Laptop standard",
        "model": "llama3.2:3b",
        "device": "Normal laptop or small server. Default balance.",
    },
    "workstation-strong": {
        "label": "Workstation strong",
        "model": "qwen2.5:7b",
        "device": "Gaming laptop, workstation, or GPU server. Stronger answers.",
    },
}


@dataclass(frozen=True)
class ServerSetupSettings:
    configured: bool
    deployment_mode: str
    ai_enabled: bool
    ai_profile: str
    ai_model: str
    ollama_base_url: str
    recorder_sources: str
    recorder_poll_interval: str
    recorder_include_condition: bool
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


class ServerSetupError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_settings(*, configured: bool = False) -> ServerSetupSettings:
    return ServerSetupSettings(
        configured=configured,
        deployment_mode="web-workbench",
        ai_enabled=True,
        ai_profile="laptop-standard",
        ai_model=AI_MODEL_CHOICES["laptop-standard"]["model"],
        ollama_base_url=DEFAULT_OLLAMA_BASE_URL,
        recorder_sources="",
        recorder_poll_interval="0.2",
        recorder_include_condition=False,
        updated_at="",
    )


def load_settings(path: Path | str = SETTINGS_PATH) -> ServerSetupSettings:
    path = Path(path)
    if not path.exists():
        return default_settings(configured=False)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ServerSetupError(f"Could not read server setup settings: {path}") from exc
    if not isinstance(payload, dict):
        raise ServerSetupError(f"Server setup settings must be a JSON object: {path}")
    defaults = default_settings(configured=False).to_dict()
    values = {**defaults, **payload}
    if values.get("ai_profile") in AI_MODEL_CHOICES:
        values["ai_model"] = AI_MODEL_CHOICES[str(values["ai_profile"])] ["model"]
    return ServerSetupSettings(
        configured=bool(values.get("configured")),
        deployment_mode=str(values.get("deployment_mode") or defaults["deployment_mode"]),
        ai_enabled=bool(values.get("ai_enabled")),
        ai_profile=str(values.get("ai_profile") or defaults["ai_profile"]),
        ai_model=str(values.get("ai_model") or defaults["ai_model"]),
        ollama_base_url=str(values.get("ollama_base_url") or defaults["ollama_base_url"]),
        recorder_sources=str(values.get("recorder_sources") or ""),
        recorder_poll_interval=str(values.get("recorder_poll_interval") or defaults["recorder_poll_interval"]),
        recorder_include_condition=bool(values.get("recorder_include_condition")),
        updated_at=str(values.get("updated_at") or ""),
    )


def settings_from_form(form: Any) -> ServerSetupSettings:
    deployment_mode = str(form.get("deployment_mode") or "web-workbench").strip()
    if deployment_mode not in DEPLOYMENT_MODES:
        raise ServerSetupError("Unknown deployment mode.")
    ai_enabled = form.get("ai_enabled") == "on"
    ai_profile = str(form.get("ai_profile") or "laptop-standard").strip()
    if ai_profile not in AI_MODEL_CHOICES:
        raise ServerSetupError("Unknown AI model profile.")
    return ServerSetupSettings(
        configured=True,
        deployment_mode=deployment_mode,
        ai_enabled=ai_enabled,
        ai_profile=ai_profile,
        ai_model=AI_MODEL_CHOICES[ai_profile]["model"],
        ollama_base_url=DEFAULT_OLLAMA_BASE_URL,
        recorder_sources=str(form.get("recorder_sources") or "").strip(),
        recorder_poll_interval=str(form.get("recorder_poll_interval") or "0.2").strip() or "0.2",
        recorder_include_condition=form.get("recorder_include_condition") == "on",
        updated_at=utc_now(),
    )


def save_settings(settings: ServerSetupSettings, path: Path | str = SETTINGS_PATH) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema": "msh.server_setup.v1", **settings.to_dict()}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def runtime_should_start(settings: ServerSetupSettings | None = None) -> bool:
    settings = settings or load_settings()
    if not settings.configured:
        return False
    return settings.deployment_mode in {"full-server", "web-workbench"}


def compose_profiles_for(settings: ServerSetupSettings) -> str:
    profiles: list[str] = []
    if settings.deployment_mode == "full-server":
        profiles.append("full")
    elif settings.deployment_mode == "recorder-only":
        profiles.append("recorder")
    elif settings.deployment_mode in {"web-workbench", "web-ui-only"}:
        profiles.append("web")
    if settings.ai_enabled:
        profiles.append("ai")
    return ",".join(profiles)


def env_lines_for(settings: ServerSetupSettings) -> list[str]:
    skip = "0" if runtime_should_start(settings) else "1"
    return [
        f"MSH_DEPLOYMENT_MODE={settings.deployment_mode}",
        f"COMPOSE_PROFILES={compose_profiles_for(settings)}",
        f"MSH_SKIP_ORCHESTRATION={skip}",
        "MSH_SCAN_DIRS=results,data",
        f"MSH_AI_PROFILE={settings.ai_profile}",
        f"MSH_AI_MODEL={settings.ai_model}",
        f"OLLAMA_BASE_URL={settings.ollama_base_url}",
        f"MSH_RECORDER_SOURCES={settings.recorder_sources}",
        "MSH_RECORDER_DATA_DIR=data",
        "MSH_RECORDER_STATE_FILE=data/source_state/mtconnect_recorder_state.json",
        f"MSH_RECORDER_POLL_INTERVAL={settings.recorder_poll_interval}",
        "MSH_RECORDER_FLUSH_INTERVAL=1.0",
        "MSH_RECORDER_REQUEST_TIMEOUT=1.0",
        f"MSH_RECORDER_INCLUDE_CONDITION={'true' if settings.recorder_include_condition else 'false'}",
    ]


def _model_aliases(model_name: str) -> set[str]:
    base = model_name.split(":", 1)[0]
    return {model_name, base}


def ollama_status(settings: ServerSetupSettings | None = None, timeout_seconds: float = 2.0) -> dict[str, Any]:
    settings = settings or load_settings()
    if not settings.ai_enabled:
        return {
            "running": False,
            "selected_model": settings.ai_model,
            "selected_model_installed": False,
            "models": [],
            "installed_by_profile": {key: False for key in AI_MODEL_CHOICES},
            "installed_by_model": {choice["model"]: False for choice in AI_MODEL_CHOICES.values()},
            "message": "AI is disabled in setup.",
        }
    try:
        req = request.Request(f"{settings.ollama_base_url.rstrip('/')}/api/tags", method="GET")
        with request.urlopen(req, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # pragma: no cover - depends on local Docker/Ollama runtime
        return {
            "running": False,
            "selected_model": settings.ai_model,
            "selected_model_installed": False,
            "models": [],
            "installed_by_profile": {key: False for key in AI_MODEL_CHOICES},
            "installed_by_model": {choice["model"]: False for choice in AI_MODEL_CHOICES.values()},
            "message": f"Ollama is not reachable yet: {exc}",
        }
    models = sorted(str(item.get("name") or "") for item in payload.get("models", []) if item.get("name"))
    installed_names = set(models)
    installed_by_profile = {
        key: bool(installed_names & _model_aliases(choice["model"])) for key, choice in AI_MODEL_CHOICES.items()
    }
    installed_by_model = {
        choice["model"]: installed_by_profile[key] for key, choice in AI_MODEL_CHOICES.items()
    }
    selected = settings.ai_model
    selected_installed = bool(installed_names & _model_aliases(selected))
    return {
        "running": True,
        "selected_model": selected,
        "selected_model_installed": selected_installed,
        "models": models,
        "installed_by_profile": installed_by_profile,
        "installed_by_model": installed_by_model,
        "message": "Ollama is running." if selected_installed else f"Ollama is running, but {selected} is not installed yet.",
    }


def pull_ollama_model(settings: ServerSetupSettings, timeout_seconds: int = 900) -> tuple[bool, str]:
    if not settings.ai_enabled:
        return False, "AI is not enabled in server setup."
    payload = json.dumps({"name": settings.ai_model, "stream": False}).encode("utf-8")
    req = request.Request(
        f"{settings.ollama_base_url.rstrip('/')}/api/pull",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
    except Exception as exc:  # pragma: no cover - depends on local Docker/Ollama runtime
        return False, f"Could not pull {settings.ai_model}: {exc}"
    return True, f"Ollama model is installed or updated: {settings.ai_model}. Response: {body[:200]}"
