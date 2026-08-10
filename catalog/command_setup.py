"""Role-free command deployment planning for scripted FCP installs.

Command profiles are local process-composition choices. They are deliberately
separate from capability onboarding and contribution authority. Technical AI and
recorder parameters live in ``CapabilityConfig``; selecting a command profile
never enables a contribution or marks onboarding complete.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from urllib import request

from catalog.flask_app.services.capability_config_service import (
    AI_MODEL_CHOICES,
    AI_PROVIDER_MODES,
    CapabilityConfig,
    CapabilityConfigError,
    default_capability_config,
    update_language_model_config,
    update_recorder_config,
)

COMMAND_PROFILES: dict[str, dict[str, object]] = {
    "workbench": {
        "label": "Workbench",
        "description": "Start the normal FCP product; capability intent decides optional contributions.",
        "skip_orchestration": False,
    },
    "web-only": {
        "label": "Web only",
        "description": "Start the web product while suppressing background orchestration locally.",
        "skip_orchestration": True,
    },
    "recorder": {
        "label": "Recorder host",
        "description": "Start the web and managed recorder processes; contribution authority still gates recording.",
        "skip_orchestration": True,
    },
    "language-model-provider": {
        "label": "Language-model provider",
        "description": "Run only the headless Ollama provider service.",
        "skip_orchestration": True,
        "compose_profile": "provider",
    },
    "prep": {
        "label": "Prep one-shot",
        "description": "Run the orchestration preparation CLI once.",
        "skip_orchestration": True,
        "compose_profile": "prep",
        "one_shot_service": "prep",
    },
    "observer-sync": {
        "label": "Observer sync one-shot",
        "description": "Run one Observer Phoenix synchronization.",
        "skip_orchestration": True,
        "compose_profile": "observer-sync",
        "one_shot_service": "observer-sync",
    },
}

# Existing scripts may keep their old spelling. These aliases are command-line
# compatibility only; they are never persisted as device roles.
LEGACY_PROFILE_ALIASES = {
    "full-server": "workbench",
    "web-workbench": "workbench",
    "web-ui-only": "web-only",
    "recorder-only": "recorder",
    "prep-only": "prep",
    "observer-sync-only": "observer-sync",
}


@dataclass(frozen=True)
class CommandDeploymentPlan:
    profile: str
    config: CapabilityConfig
    skip_orchestration: bool
    compose_profile: str
    one_shot_service: str | None
    provision_local_model: bool

    @property
    def provider_node(self) -> bool:
        return self.profile == "language-model-provider"


def accepted_profile_names() -> tuple[str, ...]:
    return tuple(sorted({*COMMAND_PROFILES, *LEGACY_PROFILE_ALIASES}))


def normalize_command_profile(value: str | None) -> str:
    raw = str(value or "workbench").strip()
    profile = LEGACY_PROFILE_ALIASES.get(raw, raw)
    if profile not in COMMAND_PROFILES:
        raise CapabilityConfigError(f"Unknown command profile: {raw}")
    return profile


def build_command_plan(
    *,
    profile: str | None,
    ai_profile: str | None,
    no_ai: bool,
    ai_provider: str | None,
    ai_provider_name: str,
    ollama_url: str,
    recorder_sources: str,
    recorder_poll_interval: str,
    recorder_include_condition: bool,
) -> CommandDeploymentPlan:
    profile = normalize_command_profile(profile)
    definition = COMMAND_PROFILES[profile]
    provider_node = profile == "language-model-provider"
    one_shot = definition.get("one_shot_service") is not None

    selected_ai_profile = ai_profile or (
        "edge-small" if provider_node else "laptop-standard"
    )
    if selected_ai_profile not in AI_MODEL_CHOICES:
        raise CapabilityConfigError(f"Unknown AI profile: {selected_ai_profile}")
    if provider_node and no_ai:
        raise CapabilityConfigError(
            "--no-ai cannot be used with the language-model-provider profile."
        )

    requested_provider_mode = str(ai_provider or "local").strip()
    if requested_provider_mode not in AI_PROVIDER_MODES:
        raise CapabilityConfigError(f"Unknown AI provider: {requested_provider_mode}")
    if provider_node and requested_provider_mode == "connected":
        raise CapabilityConfigError(
            "A language-model provider must run its own local model."
        )
    provider_mode = "local" if provider_node else requested_provider_mode

    config = default_capability_config()
    config = update_language_model_config(
        config,
        {
            "ai_profile": selected_ai_profile,
            "ai_provider_mode": provider_mode,
            "ai_provider_name": ai_provider_name,
            "ollama_base_url": ollama_url,
        },
    )
    config = update_recorder_config(
        config,
        recorder_sources=recorder_sources,
        recorder_poll_interval=recorder_poll_interval,
        recorder_include_condition=recorder_include_condition,
    )

    provision_local_model = (
        not no_ai
        and not one_shot
        and profile != "recorder"
        and config.ai_provider_mode == "local"
    )
    return CommandDeploymentPlan(
        profile=profile,
        config=config,
        skip_orchestration=bool(definition.get("skip_orchestration", True)),
        compose_profile=str(definition.get("compose_profile") or ""),
        one_shot_service=(
            str(definition["one_shot_service"])
            if definition.get("one_shot_service")
            else None
        ),
        provision_local_model=provision_local_model,
    )


def env_lines_for_plan(plan: CommandDeploymentPlan) -> list[str]:
    """Return process configuration without emitting legacy role authority."""

    config = plan.config
    lines = [
        f"COMPOSE_PROFILES={plan.compose_profile}",
        f"FCP_SKIP_ORCHESTRATION={'1' if plan.skip_orchestration else '0'}",
        "FCP_SCAN_DIRS=results,data",
        f"FCP_AI_PROFILE={config.ai_profile}",
        f"FCP_AI_MODEL={config.ai_model}",
        f"OLLAMA_BASE_URL={config.ollama_base_url}",
        f"FCP_RECORDER_SOURCES={config.recorder_sources}",
        "FCP_RECORDER_DATA_DIR=data",
        "FCP_RECORDER_STATE_FILE=data/source_state/mtconnect_recorder_state.json",
        f"FCP_RECORDER_POLL_INTERVAL={config.recorder_poll_interval}",
        "FCP_RECORDER_FLUSH_INTERVAL=1.0",
        "FCP_RECORDER_REQUEST_TIMEOUT=1.0",
        "FCP_RECORDER_INCLUDE_CONDITION="
        + ("true" if config.recorder_include_condition else "false"),
    ]
    if plan.provider_node:
        lines.extend(
            [
                "FCP_PROVIDER_BIND=0.0.0.0",
                "FCP_PROVIDER_PORT=11434",
                f"FCP_PROVIDER_MODEL={config.ai_model}",
            ]
        )
    return lines


def pull_connected_model(
    config: CapabilityConfig,
    *,
    timeout_seconds: int = 900,
) -> tuple[bool, str]:
    """Request model installation from an explicitly connected Ollama endpoint."""

    if config.ai_provider_mode != "connected":
        return False, "The configured AI provider is not a connected computer."
    payload = json.dumps({"name": config.ai_model, "stream": False}).encode("utf-8")
    req = request.Request(
        f"{config.ollama_base_url.rstrip('/')}/api/pull",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
    except (OSError, TimeoutError) as exc:  # pragma: no cover - remote runtime
        return False, f"Could not pull {config.ai_model}: {exc}"
    provider_name = config.ai_provider_name or "Connected computer"
    return True, (
        f"Ollama model is installed or updated on {provider_name}: "
        f"{config.ai_model}. Response: {body[:200]}"
    )


__all__ = [
    "COMMAND_PROFILES",
    "LEGACY_PROFILE_ALIASES",
    "CommandDeploymentPlan",
    "accepted_profile_names",
    "build_command_plan",
    "env_lines_for_plan",
    "normalize_command_profile",
    "pull_connected_model",
]
