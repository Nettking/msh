"""Deprecated technical import shim during CF8 source cleanup.

The persisted server-role model has been removed. Runtime/product configuration is
``CapabilityConfig`` and legacy role-shaped JSON is parsed only by
``legacy_settings_migration``. New code must import capability services directly;
this module contains no deployment-role state, parser, writer, or authority logic.
"""

from __future__ import annotations

from .capability_ai_service import (
    AI_BENCHMARK_PROMPT,
    AI_RESPONSE_TIME_BANDS,
    benchmark_ollama_response_time,
    ollama_status,
    pull_ollama_model,
    response_time_assessment,
)
from .capability_config_service import (
    AI_MODEL_CHOICES,
    AI_PROVIDER_MODES,
    CAPABILITY_CONFIG_PATH,
    DEFAULT_AI_PROVIDER_MODE,
    DEFAULT_OLLAMA_BASE_URL,
    CapabilityConfig,
    CapabilityConfigError,
    default_capability_config,
    format_recorder_sources,
    load_capability_config,
    normalize_ollama_base_url,
    parse_recorder_sources,
    save_capability_config,
    update_language_model_config,
    update_recorder_config,
)

# Temporary source-compatibility aliases for modules being retargeted in CF8.
# They point at capability configuration and cannot read or write deployment roles.
SETTINGS_PATH = CAPABILITY_CONFIG_PATH
ServerSetupError = CapabilityConfigError
load_settings = load_capability_config
save_settings = save_capability_config


__all__ = [
    "AI_BENCHMARK_PROMPT",
    "AI_MODEL_CHOICES",
    "AI_PROVIDER_MODES",
    "AI_RESPONSE_TIME_BANDS",
    "CAPABILITY_CONFIG_PATH",
    "DEFAULT_AI_PROVIDER_MODE",
    "DEFAULT_OLLAMA_BASE_URL",
    "SETTINGS_PATH",
    "CapabilityConfig",
    "CapabilityConfigError",
    "ServerSetupError",
    "benchmark_ollama_response_time",
    "default_capability_config",
    "format_recorder_sources",
    "load_capability_config",
    "load_settings",
    "normalize_ollama_base_url",
    "ollama_status",
    "parse_recorder_sources",
    "pull_ollama_model",
    "response_time_assessment",
    "save_capability_config",
    "save_settings",
    "update_language_model_config",
    "update_recorder_config",
]
