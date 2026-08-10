"""Read-only compatibility helpers for role-first setup migration."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar

from .errors import FederationValidationError
from .onboarding_models import (
    ContributionDesiredState,
    OnboardingModel,
    _optional_safe_text,
    _safe_text,
    _safe_text_tuple,
    _uint,
    _utc,
)

_RETIRED_PRODUCT = bytes((109, 115, 104)).decode("ascii")
LEGACY_SETUP_SCHEMAS = {
    None,
    "fcp.server_setup.v1",
    "fcp.server_setup.v2",
    "fcp.server_setup.v3",
    f"{_RETIRED_PRODUCT}.server_setup.v3",
}
LEGACY_DEPLOYMENT_MODES = {
    "full-server",
    "web-workbench",
    "web-ui-only",
    "recorder-only",
    "language-model-provider",
}
CONTRIBUTION_KEYS = (
    "workbench",
    "runtime",
    "recorder",
    "language-model",
    "compute",
    "storage",
)


def federation_id_from_session_id(session_id: str) -> str:
    """Derive a stable public federation ID from an existing session ID."""

    session_id = _safe_text(session_id, "session_id")
    digest = hashlib.sha256(
        ("fcp-federation-id-v1\0" + session_id).encode("utf-8")
    ).hexdigest()
    return f"federation-{digest[:32]}"


def federation_id_matches_session(
    federation_id: str,
    internal_session_id: str,
) -> bool:
    federation_id = _safe_text(federation_id, "federation_id")
    return federation_id == federation_id_from_session_id(internal_session_id)


@dataclass(frozen=True)
class LegacySetupMigrationPreview(OnboardingModel):
    """Safe preview of a future migration; never an activation command."""

    SCHEMA: ClassVar[str] = "fcp.onboarding.legacy-migration-preview.v1"

    source_schema: str
    source_mode: str
    contribution_intents: dict[str, str]
    preserved_private_fields: tuple[str, ...]
    preserved_safe_fields: dict[str, Any]
    warnings: tuple[str, ...]
    preview_revision: int
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_schema",
            _safe_text(self.source_schema, "source_schema"),
        )
        object.__setattr__(
            self,
            "source_mode",
            _safe_text(self.source_mode, "source_mode"),
        )
        if self.source_mode not in LEGACY_DEPLOYMENT_MODES:
            raise FederationValidationError(
                "unknown-deployment-mode",
                "source_mode",
                "is not a supported legacy deployment mode",
            )
        if not isinstance(self.contribution_intents, dict):
            raise FederationValidationError(
                "invalid-object",
                "contribution_intents",
                "must be an object",
            )
        if set(self.contribution_intents) != set(CONTRIBUTION_KEYS):
            raise FederationValidationError(
                "invalid-contribution-set",
                "contribution_intents",
                "must contain exactly the supported contribution keys",
            )
        normalized: dict[str, str] = {}
        for key in CONTRIBUTION_KEYS:
            try:
                normalized[key] = ContributionDesiredState(
                    self.contribution_intents[key]
                ).value
            except (TypeError, ValueError) as exc:
                raise FederationValidationError(
                    "invalid-enum",
                    f"contribution_intents.{key}",
                    "unknown desired state",
                ) from exc
        object.__setattr__(self, "contribution_intents", normalized)
        object.__setattr__(
            self,
            "preserved_private_fields",
            _safe_text_tuple(
                self.preserved_private_fields,
                "preserved_private_fields",
            ),
        )
        if not isinstance(self.preserved_safe_fields, dict):
            raise FederationValidationError(
                "invalid-object",
                "preserved_safe_fields",
                "must be an object",
            )
        allowed = {
            "ai_enabled",
            "ai_profile",
            "ai_model",
            "ai_provider_mode",
            "ai_provider_name",
            "recorder_configured",
            "recorder_poll_interval",
            "recorder_include_condition",
            "user_setup_complete",
        }
        if not set(self.preserved_safe_fields).issubset(allowed):
            raise FederationValidationError(
                "unexpected-field",
                "preserved_safe_fields",
                "contains a field that is not safe for migration preview",
            )
        safe_fields: dict[str, Any] = {}
        for key, value in self.preserved_safe_fields.items():
            if isinstance(value, bool) or value is None:
                safe_fields[key] = value
            else:
                safe_fields[key] = _safe_text(
                    value,
                    f"preserved_safe_fields.{key}",
                )
        object.__setattr__(self, "preserved_safe_fields", safe_fields)
        object.__setattr__(
            self,
            "warnings",
            _safe_text_tuple(self.warnings, "warnings"),
        )
        object.__setattr__(
            self,
            "preview_revision",
            _uint(self.preview_revision, "preview_revision"),
        )
        object.__setattr__(
            self,
            "created_at",
            _utc(self.created_at, "created_at"),
        )


def _boolean(payload: dict[str, Any], key: str, default: bool) -> bool:
    value = payload.get(key, default)
    if not isinstance(value, bool):
        raise FederationValidationError(
            "invalid-boolean",
            key,
            "must be a boolean",
        )
    return value


def _optional_text_field(
    payload: dict[str, Any],
    key: str,
    default: str | None = None,
) -> str | None:
    return _optional_safe_text(payload.get(key, default), key)


def _base_intents() -> dict[str, str]:
    return {
        key: ContributionDesiredState.DISABLED.value
        for key in CONTRIBUTION_KEYS
    }


def preview_legacy_setup(
    payload: dict[str, Any],
    *,
    created_at: datetime,
) -> LegacySetupMigrationPreview:
    """Create a deterministic preview without mutating files or runtime state."""

    if not isinstance(payload, dict):
        raise FederationValidationError(
            "invalid-object",
            "legacy_setup",
            "must be an object",
        )
    schema = payload.get("schema")
    if schema not in LEGACY_SETUP_SCHEMAS:
        raise FederationValidationError(
            "unsupported-legacy-schema",
            "schema",
            "is not a supported server setup schema",
        )
    mode = _safe_text(
        payload.get("deployment_mode", "web-workbench"),
        "deployment_mode",
    )
    if mode not in LEGACY_DEPLOYMENT_MODES:
        raise FederationValidationError(
            "unknown-deployment-mode",
            "deployment_mode",
            "is not a supported legacy deployment mode",
        )

    ai_enabled = _boolean(payload, "ai_enabled", True)
    recorder_sources = payload.get("recorder_sources", "")
    if not isinstance(recorder_sources, str):
        raise FederationValidationError(
            "invalid-text",
            "recorder_sources",
            "must be a string",
        )
    recorder_configured = bool(recorder_sources.strip())
    configured = _boolean(payload, "configured", False)
    user_setup_complete = _boolean(
        payload,
        "user_setup_complete",
        configured,
    )

    intents = _base_intents()
    if mode == "full-server":
        intents["workbench"] = ContributionDesiredState.ENABLED.value
        intents["runtime"] = ContributionDesiredState.ENABLED.value
        intents["recorder"] = ContributionDesiredState.ENABLED.value
    elif mode == "web-workbench":
        intents["workbench"] = ContributionDesiredState.ENABLED.value
        intents["runtime"] = ContributionDesiredState.ENABLED.value
    elif mode == "web-ui-only":
        intents["workbench"] = ContributionDesiredState.ENABLED.value
    elif mode == "recorder-only":
        intents["recorder"] = ContributionDesiredState.ENABLED.value

    if ai_enabled and mode != "recorder-only":
        intents["language-model"] = ContributionDesiredState.ENABLED.value
    if mode == "language-model-provider":
        intents["language-model"] = ContributionDesiredState.ENABLED.value

    warnings: list[str] = []
    if (
        intents["recorder"] == ContributionDesiredState.ENABLED.value
        and not recorder_configured
    ):
        warnings.append("Recorder contribution needs a selected data source.")
    if (
        intents["language-model"] == ContributionDesiredState.ENABLED.value
        and not _optional_text_field(payload, "ai_model")
    ):
        warnings.append("Language-model contribution needs a selected model.")
    warnings.append(
        "Compute and storage remain disabled until the user reviews new "
        "benchmark candidates."
    )

    private_fields = tuple(
        key
        for key in ("ollama_base_url", "recorder_sources")
        if payload.get(key)
    )
    safe_fields = {
        "ai_enabled": ai_enabled,
        "ai_profile": _optional_text_field(payload, "ai_profile"),
        "ai_model": _optional_text_field(payload, "ai_model"),
        "ai_provider_mode": _optional_text_field(
            payload,
            "ai_provider_mode",
            "local",
        ),
        "ai_provider_name": _optional_text_field(
            payload,
            "ai_provider_name",
        ),
        "recorder_configured": recorder_configured,
        "recorder_poll_interval": _optional_text_field(
            payload,
            "recorder_poll_interval",
            "0.2",
        ),
        "recorder_include_condition": _boolean(
            payload,
            "recorder_include_condition",
            False,
        ),
        "user_setup_complete": user_setup_complete,
    }

    return LegacySetupMigrationPreview(
        source_schema=str(schema or "legacy-unversioned"),
        source_mode=mode,
        contribution_intents=intents,
        preserved_private_fields=private_fields,
        preserved_safe_fields=safe_fields,
        warnings=tuple(warnings),
        preview_revision=1,
        created_at=created_at,
    )
