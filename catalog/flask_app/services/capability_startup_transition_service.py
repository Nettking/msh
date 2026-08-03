"""CFI-6 compatibility migration and capability-first startup state."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

from flask import current_app

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.onboarding_compat import (
    CONTRIBUTION_KEYS,
    LegacySetupMigrationPreview,
    preview_legacy_setup,
)
from catalog.federation.onboarding_models import (
    ContributionDesiredState,
    FederationConnectionState,
    OnboardingModel,
    _optional_safe_text,
    _safe_text,
    _uint,
    _utc,
)

from .capability_contribution_service import (
    CapabilityContributionService,
    get_capability_contribution_service,
)
from .capability_inspection_service import (
    CapabilityInspectionService,
    get_capability_inspection_service,
)
from .capability_onboarding_service import (
    CapabilityOnboardingService,
    get_capability_onboarding_service,
)
from .server_setup_service import (
    ServerSetupError,
    ServerSetupSettings,
    default_settings,
    load_settings,
    save_settings,
)

_STATE_SCHEMA_VERSION = 1
_MAX_STATE_BYTES = 65_536
_SERVICE_EXTENSION_KEY = "capability_startup_transition_service"
_SOURCE_KINDS = frozenset({"legacy-migration", "capability-first"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _intents(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != set(CONTRIBUTION_KEYS):
        raise FederationValidationError(
            "invalid-startup-transition-intents",
            "contribution_intents",
            "must contain exactly the supported capability keys",
        )
    normalized: dict[str, str] = {}
    for key in CONTRIBUTION_KEYS:
        try:
            normalized[key] = ContributionDesiredState(value[key]).value
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-startup-transition-intent",
                f"contribution_intents.{key}",
                "must be enabled, disabled, or ask-later",
            ) from exc
    return normalized


@dataclass(frozen=True)
class CapabilityStartupState(OnboardingModel):
    """Local startup intent. It is not a grant or runtime authority record."""

    SCHEMA: ClassVar[str] = "msh.onboarding.v1"

    device_id: str
    federation_id: str
    internal_session_id: str
    federation_state: FederationConnectionState
    inspection_revision: int
    contribution_intents: dict[str, str]
    completed: bool
    source_kind: str
    source_schema: str | None
    source_mode: str | None
    source_revision: int
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in ("device_id", "federation_id", "internal_session_id"):
            object.__setattr__(self, name, _safe_text(getattr(self, name), name))
        try:
            state = FederationConnectionState(self.federation_state)
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-startup-transition-state",
                "federation_state",
                "unknown federation state",
            ) from exc
        if state is not FederationConnectionState.CONNECTED:
            raise FederationValidationError(
                "unconnected-startup-transition",
                "federation_state",
                "a trusted connected binding is required",
            )
        object.__setattr__(self, "federation_state", state)
        object.__setattr__(
            self,
            "inspection_revision",
            _uint(self.inspection_revision, "inspection_revision"),
        )
        object.__setattr__(self, "contribution_intents", _intents(self.contribution_intents))
        if not isinstance(self.completed, bool):
            raise FederationValidationError(
                "invalid-startup-transition-completion",
                "completed",
                "must be a boolean",
            )
        source_kind = _safe_text(self.source_kind, "source_kind")
        if source_kind not in _SOURCE_KINDS:
            raise FederationValidationError(
                "invalid-startup-transition-source",
                "source_kind",
                "unknown transition source",
            )
        object.__setattr__(self, "source_kind", source_kind)
        object.__setattr__(
            self,
            "source_schema",
            _optional_safe_text(self.source_schema, "source_schema"),
        )
        object.__setattr__(
            self,
            "source_mode",
            _optional_safe_text(self.source_mode, "source_mode"),
        )
        source_revision = _uint(self.source_revision, "source_revision")
        if source_revision == 0:
            raise FederationValidationError(
                "invalid-startup-transition-revision",
                "source_revision",
                "must be greater than zero",
            )
        object.__setattr__(self, "source_revision", source_revision)
        object.__setattr__(self, "updated_at", _utc(self.updated_at, "updated_at"))
        if self.source_kind == "legacy-migration" and (
            self.source_schema is None or self.source_mode is None
        ):
            raise FederationValidationError(
                "incomplete-startup-transition-source",
                "source_kind",
                "legacy migration requires source schema and mode",
            )


class CapabilityStartupStateStore:
    """Transactional singleton state in the existing onboarding database."""

    def __init__(self, database: Path | str) -> None:
        self.database = Path(database)
        self._lock = threading.RLock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _decode(encoded: str) -> CapabilityStartupState:
        if len(encoded.encode("utf-8")) > _MAX_STATE_BYTES:
            raise FederationValidationError(
                "startup-transition-too-large",
                "state",
                "persisted transition exceeds its size bound",
            )
        try:
            payload = json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise FederationValidationError(
                "malformed-startup-transition",
                "state",
                "persisted transition is not valid JSON",
            ) from exc
        try:
            return CapabilityStartupState.from_dict(payload)
        except ProtocolCompatibilityError:
            raise
        except FederationValidationError as exc:
            raise FederationValidationError(
                "malformed-startup-transition",
                exc.field,
                "persisted transition violates the CFI-6 contract",
            ) from exc

    @staticmethod
    def _initialize(connection: sqlite3.Connection) -> None:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS onboarding_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                version INTEGER NOT NULL CHECK(version > 0)
            )
            """
        )
        row = connection.execute(
            "SELECT version FROM onboarding_schema WHERE singleton=1"
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO onboarding_schema(singleton,version) VALUES(1,?)",
                (_STATE_SCHEMA_VERSION,),
            )
        elif row["version"] != _STATE_SCHEMA_VERSION:
            raise ProtocolCompatibilityError(
                "unsupported-onboarding-state-schema",
                "version",
                f"received {row['version']}",
            )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS capability_startup_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def load(self) -> CapabilityStartupState | None:
        if not self.database.exists():
            return None
        with self._lock:
            try:
                with self._connect() as connection:
                    table = connection.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type='table' AND name='capability_startup_state'
                        """
                    ).fetchone()
                    if table is None:
                        return None
                    row = connection.execute(
                        """
                        SELECT state_json FROM capability_startup_state
                        WHERE singleton=1
                        """
                    ).fetchone()
            except sqlite3.Error as exc:
                raise FederationValidationError(
                    "malformed-startup-transition",
                    "database",
                    "transition state could not be read safely",
                ) from exc
        return None if row is None else self._decode(row["state_json"])

    def save(self, state: CapabilityStartupState) -> CapabilityStartupState:
        if not isinstance(state, CapabilityStartupState):
            raise FederationValidationError(
                "invalid-startup-transition",
                "state",
                "must be a CapabilityStartupState",
            )
        encoded = json.dumps(
            state.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if len(encoded.encode("utf-8")) > _MAX_STATE_BYTES:
            raise FederationValidationError(
                "startup-transition-too-large",
                "state",
                "transition exceeds its size bound",
            )
        self.database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self._lock:
            try:
                connection = self._connect()
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    self._initialize(connection)
                    row = connection.execute(
                        """
                        SELECT state_json FROM capability_startup_state
                        WHERE singleton=1
                        """
                    ).fetchone()
                    if row is not None:
                        current = self._decode(row["state_json"])
                        if (
                            current.device_id != state.device_id
                            or current.federation_id != state.federation_id
                            or current.internal_session_id != state.internal_session_id
                        ):
                            raise FederationValidationError(
                                "startup-transition-replacement-forbidden",
                                "state",
                                "device and federation binding cannot be replaced",
                            )
                    stamp = state.updated_at.isoformat().replace("+00:00", "Z")
                    connection.execute(
                        """
                        INSERT INTO capability_startup_state(
                            singleton,state_json,updated_at
                        ) VALUES(1,?,?)
                        ON CONFLICT(singleton) DO UPDATE SET
                            state_json=excluded.state_json,
                            updated_at=excluded.updated_at
                        """,
                        (encoded, stamp),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                finally:
                    connection.close()
            except (FederationValidationError, ProtocolCompatibilityError):
                raise
            except sqlite3.Error as exc:
                raise FederationValidationError(
                    "startup-transition-write-failed",
                    "database",
                    "transition state could not be persisted safely",
                ) from exc
        try:
            self.database.chmod(0o600)
        except OSError as exc:
            raise FederationValidationError(
                "startup-transition-permission-failed",
                "database",
                "transition state permissions could not be restricted",
            ) from exc
        return state


class CapabilityStartupTransitionService:
    """Migrate legacy behavior and make capability-first startup authoritative."""

    def __init__(
        self,
        *,
        onboarding_service: CapabilityOnboardingService,
        inspection_service: CapabilityInspectionService,
        contribution_service: CapabilityContributionService,
        state_database: Path | str,
        setup_loader: Callable[[], ServerSetupSettings] = load_settings,
        setup_saver: Callable[[ServerSetupSettings], object] = save_settings,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.onboarding_service = onboarding_service
        self.inspection_service = inspection_service
        self.contribution_service = contribution_service
        self.store = CapabilityStartupStateStore(state_database)
        self._setup_loader = setup_loader
        self._setup_saver = setup_saver
        self._clock = clock

    def load(self) -> CapabilityStartupState | None:
        return self.store.load()

    def legacy_settings(self) -> ServerSetupSettings | None:
        try:
            settings = self._setup_loader()
        except (OSError, ServerSetupError, TypeError, ValueError):
            return None
        if not settings.configured or not settings.user_setup_complete:
            return None
        return settings

    def legacy_preview(self) -> LegacySetupMigrationPreview | None:
        settings = self.legacy_settings()
        if settings is None:
            return None
        return preview_legacy_setup(settings.to_dict(), created_at=self._clock())

    def _inspection_revision(self, device_id: str) -> int:
        snapshot = self.inspection_service.load()
        if snapshot is None or snapshot.device_id != device_id:
            return 0
        return snapshot.revision

    def _connected_context(self, *, request_id: str | None = None):
        credentials = self.onboarding_service.identity_or_none()
        if credentials is None:
            credentials = self.onboarding_service.create_identity()
        context = self.onboarding_service.authorized_context()
        if context is not None:
            return context
        if self.onboarding_service.binding_or_none() is not None:
            raise FederationOperationError(
                "startup-transition-reconnect-required",
                "repair the saved membership before migration",
                "binding",
            )
        if request_id is None:
            raise FederationOperationError(
                "startup-transition-federation-required",
                "connect or create a federation before finishing onboarding",
                "binding",
            )
        self.onboarding_service.connect(request_id=request_id)
        context = self.onboarding_service.authorized_context()
        if context is None:
            raise FederationOperationError(
                "startup-transition-federation-required",
                "trusted federation context was not established",
                "binding",
            )
        return context

    def migrate_legacy(self, *, request_id: str) -> CapabilityStartupState:
        current = self.load()
        if current is not None:
            return current
        preview = self.legacy_preview()
        if preview is None:
            raise FederationOperationError(
                "startup-transition-legacy-required",
                "no completed supported legacy setup is available",
                "legacy_setup",
            )
        context = self._connected_context(request_id=request_id)
        return self.store.save(
            CapabilityStartupState(
                device_id=context.credentials.identity.node_id,
                federation_id=context.binding.federation_id,
                internal_session_id=context.binding.internal_session_id,
                federation_state=context.binding.state,
                inspection_revision=self._inspection_revision(
                    context.credentials.identity.node_id
                ),
                contribution_intents=preview.contribution_intents,
                completed=True,
                source_kind="legacy-migration",
                source_schema=preview.source_schema,
                source_mode=preview.source_mode,
                source_revision=preview.preview_revision,
                updated_at=self._clock(),
            )
        )

    @staticmethod
    def _combine_desired(states: Sequence[ContributionDesiredState]) -> str:
        if ContributionDesiredState.ENABLED in states:
            return ContributionDesiredState.ENABLED.value
        if states and all(
            state is ContributionDesiredState.DISABLED for state in states
        ):
            return ContributionDesiredState.DISABLED.value
        return ContributionDesiredState.ASK_LATER.value

    def _current_intents(self) -> dict[str, str]:
        snapshot, complete = self.contribution_service._authorized_snapshot(
            require_benchmark_review=True
        )
        if not complete:
            raise FederationOperationError(
                "startup-transition-benchmarks-required",
                "review current benchmarks before finishing",
                "benchmarks",
            )
        candidates = self.contribution_service.recommend(
            require_benchmark_review=True
        )
        summary, _cards, choices_complete = self.contribution_service.view_model(
            snapshot,
            connected=True,
            benchmark_complete=True,
        )
        if not choices_complete or summary.get("state") != "complete":
            raise FederationOperationError(
                "startup-transition-contributions-required",
                "review every supported contribution before finishing",
                "contributions",
            )
        persisted = {
            intent.candidate_id: intent.desired_state
            for intent in self.contribution_service.intents()
        }
        by_type: dict[str, list[ContributionDesiredState]] = {}
        for candidate in candidates:
            by_type.setdefault(candidate.capability_type, []).append(
                persisted.get(
                    candidate.candidate_id,
                    ContributionDesiredState.DISABLED,
                )
            )
        intents = {
            key: ContributionDesiredState.DISABLED.value
            for key in CONTRIBUTION_KEYS
        }
        intents["workbench"] = ContributionDesiredState.ENABLED.value
        intents["runtime"] = ContributionDesiredState.ENABLED.value
        for capability_type in ("recorder", "language-model", "compute", "storage"):
            if capability_type in by_type:
                intents[capability_type] = self._combine_desired(
                    by_type[capability_type]
                )
        return intents

    @staticmethod
    def _compatibility_mode(intents: Mapping[str, str]) -> str:
        recorder = intents["recorder"] == ContributionDesiredState.ENABLED.value
        runtime = intents["runtime"] == ContributionDesiredState.ENABLED.value
        if recorder and runtime:
            return "full-server"
        if recorder:
            return "recorder-only"
        if runtime:
            return "web-workbench"
        return "web-ui-only"

    def _write_compatibility_settings(
        self,
        intents: Mapping[str, str],
    ) -> None:
        try:
            settings = self._setup_loader()
        except (OSError, ServerSetupError, TypeError, ValueError):
            settings = default_settings(configured=False)
        if (
            intents["recorder"] == ContributionDesiredState.ENABLED.value
            and not settings.recorder_sources.strip()
        ):
            raise FederationOperationError(
                "startup-transition-recorder-source-required",
                "an enabled recorder requires an existing selected source",
                "recorder",
            )
        self._setup_saver(
            replace(
                settings,
                configured=True,
                user_setup_complete=True,
                deployment_mode=self._compatibility_mode(intents),
                ai_enabled=(
                    intents["language-model"]
                    == ContributionDesiredState.ENABLED.value
                ),
                updated_at=self._clock()
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
            )
        )

    def complete_current(self) -> CapabilityStartupState:
        current = self.load()
        if current is not None:
            return current
        context = self._connected_context()
        intents = self._current_intents()
        self._write_compatibility_settings(intents)
        return self.store.save(
            CapabilityStartupState(
                device_id=context.credentials.identity.node_id,
                federation_id=context.binding.federation_id,
                internal_session_id=context.binding.internal_session_id,
                federation_state=context.binding.state,
                inspection_revision=self._inspection_revision(
                    context.credentials.identity.node_id
                ),
                contribution_intents=intents,
                completed=True,
                source_kind="capability-first",
                source_schema=None,
                source_mode=None,
                source_revision=1,
                updated_at=self._clock(),
            )
        )

    def refresh_legacy(
        self,
        settings: ServerSetupSettings,
    ) -> CapabilityStartupState | None:
        current = self.load()
        if current is None or current.source_kind != "legacy-migration":
            return current
        preview = preview_legacy_setup(settings.to_dict(), created_at=self._clock())
        return self.store.save(
            replace(
                current,
                contribution_intents=preview.contribution_intents,
                source_schema=preview.source_schema,
                source_mode=preview.source_mode,
                source_revision=current.source_revision + 1,
                updated_at=self._clock(),
            )
        )

    def capability_flags(
        self,
        settings: ServerSetupSettings | None = None,
    ) -> dict[str, object]:
        state = self.load()
        preview = None
        if state is None:
            settings = settings or self.legacy_settings()
            if settings is not None:
                preview = preview_legacy_setup(
                    settings.to_dict(),
                    created_at=self._clock(),
                )
        intents = (
            state.contribution_intents
            if state is not None and state.completed
            else (
                preview.contribution_intents
                if preview is not None
                else {
                    key: ContributionDesiredState.DISABLED.value
                    for key in CONTRIBUTION_KEYS
                }
            )
        )
        return {
            **{
                key.replace("-", "_"): value
                == ContributionDesiredState.ENABLED.value
                for key, value in intents.items()
            },
            "completed": bool(state is not None and state.completed),
            "needs_migration": state is None and preview is not None,
            "source": (
                state.source_kind
                if state is not None
                else ("legacy-preview" if preview is not None else "unconfigured")
            ),
            "state": state,
        }

    def runtime_should_start(
        self,
        settings: ServerSetupSettings | None = None,
    ) -> bool:
        settings = settings or self.legacy_settings()
        if settings is None:
            return False
        return bool(self.capability_flags(settings)["runtime"])

    def apply_to_onboarding_view_model(
        self,
        view_model: dict[str, object],
        *,
        requested_step: str | None,
    ) -> dict[str, object]:
        state = self.load()
        preview = self.legacy_preview()
        migration = view_model.setdefault("migration", {})
        assert isinstance(migration, dict)
        migration.update(
            {
                "persisted": state is not None,
                "can_migrate": state is None and preview is not None,
                "source_kind": "" if state is None else state.source_kind,
            }
        )
        actions = view_model.get("actions")
        if isinstance(actions, dict):
            actions.update(
                {
                    "migration_url": "/onboarding/migrate",
                    "finish_url": "/onboarding/finish",
                    "legacy_setup_url": "/startup?legacy=1&edit=1",
                }
            )
        finish = view_model.get("finish")
        steps = view_model.get("steps")
        completed_steps = view_model.get("completed_steps")
        finish_available = isinstance(steps, list) and any(
            isinstance(step, dict)
            and step.get("key") == "finish"
            and bool(step.get("available"))
            for step in steps
        )
        if state is not None and state.completed:
            view_model["overall_state"] = "connected"
            view_model["overall_state_label"] = "Capability-first setup complete"
            if isinstance(finish, dict):
                finish.update(
                    {
                        "title": "This device is ready",
                        "message": (
                            "Capability intent now controls startup and navigation "
                            "through the retained compatibility boundary."
                        ),
                        "state": "connected",
                        "state_label": "Ready",
                        "transition_action": None,
                        "next_action": {
                            "title": "Review the connected federation",
                            "message": "Open the safe read-only Federation overview.",
                            "label": "Open Federation overview",
                            "url": "/federation",
                        },
                    }
                )
            if isinstance(steps, list):
                for step in steps:
                    if isinstance(step, dict) and step.get("key") == "finish":
                        step.update({"available": True, "summary": "Ready"})
            if isinstance(completed_steps, list) and "finish" not in completed_steps:
                completed_steps.append("finish")
            if requested_step in {None, "finish"}:
                view_model["current_step"] = "finish"
        elif preview is not None:
            view_model["overall_state"] = "migration-required"
            view_model["overall_state_label"] = "Existing setup ready to migrate"
            if isinstance(finish, dict):
                finish.update(
                    {
                        "title": "Carry the existing setup forward",
                        "message": (
                            "Confirm the deterministic migration before "
                            "capability-first startup becomes the default."
                        ),
                        "state": "pending",
                        "state_label": "Confirmation required",
                        "transition_action": {
                            "kind": "migrate",
                            "label": "Migrate existing setup",
                            "url": "/onboarding/migrate",
                        },
                        "next_action": None,
                    }
                )
            if isinstance(steps, list):
                for step in steps:
                    if isinstance(step, dict) and step.get("key") == "finish":
                        step.update(
                            {
                                "available": True,
                                "summary": "Migrate existing setup",
                            }
                        )
            if requested_step in {None, "finish"}:
                view_model["current_step"] = "finish"
        elif isinstance(finish, dict) and finish_available:
            finish.update(
                {
                    "state": "pending",
                    "state_label": "Ready to finish",
                    "transition_action": {
                        "kind": "finish",
                        "label": "Finish capability-first setup",
                        "url": "/onboarding/finish",
                    },
                }
            )
            if requested_step == "finish":
                view_model["current_step"] = "finish"
        elif isinstance(finish, dict):
            finish["transition_action"] = None
        view_model["storage_key"] = str(
            view_model.get("storage_key") or "msh.onboarding.pending"
        ).replace(".cfi5", ".cfi6")
        return view_model


def get_capability_startup_transition_service() -> CapabilityStartupTransitionService:
    configured = current_app.config.get("CAPABILITY_STARTUP_TRANSITION_SERVICE")
    if isinstance(configured, CapabilityStartupTransitionService):
        return configured
    existing = current_app.extensions.get(_SERVICE_EXTENSION_KEY)
    if isinstance(existing, CapabilityStartupTransitionService):
        return existing
    onboarding = get_capability_onboarding_service()
    service = CapabilityStartupTransitionService(
        onboarding_service=onboarding,
        inspection_service=get_capability_inspection_service(),
        contribution_service=get_capability_contribution_service(),
        state_database=current_app.config.get(
            "CAPABILITY_ONBOARDING_TRANSITION_DATABASE",
            current_app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
        ),
        setup_loader=current_app.config.get(
            "CAPABILITY_ONBOARDING_SETUP_LOADER",
            load_settings,
        ),
        setup_saver=current_app.config.get(
            "CAPABILITY_ONBOARDING_SETUP_SAVER",
            save_settings,
        ),
        clock=getattr(onboarding, "_clock", _utc_now),
    )
    current_app.extensions[_SERVICE_EXTENSION_KEY] = service
    return service


__all__ = [
    "CapabilityStartupState",
    "CapabilityStartupStateStore",
    "CapabilityStartupTransitionService",
    "get_capability_startup_transition_service",
]
