"""Compose existing capability-first onboarding contracts into Flask."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import current_app

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.onboarding_compat import (
    LegacySetupMigrationPreview,
    federation_id_from_session_id,
    preview_legacy_setup,
)
from catalog.federation.onboarding_discovery import (
    ConfiguredFederationDiscoveryAdapter,
    FederationDiscoveryCandidate,
    FederationDiscoverySource,
    FederationOnboardingDiscoveryService,
    SessionOnboardingAuthority,
)
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationDiscoveryResult,
    FederationSessionBinding,
)
from catalog.node.identity import (
    IdentityStore,
    NodeCredentials,
    NodeIdentityStateError,
)

from .server_setup_service import (
    COMMAND_ONLY_DEPLOYMENT_MODES,
    DEPLOYMENT_MODES,
    ServerSetupError,
    load_settings,
)

_MAX_BINDING_BYTES = 65_536
_STATE_SCHEMA_VERSION = 1
_SERVICE_EXTENSION_KEY = "capability_onboarding_service"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FederationBindingStore:
    """Transactional local persistence for one trusted federation binding.

    This store persists only the frozen CF1 binding. Discovery tokens, invitation
    material, endpoints, credentials, and provider-local configuration never
    enter this database.
    """

    def __init__(self, database: Path | str) -> None:
        self.database = Path(database)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self, connection: sqlite3.Connection) -> None:
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
            raise FederationValidationError(
                "unsupported-onboarding-state-schema",
                "version",
                f"received {row['version']}",
            )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS federation_binding (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                binding_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    @staticmethod
    def _decode(encoded: str) -> FederationSessionBinding:
        if len(encoded.encode("utf-8")) > _MAX_BINDING_BYTES:
            raise FederationValidationError(
                "onboarding-binding-too-large",
                "binding",
                "persisted binding exceeds its size bound",
            )
        try:
            payload = json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise FederationValidationError(
                "malformed-onboarding-binding",
                "binding",
                "persisted binding is not valid JSON",
            ) from exc
        try:
            return FederationSessionBinding.from_dict(payload)
        except FederationValidationError as exc:
            raise FederationValidationError(
                "malformed-onboarding-binding",
                exc.field,
                "persisted binding does not satisfy the frozen CF1 contract",
            ) from exc

    def load(self) -> FederationSessionBinding | None:
        if not self.database.exists():
            return None
        try:
            with self._connect() as connection:
                tables = {
                    str(row["name"])
                    for row in connection.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type='table'
                          AND name IN ('onboarding_schema', 'federation_binding')
                        """
                    ).fetchall()
                }
                if not tables:
                    return None
                if "onboarding_schema" not in tables:
                    raise FederationValidationError(
                        "unsupported-onboarding-state-schema",
                        "version",
                        "the onboarding state schema is missing or unsupported",
                    )
                schema = connection.execute(
                    "SELECT version FROM onboarding_schema WHERE singleton=1"
                ).fetchone()
                if schema is None or schema["version"] != _STATE_SCHEMA_VERSION:
                    raise FederationValidationError(
                        "unsupported-onboarding-state-schema",
                        "version",
                        "the onboarding state schema is missing or unsupported",
                    )
                if "federation_binding" not in tables:
                    return None
                row = connection.execute(
                    "SELECT binding_json FROM federation_binding WHERE singleton=1"
                ).fetchone()
        except FederationValidationError:
            raise
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "malformed-onboarding-state",
                "database",
                "the onboarding state database could not be read safely",
            ) from exc
        return None if row is None else self._decode(row["binding_json"])

    def save(self, binding: FederationSessionBinding) -> FederationSessionBinding:
        if not isinstance(binding, FederationSessionBinding):
            raise FederationValidationError(
                "invalid-onboarding-binding",
                "binding",
                "must be a FederationSessionBinding",
            )
        encoded = json.dumps(
            binding.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if len(encoded.encode("utf-8")) > _MAX_BINDING_BYTES:
            raise FederationValidationError(
                "onboarding-binding-too-large",
                "binding",
                "binding exceeds its size bound",
            )
        self.database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._initialize(connection)
                row = connection.execute(
                    "SELECT binding_json FROM federation_binding WHERE singleton=1"
                ).fetchone()
                if row is not None:
                    current = self._decode(row["binding_json"])
                    if (
                        current.device_id != binding.device_id
                        or current.federation_id != binding.federation_id
                        or current.internal_session_id
                        != binding.internal_session_id
                    ):
                        raise FederationValidationError(
                            "onboarding-binding-replacement-forbidden",
                            "binding",
                            (
                                "CFI-2 cannot replace an existing device or "
                                "federation binding"
                            ),
                        )
                stamp = _utc_now().isoformat().replace("+00:00", "Z")
                connection.execute(
                    """
                    INSERT INTO federation_binding(
                        singleton,binding_json,updated_at
                    ) VALUES(1,?,?)
                    ON CONFLICT(singleton) DO UPDATE SET
                        binding_json=excluded.binding_json,
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
        except FederationValidationError:
            raise
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "onboarding-state-write-failed",
                "database",
                "the trusted binding could not be persisted safely",
            ) from exc
        try:
            self.database.chmod(0o600)
        except OSError as exc:
            raise FederationValidationError(
                "onboarding-state-permission-failed",
                "database",
                "the onboarding database permissions could not be restricted",
            ) from exc
        return binding


@dataclass(frozen=True)
class AuthorizedOnboardingContext:
    """Server-bound context proven through existing identity and membership."""

    credentials: NodeCredentials
    binding: FederationSessionBinding
    coordinator: SessionCoordinator


class CapabilityOnboardingService:
    """Narrow CFI-2 composition over CF1, CF3, and existing authority."""

    def __init__(
        self,
        *,
        identity_directory: Path | str,
        state_database: Path | str,
        coordinator_database: Path | str | SessionCoordinator,
        device_name: str,
        discovery_sources: (
            Iterable[FederationDiscoverySource]
            | Callable[[], Iterable[FederationDiscoverySource]]
        ) = (),
        setup_loader: Callable[[], object] = load_settings,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.identity_store = IdentityStore(
            identity_directory,
            display_name=device_name,
        )
        self.binding_store = FederationBindingStore(state_database)
        self._coordinator_database = coordinator_database
        self._coordinator: SessionCoordinator | None = (
            coordinator_database
            if isinstance(coordinator_database, SessionCoordinator)
            else None
        )
        self._discovery_sources = discovery_sources
        self._setup_loader = setup_loader
        self._clock = clock
        self._lock = threading.RLock()
        self._discovery: FederationOnboardingDiscoveryService | None = None
        self._last_results: tuple[FederationDiscoveryResult, ...] = ()

    @property
    def coordinator(self) -> SessionCoordinator:
        with self._lock:
            if self._coordinator is None:
                self._coordinator = SessionCoordinator(
                    self._coordinator_database,
                    clock=self._clock,
                )
            return self._coordinator

    @property
    def authority(self) -> SessionOnboardingAuthority:
        return SessionOnboardingAuthority(
            self.coordinator,
            clock=self._clock,
        )

    def identity_or_none(self) -> NodeCredentials | None:
        try:
            return self.identity_store.load()
        except NodeIdentityStateError as exc:
            if exc.code == "identity-state-missing":
                return None
            raise

    def create_identity(self) -> NodeCredentials:
        return self.identity_store.load_or_create(now=self._clock())

    def binding_or_none(self) -> FederationSessionBinding | None:
        return self.binding_store.load()

    def _configured_sources(self) -> tuple[FederationDiscoverySource, ...]:
        raw = (
            self._discovery_sources()
            if callable(self._discovery_sources)
            else self._discovery_sources
        )
        if raw is None or isinstance(raw, (str, bytes, dict)):
            raise FederationValidationError(
                "invalid-discovery-sources",
                "discovery_sources",
                "must be an iterable of discovery source objects",
            )
        values = tuple(raw)
        if any(not callable(getattr(value, "discover", None)) for value in values):
            raise FederationValidationError(
                "invalid-discovery-source",
                "discovery_sources",
                "every source must implement discover()",
            )
        return values

    def _trusted_membership_source(
        self,
        credentials: NodeCredentials,
    ) -> ConfiguredFederationDiscoveryAdapter | None:
        now = self._clock().astimezone(timezone.utc)
        candidates: list[FederationDiscoveryCandidate] = []
        for session_id in self.coordinator.session_ids_for_node(
            credentials.identity.node_id
        ):
            session = self.coordinator.store.get_session(session_id)
            if session is None:
                continue
            federation_id = federation_id_from_session_id(session_id)
            digest = hashlib.sha256(
                ("fcp-trusted-membership-v1\0" + federation_id).encode("utf-8")
            ).hexdigest()
            candidates.append(
                FederationDiscoveryCandidate(
                    federation_label=session.display_name,
                    internal_session_id=session_id,
                    federation_fingerprint=f"trusted-{digest[:32]}",
                    transport_kind="trusted-existing-membership",
                    verification_code=f"TRUSTED-{digest[:6].upper()}",
                    prior_trust=True,
                    discovered_at=now,
                    expires_at=now + timedelta(minutes=5),
                )
            )
        if not candidates:
            return None
        return ConfiguredFederationDiscoveryAdapter(
            candidates,
            source_id="trusted-memberships",
        )

    def discover(self) -> tuple[FederationDiscoveryResult, ...]:
        credentials = self.identity_or_none()
        if credentials is None:
            raise FederationOperationError(
                "onboarding-identity-required",
                "create or load the stable device identity before discovery",
                "device_id",
            )
        with self._lock:
            sources: list[FederationDiscoverySource] = []
            trusted = self._trusted_membership_source(credentials)
            if trusted is not None:
                sources.append(trusted)
            sources.extend(self._configured_sources())
            self._discovery = FederationOnboardingDiscoveryService(
                self.authority,
                sources,
                clock=self._clock,
            )
            self._last_results = self._discovery.discover()
            return self._last_results

    def connect(
        self,
        *,
        request_id: str,
        discovery_id: str | None = None,
        verification_code: str | None = None,
    ) -> FederationSessionBinding:
        credentials = self.identity_or_none()
        if credentials is None:
            raise FederationOperationError(
                "onboarding-identity-required",
                "create or load the stable device identity before connecting",
                "device_id",
            )
        with self._lock:
            if self._discovery is None:
                self.discover()
            assert self._discovery is not None
            binding = self._discovery.connect(
                credentials.identity,
                request_id=request_id,
                discovery_id=discovery_id,
                verification_code=verification_code,
                local_display_name=f"{credentials.identity.display_name} federation",
            )
            return self.binding_store.save(binding)

    def reconnect(self) -> FederationSessionBinding:
        credentials = self.identity_or_none()
        binding = self.binding_or_none()
        if credentials is None or binding is None:
            raise FederationOperationError(
                "onboarding-binding-unavailable",
                "a trusted saved binding is required for reconnect",
                "binding",
            )
        reconnected = self.authority.reconnect(credentials.identity, binding)
        return self.binding_store.save(reconnected)

    def authorized_context(self) -> AuthorizedOnboardingContext | None:
        """Revalidate saved context without creating or changing authority."""

        credentials = self.identity_or_none()
        binding = self.binding_or_none()
        if credentials is None or binding is None:
            return None
        reconnected = self.authority.reconnect(credentials.identity, binding)
        return AuthorizedOnboardingContext(
            credentials=credentials,
            binding=reconnected,
            coordinator=self.coordinator,
        )

    def legacy_preview(self) -> LegacySetupMigrationPreview | None:
        try:
            settings = self._setup_loader()
        except ServerSetupError:
            return None
        payload = (
            settings.to_dict()
            if callable(getattr(settings, "to_dict", None))
            else settings
        )
        if not isinstance(payload, dict):
            return None
        if not payload.get("configured") and not payload.get(
            "user_setup_complete"
        ):
            return None
        try:
            return preview_legacy_setup(payload, created_at=self._clock())
        except FederationValidationError:
            return None

    @staticmethod
    def _candidate_model(
        result: FederationDiscoveryResult,
        *,
        selected: bool,
    ) -> dict[str, object]:
        state = result.state.value
        labels = {
            FederationConnectionState.DISCOVERED: "Trusted relationship found",
            FederationConnectionState.VERIFICATION_REQUIRED: "Confirm first join",
            FederationConnectionState.UNAVAILABLE: "Unavailable",
        }
        return {
            "discovery_id": result.discovery_id,
            "label": result.federation_label,
            "transport_label": result.transport_kind.replace("-", " ").title(),
            "state": state,
            "state_label": labels.get(result.state, state.replace("-", " ").title()),
            "verification_code": result.verification_code,
            "fingerprint": result.federation_fingerprint,
            "selected": selected,
            "available": result.state is not FederationConnectionState.UNAVAILABLE,
            "reason": result.unavailable_reason,
        }

    @staticmethod
    def _migration_model(
        preview: LegacySetupMigrationPreview | None,
    ) -> dict[str, object]:
        if preview is None:
            return {
                "visible": False,
                "source_label": "",
                "preserved": [],
                "warnings": [],
            }
        definitions = {
            **DEPLOYMENT_MODES,
            **COMMAND_ONLY_DEPLOYMENT_MODES,
        }
        source = definitions.get(preview.source_mode, {})
        capability_labels = {
            "workbench": "Workbench access",
            "runtime": "Background runtime behavior",
            "recorder": "Recorder behavior",
            "language-model": "Language-model configuration",
            "compute": "Compute contribution",
            "storage": "Storage contribution",
        }
        preserved = [
            capability_labels[key]
            for key, state in preview.contribution_intents.items()
            if state == "enabled"
        ]
        if preview.preserved_private_fields:
            preserved.append("Private connection settings remain local")
        return {
            "visible": True,
            "source_label": source.get("label", preview.source_mode),
            "preserved": preserved,
            "warnings": list(preview.warnings),
        }

    def build_view_model(
        self,
        *,
        credentials: NodeCredentials | None,
        saved_binding: FederationSessionBinding | None,
        connected_binding: FederationSessionBinding | None,
        candidates: tuple[FederationDiscoveryResult, ...],
        preview: LegacySetupMigrationPreview | None,
        csrf_token: str,
        command_id: str,
        requested_step: str | None,
        repair_reason: str | None = None,
    ) -> dict[str, object]:
        identity_ready = credentials is not None
        connected = connected_binding is not None
        available_candidates = [
            item
            for item in candidates
            if item.state is not FederationConnectionState.UNAVAILABLE
        ]
        available_steps = {"identity"}
        if identity_ready:
            available_steps.add("federation")
        default_step = "identity" if not identity_ready else "federation"
        current_step = (
            requested_step
            if requested_step in available_steps
            else default_step
        )
        completed_steps = []
        if identity_ready:
            completed_steps.append("identity")
        if connected:
            completed_steps.append("federation")

        if connected:
            overall_state = "connected"
            overall_label = "Federation connected"
        elif saved_binding is not None:
            overall_state = "repair"
            overall_label = "Reconnection needed"
        elif available_candidates:
            overall_state = "verification-required"
            overall_label = "Setup in progress"
        else:
            overall_state = "setup-needed"
            overall_label = "Setup in progress"

        steps = [
            {
                "key": "identity",
                "label": "Identity",
                "summary": "This device",
                "available": True,
            },
            {
                "key": "federation",
                "label": "Federation",
                "summary": "Connect safely",
                "available": identity_ready,
            },
            {
                "key": "inspect",
                "label": "Inspect",
                "summary": "Later integration step",
                "available": False,
            },
            {
                "key": "benchmarks",
                "label": "Benchmarks",
                "summary": "Later integration step",
                "available": False,
            },
            {
                "key": "contributions",
                "label": "Contributions",
                "summary": "Later integration step",
                "available": False,
            },
            {
                "key": "finish",
                "label": "Finish",
                "summary": "Full CF7 remains blocked",
                "available": False,
            },
        ]

        identity = credentials.identity if credentials is not None else None
        selected_id = (
            available_candidates[0].discovery_id
            if len(available_candidates) == 1
            else None
        )
        candidate_models = [
            self._candidate_model(
                result,
                selected=result.discovery_id == selected_id,
            )
            for result in candidates
        ]
        discovery_failed = bool(candidates) and not available_candidates
        if connected:
            federation_state = "connected"
            federation_label = "Connected"
        elif saved_binding is not None:
            federation_state = "repair"
            federation_label = "Reconnect required"
        elif available_candidates:
            federation_state = "verification-required"
            federation_label = "Verification required"
        elif discovery_failed:
            federation_state = "unavailable"
            federation_label = "Discovery unavailable"
        else:
            federation_state = "empty"
            federation_label = "No federation found"

        return {
            "page_title": "Set up this FCP device",
            "page_intro": (
                "Load one stable device identity, then join a trusted "
                "federation or create one locally."
            ),
            "overall_state": overall_state,
            "overall_state_label": overall_label,
            "current_step": current_step,
            "progress_revision": (
                connected_binding.revision if connected_binding is not None else 1
            ),
            "storage_key": (
                f"fcp.onboarding.{identity.node_id}.cfi2"
                if identity is not None
                else "fcp.onboarding.pending.cfi2"
            ),
            "completed_steps": completed_steps,
            "steps": steps,
            "device": {
                "id": identity.node_id if identity is not None else "Not created",
                "label": (
                    identity.display_name
                    if identity is not None
                    else self.identity_store.display_name
                ),
                "identity_state": "trusted" if identity is not None else "empty",
                "identity_state_label": (
                    "Identity loaded" if identity is not None else "Identity needed"
                ),
                "notice": (
                    "The private signing key remains on this device."
                    if identity is not None
                    else "Creating an identity does not join a federation."
                ),
                "can_create": identity is None,
            },
            "federation": {
                "state": federation_state,
                "state_label": federation_label,
                "candidates": candidate_models if not connected else [],
                "empty_action": {
                    "label": "Scan again",
                    "url": "/onboarding?step=federation",
                },
                "can_create_local": (
                    identity_ready
                    and not connected
                    and not candidates
                ),
                "can_reconnect": saved_binding is not None and not connected,
                "connected_label": (
                    connected_binding.federation_id
                    if connected_binding is not None
                    else ""
                ),
                "repair_reason": repair_reason or "",
            },
            "inspection": {
                "state": "blocked",
                "state_label": "Not integrated in CFI-2",
                "revision": 0,
                "observations": [],
                "services": [],
                "data_sources": [],
                "warnings": [
                    (
                        "Device inspection remains read-only and unavailable "
                        "until the next bounded integration change."
                    )
                ],
            },
            "benchmark_summary": {
                "state": "blocked",
                "label": "Benchmark product flow remains blocked",
            },
            "benchmarks": [],
            "contributions": [],
            "migration": self._migration_model(preview),
            "finish": {
                "title": "Further setup remains intentionally blocked",
                "message": (
                    "CFI-2 connects identity and federation only. Benchmarks "
                    "and contribution actions require separate authority reviews."
                ),
                "state": "blocked",
                "state_label": "Not accepted",
                "summary": [
                    {
                        "label": "Device",
                        "value": (
                            identity.display_name
                            if identity is not None
                            else "Not created"
                        ),
                        "detail": "Stable identity boundary",
                    },
                    {
                        "label": "Federation",
                        "value": federation_label,
                        "detail": "Existing membership authority",
                    },
                ],
                "next_action": None,
            },
            "actions": {
                "identity_url": "/onboarding/identity",
                "docs_url": "/docs/implementation/capability_first_federation_plan",
                "federation_url": "/onboarding/federation",
                "inspect_url": "/onboarding/inspect",
                "skip_benchmarks_url": "/onboarding/benchmarks/skip",
                "contributions_url": "/onboarding/contributions",
                "overview_url": "/federation",
            },
            "csrf_token": csrf_token,
            "command_id": command_id,
        }


def get_capability_onboarding_service() -> CapabilityOnboardingService:
    configured = current_app.config.get("CAPABILITY_ONBOARDING_SERVICE")
    if isinstance(configured, CapabilityOnboardingService):
        return configured
    existing = current_app.extensions.get(_SERVICE_EXTENSION_KEY)
    if isinstance(existing, CapabilityOnboardingService):
        return existing
    service = CapabilityOnboardingService(
        identity_directory=current_app.config[
            "CAPABILITY_ONBOARDING_IDENTITY_DIRECTORY"
        ],
        state_database=current_app.config[
            "CAPABILITY_ONBOARDING_STATE_DATABASE"
        ],
        coordinator_database=current_app.config[
            "CAPABILITY_ONBOARDING_COORDINATOR_DATABASE"
        ],
        device_name=current_app.config["CAPABILITY_ONBOARDING_DEVICE_NAME"],
        discovery_sources=current_app.config.get(
            "CAPABILITY_ONBOARDING_DISCOVERY_SOURCES",
            (),
        ),
    )
    current_app.extensions[_SERVICE_EXTENSION_KEY] = service
    return service


__all__ = [
    "AuthorizedOnboardingContext",
    "CapabilityOnboardingService",
    "FederationBindingStore",
    "get_capability_onboarding_service",
]
