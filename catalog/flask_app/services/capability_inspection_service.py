"""CFI-3 composition of existing read-only device inspection into Flask."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from flask import current_app

from catalog.capabilities.benchmarking import (
    COMPUTE_BENCHMARK_ID,
    MTCONNECT_BENCHMARK_ID,
    NETWORK_BENCHMARK_ID,
    OLLAMA_BENCHMARK_ID,
    STORAGE_BENCHMARK_ID,
    BenchmarkRegistry,
    DeviceInspector,
    MtconnectSourceAdapter,
    OllamaBenchmarkAdapter,
    OllamaProbeTarget,
    register_concrete_adapters,
)
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.onboarding_models import DeviceInspectionSnapshot

from .capability_onboarding_service import (
    CapabilityOnboardingService,
    get_capability_onboarding_service,
)
from .mtconnect_discovery_service import get_mtconnect_discovery_service
from .server_setup_service import ServerSetupError, load_settings

_MAX_SNAPSHOT_BYTES = 262_144
_SERVICE_EXTENSION_KEY = "capability_inspection_service"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class InspectionSnapshotStore:
    """Persist one monotonic, device-bound inspection snapshot.

    The snapshot is evidence only. It contains no federation authority, benchmark
    result, provider activation, endpoint, credential, path, grant, or fencing
    material.
    """

    def __init__(self, database: Path | str) -> None:
        self.database = Path(database)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _create_table(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS device_inspection_snapshot (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                device_id TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK(revision > 0),
                snapshot_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def initialize(self) -> None:
        """Create the additive schema before acquiring a write transaction."""

        self.database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            connection = self._connect()
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                self._create_table(connection)
                connection.commit()
            finally:
                connection.close()
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "inspection-state-initialize-failed",
                "database",
                "inspection evidence storage could not be initialized safely",
            ) from exc

    @staticmethod
    def _decode(encoded: str) -> DeviceInspectionSnapshot:
        if len(encoded.encode("utf-8")) > _MAX_SNAPSHOT_BYTES:
            raise FederationValidationError(
                "inspection-snapshot-too-large",
                "snapshot",
                "persisted inspection exceeds its size bound",
            )
        try:
            payload = json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise FederationValidationError(
                "malformed-inspection-snapshot",
                "snapshot",
                "persisted inspection is not valid JSON",
            ) from exc
        try:
            return DeviceInspectionSnapshot.from_dict(payload)
        except FederationValidationError as exc:
            raise FederationValidationError(
                "malformed-inspection-snapshot",
                exc.field,
                "persisted inspection does not satisfy the frozen CF1 contract",
            ) from exc

    @staticmethod
    def _encode(snapshot: DeviceInspectionSnapshot) -> str:
        encoded = json.dumps(
            snapshot.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if len(encoded.encode("utf-8")) > _MAX_SNAPSHOT_BYTES:
            raise FederationValidationError(
                "inspection-snapshot-too-large",
                "snapshot",
                "inspection exceeds its size bound",
            )
        return encoded

    def load(self) -> DeviceInspectionSnapshot | None:
        if not self.database.exists():
            return None
        try:
            with self._connect() as connection:
                table = connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='device_inspection_snapshot'
                    """
                ).fetchone()
                if table is None:
                    return None
                row = connection.execute(
                    """
                    SELECT snapshot_json FROM device_inspection_snapshot
                    WHERE singleton=1
                    """
                ).fetchone()
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "malformed-inspection-state",
                "database",
                "the inspection database could not be read safely",
            ) from exc
        return None if row is None else self._decode(row["snapshot_json"])

    def save(self, snapshot: DeviceInspectionSnapshot) -> DeviceInspectionSnapshot:
        if not isinstance(snapshot, DeviceInspectionSnapshot):
            raise FederationValidationError(
                "invalid-inspection-snapshot",
                "snapshot",
                "must be a DeviceInspectionSnapshot",
            )
        encoded = self._encode(snapshot)
        self.initialize()
        try:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._create_table(connection)
                row = connection.execute(
                    """
                    SELECT snapshot_json FROM device_inspection_snapshot
                    WHERE singleton=1
                    """
                ).fetchone()
                expected_revision = 1
                if row is not None:
                    current = self._decode(row["snapshot_json"])
                    if current.device_id != snapshot.device_id:
                        raise FederationValidationError(
                            "inspection-device-replacement-forbidden",
                            "device_id",
                            "inspection evidence cannot be rebound to another device",
                        )
                    expected_revision = current.revision + 1
                if snapshot.revision != expected_revision:
                    raise FederationValidationError(
                        "inspection-revision-conflict",
                        "revision",
                        f"expected revision {expected_revision}",
                    )
                stamp = _utc_now().isoformat().replace("+00:00", "Z")
                connection.execute(
                    """
                    INSERT INTO device_inspection_snapshot(
                        singleton,device_id,revision,snapshot_json,updated_at
                    ) VALUES(1,?,?,?,?)
                    ON CONFLICT(singleton) DO UPDATE SET
                        device_id=excluded.device_id,
                        revision=excluded.revision,
                        snapshot_json=excluded.snapshot_json,
                        updated_at=excluded.updated_at
                    """,
                    (snapshot.device_id, snapshot.revision, encoded, stamp),
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
                "inspection-state-write-failed",
                "database",
                "inspection evidence could not be persisted safely",
            ) from exc
        try:
            self.database.chmod(0o600)
        except OSError as exc:
            raise FederationValidationError(
                "inspection-state-permission-failed",
                "database",
                "inspection database permissions could not be restricted",
            ) from exc
        return snapshot


class CapabilityInspectionService:
    """Run and render the existing CF2 inspection kernel without authority."""

    def __init__(
        self,
        *,
        onboarding_service: CapabilityOnboardingService,
        state_database: Path | str,
        adapters: Iterable[object] | Callable[[], Iterable[object]] | None = None,
        setup_loader: Callable[[], object] = load_settings,
        mtconnect_scan_supplier: Callable[[], Mapping[str, Any]] | None = None,
        inspection_ttl_seconds: int = 900,
        clock: Callable[[], datetime] = _utc_now,
        system_observer: Callable[[], Mapping[str, Any]] | None = None,
    ) -> None:
        if isinstance(inspection_ttl_seconds, bool) or inspection_ttl_seconds <= 0:
            raise ValueError("inspection_ttl_seconds must be positive")
        self.onboarding_service = onboarding_service
        self.store = InspectionSnapshotStore(state_database)
        self._configured_adapters = adapters
        self._setup_loader = setup_loader
        self._scan_supplier = (
            mtconnect_scan_supplier
            or get_mtconnect_discovery_service().last_scan
        )
        self._inspection_ttl_seconds = int(inspection_ttl_seconds)
        self._clock = clock
        self._system_observer = system_observer
        self._lock = threading.RLock()
        self._inspector: DeviceInspector | None = None
        self._composition_warnings: tuple[str, ...] = ()

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise FederationValidationError(
                "invalid-inspection-clock",
                "clock",
                "inspection clock must be timezone-aware",
            )
        return value.astimezone(timezone.utc)

    def _setup_payload(self) -> dict[str, Any]:
        try:
            settings = self._setup_loader()
        except ServerSetupError:
            return {}
        payload = (
            settings.to_dict()
            if callable(getattr(settings, "to_dict", None))
            else settings
        )
        return dict(payload) if isinstance(payload, Mapping) else {}

    def _default_adapters(self) -> tuple[object, ...]:
        adapters: list[object] = [MtconnectSourceAdapter(self._scan_supplier)]
        warnings: list[str] = []
        payload = self._setup_payload()
        if payload.get("configured") and payload.get("ai_enabled"):
            base_url = str(payload.get("ollama_base_url") or "").strip()
            model = str(payload.get("ai_model") or "").strip()
            try:
                parsed = urlsplit(base_url)
                expected_host = (parsed.hostname or "").casefold()
                target = OllamaProbeTarget(
                    service_id="ollama-configured",
                    display_label="Configured Ollama service",
                    base_url=base_url,
                    model=model,
                )
                adapters.append(
                    OllamaBenchmarkAdapter(
                        (target,),
                        trusted_host_predicate=(
                            lambda host, expected=expected_host: (
                                bool(expected)
                                and host.casefold() == expected
                            )
                        ),
                    )
                )
            except (TypeError, ValueError):
                warnings.append(
                    "Configured language-model service could not be prepared for inspection"
                )
        self._composition_warnings = tuple(warnings)
        return tuple(adapters)

    def _adapters(self) -> tuple[object, ...]:
        configured = self._configured_adapters
        if configured is None:
            return self._default_adapters()
        raw = (
            configured()
            if callable(configured) and not hasattr(configured, "definition")
            else configured
        )
        if raw is None or isinstance(raw, (str, bytes, Mapping)):
            raise FederationValidationError(
                "invalid-inspection-adapters",
                "adapters",
                "inspection adapters must be an iterable",
            )
        return tuple(raw)

    def _device_inspector(self) -> DeviceInspector:
        with self._lock:
            if self._inspector is not None:
                return self._inspector
            registry = BenchmarkRegistry()
            try:
                registration = register_concrete_adapters(
                    registry,
                    self._adapters(),
                )
            except (TypeError, ValueError) as exc:
                raise FederationValidationError(
                    "invalid-inspection-adapter-composition",
                    "adapters",
                    "configured inspection adapters are not safe CF2 adapters",
                ) from exc
            if len(registration.benchmark_ids) != len(set(registration.benchmark_ids)):
                raise FederationValidationError(
                    "duplicate-inspection-benchmark",
                    "adapters",
                    "inspection adapters contain duplicate benchmark definitions",
                )
            self._inspector = DeviceInspector(
                registry,
                probes=registration.inspection_probes,
                inspection_ttl_seconds=self._inspection_ttl_seconds,
                now=self._clock,
                system_observer=self._system_observer,
            )
            return self._inspector

    def load(self) -> DeviceInspectionSnapshot | None:
        snapshot = self.store.load()
        credentials = self.onboarding_service.identity_or_none()
        if snapshot is not None and (
            credentials is None
            or snapshot.device_id != credentials.identity.node_id
        ):
            raise FederationValidationError(
                "inspection-device-mismatch",
                "device_id",
                "saved inspection does not belong to the current device identity",
            )
        return snapshot

    def run(self) -> DeviceInspectionSnapshot:
        context = self.onboarding_service.authorized_context()
        if context is None:
            raise FederationOperationError(
                "inspection-federation-required",
                "a trusted federation connection is required before inspection",
                "binding",
            )
        with self._lock:
            current = self.store.load()
            if current is not None and (
                current.device_id != context.credentials.identity.node_id
            ):
                raise FederationValidationError(
                    "inspection-device-mismatch",
                    "device_id",
                    "saved inspection does not belong to the connected device",
                )
            revision = 1 if current is None else current.revision + 1
            snapshot = self._device_inspector().inspect(
                device_id=context.credentials.identity.node_id,
                revision=revision,
            )
            if self._composition_warnings:
                snapshot = replace(
                    snapshot,
                    warnings=tuple(
                        dict.fromkeys(
                            (*snapshot.warnings, *self._composition_warnings)
                        )
                    ),
                )
            return self.store.save(snapshot)

    def state(self, snapshot: DeviceInspectionSnapshot | None) -> str:
        if snapshot is None:
            return "empty"
        return "expired" if snapshot.expires_at <= self._now() else "current"

    @staticmethod
    def _humanize(value: str) -> str:
        return " ".join(
            part for part in value.replace("_", "-").split("-") if part
        ).title()

    @classmethod
    def _format_value(cls, value: Any) -> str:
        if isinstance(value, bool):
            return "Yes" if value else "No"
        if isinstance(value, Mapping):
            parts = [
                f"{cls._humanize(str(key))}: {cls._format_value(item)}"
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            ]
            return "; ".join(parts) if parts else "No observation"
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return ", ".join(cls._format_value(item) for item in value)
        return str(value)

    @classmethod
    def _observation_models(
        cls,
        snapshot: DeviceInspectionSnapshot,
    ) -> list[dict[str, str]]:
        facts = [
            {"label": "Operating system", "value": cls._humanize(snapshot.os_family)},
            {"label": "Architecture", "value": cls._humanize(snapshot.architecture)},
        ]
        for key, value in sorted(snapshot.resource_observations.items()):
            facts.append(
                {
                    "label": cls._humanize(str(key)),
                    "value": cls._format_value(value),
                }
            )
        return facts

    @classmethod
    def _service_models(
        cls,
        snapshot: DeviceInspectionSnapshot,
    ) -> list[dict[str, str]]:
        known = {
            "ollama-configured": (
                "Configured language model",
                "The trusted configured Ollama service answered a read-only inventory request.",
            ),
            "mtconnect-recorder-source": (
                "MTConnect recorder source",
                "Existing bounded discovery evidence contains a supported recorder source.",
            ),
            "trusted-local-compute-inventory": (
                "Registered compute inventory",
                "Only explicit locally registered handler descriptors were inspected.",
            ),
        }
        values: list[dict[str, str]] = []
        for service_id in snapshot.detected_services:
            label, summary = known.get(
                service_id,
                (
                    cls._humanize(service_id),
                    "A trusted local inspection adapter reported this service.",
                ),
            )
            values.append({"label": label, "summary": summary})
        for handler_id in snapshot.registered_handlers:
            values.append(
                {
                    "label": f"Compute handler: {cls._humanize(handler_id)}",
                    "summary": "Registered locally; inspection did not execute or dispatch it.",
                }
            )
        return values

    @classmethod
    def _data_source_models(
        cls,
        snapshot: DeviceInspectionSnapshot,
    ) -> list[dict[str, str]]:
        return [
            {
                "label": cls._humanize(source_id),
                "summary": (
                    "Present in the existing bounded MTConnect discovery snapshot; "
                    "CFI-3 did not start a network scan or recorder."
                ),
            }
            for source_id in snapshot.detected_data_sources
        ]

    @classmethod
    def _benchmark_models(
        cls,
        snapshot: DeviceInspectionSnapshot,
    ) -> list[dict[str, str]]:
        labels = {
            OLLAMA_BENCHMARK_ID: "Language-model response check",
            MTCONNECT_BENCHMARK_ID: "MTConnect source suitability",
            COMPUTE_BENCHMARK_ID: "Registered compute-handler check",
            STORAGE_BENCHMARK_ID: "Storage candidate check",
            NETWORK_BENCHMARK_ID: "Authenticated network-path check",
        }
        return [
            {
                "label": labels.get(benchmark_id, cls._humanize(benchmark_id)),
                "summary": "Recommended by inspection; no benchmark has been run.",
            }
            for benchmark_id in snapshot.recommended_benchmark_ids
        ]

    def view_model(
        self,
        snapshot: DeviceInspectionSnapshot | None,
        *,
        connected: bool,
        error_reason: str | None = None,
    ) -> dict[str, object]:
        if error_reason:
            return {
                "state": "degraded",
                "state_label": "Inspection unavailable",
                "revision": 0,
                "observations": [],
                "services": [],
                "data_sources": [],
                "recommended_checks": [],
                "warnings": [
                    "Saved inspection evidence could not be read safely. No value was trusted."
                ],
                "can_run": connected,
            }
        state = self.state(snapshot)
        if snapshot is None:
            return {
                "state": "ready" if connected else "blocked",
                "state_label": (
                    "Ready to inspect" if connected else "Connect a federation first"
                ),
                "revision": 0,
                "observations": [],
                "services": [],
                "data_sources": [],
                "recommended_checks": [],
                "warnings": [],
                "can_run": connected,
            }
        warnings = list(snapshot.warnings)
        if state == "expired":
            warnings.insert(
                0,
                "This inspection has expired. Run it again before using its recommendations.",
            )
        return {
            "state": state,
            "state_label": (
                "Inspection current" if state == "current" else "Inspection expired"
            ),
            "revision": snapshot.revision,
            "observations": self._observation_models(snapshot),
            "services": self._service_models(snapshot),
            "data_sources": self._data_source_models(snapshot),
            "recommended_checks": self._benchmark_models(snapshot),
            "warnings": list(dict.fromkeys(warnings)),
            "can_run": connected,
        }

    def apply_to_onboarding_view_model(
        self,
        view_model: dict[str, object],
        *,
        snapshot: DeviceInspectionSnapshot | None,
        connected: bool,
        requested_step: str | None,
        error_reason: str | None = None,
    ) -> dict[str, object]:
        inspection = self.view_model(
            snapshot,
            connected=connected,
            error_reason=error_reason,
        )
        view_model["inspection"] = inspection
        view_model["page_intro"] = (
            "Load one stable identity, connect a trusted federation, then inspect "
            "supported local services and data sources without granting authority."
        )
        storage_key = str(view_model.get("storage_key") or "")
        view_model["storage_key"] = storage_key.replace(".cfi2", ".cfi3")

        steps = view_model.get("steps")
        if isinstance(steps, list):
            for step in steps:
                if not isinstance(step, dict) or step.get("key") != "inspect":
                    continue
                step["available"] = connected
                if error_reason:
                    step["summary"] = "Inspection unavailable"
                elif snapshot is None:
                    step["summary"] = "Ready" if connected else "Connect first"
                elif self.state(snapshot) == "expired":
                    step["summary"] = "Expired"
                else:
                    step["summary"] = "Current"

        completed = view_model.get("completed_steps")
        if isinstance(completed, list):
            while "inspect" in completed:
                completed.remove("inspect")
            if snapshot is not None and self.state(snapshot) == "current" and not error_reason:
                completed.append("inspect")

        if connected and requested_step in {None, "inspect"}:
            view_model["current_step"] = "inspect"

        revision = snapshot.revision if snapshot is not None else 0
        view_model["progress_revision"] = max(
            int(view_model.get("progress_revision") or 1),
            revision,
        )
        finish = view_model.get("finish")
        if isinstance(finish, dict):
            finish["title"] = "Inspection integrated; later actions remain blocked"
            finish["message"] = (
                "CFI-3 adds read-only device inspection. Benchmark execution and "
                "contribution actions still require separate reviewed integrations."
            )
        return view_model


def get_capability_inspection_service() -> CapabilityInspectionService:
    configured = current_app.config.get("CAPABILITY_INSPECTION_SERVICE")
    if isinstance(configured, CapabilityInspectionService):
        return configured
    existing = current_app.extensions.get(_SERVICE_EXTENSION_KEY)
    if isinstance(existing, CapabilityInspectionService):
        return existing

    onboarding = get_capability_onboarding_service()
    service = CapabilityInspectionService(
        onboarding_service=onboarding,
        state_database=current_app.config[
            "CAPABILITY_ONBOARDING_STATE_DATABASE"
        ],
        adapters=current_app.config.get(
            "CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS"
        ),
        setup_loader=getattr(onboarding, "_setup_loader", load_settings),
        mtconnect_scan_supplier=current_app.config.get(
            "CAPABILITY_ONBOARDING_MTCONNECT_SCAN_SUPPLIER"
        ),
        inspection_ttl_seconds=int(
            current_app.config.get(
                "CAPABILITY_ONBOARDING_INSPECTION_TTL_SECONDS",
                900,
            )
        ),
        clock=getattr(onboarding, "_clock", _utc_now),
        system_observer=current_app.config.get(
            "CAPABILITY_ONBOARDING_SYSTEM_OBSERVER"
        ),
    )
    current_app.extensions[_SERVICE_EXTENSION_KEY] = service
    return service


__all__ = [
    "CapabilityInspectionService",
    "InspectionSnapshotStore",
    "get_capability_inspection_service",
]
