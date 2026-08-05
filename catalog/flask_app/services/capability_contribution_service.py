"""CFI-5 composition of CF4 contribution intent into Flask onboarding."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import current_app

from catalog.capabilities.contributions import (
    ContributionCandidateGenerator,
    ContributionPolicyEvaluator,
    ContributionService,
    SQLiteContributionIntentStore,
)
from catalog.capabilities.contributions.errors import CandidateNotFoundError
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionIntent,
    ContributionPolicyState,
    DeviceInspectionSnapshot,
)

from .capability_benchmark_service import (
    CapabilityBenchmarkService,
    get_capability_benchmark_service,
)
from .capability_contribution_components import (
    default_components,
    default_policy,
)
from .capability_inspection_service import (
    CapabilityInspectionService,
    get_capability_inspection_service,
)
from .capability_onboarding_service import (
    CapabilityOnboardingService,
    get_capability_onboarding_service,
)
from .server_setup_service import ServerSetupError, load_settings

_SERVICE_EXTENSION_KEY = "capability_contribution_service"
_MAX_IDENTIFIER_BYTES = 512
_INTERNAL_CAPACITY_KEYS = frozenset(
    {
        "authority",
        "descriptor_fingerprint",
        "handler_id",
        "provider_id",
        "protocol_version",
    }
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _bounded_identifier(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-contribution-identifier",
            field,
            "must be a printable string",
        )
    normalized = value.strip()
    if (
        not normalized
        or len(normalized.encode("utf-8")) > _MAX_IDENTIFIER_BYTES
        or any(ord(character) < 32 for character in normalized)
    ):
        raise FederationValidationError(
            "invalid-contribution-identifier",
            field,
            "must be a bounded printable string",
        )
    return normalized


class CapabilityContributionService:
    """Bind CF4 recommendation and intent to trusted CFI-2/3/4 state."""

    def __init__(
        self,
        *,
        onboarding_service: CapabilityOnboardingService,
        inspection_service: CapabilityInspectionService,
        benchmark_service: CapabilityBenchmarkService,
        state_database: Path | str,
        sources: Iterable[object] | Callable[[], Iterable[object]] | None = None,
        adapters: Sequence[object] | Callable[[], Sequence[object]] | None = None,
        setup_loader: Callable[[], object] = load_settings,
        clock: Callable[[], datetime] = _utc_now,
        candidate_ttl_seconds: int = 900,
    ) -> None:
        if isinstance(candidate_ttl_seconds, bool) or candidate_ttl_seconds <= 0:
            raise ValueError("candidate_ttl_seconds must be positive")
        self.onboarding_service = onboarding_service
        self.inspection_service = inspection_service
        self.benchmark_service = benchmark_service
        self.state_database = Path(state_database)
        self._configured_sources = sources
        self._configured_adapters = adapters
        self._setup_loader = setup_loader
        self._clock = clock
        self._candidate_ttl_seconds = int(candidate_ttl_seconds)
        self._lock = threading.RLock()
        self._store_instance: SQLiteContributionIntentStore | None = None
        self._service_instance: ContributionService | None = None

    def _setup(self) -> object | None:
        try:
            return self._setup_loader()
        except (OSError, ServerSetupError, TypeError, ValueError):
            return None

    @staticmethod
    def _iterable(value: object, *, field: str) -> tuple[object, ...]:
        raw = value() if callable(value) and not hasattr(value, "supports") else value
        if raw is None or isinstance(raw, (str, bytes, Mapping)):
            raise FederationValidationError(
                "invalid-contribution-composition",
                field,
                "must be an iterable of contribution components",
            )
        try:
            return tuple(raw)  # type: ignore[arg-type]
        except TypeError as exc:
            raise FederationValidationError(
                "invalid-contribution-composition",
                field,
                "must be an iterable of contribution components",
            ) from exc

    def _components(self) -> tuple[tuple[object, ...], tuple[object, ...]]:
        if self._configured_sources is None and self._configured_adapters is None:
            return default_components(
                onboarding_service=self.onboarding_service,
                setup_loader=self._setup,
            )
        if self._configured_sources is None or self._configured_adapters is None:
            raise FederationValidationError(
                "invalid-contribution-composition",
                "components",
                "configured sources and adapters must be supplied together",
            )
        sources = self._iterable(self._configured_sources, field="sources")
        adapters = self._iterable(self._configured_adapters, field="adapters")
        if any(not callable(getattr(item, "descriptors", None)) for item in sources):
            raise FederationValidationError(
                "invalid-contribution-composition",
                "sources",
                "every source must provide descriptors()",
            )
        if any(not callable(getattr(item, "supports", None)) for item in adapters):
            raise FederationValidationError(
                "invalid-contribution-composition",
                "adapters",
                "every adapter must provide supports()",
            )
        return sources, adapters

    def _policy(self) -> ContributionPolicyEvaluator:
        configured = current_app.config.get(
            "CAPABILITY_ONBOARDING_CONTRIBUTION_POLICY"
        )
        if configured is None:
            return default_policy(self._setup)
        if not isinstance(configured, ContributionPolicyEvaluator):
            raise FederationValidationError(
                "invalid-contribution-policy",
                "policy",
                "must be a ContributionPolicyEvaluator",
            )
        return configured

    def _store(self) -> SQLiteContributionIntentStore:
        with self._lock:
            if self._store_instance is None:
                self.state_database.parent.mkdir(
                    parents=True,
                    exist_ok=True,
                    mode=0o700,
                )
                try:
                    self._store_instance = SQLiteContributionIntentStore(
                        self.state_database
                    )
                except sqlite3.Error as exc:
                    raise FederationValidationError(
                        "contribution-state-initialize-failed",
                        "database",
                        "contribution intent storage could not be initialized safely",
                    ) from exc
                try:
                    self.state_database.chmod(0o600)
                except OSError as exc:
                    raise FederationValidationError(
                        "contribution-state-permission-failed",
                        "database",
                        "contribution state permissions could not be restricted",
                    ) from exc
            return self._store_instance

    def _build_service(
        self,
        *,
        generator_clock: Callable[[], datetime],
    ) -> ContributionService:
        sources, adapters = self._components()
        registry, _benchmark_adapters = self.benchmark_service._components()
        return ContributionService(
            generator=ContributionCandidateGenerator(
                sources=sources,
                benchmark_definitions=registry.definitions(),
                candidate_ttl_seconds=self._candidate_ttl_seconds,
                now=generator_clock,
            ),
            store=self._store(),
            policy=self._policy(),
            adapters=adapters,
            now=self._clock,
        )

    def _service(self) -> ContributionService:
        with self._lock:
            if self._service_instance is None:
                self._service_instance = self._build_service(
                    generator_clock=self._clock
                )
            return self._service_instance

    def close(self) -> None:
        with self._lock:
            if self._store_instance is not None:
                self._store_instance.close()
                self._store_instance = None
                self._service_instance = None

    def has_persisted_intents(self) -> bool:
        if not self.state_database.exists():
            return False
        try:
            with sqlite3.connect(str(self.state_database), timeout=30) as connection:
                table = connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='contribution_intents'
                    """
                ).fetchone()
                if table is None:
                    return False
                row = connection.execute(
                    "SELECT 1 FROM contribution_intents LIMIT 1"
                ).fetchone()
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "malformed-contribution-state",
                "database",
                "contribution intent state could not be read safely",
            ) from exc
        return row is not None

    def _authorized_snapshot(
        self,
        *,
        require_benchmark_review: bool,
    ) -> tuple[DeviceInspectionSnapshot, bool]:
        context = self.onboarding_service.authorized_context()
        if context is None:
            raise FederationOperationError(
                "contribution-federation-required",
                "a trusted federation connection is required before contributions",
                "binding",
            )
        snapshot = self.inspection_service.load()
        if snapshot is None:
            raise FederationOperationError(
                "contribution-inspection-required",
                "run device inspection before choosing contributions",
                "inspection",
            )
        if snapshot.device_id != context.credentials.identity.node_id:
            raise FederationValidationError(
                "contribution-device-mismatch",
                "device_id",
                "inspection evidence belongs to another device identity",
            )
        if self.inspection_service.state(snapshot) != "current":
            raise FederationOperationError(
                "contribution-inspection-expired",
                "rerun device inspection before choosing contributions",
                "inspection",
            )
        _summary, _cards, benchmark_complete = self.benchmark_service.view_model(
            snapshot,
            connected=True,
        )
        if require_benchmark_review and not benchmark_complete:
            raise FederationOperationError(
                "contribution-benchmarks-required",
                "run or skip benchmarks before choosing contributions",
                "benchmarks",
            )
        return snapshot, benchmark_complete

    def recommend(
        self,
        *,
        require_benchmark_review: bool = True,
    ) -> tuple[ContributionCandidate, ...]:
        snapshot, _complete = self._authorized_snapshot(
            require_benchmark_review=require_benchmark_review
        )
        return self._service().recommend(
            snapshot,
            self.benchmark_service.list_results(),
        )

    def intents(self) -> tuple[ContributionIntent, ...]:
        return self._store().list_intents()

    def apply_choices(
        self,
        choices: Mapping[str, object],
    ) -> tuple[ContributionIntent, ...]:
        candidates = {
            candidate.candidate_id: candidate
            for candidate in self.recommend(require_benchmark_review=True)
        }
        normalized: dict[str, ContributionDesiredState] = {}
        for raw_candidate_id, raw_state in choices.items():
            candidate_id = _bounded_identifier(raw_candidate_id, "candidate_id")
            if candidate_id not in candidates:
                raise CandidateNotFoundError(
                    f"candidate is not in the current recommendation set: {candidate_id}"
                )
            try:
                normalized[candidate_id] = ContributionDesiredState(raw_state)
            except (TypeError, ValueError) as exc:
                raise FederationValidationError(
                    "invalid-contribution-choice",
                    "desired_state",
                    "must be enabled, disabled, or ask-later",
                ) from exc

        service = self._service()
        outcomes: list[ContributionIntent] = []
        for candidate_id in sorted(normalized):
            desired = normalized[candidate_id]
            if desired is ContributionDesiredState.ENABLED:
                outcomes.append(service.enable(candidate_id))
            elif desired is ContributionDesiredState.DISABLED:
                outcomes.append(service.disable(candidate_id))
            else:
                outcomes.append(service.ask_later(candidate_id))
        return tuple(outcomes)

    def _service_for_reconciliation(self) -> ContributionService:
        context = self.onboarding_service.authorized_context()
        if context is None:
            raise FederationOperationError(
                "contribution-federation-required",
                "a trusted federation connection is required before reconciliation",
                "binding",
            )
        snapshot = self.inspection_service.load()
        if snapshot is None:
            raise FederationOperationError(
                "contribution-inspection-required",
                "saved inspection evidence is required for reconciliation",
                "inspection",
            )
        if snapshot.device_id != context.credentials.identity.node_id:
            raise FederationValidationError(
                "contribution-device-mismatch",
                "device_id",
                "inspection evidence belongs to another device identity",
            )
        if self.inspection_service.state(snapshot) == "current":
            service = self._service()
        else:
            anchor = snapshot.created_at.astimezone(timezone.utc)
            service = self._build_service(generator_clock=lambda: anchor)
        service.recommend(snapshot, self.benchmark_service.list_results())
        return service

    def suspend(self, candidate_id: str) -> ContributionIntent:
        candidate_id = _bounded_identifier(candidate_id, "candidate_id")
        return self._service_for_reconciliation().suspend(
            candidate_id,
            reason="Temporarily suspended by the local operator.",
        )

    def reconcile(self) -> tuple[ContributionIntent, ...]:
        if not self.has_persisted_intents():
            return ()
        return self._service_for_reconciliation().reconcile()

    @staticmethod
    def _humanize(value: str) -> str:
        return " ".join(
            part for part in value.replace("_", "-").split("-") if part
        ).title()

    @classmethod
    def _format_value(cls, value: Any) -> str:
        if isinstance(value, bool):
            return "Yes" if value else "No"
        if isinstance(value, float):
            return f"{value:.3f}".rstrip("0").rstrip(".")
        if isinstance(value, Mapping):
            return "; ".join(
                f"{cls._humanize(str(key))}: {cls._format_value(item)}"
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            )
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return ", ".join(cls._format_value(item) for item in value)
        return str(value)

    @classmethod
    def _capacity_model(
        cls,
        candidate: ContributionCandidate,
    ) -> list[dict[str, str]]:
        facts: list[dict[str, str]] = []
        for key, value in sorted(candidate.capacity_envelope.items()):
            if key in _INTERNAL_CAPACITY_KEYS or key == "benchmark_metrics":
                continue
            facts.append(
                {
                    "label": cls._humanize(str(key)),
                    "value": cls._format_value(value),
                }
            )
        benchmark_metrics = candidate.capacity_envelope.get("benchmark_metrics")
        if isinstance(benchmark_metrics, Mapping):
            for key, value in sorted(benchmark_metrics.items()):
                facts.append(
                    {
                        "label": f"Benchmark {cls._humanize(str(key))}",
                        "value": cls._format_value(value),
                    }
                )
        return facts

    @staticmethod
    def _type_details(capability_type: str) -> tuple[str, str]:
        details = {
            "recorder": (
                "Recorder",
                "Record only the explicitly inspected local data sources.",
            ),
            "language-model": (
                "Language model",
                "Offer the configured local model through the existing AI runtime.",
            ),
            "compute": (
                "Compute",
                "Expose only a handler already registered in local inventory.",
            ),
            "storage": (
                "Storage candidate",
                "Request candidacy only; assignment remains control-plane owned.",
            ),
        }
        return details.get(
            capability_type,
            (
                CapabilityContributionService._humanize(capability_type),
                "Use the existing authority adapter for this capability.",
            ),
        )

    def _candidate_models(
        self,
        candidates: Sequence[ContributionCandidate],
    ) -> tuple[list[dict[str, object]], bool]:
        intents = {intent.candidate_id: intent for intent in self.intents()}
        cards: list[dict[str, object]] = []
        reviewed = 0
        for candidate in candidates:
            intent = intents.get(candidate.candidate_id)
            desired = (
                ContributionDesiredState.ASK_LATER
                if intent is None
                else intent.desired_state
            )
            activation = (
                ContributionActivationState.INACTIVE
                if intent is None
                else intent.activation_state
            )
            policy_state = (
                candidate.policy_state if intent is None else intent.policy_state
            )
            if intent is not None or policy_state is ContributionPolicyState.BLOCKED:
                reviewed += 1
            type_label, description = self._type_details(candidate.capability_type)
            if policy_state is ContributionPolicyState.BLOCKED:
                recommendation, recommendation_label = "blocked", "Blocked"
            elif activation is ContributionActivationState.ACTIVE:
                recommendation, recommendation_label = "recommended", "Active"
            elif activation is ContributionActivationState.PENDING:
                recommendation, recommendation_label = "pending", "Pending approval"
            elif activation is ContributionActivationState.SUSPENDED:
                recommendation, recommendation_label = "rerun-required", "Suspended"
            else:
                recommendation, recommendation_label = "available", "Available"
            cards.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "type_label": type_label,
                    "label": candidate.display_label,
                    "description": description,
                    "capacity": self._capacity_model(candidate),
                    "missing_prerequisites": [
                        self._humanize(value)
                        for value in candidate.missing_prerequisites
                    ],
                    "policy_state": policy_state.value,
                    "desired_state": desired.value,
                    "activation_state": activation.value,
                    "activation_label": self._humanize(activation.value),
                    "recommendation": recommendation,
                    "recommendation_label": recommendation_label,
                    "input_name": f"contribution.{candidate.candidate_id}",
                    "choices": [
                        {
                            "value": ContributionDesiredState.ENABLED.value,
                            "label": "Enable",
                            "description": "Use the existing authority path.",
                        },
                        {
                            "value": ContributionDesiredState.DISABLED.value,
                            "label": "Keep disabled",
                            "description": "Fence future use without removing membership.",
                        },
                        {
                            "value": ContributionDesiredState.ASK_LATER.value,
                            "label": "Ask later",
                            "description": "Keep this candidate inactive for now.",
                        },
                    ],
                    "detail_url": None,
                    "reason": None if intent is None else intent.reason,
                }
            )
        return cards, not candidates or reviewed == len(candidates)

    def view_model(
        self,
        snapshot: DeviceInspectionSnapshot | None,
        *,
        connected: bool,
        benchmark_complete: bool,
        error_reason: str | None = None,
    ) -> tuple[dict[str, object], list[dict[str, object]], bool]:
        blocked = {
            "state": "blocked",
            "can_reconcile": False,
        }
        if error_reason:
            return (
                {
                    "state": "degraded",
                    "label": "Contribution state unavailable",
                    "can_reconcile": False,
                },
                [],
                False,
            )
        if not connected:
            return ({**blocked, "label": "Connect a federation first"}, [], False)
        if snapshot is None:
            return ({**blocked, "label": "Run inspection first"}, [], False)
        if self.inspection_service.state(snapshot) != "current":
            return ({**blocked, "label": "Inspection expired"}, [], False)
        if not benchmark_complete:
            return ({**blocked, "label": "Review benchmarks first"}, [], False)
        candidates = self.recommend(require_benchmark_review=True)
        cards, complete = self._candidate_models(candidates)
        if not cards:
            summary = {
                "state": "complete",
                "label": "No supported contributions found",
                "can_reconcile": bool(self.intents()),
            }
        else:
            summary = {
                "state": "complete" if complete else "pending",
                "label": (
                    "Contribution choices saved"
                    if complete
                    else "Choose what this device contributes"
                ),
                "can_reconcile": bool(self.intents()),
            }
        return summary, cards, complete

    def apply_to_onboarding_view_model(
        self,
        view_model: dict[str, object],
        *,
        snapshot: DeviceInspectionSnapshot | None,
        connected: bool,
        benchmark_complete: bool,
        requested_step: str | None,
        error_reason: str | None = None,
    ) -> dict[str, object]:
        summary, cards, complete = self.view_model(
            snapshot,
            connected=connected,
            benchmark_complete=benchmark_complete,
            error_reason=error_reason,
        )
        view_model["contribution_summary"] = summary
        view_model["contributions"] = cards
        view_model["page_intro"] = (
            "Inspect and benchmark this device, then independently choose its "
            "contributions. Evidence alone never grants authority."
        )
        storage_key = str(view_model.get("storage_key") or "")
        view_model["storage_key"] = storage_key.replace(".cfi4", ".cfi5")

        available = (
            connected
            and snapshot is not None
            and self.inspection_service.state(snapshot) == "current"
            and benchmark_complete
            and error_reason is None
        )
        steps = view_model.get("steps")
        if isinstance(steps, list):
            for step in steps:
                if not isinstance(step, dict):
                    continue
                if step.get("key") == "contributions":
                    step["available"] = available
                    step["summary"] = str(summary["label"])
                elif step.get("key") == "finish":
                    step["available"] = available and complete

        completed = view_model.get("completed_steps")
        if isinstance(completed, list):
            while "contributions" in completed:
                completed.remove("contributions")
            if available and complete:
                completed.append("contributions")

        if available and requested_step in {None, "contributions"}:
            view_model["current_step"] = "contributions"
        if available and complete and requested_step == "finish":
            view_model["current_step"] = "finish"

        actions = view_model.get("actions")
        if isinstance(actions, dict):
            actions["contributions_url"] = "/onboarding/contributions"
            actions["reconcile_contributions_url"] = (
                "/onboarding/contributions/reconcile"
            )

        finish = view_model.get("finish")
        if isinstance(finish, dict):
            if available and complete:
                finish["title"] = "Contribution choices are connected"
                finish["message"] = (
                    "Explicit intent is persisted locally and reconciled through "
                    "existing authority paths. Storage remains candidate-only."
                )
            else:
                finish["title"] = "Choose contributions before finishing"
                finish["message"] = (
                    "Enable, disable, or defer each supported contribution. "
                    "Membership is never removed by these choices."
                )

        intent_revision = 0
        if error_reason is None:
            intent_revision = max(
                (intent.decision_revision for intent in self.intents()),
                default=0,
            )
        view_model["progress_revision"] = max(
            int(view_model.get("progress_revision") or 1),
            (snapshot.revision + 2) if snapshot is not None else 1,
            intent_revision,
        )
        return view_model


def get_capability_contribution_service() -> CapabilityContributionService:
    configured = current_app.config.get("CAPABILITY_CONTRIBUTION_SERVICE")
    if isinstance(configured, CapabilityContributionService):
        return configured
    existing = current_app.extensions.get(_SERVICE_EXTENSION_KEY)
    if isinstance(existing, CapabilityContributionService):
        return existing

    onboarding = get_capability_onboarding_service()
    inspection = get_capability_inspection_service()
    benchmarks = get_capability_benchmark_service()
    service = CapabilityContributionService(
        onboarding_service=onboarding,
        inspection_service=inspection,
        benchmark_service=benchmarks,
        state_database=current_app.config.get(
            "CAPABILITY_ONBOARDING_CONTRIBUTION_DATABASE",
            current_app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
        ),
        sources=current_app.config.get(
            "CAPABILITY_ONBOARDING_CONTRIBUTION_SOURCES"
        ),
        adapters=current_app.config.get(
            "CAPABILITY_ONBOARDING_CONTRIBUTION_ADAPTERS"
        ),
        setup_loader=current_app.config.get(
            "CAPABILITY_ONBOARDING_SETUP_LOADER",
            load_settings,
        ),
        clock=getattr(onboarding, "_clock", _utc_now),
        candidate_ttl_seconds=int(
            current_app.config.get(
                "CAPABILITY_ONBOARDING_CONTRIBUTION_TTL_SECONDS",
                900,
            )
        ),
    )
    current_app.extensions[_SERVICE_EXTENSION_KEY] = service
    return service


__all__ = [
    "CapabilityContributionService",
    "get_capability_contribution_service",
]
