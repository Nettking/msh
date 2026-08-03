"""Trusted authority components used by CFI-5 contribution composition."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from flask import current_app

from catalog.ai.ollama_provider import OllamaLanguageModelProvider
from catalog.capabilities.contributions import (
    AICandidateSource,
    AICandidateSpec,
    AIContributionAdapter,
    ComputeCandidateSource,
    ComputeContributionAdapter,
    ContributionPolicyEvaluator,
    PolicyEvaluation,
    RecorderCandidateSource,
    RecorderContributionAdapter,
    StorageCandidateSource,
    StorageCandidateSpec,
    StorageContributionAdapter,
)
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.onboarding_models import (
    BenchmarkResult,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionPolicyState,
)

from .capability_onboarding_service import CapabilityOnboardingService
from .recorder_control_service import get_recorder_control_service
from .server_setup_service import ai_provider_label


def _payload(settings: object | None) -> dict[str, Any]:
    if settings is None:
        return {}
    value = settings.to_dict() if callable(getattr(settings, "to_dict", None)) else settings
    return dict(value) if isinstance(value, Mapping) else {}


def default_policy(
    setup_loader: Callable[[], object | None],
) -> ContributionPolicyEvaluator:
    recorder = get_recorder_control_service()

    def recorder_ready_rule(
        candidate: ContributionCandidate,
        _evidence: tuple[BenchmarkResult, ...],
        desired_state: ContributionDesiredState,
    ) -> PolicyEvaluation | None:
        if (
            candidate.capability_type == "recorder"
            and desired_state is ContributionDesiredState.ENABLED
            and not recorder.ready(setup_loader())
        ):
            return PolicyEvaluation(
                ContributionPolicyState.BLOCKED,
                (
                    "Recorder activation requires existing configured MTConnect "
                    "sources and the compatible recorder authority."
                ),
            )
        return None

    return ContributionPolicyEvaluator(rules=(recorder_ready_rule,))


def default_components(
    *,
    onboarding_service: CapabilityOnboardingService,
    setup_loader: Callable[[], object | None],
) -> tuple[tuple[object, ...], tuple[object, ...]]:
    """Compose only authorities that already exist and are explicitly configured."""

    sources: list[object] = [RecorderCandidateSource()]
    adapters: list[object] = [
        RecorderContributionAdapter(
            get_recorder_control_service(),
            settings_provider=setup_loader,
        )
    ]

    settings = setup_loader()
    setup = _payload(settings)
    if (
        setup.get("configured")
        and setup.get("user_setup_complete")
        and setup.get("ai_enabled")
        and str(setup.get("ai_model") or "").strip()
        and str(setup.get("ollama_base_url") or "").strip()
    ):
        service_id = "ollama-configured"
        model = str(setup["ai_model"]).strip()
        display_label = (
            ai_provider_label(settings)
            if settings is not None
            else "Configured Ollama service"
        )
        sources.append(
            AICandidateSource(
                {
                    service_id: AICandidateSpec(
                        service_id=service_id,
                        protocol="msh-language-model",
                        display_label=display_label,
                        capacity_envelope={
                            "model": model,
                            "modality": "text",
                            "max_concurrent_jobs": 1,
                        },
                    )
                }
            )
        )
        manager = current_app.config.get("CAPABILITY_ONBOARDING_AI_RUNTIME_MANAGER")
        if manager is None:
            from catalog.flask_app.ai_routes import AI_RUNTIME_MANAGER

            manager = AI_RUNTIME_MANAGER
        base_url = str(setup["ollama_base_url"]).strip()

        def provider_factory(
            candidate: ContributionCandidate,
        ) -> OllamaLanguageModelProvider:
            context = onboarding_service.authorized_context()
            if context is None:
                raise FederationOperationError(
                    "contribution-federation-required",
                    "a trusted federation connection is required",
                    "binding",
                )
            provider_id = candidate.capacity_envelope.get("provider_id")
            if not isinstance(provider_id, str) or not provider_id:
                raise FederationValidationError(
                    "invalid-ai-contribution-candidate",
                    "provider_id",
                    "AI contribution lacks its logical provider identity",
                )
            return OllamaLanguageModelProvider(
                session_id=manager.session_id,
                display_name=candidate.display_label,
                base_url=base_url,
                models=(model,),
                capability_id=provider_id,
                node_id=context.credentials.identity.node_id,
            )

        adapters.append(
            AIContributionAdapter.for_runtime_manager(
                manager,
                provider_factory=provider_factory,
            )
        )

    inventory = current_app.config.get("CAPABILITY_ONBOARDING_COMPUTE_INVENTORY")
    activate_binding = current_app.config.get(
        "CAPABILITY_ONBOARDING_COMPUTE_ACTIVATE_BINDING"
    )
    fence_handler = current_app.config.get(
        "CAPABILITY_ONBOARDING_COMPUTE_FENCE_HANDLER"
    )
    is_handler_active = current_app.config.get(
        "CAPABILITY_ONBOARDING_COMPUTE_IS_HANDLER_ACTIVE"
    )
    compute_values = (inventory, activate_binding, fence_handler, is_handler_active)
    if any(value is not None for value in compute_values):
        if (
            inventory is None
            or not callable(activate_binding)
            or not callable(fence_handler)
            or not callable(is_handler_active)
        ):
            raise FederationValidationError(
                "invalid-compute-contribution-composition",
                "compute",
                "all explicit compute inventory and authority seams are required",
            )
        sources.append(ComputeCandidateSource(inventory))
        adapters.append(
            ComputeContributionAdapter(
                inventory,
                activate_binding=activate_binding,
                fence_handler=fence_handler,
                is_handler_active=is_handler_active,
            )
        )

    storage_specs = current_app.config.get(
        "CAPABILITY_ONBOARDING_STORAGE_CANDIDATES"
    )
    is_assigned = current_app.config.get(
        "CAPABILITY_ONBOARDING_STORAGE_IS_ASSIGNED"
    )
    fence_candidate = current_app.config.get(
        "CAPABILITY_ONBOARDING_STORAGE_FENCE_CANDIDATE"
    )
    storage_values = (storage_specs, is_assigned, fence_candidate)
    if any(value is not None for value in storage_values):
        if (
            not isinstance(storage_specs, Mapping)
            or not callable(is_assigned)
            or not callable(fence_candidate)
        ):
            raise FederationValidationError(
                "invalid-storage-contribution-composition",
                "storage",
                "storage candidates and existing control-plane seams are required",
            )
        normalized: dict[str, StorageCandidateSpec] = {}
        for provider_id, spec in storage_specs.items():
            if not isinstance(provider_id, str) or not isinstance(
                spec, StorageCandidateSpec
            ):
                raise FederationValidationError(
                    "invalid-storage-contribution-composition",
                    "storage",
                    "storage candidate mapping is invalid",
                )
            normalized[provider_id] = spec
        sources.append(StorageCandidateSource(normalized))
        adapters.append(
            StorageContributionAdapter(
                is_assigned=is_assigned,
                fence_candidate=fence_candidate,
            )
        )

    return tuple(sources), tuple(adapters)


__all__ = ["default_components", "default_policy"]
