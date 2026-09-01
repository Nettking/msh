from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from flask import Flask

from catalog.capabilities.contributions import ComputeContributionAdapter
from catalog.capabilities.worker_activation import LocalComputeHandlerDescriptor
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionIntent,
    ContributionPolicyState,
)
from catalog.flask_app.services import capability_contribution_components as components

NOW = datetime(2026, 9, 1, 9, 15, tzinfo=timezone.utc)


class _Inventory:
    def __init__(self) -> None:
        descriptor = LocalComputeHandlerDescriptor(
            handler_id="wired-handler",
            capability_type="wired-compute",
            protocol="fcp-wired",
            protocol_version="1.0",
            attributes={"mode": "bounded"},
        )
        self.binding = SimpleNamespace(descriptor=descriptor)

    def list_bindings(self):
        return (self.binding,)

    def get(self, handler_id):
        return self.binding if handler_id == self.binding.descriptor.handler_id else None


def _candidate(node_id: str, inventory: _Inventory) -> ContributionCandidate:
    descriptor = inventory.binding.descriptor
    return ContributionCandidate(
        candidate_id="candidate-wired-provider",
        device_id=node_id,
        capability_type="compute",
        capability_protocol=descriptor.protocol,
        display_label="Wired provider",
        inspection_revision=1,
        benchmark_run_ids=(),
        capacity_envelope={
            "kind": "registered-compute-handler",
            "handler_id": descriptor.handler_id,
            "handler_capability_type": descriptor.capability_type,
            "protocol_version": descriptor.protocol_version,
            "descriptor_fingerprint": descriptor.descriptor_fingerprint,
            "handler_attributes": descriptor.attributes,
        },
        missing_prerequisites=(),
        policy_state=ContributionPolicyState.ALLOWED,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


def test_default_explicit_compute_adapter_projects_persisted_intent(
    monkeypatch,
) -> None:
    monkeypatch.delenv("FCP_AI_MODEL", raising=False)
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    calls = []
    monkeypatch.setattr(
        components,
        "reconcile_registered_compute_provider",
        lambda **kwargs: calls.append(kwargs),
    )
    inventory = _Inventory()
    active = set()
    node_id = "node-wired-provider"
    onboarding = SimpleNamespace(_clock=lambda: NOW)
    app = Flask(__name__)
    app.config.update(
        CAPABILITY_ONBOARDING_COMPUTE_INVENTORY=inventory,
        CAPABILITY_ONBOARDING_COMPUTE_ACTIVATE_BINDING=lambda binding: active.add(
            binding.descriptor.handler_id
        ),
        CAPABILITY_ONBOARDING_COMPUTE_FENCE_HANDLER=lambda handler_id, _fingerprint: active.discard(
            handler_id
        ),
        CAPABILITY_ONBOARDING_COMPUTE_IS_HANDLER_ACTIVE=lambda handler_id, _fingerprint: handler_id
        in active,
    )

    with app.app_context():
        _sources, adapters = components.default_components(
            onboarding_service=onboarding,
            config_loader=lambda: None,
        )

    compute = next(item for item in adapters if isinstance(item, ComputeContributionAdapter))
    candidate = _candidate(node_id, inventory)
    intent = ContributionIntent(
        candidate_id=candidate.candidate_id,
        device_id=node_id,
        desired_state=ContributionDesiredState.ENABLED,
        decision_revision=3,
        policy_state=ContributionPolicyState.ALLOWED,
        activation_state=ContributionActivationState.ACTIVE,
        reason=None,
        decided_at=NOW,
    )
    compute.reconcile_persisted(candidate, intent)

    assert len(calls) == 1
    assert calls[0]["onboarding_service"] is onboarding
    assert calls[0]["candidate"] is candidate
    assert calls[0]["intent"] is intent
    assert calls[0]["clock"]() == NOW
