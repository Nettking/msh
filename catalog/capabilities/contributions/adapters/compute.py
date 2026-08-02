"""Registered-compute-handler candidate and authority adapters."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    DeviceInspectionSnapshot,
)

from ..types import AdapterOutcome, LocalContributionDescriptor


class ComputeInventory(Protocol):
    def get(self, handler_id: str) -> Any | None: ...

    def list_bindings(self) -> tuple[Any, ...]: ...


class ComputeCandidateSource:
    """Expose only handlers already present in the trusted local inventory."""

    def __init__(self, inventory: ComputeInventory) -> None:
        self._inventory = inventory

    def descriptors(
        self,
        inspection: DeviceInspectionSnapshot,
    ) -> tuple[LocalContributionDescriptor, ...]:
        inspected = set(inspection.registered_handlers)
        descriptors: list[LocalContributionDescriptor] = []
        for binding in self._inventory.list_bindings():
            descriptor = binding.descriptor
            if descriptor.handler_id not in inspected:
                continue
            descriptors.append(
                LocalContributionDescriptor(
                    logical_service_id=descriptor.handler_id,
                    capability_type="compute",
                    capability_protocol=descriptor.protocol,
                    display_label=f"Compute {descriptor.handler_id}",
                    capacity_envelope={
                        "kind": "registered-compute-handler",
                        "handler_id": descriptor.handler_id,
                        "handler_capability_type": descriptor.capability_type,
                        "protocol_version": descriptor.protocol_version,
                        "descriptor_fingerprint": descriptor.descriptor_fingerprint,
                    },
                )
            )
        return tuple(sorted(descriptors, key=lambda item: item.logical_service_id))


class ComputeContributionAdapter:
    candidate_only = False

    def __init__(
        self,
        inventory: ComputeInventory,
        *,
        activate_binding: Callable[[Any], None],
        fence_handler: Callable[[str, str], None],
        is_handler_active: Callable[[str, str], bool],
    ) -> None:
        self._inventory = inventory
        self._activate_binding = activate_binding
        self._fence_handler = fence_handler
        self._is_handler_active = is_handler_active

    def supports(self, candidate: ContributionCandidate) -> bool:
        return (
            candidate.capability_type == "compute"
            and candidate.capacity_envelope.get("kind")
            == "registered-compute-handler"
        )

    def enable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        binding = self._current_binding(candidate)
        self._activate_binding(binding)
        return AdapterOutcome(ContributionActivationState.ACTIVE)

    def disable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        handler_id, fingerprint = self._identity(candidate)
        self._fence_handler(handler_id, fingerprint)
        return AdapterOutcome(ContributionActivationState.INACTIVE)

    def suspend(
        self,
        candidate: ContributionCandidate,
        *,
        reason: str,
    ) -> AdapterOutcome:
        handler_id, fingerprint = self._identity(candidate)
        self._fence_handler(handler_id, fingerprint)
        return AdapterOutcome(ContributionActivationState.SUSPENDED, reason)

    def reconcile(
        self,
        candidate: ContributionCandidate,
        *,
        desired_state: ContributionDesiredState,
    ) -> AdapterOutcome:
        handler_id, fingerprint = self._identity(candidate)
        if desired_state is ContributionDesiredState.DISABLED:
            self._fence_handler(handler_id, fingerprint)
            return AdapterOutcome(ContributionActivationState.INACTIVE)
        binding = self._current_binding(candidate)
        if not self._is_handler_active(handler_id, fingerprint):
            self._activate_binding(binding)
        return AdapterOutcome(ContributionActivationState.ACTIVE)

    def _current_binding(self, candidate: ContributionCandidate) -> Any:
        handler_id, fingerprint = self._identity(candidate)
        binding = self._inventory.get(handler_id)
        if binding is None:
            raise ValueError("compute handler is no longer registered")
        current = getattr(binding.descriptor, "descriptor_fingerprint", None)
        if current != fingerprint:
            raise ValueError("compute handler registration changed")
        return binding

    def _identity(self, candidate: ContributionCandidate) -> tuple[str, str]:
        if not self.supports(candidate):
            raise ValueError("compute adapter received an unsupported candidate")
        handler_id = candidate.capacity_envelope.get("handler_id")
        fingerprint = candidate.capacity_envelope.get("descriptor_fingerprint")
        if not isinstance(handler_id, str) or not handler_id:
            raise ValueError("compute candidate is missing handler_id")
        if not isinstance(fingerprint, str) or not fingerprint:
            raise ValueError("compute candidate is missing descriptor_fingerprint")
        return handler_id, fingerprint
