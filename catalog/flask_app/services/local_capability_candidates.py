"""Safe built-in compute and storage candidates for capability-first onboarding.

The built-ins make ordinary Docker installations useful without deriving authority
from benchmark evidence:

* compute exposes one explicitly registered, read-only local handler descriptor;
  selecting it remains pending until an existing worker/control-plane authority
  activates that handler;
* storage exposes the host-mounted FCP data area through a bounded temporary
  write/read/cleanup probe. When the authenticated device created the federation
  and no storage topology exists yet, an explicit Enable choice may register and
  assign this provider through the existing durable storage control plane.

A joined device never self-approves storage, and an existing storage topology is
never replaced by this bootstrap path.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flask import current_app

from catalog.capabilities.benchmarking import (
    RegisteredComputeHandlerAdapter,
    StorageCandidateAdapter,
    StorageCandidateTarget,
)
from catalog.capabilities.contributions import (
    AdapterOutcome,
    ComputeCandidateSource,
    StorageCandidateSource,
    StorageCandidateSpec,
    StorageContributionAdapter,
)
from catalog.capabilities.dispatch import ExecutionResult
from catalog.capabilities.storage_authority_enrollment import (
    DEFAULT_STORAGE_GROUP_ID,
    federation_storage_provider_id,
)
from catalog.capabilities.worker_activation import (
    LocalComputeHandlerDescriptor,
    LocalComputeHandlerInventory,
)
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
)
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)

_EXTENSION_KEY = "capability_local_candidate_bundle"
_COMPUTE_HANDLER_ID = "fcp-system-summary"
_STORAGE_PROVIDER_ID = "fcp-local-data-storage"
_STORAGE_GROUP_ID = DEFAULT_STORAGE_GROUP_ID
_STORAGE_FINGERPRINT = "sha256:" + hashlib.sha256(
    b"fcp-local-data-storage-candidate-v1"
).hexdigest()


class _ReadOnlySystemSummaryHandler:
    """A preinstalled bounded handler with no mutation or external I/O."""

    async def execute(self, _job: object) -> ExecutionResult:
        return ExecutionResult(
            True,
            {
                "operation": "system-summary",
                "mode": "read-only",
                "authority": "dispatch-required",
            },
        )


class PendingComputeContributionAdapter:
    """Persist compute candidacy without pretending a worker is active."""

    candidate_only = True

    def __init__(self, inventory: LocalComputeHandlerInventory) -> None:
        self._inventory = inventory

    def supports(self, candidate: ContributionCandidate) -> bool:
        return (
            candidate.capability_type == "compute"
            and candidate.capacity_envelope.get("kind")
            == "registered-compute-handler"
        )

    def enable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        self._current_binding(candidate)
        return AdapterOutcome(
            ContributionActivationState.PENDING,
            "Registered locally; waiting for existing compute worker authority.",
        )

    def disable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        self._identity(candidate)
        return AdapterOutcome(ContributionActivationState.INACTIVE)

    def suspend(
        self,
        candidate: ContributionCandidate,
        *,
        reason: str,
    ) -> AdapterOutcome:
        self._identity(candidate)
        return AdapterOutcome(ContributionActivationState.SUSPENDED, reason)

    def reconcile(
        self,
        candidate: ContributionCandidate,
        *,
        desired_state: ContributionDesiredState,
    ) -> AdapterOutcome:
        if desired_state is ContributionDesiredState.DISABLED:
            return self.disable(candidate)
        return self.enable(candidate)

    def _current_binding(self, candidate: ContributionCandidate) -> object:
        handler_id, fingerprint = self._identity(candidate)
        binding = self._inventory.get(handler_id)
        if binding is None:
            raise ValueError("compute handler is no longer registered")
        if binding.descriptor.descriptor_fingerprint != fingerprint:
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


class _LocalStorageProbe:
    """Bounded temporary storage probe under the mounted FCP data directory."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, probe_id: str) -> Path:
        name = hashlib.sha256(probe_id.encode("utf-8")).hexdigest() + ".probe"
        return self.root / name

    def write(self, probe_id: str, payload: bytes, _context: object) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        path = self._path(probe_id)
        with path.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())

    def read(self, probe_id: str, _context: object) -> bytes:
        return self._path(probe_id).read_bytes()

    def cleanup(self, probe_id: str, _context: object) -> bool:
        path = self._path(probe_id)
        path.unlink(missing_ok=True)
        return not path.exists()

    def fence(self, _provider_id: str) -> None:
        if not self.root.exists():
            return
        for path in self.root.glob("*.probe"):
            path.unlink(missing_ok=True)


class CreatorBootstrapStorageContributionAdapter:
    """Bootstrap only the creator's first local storage assignment.

    The adapter consumes an already authenticated onboarding context. It never
    treats a benchmark as authority, never approves a joining node, and never
    replaces an existing provider or storage-group assignment.
    """

    candidate_only = True

    def __init__(self, onboarding_service: object, probe: _LocalStorageProbe) -> None:
        self._onboarding_service = onboarding_service
        self._probe = probe

    def supports(self, candidate: ContributionCandidate) -> bool:
        return (
            candidate.capability_type == "storage"
            and candidate.capacity_envelope.get("authority") == "candidate-only"
            and candidate.capacity_envelope.get("provider_id")
            == _STORAGE_PROVIDER_ID
        )

    def enable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        local_provider_id = self._provider_id(candidate)
        context, session = self._context_and_session()
        node_id = context.credentials.identity.node_id
        # Every device detects the same local candidate id, so the control-plane
        # identity must be scoped to this member.
        provider_id = federation_storage_provider_id(node_id, local_provider_id)
        creator_node_id = getattr(session, "created_by_node_id", None)
        if creator_node_id != node_id:
            return AdapterOutcome(
                ContributionActivationState.PENDING,
                "Waiting for the federation creator to assign this storage provider.",
            )

        control = PhaseDControlPlane(context.coordinator.store.database)
        snapshot = control.snapshot(session.session_id)
        foreign_groups = set(snapshot.groups).difference({_STORAGE_GROUP_ID})
        foreign_providers = set(snapshot.providers).difference({provider_id})
        if foreign_groups or foreign_providers:
            return AdapterOutcome(
                ContributionActivationState.PENDING,
                "Existing storage topology requires an explicit coordinator assignment.",
            )

        if _STORAGE_GROUP_ID not in snapshot.groups:
            control.create_group(
                session.session_id,
                node_id,
                _STORAGE_GROUP_ID,
            )
            snapshot = control.snapshot(session.session_id)

        existing = snapshot.providers.get(provider_id)
        registration = StorageProviderRegistration(
            session_id=session.session_id,
            provider_id=provider_id,
            node_id=node_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status="ready",
        )
        if existing is None:
            control.register_provider(session.session_id, node_id, registration)
            snapshot = control.snapshot(session.session_id)
        elif existing != registration:
            raise FederationValidationError(
                "storage-bootstrap-provider-conflict",
                "provider_id",
                "existing storage provider registration does not match this device",
            )

        assignment = snapshot.groups[_STORAGE_GROUP_ID]
        if (
            assignment.primary_provider_id is None
            and not assignment.replica_provider_ids
        ):
            control.change_assignment(
                session.session_id,
                node_id,
                _STORAGE_GROUP_ID,
                provider_id,
            )
            assignment = control.snapshot(session.session_id).groups[
                _STORAGE_GROUP_ID
            ]

        if assignment.primary_provider_id != provider_id:
            return AdapterOutcome(
                ContributionActivationState.PENDING,
                "Existing storage assignment requires an explicit coordinator decision.",
            )
        return AdapterOutcome(
            ContributionActivationState.ACTIVE,
            "Authorized by the federation creator and assigned as the initial storage primary.",
            authority_confirmed=True,
        )

    def disable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        local_provider_id = self._provider_id(candidate)
        self._probe.fence(local_provider_id)
        try:
            context, session = self._context_and_session()
        except FederationOperationError:
            return AdapterOutcome(
                ContributionActivationState.INACTIVE,
                "Local storage use is fenced.",
            )

        node_id = context.credentials.identity.node_id
        provider_id = federation_storage_provider_id(node_id, local_provider_id)
        if getattr(session, "created_by_node_id", None) == node_id:
            control = PhaseDControlPlane(context.coordinator.store.database)
            snapshot = control.snapshot(session.session_id)
            assignment = snapshot.groups.get(_STORAGE_GROUP_ID)
            if assignment is not None and (
                assignment.primary_provider_id == provider_id
                or provider_id in assignment.replica_provider_ids
            ):
                primary = (
                    None
                    if assignment.primary_provider_id == provider_id
                    else assignment.primary_provider_id
                )
                replicas = tuple(
                    item
                    for item in assignment.replica_provider_ids
                    if item != provider_id
                )
                control.change_assignment(
                    session.session_id,
                    node_id,
                    _STORAGE_GROUP_ID,
                    primary,
                    replicas,
                )
        return AdapterOutcome(
            ContributionActivationState.INACTIVE,
            "Local storage contribution disabled and future use fenced.",
        )

    def suspend(
        self,
        candidate: ContributionCandidate,
        *,
        reason: str,
    ) -> AdapterOutcome:
        disabled = self.disable(candidate)
        return AdapterOutcome(
            ContributionActivationState.SUSPENDED,
            reason or disabled.reason,
        )

    def reconcile(
        self,
        candidate: ContributionCandidate,
        *,
        desired_state: ContributionDesiredState,
    ) -> AdapterOutcome:
        if desired_state is ContributionDesiredState.DISABLED:
            return self.disable(candidate)
        return self.enable(candidate)

    def _context_and_session(self) -> tuple[Any, Any]:
        loader = getattr(self._onboarding_service, "authorized_context", None)
        context = loader() if callable(loader) else None
        if context is None:
            raise FederationOperationError(
                "contribution-federation-required",
                "a trusted federation connection is required",
                "binding",
            )
        session_id = context.binding.internal_session_id
        context.coordinator.store.require_membership(
            session_id=session_id,
            node_id=context.credentials.identity.node_id,
        )
        session = context.coordinator.store.get_session(session_id)
        if session is None:
            raise FederationOperationError(
                "onboarding-session-unavailable",
                "the trusted federation session is unavailable",
                "internal_session_id",
            )
        return context, session

    def _provider_id(self, candidate: ContributionCandidate) -> str:
        if not self.supports(candidate):
            raise ValueError("storage adapter received an unsupported candidate")
        value = candidate.capacity_envelope.get("provider_id")
        if not isinstance(value, str) or not value:
            raise ValueError("storage candidate is missing provider_id")
        return value


@dataclass(frozen=True)
class LocalCandidateBundle:
    inventory: LocalComputeHandlerInventory
    compute_adapter: PendingComputeContributionAdapter
    storage_probe: _LocalStorageProbe
    storage_target: StorageCandidateTarget
    storage_spec: StorageCandidateSpec


def _probe_root() -> Path:
    configured = current_app.config.get(
        "CAPABILITY_ONBOARDING_STORAGE_PROBE_DIRECTORY",
        "data/federation/storage-probe",
    )
    return Path(str(configured))


def _build_bundle() -> LocalCandidateBundle:
    inventory = LocalComputeHandlerInventory(maximum_handlers=1)
    descriptor = LocalComputeHandlerDescriptor(
        handler_id=_COMPUTE_HANDLER_ID,
        capability_type="system-summary",
        protocol="fcp-compute-handler",
        protocol_version="1.0",
        attributes={
            "operation": "system-summary",
            "mode": "read-only",
            "side_effects": False,
        },
    )
    inventory.register(descriptor, _ReadOnlySystemSummaryHandler())

    storage_probe = _LocalStorageProbe(_probe_root())
    storage_target = StorageCandidateTarget(
        candidate_id=_STORAGE_PROVIDER_ID,
        display_label="Local FCP data storage",
        candidate_fingerprint=_STORAGE_FINGERPRINT,
        write_probe=storage_probe.write,
        read_probe=storage_probe.read,
        cleanup_probe=storage_probe.cleanup,
        payload_bytes=256,
    )
    storage_spec = StorageCandidateSpec(
        provider_id=_STORAGE_PROVIDER_ID,
        protocol="fcp-storage-candidate",
        display_label="Local FCP data storage",
        capacity_envelope={
            "kind": "host-mounted-local-data",
            "scope": "candidate-only",
            "probe_payload_bytes": 256,
        },
    )
    return LocalCandidateBundle(
        inventory=inventory,
        compute_adapter=PendingComputeContributionAdapter(inventory),
        storage_probe=storage_probe,
        storage_target=storage_target,
        storage_spec=storage_spec,
    )


def get_local_candidate_bundle() -> LocalCandidateBundle:
    existing = current_app.extensions.get(_EXTENSION_KEY)
    if isinstance(existing, LocalCandidateBundle):
        return existing
    bundle = _build_bundle()
    current_app.extensions[_EXTENSION_KEY] = bundle
    return bundle


def local_inspection_adapters() -> tuple[object, ...]:
    bundle = get_local_candidate_bundle()
    return (
        RegisteredComputeHandlerAdapter(bundle.inventory),
        StorageCandidateAdapter((bundle.storage_target,)),
    )


def local_contribution_components(
    onboarding_service: object | None = None,
) -> tuple[tuple[object, ...], tuple[object, ...]]:
    bundle = get_local_candidate_bundle()
    storage_adapter: object
    if onboarding_service is None:
        storage_adapter = StorageContributionAdapter(
            is_assigned=lambda _provider_id: False,
            fence_candidate=bundle.storage_probe.fence,
        )
    else:
        storage_adapter = CreatorBootstrapStorageContributionAdapter(
            onboarding_service,
            bundle.storage_probe,
        )
    return (
        (
            ComputeCandidateSource(bundle.inventory),
            StorageCandidateSource({_STORAGE_PROVIDER_ID: bundle.storage_spec}),
        ),
        (
            bundle.compute_adapter,
            storage_adapter,
        ),
    )


__all__ = [
    "CreatorBootstrapStorageContributionAdapter",
    "LocalCandidateBundle",
    "PendingComputeContributionAdapter",
    "get_local_candidate_bundle",
    "local_contribution_components",
    "local_inspection_adapters",
]
