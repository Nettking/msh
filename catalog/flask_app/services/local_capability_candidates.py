"""Safe built-in compute and storage candidates for capability-first onboarding.

The built-ins make ordinary Docker installations useful without inventing authority:

* compute exposes one explicitly registered, read-only local handler descriptor;
  selecting it remains pending until an existing worker/control-plane authority
  activates that handler;
* storage exposes the host-mounted MSH data area through a bounded temporary
  write/read/cleanup probe; selecting it remains candidate-only until assigned by
  the existing storage control plane.

No benchmark or contribution choice grants dispatch or storage authority here.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

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
from catalog.capabilities.worker_activation import (
    LocalComputeHandlerDescriptor,
    LocalComputeHandlerInventory,
)
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
)

_EXTENSION_KEY = "capability_local_candidate_bundle"
_COMPUTE_HANDLER_ID = "msh-system-summary"
_STORAGE_PROVIDER_ID = "msh-local-data-storage"
_STORAGE_FINGERPRINT = "sha256:" + hashlib.sha256(
    b"msh-local-data-storage-candidate-v1"
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
    """Bounded temporary storage probe under the mounted MSH data directory."""

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
        protocol="msh-compute-handler",
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
        display_label="Local MSH data storage",
        candidate_fingerprint=_STORAGE_FINGERPRINT,
        write_probe=storage_probe.write,
        read_probe=storage_probe.read,
        cleanup_probe=storage_probe.cleanup,
        payload_bytes=256,
    )
    storage_spec = StorageCandidateSpec(
        provider_id=_STORAGE_PROVIDER_ID,
        protocol="msh-storage-candidate",
        display_label="Local MSH data storage",
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


def local_contribution_components() -> tuple[tuple[object, ...], tuple[object, ...]]:
    bundle = get_local_candidate_bundle()
    return (
        (
            ComputeCandidateSource(bundle.inventory),
            StorageCandidateSource({_STORAGE_PROVIDER_ID: bundle.storage_spec}),
        ),
        (
            bundle.compute_adapter,
            StorageContributionAdapter(
                is_assigned=lambda _provider_id: False,
                fence_candidate=bundle.storage_probe.fence,
            ),
        ),
    )


__all__ = [
    "LocalCandidateBundle",
    "PendingComputeContributionAdapter",
    "get_local_candidate_bundle",
    "local_contribution_components",
    "local_inspection_adapters",
]
