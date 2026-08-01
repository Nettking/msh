"""F7.6 transfer planning over the existing F6 verified/resumable boundary."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum

from catalog.federation.errors import FederationValidationError
from catalog.federation.object_transfer import (
    DEFAULT_TRANSFER_CHUNK_BYTES,
    MAX_TRANSFER_CHUNK_BYTES,
    MAX_TRANSFER_OBJECT_BYTES,
    ObjectTransferManifest,
)

from .artifact_contracts import ArtifactDescriptor, OutputPlacementPolicy, _text
from .artifact_endpoint import validate_logical_artifact_endpoint


class ArtifactTransferMode(str, Enum):
    VERIFIED_CHUNK = "verified-chunk"
    RESUMABLE_CHUNK = "resumable-chunk"


@dataclass(frozen=True)
class ArtifactTransferPlan:
    descriptor: ArtifactDescriptor
    placement_policy_id: str
    mode: ArtifactTransferMode
    manifest: ObjectTransferManifest

    def __post_init__(self) -> None:
        if not isinstance(self.descriptor, ArtifactDescriptor):
            raise FederationValidationError(
                "invalid-artifact-descriptor",
                "descriptor",
                "must be an ArtifactDescriptor",
            )
        validate_logical_artifact_endpoint(self.descriptor.endpoint_id)
        object.__setattr__(
            self,
            "placement_policy_id",
            _text(self.placement_policy_id, "placement_policy_id"),
        )
        try:
            mode = ArtifactTransferMode(self.mode)
        except (TypeError, ValueError) as error:
            raise FederationValidationError(
                "invalid-artifact-transfer-mode",
                "mode",
                "unsupported artifact transfer mode",
            ) from error
        object.__setattr__(self, "mode", mode)
        if not isinstance(self.manifest, ObjectTransferManifest):
            raise FederationValidationError(
                "invalid-object-transfer-manifest",
                "manifest",
                "must use the F6 ObjectTransferManifest",
            )
        if (
            self.manifest.object_id != self.descriptor.artifact_id
            or self.manifest.total_size != self.descriptor.size_bytes
            or self.manifest.object_hash != self.descriptor.content_hash
            or self.manifest.transfer_id != self.descriptor.transfer_id
        ):
            raise FederationValidationError(
                "artifact-transfer-identity-mismatch",
                "manifest",
                "F6 manifest must match the artifact identity exactly",
            )

    @property
    def resumable(self) -> bool:
        return self.mode is ArtifactTransferMode.RESUMABLE_CHUNK

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.descriptor.to_dict(),
            "placement_policy_id": self.placement_policy_id,
            "mode": self.mode.value,
            "manifest": self.manifest.to_dict(),
        }


class F6ArtifactTransferPlanner:
    """Select the F6 verified or durable resumable path without moving bytes."""

    def __init__(self, *, chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES) -> None:
        if (
            isinstance(chunk_size, bool)
            or not isinstance(chunk_size, int)
            or chunk_size <= 0
            or chunk_size > MAX_TRANSFER_CHUNK_BYTES
        ):
            raise FederationValidationError(
                "invalid-artifact-chunk-size",
                "chunk_size",
                f"must be between 1 and {MAX_TRANSFER_CHUNK_BYTES}",
            )
        self.chunk_size = chunk_size

    def plan(
        self,
        descriptor: ArtifactDescriptor,
        placement_policy: OutputPlacementPolicy,
        *,
        now,
    ) -> ArtifactTransferPlan:
        if not isinstance(descriptor, ArtifactDescriptor):
            raise FederationValidationError(
                "invalid-artifact-descriptor",
                "descriptor",
                "must be an ArtifactDescriptor",
            )
        if not isinstance(placement_policy, OutputPlacementPolicy):
            raise FederationValidationError(
                "invalid-placement-policy",
                "placement_policy",
                "must be an OutputPlacementPolicy",
            )
        validate_logical_artifact_endpoint(descriptor.endpoint_id)
        validate_logical_artifact_endpoint(placement_policy.endpoint_id)
        if not placement_policy.accepts(descriptor, now=now):
            raise FederationValidationError(
                "artifact-placement-rejected",
                "descriptor",
                "artifact does not satisfy the placement policy",
            )
        if descriptor.size_bytes > MAX_TRANSFER_OBJECT_BYTES:
            raise FederationValidationError(
                "artifact-too-large-for-f6-transfer",
                "size_bytes",
                f"must not exceed {MAX_TRANSFER_OBJECT_BYTES} bytes",
            )
        if descriptor.transfer_id is None:
            raise FederationValidationError(
                "artifact-transfer-id-required",
                "transfer_id",
                "transferred artifacts require an explicit stable transfer ID",
            )
        manifest = ObjectTransferManifest(
            transfer_id=descriptor.transfer_id,
            object_id=descriptor.artifact_id,
            total_size=descriptor.size_bytes,
            object_hash=descriptor.content_hash,
            chunk_size=self.chunk_size,
            chunk_count=(
                math.ceil(descriptor.size_bytes / self.chunk_size)
                if descriptor.size_bytes
                else 0
            ),
        )
        mode = (
            ArtifactTransferMode.RESUMABLE_CHUNK
            if placement_policy.requires_resumable_transfer(descriptor.size_bytes)
            else ArtifactTransferMode.VERIFIED_CHUNK
        )
        return ArtifactTransferPlan(
            descriptor=descriptor,
            placement_policy_id=placement_policy.policy_id,
            mode=mode,
            manifest=manifest,
        )
