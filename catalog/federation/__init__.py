"""Pure domain contracts for federated MSH sessions."""

from .errors import FederationValidationError
from .leader_selection import (
    LeaderSelectionResult,
    StorageGroupPolicy,
    select_storage_leader,
    validate_primary_write,
)
from .models import (
    CapabilityAnnouncement,
    CapabilityStatus,
    CommitState,
    DatasetCoverage,
    LeadershipGrant,
    NodeIdentity,
    Session,
    SessionEvent,
    SessionState,
    StorageBatch,
    StorageGroupState,
    StorageManifest,
    StorageNodeState,
    StorageRole,
    StorageWriteRequest,
)

__all__ = [
    "FederationValidationError",
    "CapabilityAnnouncement",
    "CapabilityStatus",
    "CommitState",
    "DatasetCoverage",
    "LeadershipGrant",
    "LeaderSelectionResult",
    "NodeIdentity",
    "Session",
    "SessionEvent",
    "SessionState",
    "StorageBatch",
    "StorageGroupState",
    "StorageGroupPolicy",
    "StorageManifest",
    "StorageNodeState",
    "StorageRole",
    "StorageWriteRequest",
    "select_storage_leader",
    "validate_primary_write",
]
