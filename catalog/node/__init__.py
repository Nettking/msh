"""Persistent identity and local state for an outbound federation node."""

from .identity import (
    IdentityStore,
    NodeCredentials,
    NodeIdentityStateError,
    derive_node_id,
    derive_node_id_from_public_key,
    encode_public_key,
    verify_signature,
)
from .state import (
    ConnectionState,
    EnrollmentState,
    EventApplyResult,
    EventApplyStatus,
    JoinedSession,
    NodeState,
    NodeStateError,
    ReconnectPolicy,
    SessionMembershipState,
)

__all__ = [
    "ConnectionState",
    "EnrollmentState",
    "EventApplyResult",
    "EventApplyStatus",
    "IdentityStore",
    "JoinedSession",
    "NodeCredentials",
    "NodeIdentityStateError",
    "NodeState",
    "NodeStateError",
    "ReconnectPolicy",
    "SessionMembershipState",
    "derive_node_id",
    "derive_node_id_from_public_key",
    "encode_public_key",
    "verify_signature",
]
