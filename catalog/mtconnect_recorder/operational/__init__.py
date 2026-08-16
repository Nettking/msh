"""Machine-neutral operational semantics over canonical MTConnect evidence."""

from .policy import SEGMENTATION_POLICY, device_key
from .roles import (
    DeviceRoleResolution,
    RoleCandidate,
    RoleResolution,
    RoleResolutionStatus,
    SemanticRole,
    resolve_device_roles,
)
from .timeline import (
    AgentSequenceDiscontinuity,
    AgentStreamError,
    AgentStreamReport,
    build_agent_stream_reports,
)

__all__ = [
    "SEGMENTATION_POLICY",
    "AgentSequenceDiscontinuity",
    "AgentStreamError",
    "AgentStreamReport",
    "DeviceRoleResolution",
    "RoleCandidate",
    "RoleResolution",
    "RoleResolutionStatus",
    "SemanticRole",
    "build_agent_stream_reports",
    "device_key",
    "resolve_device_roles",
]
