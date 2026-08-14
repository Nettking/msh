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

__all__ = [
    "SEGMENTATION_POLICY",
    "DeviceRoleResolution",
    "RoleCandidate",
    "RoleResolution",
    "RoleResolutionStatus",
    "SemanticRole",
    "device_key",
    "resolve_device_roles",
]
