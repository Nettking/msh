"""Durable authority-readiness marker for C03 bootstrap/migration."""

from __future__ import annotations

from typing import Any, Mapping

BOOTSTRAP_SEAL_CAPABILITY_ID = "fcp.control-plane.bootstrap-complete"
BOOTSTRAP_SEAL_CAPABILITY_TYPE = "fcp-control-plane-bootstrap-seal"


def authority_ready(state: Mapping[str, Any]) -> bool:
    """Return true only when the final bootstrap seal is in replicated state."""
    sessions = state.get("sessions")
    capabilities = state.get("capabilities")
    if not isinstance(sessions, dict) or not isinstance(capabilities, dict):
        return False
    if len(sessions) != 1:
        return False
    session_id = next(iter(sessions))
    session = sessions.get(session_id)
    if not isinstance(session, dict):
        return False
    creator = session.get("creator_node_id")
    values = capabilities.get(session_id)
    if not isinstance(values, dict):
        return False
    seal = values.get(BOOTSTRAP_SEAL_CAPABILITY_ID)
    return bool(
        isinstance(seal, dict)
        and seal.get("capability_id") == BOOTSTRAP_SEAL_CAPABILITY_ID
        and seal.get("capability_type") == BOOTSTRAP_SEAL_CAPABILITY_TYPE
        and isinstance(creator, str)
        and seal.get("owner_node_id") == creator
    )


__all__ = [
    "BOOTSTRAP_SEAL_CAPABILITY_ID",
    "BOOTSTRAP_SEAL_CAPABILITY_TYPE",
    "authority_ready",
]
