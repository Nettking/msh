"""Durable authority-readiness marker for C03 bootstrap/migration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

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
    if not (
        isinstance(seal, dict)
        and seal.get("capability_id") == BOOTSTRAP_SEAL_CAPABILITY_ID
        and seal.get("capability_type") == BOOTSTRAP_SEAL_CAPABILITY_TYPE
        and isinstance(creator, str)
        and seal.get("owner_node_id") in state.get("voter_ids", [])
    ):
        return False

    # Creator identity is immutable provenance. A quorum-witnessed migration
    # may instead be completed by a surviving operational leader. Bind the seal
    # to leadership when it was committed, so subsequent failover or revocation
    # of that leader does not erase a completed bootstrap.
    journals = state.get("session_events")
    events = journals.get(session_id) if isinstance(journals, dict) else None
    if not isinstance(events, list):
        return False
    leader = creator
    for event in events:
        if not isinstance(event, dict):
            return False
        payload = event.get("payload")
        if not isinstance(payload, dict):
            return False
        if event.get("event_type") == "session.leader.changed":
            leader = payload.get("leader_node_id")
        elif (
            event.get("event_type") == "capability.registered"
            and payload.get("capability_id") == BOOTSTRAP_SEAL_CAPABILITY_ID
        ):
            return bool(
                payload.get("capability_type") == BOOTSTRAP_SEAL_CAPABILITY_TYPE
                and payload.get("owner_node_id") == seal.get("owner_node_id") == leader
                and event.get("actor_node_id") == leader
            )
    return False


__all__ = [
    "BOOTSTRAP_SEAL_CAPABILITY_ID",
    "BOOTSTRAP_SEAL_CAPABILITY_TYPE",
    "authority_ready",
]
