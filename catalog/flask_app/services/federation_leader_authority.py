"""Resolve current Federation leader authority from ordered session history.

This reader works against both the local :class:`SessionCoordinator` and the
read-only remote coordinator facade used by paired members. The immutable
session creator is term 1. Only coordinator-authored ``session.leader.changed``
events may advance the active leader and every transition must form one
contiguous monotonic chain.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from catalog.federation.errors import FederationOperationError
from catalog.federation.session_leadership import (
    INITIAL_TERM,
    LEADER_CHANGED_EVENT,
    LEADERSHIP_SCHEMA,
)


@dataclass(frozen=True)
class FederationLeaderAuthority:
    session_id: str
    creator_node_id: str
    leader_node_id: str
    term: int


def _session(context: Any) -> tuple[str, Any, str]:
    session_id = context.binding.internal_session_id
    coordinator = context.coordinator
    session = coordinator.store.get_session(session_id)
    creator = getattr(session, "created_by_node_id", None)
    if not isinstance(creator, str) or not creator:
        # Older remote status snapshots can omit creator identity. Recover it
        # from the immutable first event below.
        creator = ""
    return session_id, coordinator, creator


def resolve_federation_leader(context: Any) -> FederationLeaderAuthority:
    session_id, coordinator, creator = _session(context)
    actor = context.credentials.identity.node_id
    coordinator_id = str(getattr(coordinator, "coordinator_id", "") or "")
    leader = creator
    term = INITIAL_TERM
    last_revision = 0
    for _ in range(128):
        events, current_revision = coordinator.replay_page(
            session_id=session_id,
            actor_node_id=actor,
            last_applied_revision=last_revision,
            limit=1_000,
        )
        for event in events:
            last_revision = int(event.revision)
            if event.event_type == "session.created":
                candidate = getattr(event, "actor_node_id", None)
                if not isinstance(candidate, str) or not candidate:
                    raise FederationOperationError(
                        "malformed-federation-leadership",
                        "session creator identity is missing from authoritative history",
                    )
                if creator and creator != candidate:
                    raise FederationOperationError(
                        "malformed-federation-leadership",
                        "session metadata and creation history disagree",
                    )
                creator = candidate
                if not leader:
                    leader = candidate
                continue
            if event.event_type != LEADER_CHANGED_EVENT:
                continue
            if event.actor_node_id != coordinator_id:
                # Members cannot promote themselves through the generic event API.
                continue
            payload = event.payload
            next_leader = payload.get("leader_node_id") if isinstance(payload, dict) else None
            previous = payload.get("previous_leader_node_id") if isinstance(payload, dict) else None
            next_term = payload.get("term") if isinstance(payload, dict) else None
            if (
                not isinstance(payload, dict)
                or payload.get("schema") != LEADERSHIP_SCHEMA
                or payload.get("session_id") != session_id
                or not leader
                or previous != leader
                or not isinstance(next_leader, str)
                or not next_leader
                or isinstance(next_term, bool)
                or not isinstance(next_term, int)
                or next_term != term + 1
            ):
                raise FederationOperationError(
                    "malformed-federation-leadership",
                    "leader transition history is not a contiguous monotonic chain",
                )
            leader = next_leader
            term = next_term
        if not events or last_revision >= current_revision:
            break
    if not creator or not leader:
        raise FederationOperationError(
            "federation-leadership-unavailable",
            "current Federation leader could not be resolved",
        )
    return FederationLeaderAuthority(
        session_id=session_id,
        creator_node_id=creator,
        leader_node_id=leader,
        term=term,
    )


def require_federation_leader(context: Any) -> FederationLeaderAuthority:
    authority = resolve_federation_leader(context)
    actor = context.credentials.identity.node_id
    if authority.leader_node_id != actor:
        raise PermissionError("federation_leader_required")
    return authority


__all__ = [
    "FederationLeaderAuthority",
    "require_federation_leader",
    "resolve_federation_leader",
]
