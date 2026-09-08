"""Resolve current Federation leader authority from ordered session history.

This reader works against both the local :class:`SessionCoordinator` and the
read-only remote coordinator facade used by paired members. The immutable
session creator is term 1. Only coordinator-authored ``session.leader.changed``
events may advance the active leader and every transition must form one
contiguous monotonic chain.

Leadership is an authority decision, so a bounded read of the authoritative log
is only usable once it has reached the coordinator's current revision. A page
budget that runs out before then is reported as an explicit bounded failure:
answering from the prefix would keep granting leader authority to a node whose
handover is recorded past the ceiling, and would refuse it to the node that
actually holds it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from catalog.federation.authoritative_replay import replay_authoritative_history
from catalog.federation.errors import FederationOperationError
from catalog.federation.models import Session
from catalog.federation.session_leadership import (
    INITIAL_TERM,
    LEADER_CHANGED_EVENT,
    LEADERSHIP_SCHEMA,
    SessionLeadership,
)

#: One authoritative leadership read stays within this bounded page budget.
MAX_LEADERSHIP_REPLAY_PAGES = 128
LEADERSHIP_REPLAY_PAGE_EVENTS = 1_000


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
        # from the immutable first event below when replay is available.
        creator = ""
    return session_id, coordinator, creator


def _replay_page(
    coordinator: Any,
    *,
    session_id: str,
    actor_node_id: str,
    last_revision: int,
) -> tuple[tuple[Any, ...], int] | None:
    replay_page = getattr(coordinator, "replay_page", None)
    if callable(replay_page):
        page, current_revision = replay_page(
            session_id=session_id,
            actor_node_id=actor_node_id,
            last_applied_revision=last_revision,
            limit=LEADERSHIP_REPLAY_PAGE_EVENTS,
        )
        return tuple(page), current_revision
    replay = getattr(coordinator, "replay", None)
    if callable(replay):
        # Compatibility facades that expose only the unpaged reader answer in
        # one window. ``EventLog.replay`` already fails closed above that
        # window, so the revision the events carry is the whole answer; this
        # branch claims no completeness beyond what that reader proved.
        events = tuple(
            replay(
                session_id=session_id,
                actor_node_id=actor_node_id,
                last_applied_revision=last_revision,
            )
        )
        current_revision = max(
            (int(getattr(event, "revision", 0)) for event in events),
            default=last_revision,
        )
        return events, current_revision
    return None


def resolve_federation_leader(context: Any) -> FederationLeaderAuthority:
    authenticated_authority = getattr(
        context.coordinator, "authenticated_session_authority", None
    )
    if callable(authenticated_authority):
        # Configured C03 resolves witnessed legacy rows inside the single relay
        # owner. Generic readers retain their existing coordinator-only replay
        # policy; historical actor IDs are never globally trusted here.
        session_id = context.binding.internal_session_id
        session, leadership = authenticated_authority(
            session_id=session_id,
            actor_node_id=context.credentials.identity.node_id,
        )
        if (
            not isinstance(session, Session)
            or not isinstance(leadership, SessionLeadership)
            or session.session_id != session_id
            or leadership.session_id != session_id
            or leadership.creator_node_id != session.created_by_node_id
            or not isinstance(leadership.leader_node_id, str)
            or not leadership.leader_node_id.strip()
            or isinstance(leadership.term, bool)
            or not isinstance(leadership.term, int)
            or leadership.term < INITIAL_TERM
            or not isinstance(leadership.leader_connected, bool)
        ):
            raise FederationOperationError(
                "malformed-federation-leadership",
                "authenticated session and leadership metadata disagree",
            )
        return FederationLeaderAuthority(
            session_id=session_id,
            creator_node_id=leadership.creator_node_id,
            leader_node_id=leadership.leader_node_id,
            term=leadership.term,
        )

    session_id, coordinator, creator = _session(context)
    actor = context.credentials.identity.node_id
    coordinator_id = str(getattr(coordinator, "coordinator_id", "") or "")
    leader = creator
    term = INITIAL_TERM

    # Some compatibility/test facades expose only the historical session row.
    # With no event replay surface there is provably no transferable-leadership
    # evidence to consume, so retain the established term-1 creator semantics.
    if not callable(getattr(coordinator, "replay_page", None)) and not callable(
        getattr(coordinator, "replay", None)
    ):
        if not creator:
            raise FederationOperationError(
                "federation-leadership-unavailable",
                "current Federation leader could not be resolved",
            )
        return FederationLeaderAuthority(
            session_id=session_id,
            creator_node_id=creator,
            leader_node_id=creator,
            term=INITIAL_TERM,
        )

    def _apply(events: tuple[Any, ...]) -> None:
        nonlocal creator, leader, term
        for event in events:
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
            if not coordinator_id or event.actor_node_id != coordinator_id:
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

    # A leader resolved from an unfinished read is not the current leader. The
    # bounded reader raises rather than returning the prefix it managed to fold.
    replay_authoritative_history(
        lambda last_revision: _replay_page(
            coordinator,
            session_id=session_id,
            actor_node_id=actor,
            last_revision=last_revision,
        ),
        apply_page=_apply,
        max_pages=MAX_LEADERSHIP_REPLAY_PAGES,
    )

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
    "LEADERSHIP_REPLAY_PAGE_EVENTS",
    "MAX_LEADERSHIP_REPLAY_PAGES",
    "FederationLeaderAuthority",
    "require_federation_leader",
    "resolve_federation_leader",
]
