"""Append-only authoritative session history, independent of delivery outboxes."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .models import SessionEvent
from .persistence import CoordinatorStore


class AuthoritativeSessionEventLog:
    """Small explicit abstraction over the coordinator's event tables."""

    authoritative = True
    delivery_state = False

    def __init__(self, store: CoordinatorStore) -> None:
        self.store = store

    def append(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        request_id: str,
        event_type: str,
        payload: dict[str, Any],
        now: datetime,
    ) -> tuple[SessionEvent, bool]:
        return self.store.append_event(
            session_id=session_id,
            actor_node_id=actor_node_id,
            request_id=request_id,
            event_type=event_type,
            payload=payload,
            now=now,
        )

    def replay(
        self,
        *,
        session_id: str,
        last_applied_revision: int,
        actor_node_id: str | None = None,
    ) -> tuple[SessionEvent, ...]:
        return self.store.replay_events(
            session_id=session_id,
            last_applied_revision=last_applied_revision,
            actor_node_id=actor_node_id,
        )

    def replay_page(
        self,
        *,
        session_id: str,
        last_applied_revision: int,
        actor_node_id: str,
        limit: int,
    ) -> tuple[tuple[SessionEvent, ...], int]:
        return self.store.replay_event_page(
            session_id=session_id,
            last_applied_revision=last_applied_revision,
            actor_node_id=actor_node_id,
            limit=limit,
        )
