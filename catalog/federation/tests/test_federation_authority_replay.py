"""B09 consequence tests for fail-closed Federation authority replay."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.federation.projections import (
    FederationAuthorityAdapter,
    authority_adapter,
)

NOW = datetime(2026, 8, 2, 20, 37, tzinfo=timezone.utc)


class _PagedProjectionCoordinator:
    """A coordinator that answers exact contiguous pages by revision."""

    coordinator_id = "fcp-coordinator"

    def __init__(self, session: object, events: tuple[Any, ...]) -> None:
        self._session = session
        self.events = events
        self.store = SimpleNamespace(get_session=lambda _session_id: session)
        self.pages_served = 0

    def status(self, *, actor_node_id: str, cursor: str | None = None) -> object:
        del actor_node_id, cursor
        return {
            "nodes": [
                {
                    "node_id": "node-local",
                    "display_name": "Local",
                    "connection_state": "connected",
                },
                {
                    "node_id": "node-recorder",
                    "display_name": "Recorder",
                    "connection_state": "connected",
                },
            ],
            "capabilities": [],
            "pagination": {"has_more": False, "next_cursor": None},
        }

    def replay_page(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        last_applied_revision: int,
        limit: int,
    ) -> tuple[tuple[Any, ...], int]:
        del session_id, actor_node_id
        self.pages_served += 1
        page = self.events[last_applied_revision : last_applied_revision + limit]
        return page, len(self.events)


def _projection_event(revision: int, event_type: str, payload: dict[str, Any]):
    return SimpleNamespace(
        revision=revision,
        event_type=event_type,
        occurred_at=NOW,
        actor_node_id="fcp-coordinator",
        payload=payload,
    )


def _history_with_a_revocation_last(*, filler_events: int) -> tuple[Any, ...]:
    """Real-shaped history whose newest authoritative event revokes a member."""

    events: list[Any] = [
        _projection_event(1, "node.joined", {"node_id": "node-local"}),
        _projection_event(2, "node.joined", {"node_id": "node-recorder"}),
    ]
    for index in range(filler_events):
        events.append(
            _projection_event(
                len(events) + 1,
                "capability.activity.recorded",
                {"index": index},
            )
        )
    events.append(
        _projection_event(
            len(events) + 1,
            "node.revoked",
            {"node_id": "node-recorder"},
        )
    )
    return tuple(events)


def _adapter(coordinator: object) -> FederationAuthorityAdapter:
    return FederationAuthorityAdapter(
        coordinator,
        actor_node_id="node-local",
        internal_session_id="session-secret",
    )


def test_a_revocation_past_the_projection_ceiling_is_refused_not_answered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A prefix of the log must not present a revoked device as current."""

    monkeypatch.setattr(authority_adapter, "PROJECTION_REPLAY_PAGE_EVENTS", 1)
    events = _history_with_a_revocation_last(
        filler_events=authority_adapter.MAX_PROJECTION_REPLAY_PAGES + 8
    )
    session = SimpleNamespace(
        display_name="Workshop",
        state="active",
        revision=len(events),
    )
    coordinator = _PagedProjectionCoordinator(session, events)

    snapshot = _adapter(coordinator).snapshot()

    assert snapshot.available is False
    assert snapshot.reason_code == "authoritative-replay-incomplete"
    assert snapshot.devices == ()


def test_an_empty_page_before_the_head_is_refused_not_answered() -> None:
    """The coordinator stopping pages before head must fail closed."""

    events = _history_with_a_revocation_last(filler_events=1)
    session = SimpleNamespace(
        display_name="Workshop",
        state="active",
        revision=len(events),
    )

    class TruncatingCoordinator(_PagedProjectionCoordinator):
        def replay_page(self, *, last_applied_revision: int, **kwargs: object):
            page, current_revision = super().replay_page(
                last_applied_revision=last_applied_revision,
                **kwargs,
            )
            visible = 2
            return page[: max(0, visible - last_applied_revision)], current_revision

    coordinator = TruncatingCoordinator(session, events)

    snapshot = _adapter(coordinator).snapshot()

    assert snapshot.available is False
    assert snapshot.reason_code == "authoritative-replay-incomplete"


def test_a_non_contiguous_page_is_refused_not_counted() -> None:
    """Progress is proven by revision, never by how many rows arrived."""

    events = _history_with_a_revocation_last(filler_events=1)
    session = SimpleNamespace(
        display_name="Workshop",
        state="active",
        revision=len(events),
    )

    class SkippingCoordinator(_PagedProjectionCoordinator):
        def replay_page(self, *, last_applied_revision: int, **kwargs: object):
            page, current_revision = super().replay_page(
                last_applied_revision=last_applied_revision,
                **kwargs,
            )
            if last_applied_revision == 0:
                page = tuple(item for item in page if item.revision != 2)
            return page, current_revision

    coordinator = SkippingCoordinator(session, events)

    snapshot = _adapter(coordinator).snapshot()

    assert snapshot.available is False
    assert snapshot.reason_code == "authoritative-replay-incomplete"


def test_a_complete_read_still_applies_the_newest_revocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refusing prefixes must not reject a readable complete history."""

    monkeypatch.setattr(authority_adapter, "PROJECTION_REPLAY_PAGE_EVENTS", 1)
    events = _history_with_a_revocation_last(filler_events=4)
    session = SimpleNamespace(
        display_name="Workshop",
        state="active",
        revision=len(events),
    )
    coordinator = _PagedProjectionCoordinator(session, events)

    snapshot = _adapter(coordinator).snapshot()

    assert snapshot.available is True
    assert snapshot.reason_code == "current"
    assert [device.node_id for device in snapshot.devices] == ["node-local"]
    assert coordinator.pages_served > 1
