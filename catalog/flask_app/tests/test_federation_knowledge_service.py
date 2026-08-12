"""Contracts for the shared operator-knowledge repository.

These records are the only free-text operator content that reaches the
authoritative session log, and that log is append-only with no compaction. The
cases below pin the behaviour that keeps a shared collection editable, readable
past one replay page, safe to write, and usable while the Federation is
unreachable.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationOperationError
from catalog.federation.persistence import CoordinatorStore
from catalog.flask_app.services.federation_knowledge_service import (
    FederationKnowledgeRejected,
    FederationKnowledgeRepository,
    KnowledgeContext,
)
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 8, 12, 9, tzinfo=timezone.utc)

STRATEGY_COLLECTION = "operator-strategies"
STRATEGY_SCHEMA = "fcp.operator_strategy_records.v3"
STRATEGY_ITEMS = "records"


def _enrolled_context(tmp_path: Path) -> tuple[KnowledgeContext, SessionCoordinator]:
    coordinator = SessionCoordinator(tmp_path / "control.sqlite3")
    store: CoordinatorStore = coordinator.store
    credentials = IdentityStore(tmp_path / "identity", display_name="Device").create(
        now=NOW
    )
    token = store.create_enrollment_token(now=NOW, ttl_seconds=60, max_uses=1)["token"]
    store.enroll_node(credentials.identity, raw_token=str(token), now=NOW)
    session = store.create_session(
        actor_node_id=credentials.identity.node_id,
        display_name="Federation",
        request_id="create-session",
        now=NOW,
    )
    context = KnowledgeContext(
        session_id=session.session_id,
        actor_node_id=credentials.identity.node_id,
        coordinator=coordinator,
    )
    return context, coordinator


@pytest.fixture()
def repository(tmp_path: Path) -> FederationKnowledgeRepository:
    context, _coordinator = _enrolled_context(tmp_path)
    return FederationKnowledgeRepository(context_provider=lambda: context)


def _record(decision: str, *, updated_at: str) -> list[dict[str, object]]:
    return [{"id": "rec-1", "decision": decision, "updated_at": updated_at}]


def _write(
    repository: FederationKnowledgeRepository,
    records: list[dict[str, object]],
) -> list[dict[str, object]]:
    payload = repository.write_payload(
        collection=STRATEGY_COLLECTION,
        document_schema=STRATEGY_SCHEMA,
        items_key=STRATEGY_ITEMS,
        desired_payload={
            "schema": STRATEGY_SCHEMA,
            "updated_at": "",
            STRATEGY_ITEMS: records,
        },
    )
    return list(payload[STRATEGY_ITEMS])


def _decisions(records: list[dict[str, object]]) -> list[str]:
    return [str(item.get("decision")) for item in records]


def test_restoring_earlier_content_is_accepted(repository):
    """An undo is a new change, not a replay of the change it reverses."""

    first = _record("coolant on", updated_at="2026-08-12T10:00:00Z")
    second = _record("coolant off", updated_at="2026-08-12T10:00:01Z")

    assert _decisions(_write(repository, first)) == ["coolant on"]
    assert _decisions(_write(repository, second)) == ["coolant off"]
    assert _decisions(_write(repository, first)) == ["coolant on"]


def test_rewriting_unchanged_content_appends_nothing(repository):
    records = _record("coolant on", updated_at="2026-08-12T10:00:00Z")
    _write(repository, records)

    before = repository._reduce(
        repository._events(repository._context_provider()),
        collection=STRATEGY_COLLECTION,
    ).latest_revision
    _write(repository, records)
    after = repository._reduce(
        repository._events(repository._context_provider()),
        collection=STRATEGY_COLLECTION,
    ).latest_revision

    assert after == before


def test_deleted_document_can_be_recreated_with_its_original_content(repository):
    records = _record("coolant on", updated_at="2026-08-12T10:00:00Z")
    _write(repository, records)

    assert repository.delete_document(
        collection=STRATEGY_COLLECTION, document_id="rec-1"
    )
    assert _decisions(_write(repository, records)) == ["coolant on"]


def test_deleted_document_is_not_reimported_from_the_local_cache(repository):
    records = _record("coolant on", updated_at="2026-08-12T10:00:00Z")
    _write(repository, records)
    repository.delete_document(collection=STRATEGY_COLLECTION, document_id="rec-1")

    payload = repository.load_payload(
        collection=STRATEGY_COLLECTION,
        document_schema=STRATEGY_SCHEMA,
        items_key=STRATEGY_ITEMS,
        local_payload={
            "schema": STRATEGY_SCHEMA,
            "updated_at": "",
            STRATEGY_ITEMS: records,
        },
    )

    assert payload[STRATEGY_ITEMS] == []


def test_a_newer_local_document_is_imported_after_reconnecting(repository):
    shared = _record("coolant on", updated_at="2026-08-12T10:00:00Z")
    _write(repository, shared)
    offline_edit = _record("coolant off", updated_at="2026-08-12T11:00:00Z")

    payload = repository.load_payload(
        collection=STRATEGY_COLLECTION,
        document_schema=STRATEGY_SCHEMA,
        items_key=STRATEGY_ITEMS,
        local_payload={
            "schema": STRATEGY_SCHEMA,
            "updated_at": "",
            STRATEGY_ITEMS: offline_edit,
        },
    )

    assert _decisions(list(payload[STRATEGY_ITEMS])) == ["coolant off"]


def test_collections_stay_readable_past_one_replay_page(tmp_path, monkeypatch):
    """The unpaged replay API fails closed above its own window.

    The coordinator device reads its own log directly, so an unpaged read would
    take the shared knowledge surface offline there while every member — which
    pages — kept working.
    """

    monkeypatch.setattr("catalog.federation.persistence.MAX_REPLAY_EVENTS", 5)
    monkeypatch.setattr(
        "catalog.flask_app.services.federation_knowledge_service.REPLAY_PAGE_EVENTS",
        2,
    )
    context, _coordinator = _enrolled_context(tmp_path)
    repository = FederationKnowledgeRepository(context_provider=lambda: context)

    for index in range(12):
        payload = repository.write_payload(
            collection="machine-notes",
            document_schema="fcp.machine_notes.v1",
            items_key="notes",
            desired_payload={
                "schema": "fcp.machine_notes.v1",
                "updated_at": "",
                "notes": [
                    {
                        "id": f"note-{index}",
                        "note": f"note {index}",
                        "created_at": f"2026-08-12T10:00:{index:02d}Z",
                    }
                ],
            },
        )

    assert len(payload["notes"]) == 12


def test_credentials_are_refused_rather_than_shared(repository):
    with pytest.raises(FederationKnowledgeRejected) as refused:
        _write(
            repository,
            [
                {
                    "id": "rec-1",
                    "decision": "pair with fcp_join_5f3a9c to reconnect",
                    "updated_at": "2026-08-12T10:00:00Z",
                }
            ],
        )

    assert refused.value.code == "knowledge-document-contains-credentials"


def test_ordinary_location_text_is_still_accepted(repository):
    """Operators legitimately record addresses; only credentials are refused."""

    records = _record(
        "spindle drive at 192.168.1.50 runs hot after warm-up",
        updated_at="2026-08-12T10:00:00Z",
    )

    assert _decisions(_write(repository, records)) == [
        "spindle drive at 192.168.1.50 runs hot after warm-up"
    ]


def test_an_unreachable_federation_keeps_the_local_cache_readable(tmp_path):
    context, coordinator = _enrolled_context(tmp_path)
    repository = FederationKnowledgeRepository(context_provider=lambda: context)
    local = {
        "schema": STRATEGY_SCHEMA,
        "updated_at": "",
        STRATEGY_ITEMS: _record("coolant on", updated_at="2026-08-12T10:00:00Z"),
    }

    def unreachable(**_kwargs):
        raise FederationOperationError(
            "pairing-relay-disconnected", "the paired relay is not connected"
        )

    coordinator.replay_page = unreachable  # type: ignore[assignment]

    payload = repository.load_payload(
        collection=STRATEGY_COLLECTION,
        document_schema=STRATEGY_SCHEMA,
        items_key=STRATEGY_ITEMS,
        local_payload=local,
    )

    assert payload is local


def test_an_unreachable_federation_keeps_an_operator_edit(tmp_path):
    context, coordinator = _enrolled_context(tmp_path)
    repository = FederationKnowledgeRepository(context_provider=lambda: context)

    def unreachable(**_kwargs):
        raise FederationOperationError(
            "pairing-relay-disconnected", "the paired relay is not connected"
        )

    coordinator.replay_page = unreachable  # type: ignore[assignment]
    records = _record("coolant on", updated_at="2026-08-12T10:00:00Z")

    assert _decisions(_write(repository, records)) == ["coolant on"]


def test_a_cached_credential_does_not_break_the_page(tmp_path):
    """A refused document already on disk cannot be corrected from a read."""

    context, _coordinator = _enrolled_context(tmp_path)
    repository = FederationKnowledgeRepository(context_provider=lambda: context)

    payload = repository.load_payload(
        collection=STRATEGY_COLLECTION,
        document_schema=STRATEGY_SCHEMA,
        items_key=STRATEGY_ITEMS,
        local_payload={
            "schema": STRATEGY_SCHEMA,
            "updated_at": "",
            STRATEGY_ITEMS: [
                {
                    "id": "rec-1",
                    "decision": "token fcp_enroll_9ab1",
                    "updated_at": "2026-08-12T10:00:00Z",
                },
                {
                    "id": "rec-2",
                    "decision": "raise feed on the second pass",
                    "updated_at": "2026-08-12T10:00:01Z",
                },
            ],
        },
    )

    assert _decisions(list(payload[STRATEGY_ITEMS])) == [
        "raise feed on the second pass"
    ]


def test_no_wall_clock_stamp_enters_the_shared_payload(repository):
    """A per-call stamp would make otherwise identical retries differ."""

    _write(repository, _record("coolant on", updated_at="2026-08-12T10:00:00Z"))
    events = repository._events(repository._context_provider())
    upserts = [
        event for event in events if event.event_type == "knowledge.document.upserted"
    ]

    assert upserts
    assert all("changed_at" not in event.payload for event in upserts)
