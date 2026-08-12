from __future__ import annotations

from datetime import datetime, timezone

import pytest

from catalog.federation.errors import AuthorizationError, FederationValidationError
from catalog.federation.human_auth import (
    AUTHORITY_EVENT,
    AUTHORITY_SCHEMA,
    MEMBER_ENDPOINT_EVENT,
    MEMBER_ENDPOINT_SCHEMA,
    USER_EVENT,
    USER_SCHEMA,
    enforce_human_auth_event_authority,
)
from catalog.federation.models import Session, SessionState


def _session() -> Session:
    return Session(
        session_id="session-auth",
        display_name="Auth Federation",
        state=SessionState.ACTIVE,
        revision=1,
        created_at=datetime(2026, 8, 12, tzinfo=timezone.utc),
        created_by_node_id="node-leader",
        coordinator_id="coordinator-test",
    )


def test_leader_can_publish_authority_and_user_metadata():
    session = _session()
    enforce_human_auth_event_authority(
        session=session,
        actor_node_id="node-leader",
        event_type=AUTHORITY_EVENT,
        payload={
            "schema": AUTHORITY_SCHEMA,
            "node_id": "node-leader",
            "public_key": "ed25519:test",
            "base_url": "https://leader.example",
        },
    )
    enforce_human_auth_event_authority(
        session=session,
        actor_node_id="node-leader",
        event_type=USER_EVENT,
        payload={
            "schema": USER_SCHEMA,
            "email": "operator@example.com",
            "active": True,
            "roles": ["operator"],
        },
    )


def test_member_cannot_publish_leader_owned_human_auth_state():
    with pytest.raises(AuthorizationError, match="Federation creator"):
        enforce_human_auth_event_authority(
            session=_session(),
            actor_node_id="node-member",
            event_type=USER_EVENT,
            payload={
                "schema": USER_SCHEMA,
                "email": "attacker@example.com",
                "active": True,
                "roles": ["admin"],
            },
        )


def test_member_endpoint_must_belong_to_authenticated_actor():
    enforce_human_auth_event_authority(
        session=_session(),
        actor_node_id="node-member",
        event_type=MEMBER_ENDPOINT_EVENT,
        payload={
            "schema": MEMBER_ENDPOINT_SCHEMA,
            "node_id": "node-member",
            "base_url": "https://member.example",
        },
    )
    with pytest.raises(AuthorizationError, match="only for itself"):
        enforce_human_auth_event_authority(
            session=_session(),
            actor_node_id="node-member",
            event_type=MEMBER_ENDPOINT_EVENT,
            payload={
                "schema": MEMBER_ENDPOINT_SCHEMA,
                "node_id": "node-other",
                "base_url": "https://other.example",
            },
        )


def test_human_auth_control_events_reject_wrong_schema():
    with pytest.raises(FederationValidationError, match="expected"):
        enforce_human_auth_event_authority(
            session=_session(),
            actor_node_id="node-leader",
            event_type=AUTHORITY_EVENT,
            payload={"schema": "wrong"},
        )


def test_unrelated_session_events_are_unchanged():
    enforce_human_auth_event_authority(
        session=_session(),
        actor_node_id="node-member",
        event_type="knowledge.document.upserted",
        payload={"anything": "allowed by the owning subsystem"},
    )
