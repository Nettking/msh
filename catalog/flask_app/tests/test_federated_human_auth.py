from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from flask import Flask
from flask_security import hash_password

from catalog.federation.errors import FederationOperationError
from catalog.federation.human_auth import USER_EVENT
from catalog.federation.models import SessionEvent
from catalog.flask_app.auth.extension import init_human_auth
from catalog.flask_app.auth.federation import FederationHumanAuthService
from catalog.flask_app.auth.models import Role, User, db
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 8, 12, 11, 0, tzinfo=timezone.utc)


class FakeCoordinator:
    def __init__(self, leader_node_id: str, members: set[str]):
        self.leader_node_id = leader_node_id
        self.members = set(members)
        self.events: list[SessionEvent] = []
        self.store = self

    def get_session(self, session_id: str):
        assert session_id == "session-auth"
        return SimpleNamespace(created_by_node_id=self.leader_node_id)

    def require_membership(self, *, session_id: str, node_id: str) -> None:
        assert session_id == "session-auth"
        if node_id not in self.members:
            raise FederationOperationError("not-session-member", "not a member")

    def replay_page(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        last_applied_revision: int,
        limit: int,
    ):
        self.require_membership(session_id=session_id, node_id=actor_node_id)
        page = tuple(
            event
            for event in self.events
            if event.revision > last_applied_revision
        )[:limit]
        return page, len(self.events)

    def append_event(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        request_id: str,
        event_type: str,
        payload: dict,
    ):
        self.require_membership(session_id=session_id, node_id=actor_node_id)
        event = SessionEvent(
            session_id=session_id,
            revision=len(self.events) + 1,
            event_id=request_id,
            event_type=event_type,
            occurred_at=NOW,
            actor_node_id=actor_node_id,
            payload=payload,
        )
        self.events.append(event)
        return event, True


def _context(credentials, coordinator):
    return SimpleNamespace(
        credentials=credentials,
        binding=SimpleNamespace(
            federation_id="fed-auth",
            internal_session_id="session-auth",
            device_id=credentials.identity.node_id,
        ),
        coordinator=coordinator,
    )


@pytest.fixture()
def auth_app(tmp_path, monkeypatch):
    monkeypatch.delenv("FCP_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("FCP_FLASK_SECRET", "s" * 48)
    monkeypatch.setenv("FCP_PASSWORD_SALT", "p" * 48)
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "users.sqlite3"))
    app = Flask(__name__, template_folder="../templates")
    app.testing = True
    app.config["WTF_CSRF_ENABLED"] = False
    init_human_auth(app)
    return app


def test_authority_metadata_and_user_role_revert_are_distinct_events(
    auth_app, tmp_path
):
    leader = IdentityStore(tmp_path / "leader", display_name="leader").load_or_create(
        now=NOW
    )
    coordinator = FakeCoordinator(leader.identity.node_id, {leader.identity.node_id})
    service = FederationHumanAuthService(
        context_provider=lambda: _context(leader, coordinator), clock=lambda: NOW
    )

    with auth_app.app_context():
        operator = db.session.query(Role).filter_by(name="operator").one()
        viewer = db.session.query(Role).filter_by(name="viewer").one()
        user = User(
            email="operator@example.com",
            password=hash_password("correct horse battery staple"),
            active=True,
            fs_uniquifier="leader-user",
            roles=[operator],
        )
        db.session.add(user)
        db.session.commit()

        authority = service.ensure_authority("https://leader.example/")
        assert authority is not None
        assert authority.node_id == leader.identity.node_id
        assert authority.base_url == "https://leader.example"

        service.publish_user(user)
        user.roles = [viewer]
        db.session.commit()
        service.publish_user(user)
        user.roles = [operator]
        db.session.commit()
        service.publish_user(user)

    user_events = [event for event in coordinator.events if event.event_type == USER_EVENT]
    assert len(user_events) == 3
    assert len({event.event_id for event in user_events}) == 3
    assert user_events[-1].payload["roles"] == ["operator"]


def test_signed_assertion_is_target_bound_and_creates_shadow_user(auth_app, tmp_path):
    leader = IdentityStore(tmp_path / "leader", display_name="leader").load_or_create(
        now=NOW
    )
    member = IdentityStore(tmp_path / "member", display_name="member").load_or_create(
        now=NOW
    )
    coordinator = FakeCoordinator(
        leader.identity.node_id,
        {leader.identity.node_id, member.identity.node_id},
    )
    leader_service = FederationHumanAuthService(
        context_provider=lambda: _context(leader, coordinator), clock=lambda: NOW
    )
    member_service = FederationHumanAuthService(
        context_provider=lambda: _context(member, coordinator), clock=lambda: NOW
    )

    with auth_app.app_context():
        admin = db.session.query(Role).filter_by(name="admin").one()
        user = User(
            email="martin@example.com",
            password=hash_password("correct horse battery staple"),
            active=True,
            fs_uniquifier="leader-admin",
            roles=[admin],
        )
        db.session.add(user)
        db.session.commit()

        leader_service.ensure_authority("https://leader.example")
        member_service.ensure_member_endpoint("https://member.example")
        leader_service.publish_user(user)
        assertion = leader_service.issue_assertion(
            user=user,
            target_node_id=member.identity.node_id,
            state="browser-state",
        )

        shadow = member_service.consume_assertion(
            assertion,
            expected_state="browser-state",
        )
        assert shadow.email == "martin@example.com"
        assert shadow.active is True
        assert shadow.has_role("admin")
        assert shadow.fs_uniquifier.startswith("federated:")
        assert shadow.password == "!federated-sso-only"

        with pytest.raises(FederationOperationError, match="browser session"):
            member_service.consume_assertion(assertion, expected_state="wrong-state")


def test_assertion_cannot_be_consumed_by_another_member(auth_app, tmp_path):
    leader = IdentityStore(tmp_path / "leader", display_name="leader").load_or_create(
        now=NOW
    )
    member_a = IdentityStore(tmp_path / "member-a", display_name="member-a").load_or_create(
        now=NOW
    )
    member_b = IdentityStore(tmp_path / "member-b", display_name="member-b").load_or_create(
        now=NOW
    )
    coordinator = FakeCoordinator(
        leader.identity.node_id,
        {leader.identity.node_id, member_a.identity.node_id, member_b.identity.node_id},
    )
    leader_service = FederationHumanAuthService(
        context_provider=lambda: _context(leader, coordinator), clock=lambda: NOW
    )
    member_b_service = FederationHumanAuthService(
        context_provider=lambda: _context(member_b, coordinator), clock=lambda: NOW
    )

    with auth_app.app_context():
        viewer = db.session.query(Role).filter_by(name="viewer").one()
        user = User(
            email="viewer@example.com",
            password=hash_password("correct horse battery staple"),
            active=True,
            fs_uniquifier="leader-viewer",
            roles=[viewer],
        )
        db.session.add(user)
        db.session.commit()
        leader_service.ensure_authority("https://leader.example")
        assertion = leader_service.issue_assertion(
            user=user,
            target_node_id=member_a.identity.node_id,
            state="state-a",
        )
        with pytest.raises(FederationOperationError, match="not bound"):
            member_b_service.consume_assertion(assertion, expected_state="state-a")
