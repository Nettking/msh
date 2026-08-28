"""B09: bounded authority reads must not present a prefix as current truth.

Authoritative session history is append-only, and the Flask-side authority and
security projections read it through a fixed page budget. Before this coverage
existed, a projection that ran out of pages returned whatever it had folded so
far. That is not a stale cache: it is the current authority answer, computed
from a history in which the last leadership handover or the last human-auth
change had not happened yet.

These tests cross the *production* page-count ceilings. Only the page size is
accelerated, so ``MAX_LEADERSHIP_REPLAY_PAGES`` and ``MAX_EVENT_PAGES`` are the
constants actually under test.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Flask, request, url_for
from werkzeug.exceptions import HTTPException

from catalog.federation.authoritative_replay import (
    AUTHORITATIVE_REPLAY_INCOMPLETE,
    AuthoritativeReplayIncomplete,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.human_auth import (
    AUTHORITY_EVENT,
    AUTHORITY_SCHEMA,
    USER_EVENT,
    USER_SCHEMA,
)
from catalog.federation.models import SessionEvent
from catalog.flask_app.auth import federation as human_auth
from catalog.flask_app.auth import routes as auth_routes
from catalog.flask_app.auth.extension import init_human_auth
from catalog.flask_app.services import federation_leader_authority as leadership
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc)


def _context(node_id: str, session_id: str, coordinator: object) -> SimpleNamespace:
    return SimpleNamespace(
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=node_id)),
        binding=SimpleNamespace(
            federation_id="fed-b09",
            internal_session_id=session_id,
            device_id=node_id,
        ),
        coordinator=coordinator,
    )


def _enroll(coordinator: SessionCoordinator, root: Path, name: str):
    credentials = IdentityStore(root / name, display_name=name).load_or_create(now=NOW)
    token = coordinator.create_enrollment_token(ttl_seconds=300, max_uses=1)
    coordinator.enroll_node(credentials.identity, token=str(token["token"]))
    return credentials


def _federation_with_handover_past_the_ceiling(
    tmp_path: Path,
    *,
    filler_events: int,
):
    """Build a real Federation whose handover is the newest authoritative event."""

    coordinator = SessionCoordinator(tmp_path / "control.sqlite3", clock=lambda: NOW)
    creator = _enroll(coordinator, tmp_path, "creator")
    successor = _enroll(coordinator, tmp_path, "successor")
    session = coordinator.create_session(
        actor_node_id=creator.identity.node_id,
        display_name="B09 Federation",
        request_id="create-federation",
    )
    coordinator.connected(
        node_id=creator.identity.node_id,
        connection_id="creator-connection",
    )
    invitation = coordinator.create_invitation(
        session_id=session.session_id,
        actor_node_id=creator.identity.node_id,
        ttl_seconds=300,
        max_uses=1,
        request_id="invite-successor",
    )
    coordinator.join_session(
        node_id=successor.identity.node_id,
        token=str(invitation["token"]),
        request_id="join-successor",
        expected_session_id=session.session_id,
    )
    coordinator.connected(
        node_id=successor.identity.node_id,
        connection_id="successor-connection",
    )
    for index in range(filler_events):
        coordinator.append_event(
            session_id=session.session_id,
            actor_node_id=creator.identity.node_id,
            request_id=f"filler-{index}",
            event_type="capability.activity.recorded",
            payload={"index": index},
        )
    _leadership, changed = coordinator.transfer_session_leader(
        session_id=session.session_id,
        actor_node_id=creator.identity.node_id,
        target_node_id=successor.identity.node_id,
        request_id="handover-to-successor",
    )
    assert changed is not None
    return coordinator, session, creator, successor, changed


def test_a_handover_past_the_leadership_page_ceiling_is_refused_not_answered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The demoted creator must not keep leader authority behind the ceiling."""

    monkeypatch.setattr(leadership, "LEADERSHIP_REPLAY_PAGE_EVENTS", 1)
    coordinator, session, creator, successor, changed = (
        _federation_with_handover_past_the_ceiling(
            tmp_path,
            filler_events=leadership.MAX_LEADERSHIP_REPLAY_PAGES + 8,
        )
    )
    assert changed.revision > leadership.MAX_LEADERSHIP_REPLAY_PAGES

    creator_context = _context(
        creator.identity.node_id, session.session_id, coordinator
    )
    successor_context = _context(
        successor.identity.node_id, session.session_id, coordinator
    )

    # Without the completeness proof this returned the prefix leader — the node
    # that has already been demoted in authoritative history.
    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        leadership.resolve_federation_leader(creator_context)
    assert failure.value.code == AUTHORITATIVE_REPLAY_INCOMPLETE

    # The demoted creator is refused leader authority rather than granted it.
    with pytest.raises(AuthoritativeReplayIncomplete):
        leadership.require_federation_leader(creator_context)

    # The real leader is not silently accepted from an unfinished read either.
    with pytest.raises(AuthoritativeReplayIncomplete):
        leadership.require_federation_leader(successor_context)


def test_the_same_handover_inside_the_ceiling_still_resolves_the_successor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail-closed behavior must not cost the ordinary handover contract."""

    monkeypatch.setattr(leadership, "LEADERSHIP_REPLAY_PAGE_EVENTS", 1)
    coordinator, session, creator, successor, changed = (
        _federation_with_handover_past_the_ceiling(tmp_path, filler_events=2)
    )
    assert changed.revision < leadership.MAX_LEADERSHIP_REPLAY_PAGES

    authority = leadership.resolve_federation_leader(
        _context(successor.identity.node_id, session.session_id, coordinator)
    )
    assert authority.creator_node_id == creator.identity.node_id
    assert authority.leader_node_id == successor.identity.node_id
    assert authority.term == 2

    assert (
        leadership.require_federation_leader(
            _context(successor.identity.node_id, session.session_id, coordinator)
        ).leader_node_id
        == successor.identity.node_id
    )
    with pytest.raises(PermissionError, match="federation_leader_required"):
        leadership.require_federation_leader(
            _context(creator.identity.node_id, session.session_id, coordinator)
        )


def test_a_coordinator_that_stops_short_of_its_own_revision_is_refused(
    tmp_path: Path,
) -> None:
    """A short page is a truncated read, never the end of authoritative history."""

    created = SessionEvent(
        session_id="session-b09",
        revision=1,
        event_id="created",
        event_type="session.created",
        occurred_at=NOW,
        actor_node_id="node-creator",
        payload={"session_id": "session-b09"},
    )

    class TruncatingCoordinator:
        coordinator_id = "fcp-relay-coordinator"

        def __init__(self) -> None:
            self.store = SimpleNamespace(
                get_session=lambda _session_id: SimpleNamespace(
                    created_by_node_id="node-creator"
                )
            )

        def replay_page(self, *, last_applied_revision: int, **_kwargs: object):
            # The coordinator still holds revision 900; this reader is answered
            # with the first page and then with nothing at all.
            if last_applied_revision == 0:
                return (created,), 900
            return (), 900

    context = _context("node-creator", "session-b09", TruncatingCoordinator())
    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        leadership.resolve_federation_leader(context)
    assert "before the coordinator's current revision" in failure.value.message


class _PagedHumanAuthCoordinator:
    """Coordinator facade that answers exact contiguous pages by index."""

    def __init__(self, leader_node_id: str, events: list[SessionEvent]) -> None:
        self.leader_node_id = leader_node_id
        self.events = events
        self.store = self

    def get_session(self, _session_id: str) -> SimpleNamespace:
        return SimpleNamespace(created_by_node_id=self.leader_node_id)

    def replay_page(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        last_applied_revision: int,
        limit: int,
    ) -> tuple[tuple[SessionEvent, ...], int]:
        page = tuple(self.events[last_applied_revision : last_applied_revision + limit])
        return page, len(self.events)


def _human_auth_history(identity, *, filler_events: int):
    leader_node_id = identity.node_id

    def authority(revision: int, base_url: str) -> SessionEvent:
        return SessionEvent(
            session_id="session-b09",
            revision=revision,
            event_id=f"authority-{revision}",
            event_type=AUTHORITY_EVENT,
            occurred_at=NOW,
            actor_node_id=leader_node_id,
            payload={
                "schema": AUTHORITY_SCHEMA,
                "node_id": leader_node_id,
                "public_key": identity.public_key,
                "base_url": base_url,
            },
        )

    def user(revision: int, roles: list[str], active: bool) -> SessionEvent:
        return SessionEvent(
            session_id="session-b09",
            revision=revision,
            event_id=f"user-{revision}",
            event_type=USER_EVENT,
            occurred_at=NOW,
            actor_node_id=leader_node_id,
            payload={
                "schema": USER_SCHEMA,
                "email": "operator@example.com",
                "active": active,
                "roles": roles,
            },
        )

    events = [authority(1, "https://leader.example"), user(2, ["admin"], True)]
    for index in range(filler_events):
        events.append(
            SessionEvent(
                session_id="session-b09",
                revision=len(events) + 1,
                event_id=f"filler-{index}",
                event_type="capability.activity.recorded",
                occurred_at=NOW,
                actor_node_id=leader_node_id,
                payload={"index": index},
            )
        )
    events.append(authority(len(events) + 1, "https://leader-moved.example"))
    events.append(user(len(events) + 1, [], False))
    return events


def _human_auth_service(events: list[SessionEvent]):
    leader_node_id = events[0].actor_node_id
    coordinator = _PagedHumanAuthCoordinator(leader_node_id, events)
    return (
        human_auth.FederationHumanAuthService(
            context_provider=lambda: _context(
                leader_node_id, "session-b09", coordinator
            ),
            clock=lambda: NOW,
        ),
        coordinator,
    )


def _auth_leader_identity(tmp_path: Path):
    return (
        IdentityStore(tmp_path / "auth-leader", display_name="leader")
        .load_or_create(now=NOW)
        .identity
    )


def test_human_auth_state_past_the_page_ceiling_is_refused_not_answered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A superseded authority endpoint and revoked roles must not read as current."""

    monkeypatch.setattr(human_auth, "EVENT_PAGE_SIZE", 1)
    events = _human_auth_history(
        _auth_leader_identity(tmp_path),
        filler_events=human_auth.MAX_EVENT_PAGES + 8,
    )
    service, _coordinator = _human_auth_service(events)

    # Before the completeness proof both of these answered from the prefix: the
    # superseded base URL, and the role grant that the newest event revoked.
    with pytest.raises(AuthoritativeReplayIncomplete) as failure:
        service.authority(refresh=True)
    assert failure.value.code == AUTHORITATIVE_REPLAY_INCOMPLETE

    with pytest.raises(AuthoritativeReplayIncomplete):
        service.latest_user_metadata("operator@example.com", refresh=True)


def test_human_auth_state_inside_the_page_ceiling_still_reads_the_newest_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(human_auth, "EVENT_PAGE_SIZE", 1)
    events = _human_auth_history(_auth_leader_identity(tmp_path), filler_events=4)
    service, _coordinator = _human_auth_service(events)

    authority = service.authority(refresh=True)
    assert authority is not None
    assert authority.base_url == "https://leader-moved.example"

    metadata = service.latest_user_metadata("operator@example.com", refresh=True)
    assert metadata is not None
    assert metadata["roles"] == []
    assert metadata["active"] is False


def test_an_incomplete_human_auth_read_never_reports_a_prefix_login_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``mode`` degrades to the local recovery surface, never to prefix authority."""

    monkeypatch.setattr(human_auth, "EVENT_PAGE_SIZE", 1)
    events = _human_auth_history(
        _auth_leader_identity(tmp_path),
        filler_events=human_auth.MAX_EVENT_PAGES + 8,
    )
    service, _coordinator = _human_auth_service(events)

    mode = service.mode(refresh=True)
    assert mode.mode == "local"
    assert mode.authority is None


def _member_request_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Flask:
    """A member installation whose surfaces run the real before-request hooks."""

    monkeypatch.delenv("FCP_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("FCP_FLASK_SECRET", "s" * 48)
    monkeypatch.setenv("FCP_PASSWORD_SALT", "p" * 48)
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "users.sqlite3"))
    application = Flask(__name__, template_folder="../templates")
    application.testing = True
    application.config["WTF_CSRF_ENABLED"] = False
    init_human_auth(application)
    return application


def _established_member(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the durable member signal the authority surfaces are anchored on."""

    monkeypatch.setattr(human_auth, "saved_remote_member", lambda: True)
    monkeypatch.setattr(auth_routes, "saved_remote_member", lambda: True)


def test_an_incomplete_read_reports_an_unavailable_control_plane(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failing closed has to reach the operator as an unavailable control plane.

    ``mode`` degrades to ``local`` on an incomplete read, but ``saved_remote_member``
    correctly keeps this device a member, so both authority surfaces fall through
    to ``service.authority(refresh=True)`` -- which now raises rather than
    answering from a prefix. Unhandled, that bounded refusal reaches the operator
    as a broken device instead of the 503 these routes already define for an
    authority they cannot resolve.
    """

    monkeypatch.setattr(human_auth, "EVENT_PAGE_SIZE", 1)
    events = _human_auth_history(
        _auth_leader_identity(tmp_path),
        filler_events=human_auth.MAX_EVENT_PAGES + 8,
    )
    service, _coordinator = _human_auth_service(events)
    application = _member_request_app(tmp_path, monkeypatch)
    _established_member(monkeypatch)

    with application.test_request_context("/admin/users"):
        human_auth.install_federated_human_auth_service(service)
        with pytest.raises(HTTPException) as unavailable:
            auth_routes._member_admin_redirect()
    assert unavailable.value.code == 503

    with application.test_request_context("/"):
        change_password = url_for("security.change_password")
    with application.test_request_context(change_password, method="GET"):
        assert request.endpoint == "security.change_password"
        human_auth.install_federated_human_auth_service(service)
        with pytest.raises(HTTPException) as refused:
            human_auth.redirect_member_password_change()
    assert refused.value.code == 503


def test_a_complete_read_still_redirects_to_the_resolved_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reporting unavailable must not become the answer for a healthy member."""

    monkeypatch.setattr(human_auth, "EVENT_PAGE_SIZE", 1)
    events = _human_auth_history(_auth_leader_identity(tmp_path), filler_events=4)
    service, _coordinator = _human_auth_service(events)
    application = _member_request_app(tmp_path, monkeypatch)
    _established_member(monkeypatch)

    with application.test_request_context("/admin/users"):
        human_auth.install_federated_human_auth_service(service)
        outcome = auth_routes._member_admin_redirect()

    assert outcome is not None
    assert outcome.status_code in {301, 302, 303, 307, 308}
    assert outcome.headers["Location"].startswith("https://leader-moved.example")
