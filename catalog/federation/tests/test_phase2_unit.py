from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
    RevisionGapError,
)
from catalog.federation.models import (
    CapabilityAnnouncement,
    CapabilityStatus,
    NodeIdentity,
)
from catalog.federation.persistence import (
    STATUS_PAGE_MAX_BYTES,
    CoordinatorStore,
)
from catalog.federation.protocol import (
    MAX_MESSAGE_BYTES,
    RelayEnvelope,
)
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 7, 28, 12, tzinfo=timezone.utc)


def credentials(tmp_path: Path, name: str) -> NodeCredentials:
    state_directory = tmp_path / f"identity-{name.casefold().replace(' ', '-')}"
    return IdentityStore(state_directory, display_name=name).create(now=NOW)


def enroll(
    store: CoordinatorStore,
    node: NodeCredentials,
    *,
    now: datetime = NOW,
) -> str:
    token = store.create_enrollment_token(
        now=now,
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    store.enroll_node(node.identity, raw_token=token, now=now)
    return token


def create_session(
    store: CoordinatorStore,
    owner: NodeCredentials,
    *,
    session_id: str,
    now: datetime = NOW,
):
    return store.create_session(
        actor_node_id=owner.identity.node_id,
        display_name=f"Session {session_id}",
        request_id=f"create-{session_id}",
        session_id=session_id,
        now=now,
    )


def join(
    store: CoordinatorStore,
    *,
    owner: NodeCredentials,
    node: NodeCredentials,
    session_id: str,
    request_id: str,
    now: datetime = NOW,
) -> str:
    invitation = store.create_invitation(
        session_id=session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        now=now,
    )
    store.join_session(
        node_id=node.identity.node_id,
        raw_token=invitation["token"],
        request_id=request_id,
        now=now,
    )
    return invitation["token"]


def capability(
    *,
    session_id: str,
    node_id: str,
    capability_id: str,
    capability_type: str = "storage",
    status: CapabilityStatus = CapabilityStatus.READY,
    announced_at: datetime = NOW,
) -> CapabilityAnnouncement:
    return CapabilityAnnouncement(
        capability_id=capability_id,
        node_id=node_id,
        session_id=session_id,
        type=capability_type,
        protocol=f"msh-{capability_type}-test",
        protocol_version="1.0",
        status=status,
        properties={"label": capability_id},
        announced_at=announced_at,
    )


def database_bytes(database: Path) -> bytes:
    values: list[bytes] = []
    for candidate in sorted(database.parent.glob(f"{database.name}*")):
        if candidate.is_file():
            values.append(candidate.read_bytes())
    return b"".join(values)


def test_enrollment_tokens_are_hashed_and_expired_or_reused_tokens_fail(
    tmp_path: Path,
) -> None:
    database = tmp_path / "enrollment.sqlite3"
    store = CoordinatorStore(database)
    first = credentials(tmp_path, "First")
    second = credentials(tmp_path, "Second")
    third = credentials(tmp_path, "Third")

    expiring = store.create_enrollment_token(
        now=NOW,
        ttl_seconds=1,
        max_uses=1,
    )
    with pytest.raises(AuthenticationError) as expired:
        store.enroll_node(
            first.identity,
            raw_token=expiring["token"],
            now=NOW + timedelta(seconds=1),
        )
    assert expired.value.code == "expired-enrollment-token"

    single_use = store.create_enrollment_token(
        now=NOW,
        ttl_seconds=60,
        max_uses=1,
    )
    store.enroll_node(
        second.identity,
        raw_token=single_use["token"],
        now=NOW,
    )
    with pytest.raises(AuthenticationError) as reused:
        store.enroll_node(
            third.identity,
            raw_token=single_use["token"],
            now=NOW,
        )
    assert reused.value.code == "reused-enrollment-token"

    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT * FROM enrollment_tokens ORDER BY token_id"
        ).fetchall()
        columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(enrollment_tokens)"
            ).fetchall()
        }
    assert "token" not in columns
    assert {
        row["token_hash"] for row in rows
    } == {
        hashlib.sha256(expiring["token"].encode()).hexdigest(),
        hashlib.sha256(single_use["token"].encode()).hexdigest(),
    }
    persisted = database_bytes(database)
    assert expiring["token"].encode() not in persisted
    assert single_use["token"].encode() not in persisted
    audit = json.dumps(store.audit_entries(), sort_keys=True)
    assert expiring["token"] not in audit
    assert single_use["token"] not in audit
    assert {entry["reason"] for entry in store.audit_entries()} >= {
        "expired-enrollment-token",
        "reused-enrollment-token",
    }


def test_session_coordinator_binds_enrollment_to_public_identity(
    tmp_path: Path,
) -> None:
    clock = lambda: NOW
    coordinator = SessionCoordinator(tmp_path / "identity.sqlite3", clock=clock)
    valid = credentials(tmp_path, "Valid")
    token = coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]

    assert coordinator.enroll_node(valid.identity, token=token) == valid.identity
    assert coordinator.public_identity(valid.identity.node_id) == valid.identity

    replacement = credentials(tmp_path, "Replacement")
    mismatched = replace(
        replacement.identity,
        node_id=valid.identity.node_id,
    )
    mismatch_token = coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    with pytest.raises(AuthenticationError) as raised:
        coordinator.enroll_node(mismatched, token=mismatch_token)

    assert raised.value.code == "public-identity-node-id-mismatch"
    assert coordinator.public_identity(valid.identity.node_id) == valid.identity
    assert coordinator.audit_entries()[-1]["reason"] == (
        "public-identity-node-id-mismatch"
    )


def test_session_invitations_are_hashed_expiring_and_single_use(
    tmp_path: Path,
) -> None:
    database = tmp_path / "invitations.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    first = credentials(tmp_path, "First member")
    second = credentials(tmp_path, "Second member")
    for node in (owner, first, second):
        enroll(store, node)
    session = create_session(store, owner, session_id="session-invitations")

    expired_invitation = store.create_invitation(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=1,
        max_uses=1,
        now=NOW,
    )
    with pytest.raises(AuthorizationError) as expired:
        store.join_session(
            node_id=first.identity.node_id,
            raw_token=expired_invitation["token"],
            request_id="join-expired",
            now=NOW + timedelta(seconds=1),
        )
    assert expired.value.code == "expired-session-invitation"

    invitation = store.create_invitation(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        now=NOW,
    )
    store.join_session(
        node_id=first.identity.node_id,
        raw_token=invitation["token"],
        request_id="join-first",
        now=NOW,
    )
    with pytest.raises(AuthorizationError) as reused:
        store.join_session(
            node_id=second.identity.node_id,
            raw_token=invitation["token"],
            request_id="join-second",
            now=NOW,
        )
    assert reused.value.code == "reused-session-invitation"

    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT * FROM session_invitations ORDER BY invitation_id"
        ).fetchall()
        columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(session_invitations)"
            ).fetchall()
        }
    assert "token" not in columns
    assert {
        row["token_hash"] for row in rows
    } == {
        hashlib.sha256(expired_invitation["token"].encode()).hexdigest(),
        hashlib.sha256(invitation["token"].encode()).hexdigest(),
    }
    persisted = database_bytes(database)
    assert expired_invitation["token"].encode() not in persisted
    assert invitation["token"].encode() not in persisted
    store.require_membership(
        session_id=session.session_id,
        node_id=first.identity.node_id,
    )
    with pytest.raises(AuthorizationError):
        store.require_membership(
            session_id=session.session_id,
            node_id=second.identity.node_id,
        )


def test_session_join_retry_is_transactionally_idempotent(
    tmp_path: Path,
) -> None:
    database = tmp_path / "join-idempotency.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    member = credentials(tmp_path, "Member")
    enroll(store, owner)
    enroll(store, member)
    session = create_session(
        store,
        owner,
        session_id="session-join-idempotency",
    )
    invitation = store.create_invitation(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        now=NOW,
    )

    first = store.join_session(
        node_id=member.identity.node_id,
        raw_token=invitation["token"],
        request_id="stable-join-request",
        now=NOW,
    )
    revision_after_first = store.get_session(session.session_id).revision
    second = CoordinatorStore(database).join_session(
        node_id=member.identity.node_id,
        raw_token=invitation["token"],
        request_id="stable-join-request",
        now=NOW + timedelta(seconds=1),
    )

    assert second == first
    assert store.get_session(session.session_id).revision == revision_after_first
    with sqlite3.connect(database) as connection:
        use_count = connection.execute(
            """
            SELECT use_count FROM session_invitations
            WHERE invitation_id=?
            """,
            (invitation["invitation_id"],),
        ).fetchone()[0]
        join_requests = connection.execute(
            "SELECT COUNT(*) FROM session_join_requests"
        ).fetchone()[0]
    assert use_count == 1
    assert join_requests == 1

    other_invitation = store.create_invitation(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        now=NOW,
    )
    with pytest.raises(FederationOperationError) as conflict:
        store.join_session(
            node_id=member.identity.node_id,
            raw_token=other_invitation["token"],
            request_id="stable-join-request",
            now=NOW,
        )
    assert conflict.value.code == "idempotency-conflict"


def test_invitation_retry_rotates_unrecoverable_token_without_extra_effect(
    tmp_path: Path,
) -> None:
    database = tmp_path / "invitation-idempotency.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    member = credentials(tmp_path, "Member")
    enroll(store, owner)
    enroll(store, member)
    session = create_session(
        store,
        owner,
        session_id="session-invitation-idempotency",
    )

    first = store.create_invitation(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="stable-invitation-request",
        now=NOW,
    )
    replacement = CoordinatorStore(database).create_invitation(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="stable-invitation-request",
        now=NOW + timedelta(seconds=1),
    )

    assert replacement["replaced"] is True
    assert replacement["invitation_id"] == first["invitation_id"]
    assert replacement["token"] != first["token"]
    with pytest.raises(AuthorizationError) as old_token:
        store.join_session(
            node_id=member.identity.node_id,
            raw_token=first["token"],
            request_id="join-with-rotated-token",
            now=NOW,
        )
    assert old_token.value.code == "unknown-session-invitation"
    store.join_session(
        node_id=member.identity.node_id,
        raw_token=replacement["token"],
        request_id="join-with-replacement-token",
        now=NOW + timedelta(seconds=2),
    )
    with pytest.raises(FederationOperationError) as already_used:
        store.create_invitation(
            session_id=session.session_id,
            actor_node_id=owner.identity.node_id,
            ttl_seconds=60,
            max_uses=1,
            request_id="stable-invitation-request",
            now=NOW + timedelta(seconds=3),
        )
    assert already_used.value.code == "invitation-already-used"
    persisted = database_bytes(database)
    assert first["token"].encode() not in persisted
    assert replacement["token"].encode() not in persisted


def test_f2_007_enrollment_is_not_wrong_session_authorization(
    tmp_path: Path,
) -> None:
    store = CoordinatorStore(tmp_path / "wrong-session.sqlite3")
    owner = credentials(tmp_path, "Owner")
    outsider = credentials(tmp_path, "Outsider")
    enroll(store, owner)
    enroll(store, outsider)
    session = create_session(store, owner, session_id="session-private")
    coordinator = SessionCoordinator(store, clock=lambda: NOW)

    assert store.get_node(outsider.identity.node_id) is not None
    with pytest.raises(AuthorizationError) as append_rejected:
        store.append_event(
            session_id=session.session_id,
            actor_node_id=outsider.identity.node_id,
            request_id="wrong-session-event",
            event_type="test.message",
            payload={"message": "must fail"},
            now=NOW,
        )
    assert append_rejected.value.code == "not-session-member"

    with pytest.raises(AuthorizationError) as route_rejected:
        coordinator.authorize_route(
            session_id=session.session_id,
            actor_node_id=outsider.identity.node_id,
            target_node_id=owner.identity.node_id,
        )
    assert route_rejected.value.code == "not-session-member"
    assert store.get_session(session.session_id).revision == 2


def test_wrong_session_invitation_does_not_consume_or_create_membership(
    tmp_path: Path,
) -> None:
    store = CoordinatorStore(tmp_path / "wrong-invitation-session.sqlite3")
    owner = credentials(tmp_path, "Owner")
    joining = credentials(tmp_path, "Joining")
    for node in (owner, joining):
        enroll(store, node)
    invited = create_session(
        store, owner, session_id="session-invited"
    )
    other = create_session(store, owner, session_id="session-other")
    invitation = store.create_invitation(
        session_id=invited.session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        now=NOW,
    )

    with pytest.raises(AuthorizationError) as rejected:
        store.join_session(
            node_id=joining.identity.node_id,
            raw_token=invitation["token"],
            request_id="wrong-session-join",
            expected_session_id=other.session_id,
            now=NOW,
        )
    assert rejected.value.code == "wrong-session-invitation"
    with pytest.raises(AuthorizationError):
        store.require_membership(
            session_id=invited.session_id,
            node_id=joining.identity.node_id,
        )

    store.join_session(
        node_id=joining.identity.node_id,
        raw_token=invitation["token"],
        request_id="correct-session-join",
        expected_session_id=invited.session_id,
        now=NOW,
    )
    store.require_membership(
        session_id=invited.session_id,
        node_id=joining.identity.node_id,
    )


def test_session_create_request_is_idempotent_without_client_session_id(
    tmp_path: Path,
) -> None:
    store = CoordinatorStore(tmp_path / "session-idempotency.sqlite3")
    owner = credentials(tmp_path, "Owner")
    enroll(store, owner)

    first = store.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Stable session",
        request_id="stable-create-request",
        now=NOW,
    )
    duplicate = store.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Stable session",
        request_id="stable-create-request",
        now=NOW + timedelta(seconds=1),
    )

    assert duplicate == first
    assert duplicate.revision == 2
    with pytest.raises(FederationOperationError) as conflict:
        store.create_session(
            actor_node_id=owner.identity.node_id,
            display_name="Changed name",
            request_id="stable-create-request",
            now=NOW + timedelta(seconds=2),
        )
    assert conflict.value.code == "idempotency-conflict"


def test_f2_003_restart_replays_revisions_41_to_43_exactly_in_order(
    tmp_path: Path,
) -> None:
    database = tmp_path / "ordered-replay.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    enroll(store, owner)
    session = create_session(store, owner, session_id="session-replay")

    for expected_revision in range(3, 44):
        event, created = store.append_event(
            session_id=session.session_id,
            actor_node_id=owner.identity.node_id,
            request_id=f"event-request-{expected_revision}",
            event_type="test.message",
            payload={"expected_revision": expected_revision},
            now=NOW + timedelta(seconds=expected_revision),
        )
        assert created is True
        assert event.revision == expected_revision

    del store
    restarted = CoordinatorStore(database)
    replay = restarted.replay_events(
        session_id=session.session_id,
        last_applied_revision=40,
    )

    assert [event.revision for event in replay] == [41, 42, 43]
    assert [event.payload["expected_revision"] for event in replay] == [
        41,
        42,
        43,
    ]
    assert len({event.event_id for event in replay}) == 3
    assert restarted.get_session(session.session_id).revision == 43


def test_ordered_replay_pages_cover_a_backlog_larger_than_one_page(
    tmp_path: Path,
) -> None:
    store = CoordinatorStore(tmp_path / "paged-replay.sqlite3")
    owner = credentials(tmp_path, "Owner")
    enroll(store, owner)
    session = create_session(
        store,
        owner,
        session_id="session-paged-replay",
    )
    for revision in range(3, 76):
        store.append_event(
            session_id=session.session_id,
            actor_node_id=owner.identity.node_id,
            request_id=f"paged-event-{revision}",
            event_type="test.paged",
            payload={"revision": revision},
            now=NOW,
        )

    applied_revision = 0
    received: list[int] = []
    while True:
        page, authoritative_revision = store.replay_event_page(
            session_id=session.session_id,
            actor_node_id=owner.identity.node_id,
            last_applied_revision=applied_revision,
            limit=7,
        )
        received.extend(event.revision for event in page)
        if page:
            applied_revision = page[-1].revision
        if applied_revision == authoritative_revision:
            break

    assert authoritative_revision == 75
    assert received == list(range(1, 76))


def test_f2_004_event_append_is_transactional_idempotent_and_conflict_safe(
    tmp_path: Path,
) -> None:
    database = tmp_path / "event-idempotency.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    enroll(store, owner)
    session = create_session(store, owner, session_id="session-events")

    first, created = store.append_event(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        request_id="stable-request",
        event_type="test.message",
        payload={"value": 1},
        now=NOW + timedelta(seconds=1),
    )
    duplicate, duplicate_created = store.append_event(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        request_id="stable-request",
        event_type="test.message",
        payload={"value": 1},
        now=NOW + timedelta(hours=1),
    )

    assert created is True
    assert duplicate_created is False
    assert duplicate == first
    assert first.revision == 3
    with pytest.raises(FederationOperationError) as conflict:
        store.append_event(
            session_id=session.session_id,
            actor_node_id=owner.identity.node_id,
            request_id="stable-request",
            event_type="test.message",
            payload={"value": 2},
            now=NOW + timedelta(hours=2),
        )
    assert conflict.value.code == "idempotency-conflict"
    assert store.get_session(session.session_id).revision == 3

    restarted = CoordinatorStore(database)
    replay = restarted.replay_events(
        session_id=session.session_id,
        last_applied_revision=0,
    )
    assert [event.revision for event in replay] == [1, 2, 3]
    assert replay[-1] == first


def test_f2_005_authoritative_revision_gap_is_explicit_after_restart(
    tmp_path: Path,
) -> None:
    database = tmp_path / "event-gap.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    enroll(store, owner)
    session = create_session(store, owner, session_id="session-gap")
    for request_id in ("event-three", "event-four"):
        store.append_event(
            session_id=session.session_id,
            actor_node_id=owner.identity.node_id,
            request_id=request_id,
            event_type="test.message",
            payload={"request_id": request_id},
            now=NOW,
        )
    assert store.get_session(session.session_id).revision == 4

    with sqlite3.connect(database) as connection:
        connection.execute(
            "DELETE FROM session_events WHERE session_id=? AND revision=3",
            (session.session_id,),
        )
        connection.commit()

    restarted = CoordinatorStore(database)
    with pytest.raises(RevisionGapError) as gap:
        restarted.replay_events(
            session_id=session.session_id,
            last_applied_revision=2,
        )
    assert gap.value.code == "authoritative-revision-gap"
    assert restarted.get_session(session.session_id).revision == 4
    with pytest.raises(RevisionGapError) as ahead:
        restarted.replay_events(
            session_id=session.session_id,
            last_applied_revision=5,
        )
    assert ahead.value.code == "revision-ahead"


def test_member_removal_is_durable_and_rejoin_requires_a_new_invitation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "member-removal.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    member = credentials(tmp_path, "Member")
    enroll(store, owner)
    enroll(store, member)
    session = create_session(store, owner, session_id="session-membership")
    join(
        store,
        owner=owner,
        node=member,
        session_id=session.session_id,
        request_id="member-first-join",
    )
    announced = capability(
        session_id=session.session_id,
        node_id=member.identity.node_id,
        capability_id="member-storage",
    )
    store.upsert_capability(
        announced,
        actor_node_id=member.identity.node_id,
        request_id="member-capability",
        now=NOW,
    )
    accepted_before_removal, _ = store.append_event(
        session_id=session.session_id,
        actor_node_id=member.identity.node_id,
        request_id="member-event-before-removal",
        event_type="test.member-event",
        payload={"accepted": True},
        now=NOW,
    )

    removed = store.remove_member(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        target_node_id=member.identity.node_id,
        request_id="remove-member",
        reason="explicit test removal",
        now=NOW + timedelta(seconds=1),
    )
    revision_after_removal = store.get_session(session.session_id).revision
    assert removed is True
    assert store.remove_member(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        target_node_id=member.identity.node_id,
        request_id="remove-member-repeat",
        reason="repeat",
        now=NOW + timedelta(seconds=2),
    ) is False
    assert store.get_session(session.session_id).revision == revision_after_removal
    with pytest.raises(AuthorizationError):
        store.require_membership(
            session_id=session.session_id,
            node_id=member.identity.node_id,
        )
    with pytest.raises(AuthorizationError) as duplicate_after_removal:
        store.append_event(
            session_id=session.session_id,
            actor_node_id=member.identity.node_id,
            request_id="member-event-before-removal",
            event_type=accepted_before_removal.event_type,
            payload=accepted_before_removal.payload,
            now=NOW + timedelta(seconds=2),
        )
    assert duplicate_after_removal.value.code == "not-session-member"
    assert store.list_capabilities(
        session_id=session.session_id,
        node_id=member.identity.node_id,
    )[0].status is CapabilityStatus.REVOKED

    restarted = CoordinatorStore(database)
    with pytest.raises(AuthorizationError):
        restarted.append_event(
            session_id=session.session_id,
            actor_node_id=member.identity.node_id,
            request_id="removed-node-event",
            event_type="test.message",
            payload={},
            now=NOW,
        )
    join(
        restarted,
        owner=owner,
        node=member,
        session_id=session.session_id,
        request_id="member-rejoin",
        now=NOW + timedelta(seconds=3),
    )
    restarted.require_membership(
        session_id=session.session_id,
        node_id=member.identity.node_id,
    )
    revision_after_rejoin = restarted.get_session(session.session_id).revision
    with pytest.raises(FederationOperationError) as stale_removal:
        restarted.remove_member(
            session_id=session.session_id,
            actor_node_id=owner.identity.node_id,
            target_node_id=member.identity.node_id,
            request_id="remove-member",
            reason="explicit test removal",
            now=NOW + timedelta(seconds=4),
        )
    assert stale_removal.value.code == "idempotency-conflict"
    restarted.require_membership(
        session_id=session.session_id,
        node_id=member.identity.node_id,
    )
    assert (
        restarted.get_session(session.session_id).revision
        == revision_after_rejoin
    )
    assert restarted.list_capabilities(
        session_id=session.session_id,
        node_id=member.identity.node_id,
    )[0].status is CapabilityStatus.REVOKED

    updated = replace(
        announced,
        status=CapabilityStatus.READY,
        announced_at=NOW + timedelta(seconds=5),
    )
    _, created = restarted.upsert_capability(
        updated,
        actor_node_id=member.identity.node_id,
        request_id="member-capability-reannounce",
        now=NOW + timedelta(seconds=5),
    )
    assert created is False
    assert restarted.list_capabilities(
        session_id=session.session_id,
        node_id=member.identity.node_id,
    )[0].status is CapabilityStatus.READY


def test_f2_006_revocation_persists_is_auditable_and_other_node_is_usable(
    tmp_path: Path,
) -> None:
    database = tmp_path / "revocation.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    revoked = credentials(tmp_path, "Revoked")
    enroll(store, owner)
    enroll(store, revoked)
    session = create_session(store, owner, session_id="session-revocation")
    join(
        store,
        owner=owner,
        node=revoked,
        session_id=session.session_id,
        request_id="revoked-node-join",
    )
    announced = capability(
        session_id=session.session_id,
        node_id=revoked.identity.node_id,
        capability_id="revoked-storage",
    )
    store.upsert_capability(
        announced,
        actor_node_id=revoked.identity.node_id,
        request_id="revoked-capability",
        now=NOW,
    )
    store.mark_connected(
        node_id=owner.identity.node_id,
        connection_id="owner-connection",
        now=NOW,
    )
    store.mark_connected(
        node_id=revoked.identity.node_id,
        connection_id="revoked-connection",
        now=NOW,
    )

    assert store.revoke_node(
        node_id=revoked.identity.node_id,
        revoked_by="test-admin",
        reason="compromised",
        request_id="revoke-node",
        now=NOW + timedelta(seconds=1),
    )
    restarted = CoordinatorStore(database)

    with pytest.raises(AuthenticationError) as active:
        restarted.require_active_node(revoked.identity.node_id)
    assert active.value.code == "revoked-node"
    with pytest.raises(AuthenticationError) as event_rejected:
        restarted.append_event(
            session_id=session.session_id,
            actor_node_id=revoked.identity.node_id,
            request_id="revoked-event",
            event_type="test.message",
            payload={"must": "fail"},
            now=NOW + timedelta(seconds=2),
        )
    assert event_rejected.value.code == "revoked-node"
    restarted.audit_rejection(
        now=NOW + timedelta(seconds=2),
        action="session-event.append",
        reason="revoked-node",
        actor_node_id=revoked.identity.node_id,
        session_id=session.session_id,
        request_id="revoked-event",
    )

    restarted.require_active_node(owner.identity.node_id)
    accepted, created = restarted.append_event(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        request_id="owner-after-revocation",
        event_type="test.message",
        payload={"other_node": "unaffected"},
        now=NOW + timedelta(seconds=3),
    )
    assert created is True
    assert accepted.payload == {"other_node": "unaffected"}

    final = CoordinatorStore(database)
    status = final.authorized_status(actor_node_id=owner.identity.node_id)
    revoked_status = next(
        node
        for node in status["nodes"]
        if node["node_id"] == revoked.identity.node_id
    )
    assert revoked_status["revoked"] is True
    assert revoked_status["connection_state"] == "revoked"
    assert final.list_capabilities(
        session_id=session.session_id,
        node_id=revoked.identity.node_id,
    )[0].status is CapabilityStatus.REVOKED
    audit = final.audit_entries()
    assert any(entry["action"] == "node.revoke" for entry in audit)
    assert any(
        entry["action"] == "session-event.append"
        and entry["outcome"] == "rejected"
        and entry["reason"] == "revoked-node"
        for entry in audit
    )


def test_multiple_capabilities_and_same_type_across_nodes_are_deterministic(
    tmp_path: Path,
) -> None:
    store = CoordinatorStore(tmp_path / "capabilities.sqlite3")
    owner = credentials(tmp_path, "Owner")
    provider = credentials(tmp_path, "Provider")
    enroll(store, owner)
    enroll(store, provider)
    session = create_session(store, owner, session_id="session-capabilities")
    join(
        store,
        owner=owner,
        node=provider,
        session_id=session.session_id,
        request_id="provider-join",
    )
    announcements = [
        capability(
            session_id=session.session_id,
            node_id=owner.identity.node_id,
            capability_id="storage-z",
        ),
        capability(
            session_id=session.session_id,
            node_id=owner.identity.node_id,
            capability_id="storage-a",
        ),
        capability(
            session_id=session.session_id,
            node_id=provider.identity.node_id,
            capability_id="storage-provider",
        ),
        capability(
            session_id=session.session_id,
            node_id=owner.identity.node_id,
            capability_id="language-owner",
            capability_type="language-model",
        ),
    ]
    for index, announcement in enumerate(reversed(announcements)):
        stored, created = store.upsert_capability(
            announcement,
            actor_node_id=announcement.node_id,
            request_id=f"capability-{index}",
            now=NOW,
        )
        assert stored == announcement
        assert created is True

    listed = store.list_capabilities(session_id=session.session_id)
    keys = [
        (
            item.session_id,
            item.type,
            item.node_id,
            item.capability_id,
        )
        for item in listed
    ]
    assert keys == sorted(keys)
    assert len(
        store.list_capabilities(
            session_id=session.session_id,
            node_id=owner.identity.node_id,
        )
    ) == 3
    storage = store.list_capabilities(
        session_id=session.session_id,
        capability_type="storage",
    )
    assert len(storage) == 3
    assert {item.node_id for item in storage} == {
        owner.identity.node_id,
        provider.identity.node_id,
    }


def test_capability_request_is_idempotent_and_conflict_safe(
    tmp_path: Path,
) -> None:
    store = CoordinatorStore(tmp_path / "capability-idempotency.sqlite3")
    owner = credentials(tmp_path, "Owner")
    enroll(store, owner)
    session = create_session(
        store, owner, session_id="session-capability-idempotency"
    )
    announcement = capability(
        session_id=session.session_id,
        node_id=owner.identity.node_id,
        capability_id="provider",
    )

    first, created = store.upsert_capability(
        announcement,
        actor_node_id=owner.identity.node_id,
        request_id="stable-capability-request",
        now=NOW,
    )
    duplicate, duplicate_created = store.upsert_capability(
        announcement,
        actor_node_id=owner.identity.node_id,
        request_id="stable-capability-request",
        now=NOW + timedelta(seconds=1),
    )

    assert first == duplicate == announcement
    assert created is duplicate_created is True
    assert store.get_session(session.session_id).revision == 3
    with pytest.raises(FederationOperationError) as conflict:
        store.upsert_capability(
            replace(
                announcement,
                status=CapabilityStatus.DRAINING,
                announced_at=NOW + timedelta(seconds=2),
            ),
            actor_node_id=owner.identity.node_id,
            request_id="stable-capability-request",
            now=NOW + timedelta(seconds=2),
        )
    assert conflict.value.code == "idempotency-conflict"
    assert store.get_session(session.session_id).revision == 3


def test_f2_008_stale_heartbeat_marks_capability_unavailable_but_keeps_identity(
    tmp_path: Path,
) -> None:
    database = tmp_path / "heartbeat.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    enroll(store, owner)
    session = create_session(store, owner, session_id="session-heartbeat")
    announcement = capability(
        session_id=session.session_id,
        node_id=owner.identity.node_id,
        capability_id="heartbeat-capability",
    )
    store.upsert_capability(
        announcement,
        actor_node_id=owner.identity.node_id,
        request_id="heartbeat-capability",
        now=NOW,
    )
    store.mark_connected(
        node_id=owner.identity.node_id,
        connection_id="connection-one",
        now=NOW,
    )
    store.heartbeat(
        node_id=owner.identity.node_id,
        now=NOW + timedelta(seconds=1),
    )

    stale = store.sweep_stale(
        now=NOW + timedelta(seconds=12),
        heartbeat_timeout_seconds=10,
    )
    assert stale == (owner.identity.node_id,)
    assert store.list_capabilities(
        session_id=session.session_id,
    )[0].status is CapabilityStatus.UNAVAILABLE
    assert store.public_identity(owner.identity.node_id) == owner.identity

    restarted = CoordinatorStore(database)
    status = restarted.authorized_status(actor_node_id=owner.identity.node_id)
    assert status["nodes"][0]["connection_state"] == "disconnected"
    assert status["nodes"][0]["last_heartbeat"] == (
        NOW + timedelta(seconds=1)
    ).isoformat()
    assert status["capabilities"][0]["status"] == "unavailable"
    assert any(
        entry["action"] == "heartbeat.expire"
        and entry["reason"] == "stale-heartbeat"
        for entry in restarted.audit_entries()
    )


@pytest.mark.parametrize("restart", [False, True])
def test_disconnect_or_relay_restart_marks_capability_unavailable(
    tmp_path: Path, restart: bool
) -> None:
    store = CoordinatorStore(tmp_path / f"disconnect-{restart}.sqlite3")
    owner = credentials(tmp_path, f"Owner {restart}")
    enroll(store, owner)
    session = create_session(
        store, owner, session_id=f"session-disconnect-{restart}"
    )
    store.upsert_capability(
        capability(
            session_id=session.session_id,
            node_id=owner.identity.node_id,
            capability_id="provider",
        ),
        actor_node_id=owner.identity.node_id,
        request_id="announce-provider",
        now=NOW,
    )
    store.mark_connected(
        node_id=owner.identity.node_id,
        connection_id="connection-one",
        now=NOW,
    )
    previous_revision = store.get_session(session.session_id).revision

    if restart:
        store.mark_all_disconnected(now=NOW + timedelta(seconds=1))
    else:
        store.mark_disconnected(
            node_id=owner.identity.node_id,
            now=NOW + timedelta(seconds=1),
        )

    assert store.list_capabilities(
        session_id=session.session_id
    )[0].status is CapabilityStatus.UNAVAILABLE
    event = store.replay_events(
        session_id=session.session_id,
        last_applied_revision=previous_revision,
    )
    assert len(event) == 1
    assert event[0].event_type == "capability.health.changed"
    assert event[0].payload["status"] == "unavailable"


def envelope_dict(**changes: Any) -> dict[str, Any]:
    value = RelayEnvelope(
        request_id="request-one",
        session_id="session-one",
        actor_node_id="node-one",
        target_node_id="node-two",
        message_type="relay.test",
        authorization_context={"kind": "authenticated-connection"},
        payload={"value": 1},
        sent_at=NOW,
    ).to_dict()
    value.update(changes)
    return value


def test_protocol_rejects_malformed_oversized_and_unsupported_messages() -> None:
    with pytest.raises(FederationValidationError) as malformed:
        RelayEnvelope.from_json(b"\xff")
    assert malformed.value.code == "malformed-message"

    with pytest.raises(FederationValidationError) as oversized:
        RelayEnvelope.from_json(b"x" * (MAX_MESSAGE_BYTES + 1))
    assert oversized.value.code == "message-too-large"

    with pytest.raises(ProtocolCompatibilityError) as unsupported:
        RelayEnvelope.from_dict(envelope_dict(protocol_version="2.0"))
    assert unsupported.value.code == "unsupported-protocol-major"

    with pytest.raises(FederationValidationError) as missing:
        value = envelope_dict()
        del value["request_id"]
        RelayEnvelope.from_dict(value)
    assert missing.value.code == "missing-field"


def test_protocol_accepts_additive_optional_fields_within_major_version() -> None:
    value = envelope_dict(
        protocol_version="1.99",
        future_optional={"enabled": True},
    )

    parsed = RelayEnvelope.from_dict(value)

    assert parsed.protocol_version == "1.99"
    assert parsed.request_id == "request-one"
    assert parsed.payload == {"value": 1}
    assert "future_optional" not in parsed.to_dict()
    assert RelayEnvelope.from_json(parsed.to_json()) == parsed


def test_duplicate_request_ids_are_durable_and_conflicts_are_rejected(
    tmp_path: Path,
) -> None:
    database = tmp_path / "request-deduplication.sqlite3"
    store = CoordinatorStore(database)
    actor = credentials(tmp_path, "Actor")
    enroll(store, actor)

    assert store.accept_request(
        actor_node_id=actor.identity.node_id,
        request_id="request-id",
        message_type="relay.test",
        request_hash="sha256:same",
        now=NOW,
    )
    assert not store.accept_request(
        actor_node_id=actor.identity.node_id,
        request_id="request-id",
        message_type="relay.test",
        request_hash="sha256:same",
        now=NOW + timedelta(seconds=1),
    )

    restarted = CoordinatorStore(database)
    assert not restarted.accept_request(
        actor_node_id=actor.identity.node_id,
        request_id="request-id",
        message_type="relay.test",
        request_hash="sha256:same",
        now=NOW + timedelta(seconds=2),
    )
    with pytest.raises(FederationOperationError) as conflict:
        restarted.accept_request(
            actor_node_id=actor.identity.node_id,
            request_id="request-id",
            message_type="relay.test",
            request_hash="sha256:different",
            now=NOW + timedelta(seconds=3),
        )
    assert conflict.value.code == "request-id-conflict"

    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT message_type,request_hash FROM accepted_requests"
        ).fetchall()
    assert rows == [("relay.test", "sha256:same")]


def test_caller_controlled_request_ids_are_hashed_before_persistence(
    tmp_path: Path,
) -> None:
    database = tmp_path / "request-id-secrecy.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    enroll(store, owner)
    session = create_session(
        store,
        owner,
        session_id="session-request-id-secrecy",
    )
    secret_request_id = store.create_invitation(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        now=NOW,
    )["token"]

    store.append_event(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        request_id=secret_request_id,
        event_type="test.request-id-secrecy",
        payload={"safe": True},
        now=NOW,
    )
    store.accept_request(
        actor_node_id=owner.identity.node_id,
        request_id=secret_request_id,
        message_type="relay.test",
        request_hash="sha256:safe",
        now=NOW,
    )
    store.audit_rejection(
        now=NOW,
        action="relay.test",
        reason="deliberate-test-rejection",
        actor_node_id=owner.identity.node_id,
        request_id=secret_request_id,
    )
    store.audit_rejection(
        now=NOW,
        action=secret_request_id,
        reason=secret_request_id,
        actor_node_id=secret_request_id,
        session_id=secret_request_id,
        details={"label": secret_request_id, "auth-token": secret_request_id},
    )

    assert secret_request_id.encode() not in database_bytes(database)
    assert secret_request_id not in json.dumps(
        store.audit_entries(), sort_keys=True
    )
    with sqlite3.connect(database) as connection:
        stored_request_ids = [
            row[0]
            for table in (
                "session_events",
                "accepted_requests",
                "audit_log",
            )
            for row in connection.execute(
                f"SELECT request_id FROM {table} WHERE request_id IS NOT NULL"
            )
        ]
    assert stored_request_ids
    assert all(value.startswith("sha256:") for value in stored_request_ids)


def test_control_reasons_reject_embedded_credentials_before_persistence(
    tmp_path: Path,
) -> None:
    database = tmp_path / "control-reason-secrecy.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    member = credentials(tmp_path, "Member")
    enroll(store, owner)
    enroll(store, member)
    session = create_session(
        store,
        owner,
        session_id="session-control-reason-secrecy",
    )
    join(
        store,
        owner=owner,
        node=member,
        session_id=session.session_id,
        request_id="member-control-reason-join",
    )
    secret = store.create_invitation(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        request_id="embedded-secret-source",
        now=NOW,
    )["token"]
    unsafe_reason = f"operator pasted {secret} into the reason"

    with pytest.raises(FederationValidationError) as removal:
        store.remove_member(
            session_id=session.session_id,
            actor_node_id=owner.identity.node_id,
            target_node_id=member.identity.node_id,
            request_id="unsafe-removal",
            reason=unsafe_reason,
            now=NOW,
        )
    assert removal.value.code == "nonpublic-removal-reason"
    store.require_membership(
        session_id=session.session_id,
        node_id=member.identity.node_id,
    )

    with pytest.raises(FederationValidationError) as revocation:
        store.revoke_node(
            node_id=member.identity.node_id,
            revoked_by="local-admin",
            reason=unsafe_reason,
            request_id="unsafe-revocation",
            now=NOW,
        )
    assert revocation.value.code == "nonpublic-revocation-reason"
    store.require_active_node(member.identity.node_id)
    assert secret.encode() not in database_bytes(database)


def test_capability_metadata_rejects_token_shaped_values_after_mutation(
    tmp_path: Path,
) -> None:
    store = CoordinatorStore(tmp_path / "capability-secret.sqlite3")
    owner = credentials(tmp_path, "Owner")
    enroll(store, owner)
    session = create_session(
        store,
        owner,
        session_id="session-capability-secret",
    )
    announcement = capability(
        session_id=session.session_id,
        node_id=owner.identity.node_id,
        capability_id="capability-secret",
    )
    announcement.properties["label"] = "msh_join_not-for-metadata"

    with pytest.raises(FederationValidationError) as rejected:
        store.upsert_capability(
            announcement,
            actor_node_id=owner.identity.node_id,
            request_id="mutated-capability",
            now=NOW,
        )

    assert rejected.value.code == "secret-property"
    with pytest.raises(FederationValidationError) as variant:
        replace(
            capability(
                session_id=session.session_id,
                node_id=owner.identity.node_id,
                capability_id="capability-auth-token",
            ),
            properties={"auth-token": "opaque"},
        )
    assert variant.value.code == "secret-property"
    with pytest.raises(FederationValidationError) as physical_path:
        replace(
            capability(
                session_id=session.session_id,
                node_id=owner.identity.node_id,
                capability_id="capability-path",
            ),
            properties={"label": r"C:\private\recorder"},
        )
    assert physical_path.value.code == "nonpublic-property"


def test_public_identity_and_capability_fields_are_bounded_and_nonsecret(
    tmp_path: Path,
) -> None:
    owner = credentials(tmp_path, "Owner")
    token = "msh_join_not-for-public-metadata"

    with pytest.raises(FederationValidationError) as identity_secret:
        NodeIdentity(
            node_id=owner.identity.node_id,
            display_name=token,
            public_key=owner.identity.public_key,
            created_at=NOW,
            identity_version=1,
        )
    assert identity_secret.value.code == "secret-metadata"

    with pytest.raises(FederationValidationError) as identity_too_large:
        replace(owner.identity, display_name="x" * 513)
    assert identity_too_large.value.code == "metadata-too-large"

    store = CoordinatorStore(tmp_path / "public-metadata.sqlite3")
    enroll(store, owner)
    with pytest.raises(FederationValidationError) as session_secret:
        store.create_session(
            actor_node_id=owner.identity.node_id,
            display_name="Public session",
            request_id="secret-session-id",
            session_id=token,
            now=NOW,
        )
    assert session_secret.value.code == "secret-metadata"
    assert store.get_session(token) is None

    base = capability(
        session_id="session-public-metadata",
        node_id=owner.identity.node_id,
        capability_id="capability-public-metadata",
    )
    for field, value, expected_code in (
        ("capability_id", token, "secret-metadata"),
        ("type", "/private/type", "nonpublic-metadata"),
        ("protocol", r"C:\private\database", "nonpublic-metadata"),
        ("protocol_version", "x" * 513, "metadata-too-large"),
    ):
        with pytest.raises(FederationValidationError) as rejected:
            replace(base, **{field: value})
        assert rejected.value.code == expected_code

    established_contract = replace(
        base,
        properties={
            "backend": "postgresql",
            "available_bytes": 450_000_000_000,
        },
    )
    assert established_contract.properties["backend"] == "postgresql"


def test_authorized_status_is_scoped_deterministic_and_secret_free(
    tmp_path: Path,
) -> None:
    database = tmp_path / "status.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Owner")
    member = credentials(tmp_path, "Member")
    outsider = credentials(tmp_path, "Outsider")
    enrollment_tokens = [
        enroll(store, owner),
        enroll(store, member),
        enroll(store, outsider),
    ]
    shared = create_session(store, owner, session_id="session-shared")
    invitation_token = join(
        store,
        owner=owner,
        node=member,
        session_id=shared.session_id,
        request_id="status-member-join",
    )
    private = create_session(
        store,
        outsider,
        session_id="session-outsider-private",
    )
    announcement = capability(
        session_id=shared.session_id,
        node_id=member.identity.node_id,
        capability_id="status-capability",
    )
    store.upsert_capability(
        announcement,
        actor_node_id=member.identity.node_id,
        request_id="status-capability",
        now=NOW,
    )

    first = store.authorized_status(actor_node_id=owner.identity.node_id)
    second = CoordinatorStore(database).authorized_status(
        actor_node_id=owner.identity.node_id
    )

    assert first == second
    assert first["schema"] == "msh.coordinator_status.v1"
    assert [session["session_id"] for session in first["sessions"]] == [
        shared.session_id
    ]
    assert private.session_id not in json.dumps(first, sort_keys=True)
    assert {node["node_id"] for node in first["nodes"]} == {
        owner.identity.node_id,
        member.identity.node_id,
    }
    assert [item["capability_id"] for item in first["capabilities"]] == [
        "status-capability"
    ]

    encoded = json.dumps(first, sort_keys=True)
    for raw_token in [*enrollment_tokens, invitation_token]:
        assert raw_token not in encoded
    for node in (owner, member, outsider):
        assert node.identity.public_key not in encoded
    assert str(database) not in encoded
    forbidden_keys = {
        "token",
        "token_hash",
        "private_key",
        "public_key",
        "authorization",
        "credential",
        "database",
        "row_id",
    }

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            assert forbidden_keys.isdisjoint(value)
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(first)


def test_authorized_status_pages_are_bounded_complete_and_stable(
    tmp_path: Path,
) -> None:
    database = tmp_path / "status-pages.sqlite3"
    store = CoordinatorStore(database)
    owner = credentials(tmp_path, "Paged Owner")
    enroll(store, owner)
    session = create_session(
        store,
        owner,
        session_id="session-paged-status",
    )

    announcements = [
        replace(
            capability(
                session_id=session.session_id,
                node_id=owner.identity.node_id,
                capability_id=f"large-{index}",
            ),
            properties={"description": character * 44_000},
        )
        for index, character in enumerate(("a", "b"))
    ]
    announcements.extend(
        capability(
            session_id=session.session_id,
            node_id=owner.identity.node_id,
            capability_id=f"small-{index:03d}",
        )
        for index in range(65)
    )
    for index, announcement in enumerate(announcements):
        store.upsert_capability(
            announcement,
            actor_node_id=owner.identity.node_id,
            request_id=f"paged-capability-{index}",
            now=NOW,
        )

    def read_pages(source: CoordinatorStore) -> list[dict[str, Any]]:
        pages: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page = source.authorized_status(
                actor_node_id=owner.identity.node_id,
                cursor=cursor,
            )
            pages.append(page)
            encoded_payload = json.dumps(
                page,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            assert len(encoded_payload) <= STATUS_PAGE_MAX_BYTES
            encoded_envelope = RelayEnvelope(
                request_id="bounded-status-page",
                actor_node_id=source.coordinator_id,
                target_node_id=owner.identity.node_id,
                message_type="status.get.accepted",
                authorization_context={
                    "kind": "authenticated-status",
                    "actor_node_id": owner.identity.node_id,
                    "validated_by": source.coordinator_id,
                },
                payload=page,
                sent_at=NOW,
            ).to_json()
            assert len(encoded_envelope.encode("utf-8")) <= MAX_MESSAGE_BYTES
            pagination = page["pagination"]
            if not pagination["has_more"]:
                break
            cursor = pagination["next_cursor"]
            assert isinstance(cursor, str)
        return pages

    first_pages = read_pages(store)
    restarted_pages = read_pages(CoordinatorStore(database))
    assert first_pages == restarted_pages
    assert len(first_pages) >= 3

    flattened_capabilities = [
        item
        for page in first_pages
        for item in page["capabilities"]
    ]
    assert [item["capability_id"] for item in flattened_capabilities] == [
        item.capability_id
        for item in sorted(
            announcements,
            key=lambda item: (
                item.session_id,
                item.type,
                item.node_id,
                item.capability_id,
            ),
        )
    ]
    assert sum(
        page["pagination"]["item_count"] for page in first_pages
    ) == first_pages[0]["pagination"]["total_items"]

    stale_cursor = first_pages[0]["pagination"]["next_cursor"]
    assert isinstance(stale_cursor, str)
    added = capability(
        session_id=session.session_id,
        node_id=owner.identity.node_id,
        capability_id="small-added-after-first-page",
    )
    store.upsert_capability(
        added,
        actor_node_id=owner.identity.node_id,
        request_id="paged-capability-added",
        now=NOW,
    )
    with pytest.raises(FederationOperationError) as changed:
        store.authorized_status(
            actor_node_id=owner.identity.node_id,
            cursor=stale_cursor,
        )
    assert changed.value.code == "status-snapshot-changed"

    value_cursor = store.authorized_status(
        actor_node_id=owner.identity.node_id
    )["pagination"]["next_cursor"]
    assert isinstance(value_cursor, str)
    store.mark_connected(
        node_id=owner.identity.node_id,
        connection_id="status-value-change",
        now=NOW + timedelta(seconds=1),
    )
    with pytest.raises(FederationOperationError) as value_changed:
        store.authorized_status(
            actor_node_id=owner.identity.node_id,
            cursor=value_cursor,
        )
    assert value_changed.value.code == "status-snapshot-changed"

    revision_cursor = store.authorized_status(
        actor_node_id=owner.identity.node_id
    )["pagination"]["next_cursor"]
    assert isinstance(revision_cursor, str)
    store.append_event(
        session_id=session.session_id,
        actor_node_id=owner.identity.node_id,
        request_id="status-revision-change",
        event_type="test.status-revision-change",
        payload={"changed": True},
        now=NOW + timedelta(seconds=2),
    )
    with pytest.raises(FederationOperationError) as revision_changed:
        store.authorized_status(
            actor_node_id=owner.identity.node_id,
            cursor=revision_cursor,
        )
    assert revision_changed.value.code == "status-snapshot-changed"

    with pytest.raises(FederationValidationError) as malformed:
        store.authorized_status(
            actor_node_id=owner.identity.node_id,
            cursor="not-a-valid-status-cursor!%",
        )
    assert malformed.value.code == "invalid-status-cursor"
