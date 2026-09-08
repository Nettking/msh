"""Atomic canonical journal invariants at the real consensus state machine."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
from typing import Any

import pytest

from catalog.federation.control_plane_journal import (
    LEADERSHIP_SCHEMA,
    LEGACY_COORDINATOR_ID,
    PRODUCT_JOURNAL_INITIALIZE,
    PRODUCT_TRANSACTION,
    REPLICATED_COORDINATOR_ID,
    journal_history_digest,
    journal_prefix_digest,
    private_ciphertext_digest,
    public_row_content_hash,
    transaction_accepted,
)
from catalog.federation.control_plane_readiness import (
    BOOTSTRAP_SEAL_CAPABILITY_ID,
    BOOTSTRAP_SEAL_CAPABILITY_TYPE,
)
from catalog.federation.control_plane_replication import (
    MAX_COMMAND_RECEIPTS,
    AuthorityCommand,
    AuthorityStateMachine,
    ControlPlaneError,
    Snapshot,
    VoterConfiguration,
    _state_digest,
)

CONFIG = VoterConfiguration("journal-cluster", ("voter-a", "voter-b", "voter-c"))
STAMP = "2026-09-08T12:00:00+00:00"
SESSION = "session-one"


def command(kind: str, payload: dict[str, Any], *, identity: str = "operation", actor: str = "voter-a") -> AuthorityCommand:
    return AuthorityCommand(identity, kind, CONFIG.cluster_id, actor, payload)


def initial() -> tuple[AuthorityStateMachine, dict[str, Any]]:
    machine = AuthorityStateMachine(CONFIG)
    state, _ = machine.apply(machine.initial_state(), command("FEDERATION_GENESIS", {
        "federation_id": "federation-one", "session_id": SESSION,
        "creator_node_id": "voter-a", "display_name": "Public journal test",
        "voter_ids": list(CONFIG.voter_ids), "members": list(CONFIG.voter_ids),
        "nodes": [{"node_id": node, "display_name": node, "public_key": "public-" + node} for node in CONFIG.voter_ids],
        "occurred_at": STAMP,
    }, identity="genesis"))
    return machine, state


def row(
    revision: int, *, event_type: str = "demo.observation", actor: str = "voter-a",
    payload: dict[str, Any] | None = None, identity: str | None = None, session: str = SESSION,
) -> dict[str, Any]:
    identity = identity or "event-" + str(revision)
    encoded = json.dumps(payload or {"value": revision}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {
        "session_id": session, "revision": revision, "event_id": identity,
        "request_id": "sha256:" + hashlib.sha256(identity.encode()).hexdigest(),
        "event_type": event_type, "occurred_at": STAMP, "actor_node_id": actor,
        "payload_json": encoded, "content_hash": public_row_content_hash(event_type, encoded),
    }


def initialize(
    rows: list[dict[str, Any]], *, chunk: list[dict[str, Any]] | None = None,
    final: bool = True, provenance: dict[str, Any] | None = None,
) -> AuthorityCommand:
    payload = {
        "session_id": SESSION, "expected_revision": len(rows),
        "prefix_digest": journal_prefix_digest(rows),
        "public_rows": rows if chunk is None else chunk, "final": final,
    }
    if provenance is not None:
        payload["provenance"] = provenance
    return command(PRODUCT_JOURNAL_INITIALIZE, payload, identity="initialize-" + str(len(rows)) + "-" + str(final))


def ready() -> tuple[AuthorityStateMachine, dict[str, Any]]:
    machine, state = initial()
    state, _ = machine.apply(state, initialize([]))
    return machine, state


def transaction(
    rows: list[dict[str, Any]], *commands: AuthorityCommand,
    private: list[dict[str, Any]] | None = None, identity: str | None = None, actor: str = "voter-a",
) -> AuthorityCommand:
    payload = {"authority_commands": [item.to_dict() for item in commands], "public_rows": rows}
    if private is not None:
        payload["private_rows"] = private
    identity = identity or "transaction-" + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return command(PRODUCT_TRANSACTION, payload, identity=identity, actor=actor)


def member(kind: str, node: str, *, identity: str = "membership") -> AuthorityCommand:
    return command(kind, {"session_id": SESSION, "node_id": node, "occurred_at": STAMP}, identity=identity)


def private_row(value: bytes, *, previous: str | None = None) -> dict[str, Any]:
    # An opaque deterministic fixture tests the core carrier, not encryption.
    return {
        "key": "hmac-sha256:" + "1" * 64, "previous_digest": previous,
        "ciphertext": base64.b64encode(value).decode("ascii"),
    }


def test_authority_public_history_and_private_receipt_commit_as_one_state() -> None:
    machine, state = ready()
    enroll = command("NODE_ENROLL", {"node_id": "reviewer", "display_name": "Reviewer", "public_key": "reviewer-public"})
    joined = row(1, event_type="node.joined", actor="reviewer", payload={"node_id": "reviewer"})
    opaque = private_row(b"disposable-opaque-receipt-fixture" * 2)
    new, events = machine.apply(state, transaction([joined], enroll, member("SESSION_MEMBER_ADD", "reviewer"), private=[opaque]))
    assert new["memberships"][SESSION]["reviewer"] is True
    journal = new["product_journal"]["sessions"][SESSION]
    assert journal == {"revision": 1, "rows": [joined], "prefix_digest": journal_prefix_digest([joined])}
    assert new["product_journal"]["private_rows"][opaque["key"]]["ciphertext"] == opaque["ciphertext"]
    assert events == ()  # Core C03 events are not substituted for client rows.
    assert "reviewer" not in state["nodes"]
    assert state["product_journal"]["sessions"][SESSION]["rows"] == []


def test_missing_public_membership_row_rejects_the_entire_committed_envelope() -> None:
    machine, state = ready()
    removal = member("SESSION_MEMBER_REMOVE", "voter-c")
    invalid = transaction([], removal)
    with pytest.raises(ControlPlaneError, match="missing its public event"):
        machine.apply(state, invalid)
    unchanged, events, rejection = machine.apply_committed(state, invalid)
    assert unchanged == state
    assert events == () and rejection is not None
    assert unchanged["memberships"][SESSION]["voter-c"] is True


def test_public_row_must_be_new_and_follow_authority_operation_order() -> None:
    machine, state = ready()
    left = row(1, event_type="node.left", payload={"node_id": "voter-c"})
    first_removal = transaction([left], member("SESSION_MEMBER_REMOVE", "voter-c"))
    state, _ = machine.apply(state, first_removal)
    joined = row(2, event_type="node.joined", actor="voter-c", payload={"node_id": "voter-c"})
    state, _ = machine.apply(state, transaction([joined], member("SESSION_MEMBER_ADD", "voter-c")))
    # The already accepted envelope must not repeat its old side effect after
    # rejoin. The attack below is a new operation reusing that old event row.
    replayed, _ = machine.apply(state, first_removal)
    assert replayed == state and replayed["memberships"][SESSION]["voter-c"] is True
    with pytest.raises(ControlPlaneError, match="missing its public event"):
        machine.apply(state, transaction(
            [left], member("SESSION_MEMBER_REMOVE", "voter-c", identity="new-removal"),
            identity="new-operation-reusing-old-public-row",
        ))
    reversed_rows = [
        row(3, event_type="node.joined", actor="voter-c", payload={"node_id": "voter-c"}),
        row(4, event_type="node.left", payload={"node_id": "voter-c"}),
    ]
    with pytest.raises(ControlPlaneError, match="missing its public event"):
        machine.apply(state, transaction(reversed_rows,
            member("SESSION_MEMBER_REMOVE", "voter-c", identity="remove"),
            member("SESSION_MEMBER_ADD", "voter-c", identity="rejoin")))


def test_private_compare_and_set_failure_rolls_back_public_and_authority_changes() -> None:
    machine, state = ready()
    first = private_row(b"first-opaque-private-row-fixture" * 2)
    state, _ = machine.apply(state, transaction([], private=[first]))
    bad = private_row(b"second-opaque-private-row-fixture" * 2)
    left = row(1, event_type="node.left", payload={"node_id": "voter-c"})
    with pytest.raises(ControlPlaneError, match="compare-and-set"):
        machine.apply(state, transaction([left], member("SESSION_MEMBER_REMOVE", "voter-c"), private=[bad]))
    assert state["memberships"][SESSION]["voter-c"] is True
    assert state["product_journal"]["sessions"][SESSION]["rows"] == []
    good = {**bad, "previous_digest": private_ciphertext_digest(first["ciphertext"])}
    new, _ = machine.apply(state, transaction([left], member("SESSION_MEMBER_REMOVE", "voter-c"), private=[good]))
    assert new["memberships"][SESSION]["voter-c"] is False


def test_exact_public_and_opaque_retries_are_idempotent() -> None:
    machine, state = ready()
    operation = transaction([row(1)], private=[private_row(b"opaque-idempotent-receipt-fixture" * 2)])
    state, _ = machine.apply(state, operation)
    repeated, _ = machine.apply(state, operation)
    assert repeated == state
    with pytest.raises(ControlPlaneError, match="prefix conflict"):
        machine.apply(state, transaction([row(1, payload={"value": "different"})]))
    with pytest.raises(ControlPlaneError, match="revision gap"):
        machine.apply(state, transaction([row(3)]))
    with pytest.raises(ControlPlaneError, match="identity conflict"):
        machine.apply(state, transaction([row(2, identity="event-1")]))


@pytest.mark.parametrize("changes", [
    {"request_id": "raw-request-or-token"},
    {"content_hash": "0" * 64},
    {"payload_json": '{ "value": 1 }'},
    {"occurred_at": "2026-09-08T12:00:00"},
    {"revision": True},
    {"unexpected": "injected"},
])
def test_closed_public_row_schema_and_exact_content_are_enforced(changes: dict[str, Any]) -> None:
    with pytest.raises(ControlPlaneError):
        transaction([{**row(1), **changes}])


@pytest.mark.parametrize("payload", [
    {"token": "private-value"}, {"password_hash": "private-hash"},
    {"nested": {"backend_path": "/private/database.sqlite3"}},
])
def test_public_payload_cannot_carry_private_receipts(payload: dict[str, Any]) -> None:
    with pytest.raises(ControlPlaneError, match="nonpublic"):
        transaction([row(1, payload=payload)])


def test_opaque_receipt_keys_cannot_be_plain_credential_hashes() -> None:
    opaque = private_row(b"opaque-row-fixture-not-encryption" * 2)
    with pytest.raises(ControlPlaneError, match="HMAC-SHA256"):
        transaction([], private=[{**opaque, "key": "sha256:" + "1" * 64}])
    with pytest.raises(ControlPlaneError, match="unexpected or missing"):
        transaction([], private=[{**opaque, "token_hash": "plaintext-forbidden"}])


def test_membership_authorization_and_voter_submission_are_distinct() -> None:
    machine, state = ready()
    with pytest.raises(ControlPlaneError, match="configured voter"):
        machine.apply(state, transaction([row(1)], actor="unknown"))
    with pytest.raises(ControlPlaneError, match="not an enrolled node"):
        machine.apply(state, transaction([row(1, actor="unknown")]))
    with pytest.raises(ControlPlaneError, match="unsupported coordinator-authored"):
        machine.apply(state, transaction([row(1, actor=REPLICATED_COORDINATOR_ID)]))
    with pytest.raises(ControlPlaneError, match="requires PRODUCT_TRANSACTION"):
        machine.apply(state, member("SESSION_MEMBER_REMOVE", "voter-c"))


def test_recursive_and_genesis_authority_are_forbidden_inside_transaction() -> None:
    for nested in (transaction([]), command("FEDERATION_GENESIS", {
        "federation_id": "other", "session_id": "other", "creator_node_id": "voter-a",
        "display_name": "Other", "voter_ids": list(CONFIG.voter_ids), "nodes": [],
    })):
        with pytest.raises(ControlPlaneError, match="forbids genesis"):
            transaction([], nested)


def test_capability_authority_requires_complete_public_row_owned_by_its_member() -> None:
    machine, state = ready()
    declare = command("CAPABILITY_DECLARE", {
        "session_id": SESSION, "capability_id": "demo", "capability_type": "demo.reading",
        "owner_node_id": "voter-a", "occurred_at": STAMP,
    })
    with pytest.raises(ControlPlaneError, match="missing its public event"):
        machine.apply(state, transaction([], declare))
    payload = {
        "schema": "fcp.capability.v1", "session_id": SESSION,
        "capability_id": "demo", "node_id": "voter-a", "type": "demo.reading",
        "protocol": "fcp", "protocol_version": "1", "status": "ready",
        "properties": {"synthetic": True}, "announced_at": STAMP,
    }
    public = row(1, event_type="capability.registered", payload=payload)
    state, _ = machine.apply(state, transaction([public], declare))
    assert state["product_journal"]["sessions"][SESSION]["rows"] == [public]
    with pytest.raises(ControlPlaneError, match="owner authority"):
        machine.apply(state, transaction([row(2, event_type="capability.status.changed", actor="voter-b", payload=payload)]))


def test_automatic_leadership_appends_same_ordered_canonical_history() -> None:
    machine, state = ready()
    first = row(1)
    state, _ = machine.apply(state, transaction([first]))
    transition = command("LEADER_TRANSITION", {
        "session_id": SESSION, "previous_leader_node_id": "voter-a",
        "leader_node_id": "voter-b", "term": 2, "occurred_at": STAMP,
        "reason": "automatic-quorum-failover",
    }, actor="voter-b", identity="promotion-two")
    new, _ = machine.apply(state, transition)
    repeated, _ = machine.apply(copy.deepcopy(state), transition)
    assert new == repeated
    rows = new["product_journal"]["sessions"][SESSION]["rows"]
    assert rows[0] == first and rows[1]["revision"] == 2
    assert rows[1]["actor_node_id"] == REPLICATED_COORDINATOR_ID
    assert json.loads(rows[1]["payload_json"])["schema"] == LEADERSHIP_SCHEMA
    assert rows[1]["content_hash"] == public_row_content_hash(rows[1]["event_type"], rows[1]["payload_json"])
    assert new["leaders"][SESSION]["leader_node_id"] == "voter-b"
    assert len(new["session_events"][SESSION]) == 1


def test_member_named_leadership_event_does_not_gain_coordinator_authority() -> None:
    machine, state = ready()
    generic = row(1, event_type="session.leader.changed", actor="voter-b", payload={"message": "member annotation"})
    state, _ = machine.apply(state, transaction([generic]))
    assert state["leaders"][SESSION]["leader_node_id"] == "voter-a"
    assert state["product_journal"]["sessions"][SESSION]["rows"] == [generic]


def test_chunked_initialization_stays_inactive_until_exact_complete_prefix() -> None:
    machine, state = initial()
    created = row(1, event_type="session.created", payload={"session_id": SESSION, "display_name": "Public journal test"})
    joined = row(2, event_type="node.joined", payload={"node_id": "voter-a"})
    rows = [created, joined]
    state, _ = machine.apply(state, initialize(rows, chunk=[created], final=False))
    assert SESSION not in state["product_journal"]["sessions"]
    with pytest.raises(ControlPlaneError, match="initialization is required"):
        machine.apply(state, transaction([row(3)]))
    seal = command("CAPABILITY_DECLARE", {
        "session_id": SESSION, "capability_id": BOOTSTRAP_SEAL_CAPABILITY_ID,
        "capability_type": BOOTSTRAP_SEAL_CAPABILITY_TYPE,
        "owner_node_id": "voter-a", "occurred_at": STAMP,
    })
    with pytest.raises(ControlPlaneError, match="complete canonical journal"):
        machine.apply(state, seal)
    with pytest.raises(ControlPlaneError, match="complete prefix"):
        machine.apply(state, initialize(rows, chunk=[], final=True))
    state, _ = machine.apply(state, initialize(rows, chunk=[joined], final=True))
    assert state["product_journal"]["sessions"][SESSION]["rows"] == rows
    with pytest.raises(ControlPlaneError, match="cannot be reinitialized"):
        machine.apply(state, initialize(rows))


def test_initialize_refuses_prefix_digest_or_immutable_creator_mismatch() -> None:
    machine, state = initial()
    created = row(1, event_type="session.created", actor="voter-b", payload={"session_id": SESSION})
    with pytest.raises(ControlPlaneError, match="creator mismatch"):
        machine.apply(state, initialize([created]))
    bad = command(PRODUCT_JOURNAL_INITIALIZE, {
        "session_id": SESSION, "expected_revision": 0, "prefix_digest": "sha256:" + "0" * 64,
        "public_rows": [], "final": True,
    })
    with pytest.raises(ControlPlaneError, match="complete prefix"):
        machine.apply(state, bad)


def test_verified_import_preserves_old_coordinator_leader_hash_exactly() -> None:
    machine, state = initial()
    state, _ = machine.apply(state, command("LEADER_TRANSITION", {
        "session_id": SESSION, "previous_leader_node_id": "voter-a",
        "leader_node_id": "voter-b", "term": 2, "occurred_at": STAMP,
    }))
    created = row(1, event_type="session.created", payload={"session_id": SESSION})
    digest = hashlib.sha256(f"c03-leadership:{SESSION}:2:voter-b".encode()).hexdigest()
    leader = row(2, event_type="session.leader.changed", actor=REPLICATED_COORDINATOR_ID,
        identity="c03-" + digest[:32], payload={
            "schema": LEADERSHIP_SCHEMA, "session_id": SESSION,
            "previous_leader_node_id": "voter-a", "leader_node_id": "voter-b",
            "term": 2, "reason": "legacy-transition",
        })
    leader["content_hash"] = "sha256:" + hashlib.sha256(leader["payload_json"].encode()).hexdigest()
    leader["request_id"] = "sha256:" + digest
    state, _ = machine.apply(state, initialize([created, leader]))
    assert state["product_journal"]["sessions"][SESSION]["rows"][1] == leader
    with pytest.raises(ControlPlaneError, match="content hash"):
        transaction([leader])
    # The original coordinator identity is also a witnessed historical actor.
    assert LEGACY_COORDINATOR_ID != REPLICATED_COORDINATOR_ID


def test_snapshot_retains_exact_public_prefix_and_private_receipts_and_rejects_tamper() -> None:
    machine, state = ready()
    state, _ = machine.apply(state, transaction([row(1)], private=[private_row(b"opaque-snapshot-private-fixture" * 2)]))
    snapshot = Snapshot(CONFIG.cluster_id, CONFIG, 2, 1, state, _state_digest(state))
    restored = Snapshot.from_dict(snapshot.to_dict())
    assert restored.state == state
    changed = copy.deepcopy(state)
    changed["product_journal"]["sessions"][SESSION]["rows"] = []
    with pytest.raises(ControlPlaneError, match="prefix mismatch"):
        Snapshot(CONFIG.cluster_id, CONFIG, 2, 1, changed, _state_digest(changed))


def test_existing_envelope_byte_limit_is_not_expanded_for_public_journal() -> None:
    with pytest.raises(ControlPlaneError, match="size bound"):
        transaction([row(i, payload={"body": "x" * 47_900}) for i in range(1, 4)])


def test_only_applied_product_transaction_receives_durable_acceptance_proof() -> None:
    machine, state = ready()
    invalid = transaction([], member("SESSION_MEMBER_REMOVE", "voter-c"))
    rejected_state, _, rejection = machine.apply_committed(state, invalid)
    assert rejection is not None
    assert not transaction_accepted(rejected_state, invalid)
    accepted = transaction([row(1)])
    state, _ = machine.apply(state, accepted)
    assert transaction_accepted(state, accepted)
    restored = Snapshot.from_dict(Snapshot(CONFIG.cluster_id, CONFIG, 2, 1, state, _state_digest(state)).to_dict())
    assert transaction_accepted(restored.state, accepted)
    conflict = transaction([row(2)], identity=accepted.command_id)
    assert not transaction_accepted(restored.state, conflict)
    with pytest.raises(ControlPlaneError, match="command identity conflict"):
        machine.apply(restored.state, conflict)


def test_product_outcome_window_is_bounded_fifo_and_retains_newest_success() -> None:
    machine, state = ready()
    namespace = state["product_journal"]
    identities = ["past-transaction-" + str(index) for index in range(MAX_COMMAND_RECEIPTS)]
    namespace["accepted_transactions"] = {
        identity: "sha256:" + hashlib.sha256(identity.encode()).hexdigest() for identity in identities
    }
    namespace["accepted_transaction_order"] = list(identities)
    # Canonical snapshot reconstruction sorts mapping keys. FIFO must use the
    # retained order, not incidental insertion order of that rebuilt mapping.
    state = Snapshot.from_dict(Snapshot(CONFIG.cluster_id, CONFIG, 2, 1, state, _state_digest(state)).to_dict()).state
    latest = transaction([row(1)], identity="a-lexically-earlier-new-transaction")
    state, _ = machine.apply(state, latest)
    assert transaction_accepted(state, latest)
    assert len(state["product_journal"]["accepted_transactions"]) == MAX_COMMAND_RECEIPTS
    assert identities[0] not in state["product_journal"]["accepted_transactions"]
    assert state["product_journal"]["accepted_transaction_order"] == identities[1:] + [latest.command_id]


def test_prior_journal_snapshot_without_outcome_fields_remains_compatible() -> None:
    machine, state = ready()
    del state["product_journal"]["accepted_transactions"]
    del state["product_journal"]["accepted_transaction_order"]
    state = Snapshot.from_dict(Snapshot(CONFIG.cluster_id, CONFIG, 2, 1, state, _state_digest(state)).to_dict()).state
    accepted = transaction([row(1)])
    assert not transaction_accepted(state, accepted)
    state, _ = machine.apply(state, accepted)
    assert transaction_accepted(state, accepted)


@pytest.mark.parametrize("event_type", ["capability.registered", "capability.status.changed"])
def test_ordinary_capability_rows_cannot_omit_full_announcement_schema(event_type: str) -> None:
    # The omission of node_id formerly bypassed the full-payload branch. The
    # envelope constructor must reject it before any consensus proposal.
    incomplete = row(1, event_type=event_type, payload={
        "session_id": SESSION, "capability_id": "legacy-demo",
        "owner_node_id": "voter-a", "capability_type": "demo.reading",
    })
    with pytest.raises(ControlPlaneError, match="public capability payload is malformed"):
        transaction([incomplete])


def test_witnessed_provenance_is_immutable_across_chunks_and_snapshots() -> None:
    machine, state = initial()
    state, _ = machine.apply(state, command("CAPABILITY_DECLARE", {
        "session_id": SESSION, "capability_id": "legacy-demo", "capability_type": "demo.reading",
        "owner_node_id": "voter-a", "occurred_at": STAMP,
    }))
    created = row(1, event_type="session.created", payload={"session_id": SESSION})
    legacy = row(2, event_type="capability.registered", payload={
        "session_id": SESSION, "capability_id": "legacy-demo",
        "owner_node_id": "voter-a", "capability_type": "demo.reading",
    })
    rows = [created, legacy]
    # The runtime verifies witnesses. This unit checks preservation and bounds
    # of the verified identity supplied to the deterministic consensus core.
    provenance = {"kind": "witnessed", "history_digest": journal_history_digest(rows), "source_revision": 2}
    state, _ = machine.apply(state, initialize(rows, chunk=[created], final=False, provenance=provenance))
    assert state["product_journal"]["initializing"][SESSION]["provenance"] == provenance
    changed = {**provenance, "history_digest": "sha256:" + "8" * 64}
    with pytest.raises(ControlPlaneError, match="initialization identity conflict"):
        machine.apply(state, initialize(rows, chunk=[legacy], provenance=changed))
    state, _ = machine.apply(state, initialize(rows, chunk=[legacy], provenance=provenance))
    assert state["product_journal"]["sessions"][SESSION]["provenance"] == provenance
    restored = Snapshot.from_dict(Snapshot(CONFIG.cluster_id, CONFIG, 4, 1, state, _state_digest(state)).to_dict()).state
    assert restored["product_journal"]["sessions"][SESSION]["rows"] == rows
    assert restored["product_journal"]["sessions"][SESSION]["provenance"] == provenance
    restored["product_journal"]["sessions"][SESSION]["provenance"]["source_revision"] = 1
    with pytest.raises(ControlPlaneError, match="public capability payload is malformed"):
        Snapshot(CONFIG.cluster_id, CONFIG, 4, 1, restored, _state_digest(restored))


@pytest.mark.parametrize("provenance", [
    {"kind": "local", "history_digest": "sha256:" + "7" * 64, "source_revision": 1},
    {"kind": "witnessed", "history_digest": "unverified", "source_revision": 1},
    {"kind": "witnessed", "history_digest": "sha256:" + "7" * 64, "source_revision": 0},
    {"kind": "witnessed", "history_digest": "sha256:" + "7" * 64, "source_revision": True},
    {"kind": "witnessed", "history_digest": "sha256:" + "7" * 64, "source_revision": 2},
    {"kind": "witnessed", "history_digest": "sha256:" + "7" * 64, "source_revision": 1, "extra": "forbidden"},
])
def test_witnessed_provenance_is_closed_and_bounded_by_complete_prefix(provenance: dict[str, Any]) -> None:
    created = row(1, event_type="session.created", payload={"session_id": SESSION})
    with pytest.raises(ControlPlaneError):
        initialize([created], provenance=provenance)


def test_witnessed_marker_cannot_admit_new_minimal_capability_rows_after_its_prefix() -> None:
    created = row(1, event_type="session.created", payload={"session_id": SESSION})
    later = row(2, event_type="capability.status.changed", payload={"capability_id": "demo"})
    provenance = {"kind": "witnessed", "history_digest": "sha256:" + "7" * 64, "source_revision": 1}
    with pytest.raises(ControlPlaneError, match="public capability payload is malformed"):
        initialize([created, later], provenance=provenance)
    with pytest.raises(ControlPlaneError, match="public capability payload is malformed"):
        initialize([created, later])


def test_witnessed_digest_binds_exact_wire_fields_and_normalizes_sql_utc() -> None:
    machine, state = initial()
    original = row(1, event_type="session.created", payload={"session_id": SESSION})
    wire = {
        "schema": "fcp.session_event.v1", "session_id": SESSION, "revision": 1,
        "event_id": original["event_id"], "event_type": original["event_type"],
        "occurred_at": "2026-09-08T12:00:00Z", "actor_node_id": "voter-a",
        "payload": {"session_id": SESSION},
    }
    digest = "sha256:" + hashlib.sha256(json.dumps([wire], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert journal_history_digest([original]) == digest
    provenance = {"kind": "witnessed", "history_digest": digest, "source_revision": 1}
    modified = {**original, "event_id": "different-public-event-identity"}
    with pytest.raises(ControlPlaneError, match="differs from witnessed provenance"):
        machine.apply(state, initialize([modified], provenance=provenance))
    state, _ = machine.apply(state, initialize([original], provenance=provenance))
    tampered = copy.deepcopy(state)
    tampered["product_journal"]["sessions"][SESSION]["provenance"]["history_digest"] = "sha256:" + "9" * 64
    with pytest.raises(ControlPlaneError, match="differs from witnessed provenance"):
        Snapshot(CONFIG.cluster_id, CONFIG, 2, 1, tampered, _state_digest(tampered))
