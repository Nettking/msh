"""Pending-outcome recovery over actual persistent three-voter consensus.

The materializer is an explicitly observed unit seam here. Socket-level tests
exercise the complete release runtime and real SQL projection separately. These
tests inject failures at the controller boundary without changing consensus or
claiming that a fake materializer is production runtime.
"""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.control_plane_journal import (
    PRODUCT_JOURNAL_INITIALIZE,
    PRODUCT_TRANSACTION,
    journal_prefix_digest,
    public_row_content_hash,
    transaction_accepted,
)
from catalog.federation.control_plane_journal_runtime import ProductJournalController
from catalog.federation.control_plane_product import ReplicatedFederationRuntime
from catalog.federation.control_plane_replication import (
    MAX_COMMAND_RECEIPTS,
    AuthorityCommand,
    ControlPlaneError,
    LogEntry,
    PersistentReplicaStore,
    QuorumUnavailable,
    ReplicaNode,
    Snapshot,
    _state_digest,
)
from catalog.federation.errors import AuthorizationError
from catalog.federation.tests.test_control_plane_replication import (
    CONFIGURATION,
    _command,
    _elected_cluster,
)

SESSION = "session-stable"
STAMP = "2026-09-08T12:00:00+00:00"


def _public_command(identity: str, *, actor: str = "voter-a") -> AuthorityCommand:
    payload_json = json.dumps({"observation": identity}, sort_keys=True, separators=(",", ":"))
    row = {
        "session_id": SESSION, "revision": 1, "event_id": "event-" + identity,
        "request_id": "sha256:" + hashlib.sha256(identity.encode()).hexdigest(),
        "event_type": "demo.observation", "occurred_at": STAMP,
        "actor_node_id": actor, "payload_json": payload_json,
        "content_hash": public_row_content_hash("demo.observation", payload_json),
    }
    return _command(identity, PRODUCT_TRANSACTION, {
        "authority_commands": [], "public_rows": [row], "private_rows": [],
    }, issued_by=actor)


class _PendingFixture:
    """Only the unrelated materialization boundary is a test observer."""

    def __init__(self, path: Path) -> None:
        self.nodes, self.transport = _elected_cluster(path)
        self.secret = path / "disposable-product-journal.secret"
        self.secret.write_bytes(bytes(range(32)))
        self.nodes["voter-a"].propose(_command("initialize", PRODUCT_JOURNAL_INITIALIZE, {
            "session_id": SESSION, "expected_revision": 0,
            "prefix_digest": journal_prefix_digest([]), "public_rows": [], "final": True,
        }), self.transport)
        self.materialized: list[int] = []
        self.fail_materialization = False
        self.reopen_controller()

    def reopen_controller(self) -> None:
        runtime = SimpleNamespace(
            node=self.nodes["voter-a"], transport=self.transport,
            deployment=SimpleNamespace(
                transport_secret_file=self.secret,
                replica_database=Path(self.nodes["voter-a"].store.database),
            ),
            _lifecycle_lock=threading.RLock(), materialize=self._materialize,
        )
        runtime.require_quorum_leader = lambda: ReplicatedFederationRuntime.require_quorum_leader(runtime)
        self.runtime = runtime
        self.controller = ProductJournalController(runtime)

    def restart_node_and_controller(self) -> None:
        original = self.nodes["voter-a"]
        self.nodes["voter-a"] = ReplicaNode(
            "voter-a", PersistentReplicaStore(original.store.database, CONFIGURATION),
        )
        self.transport.replicas["voter-a"] = self.nodes["voter-a"]
        self.reopen_controller()

    def _materialize(self) -> None:
        if self.fail_materialization:
            raise sqlite3.OperationalError("injected projection commit failure")
        self.materialized.append(self.nodes["voter-a"].store.last_applied)

    def save(self, command: AuthorityCommand) -> None:
        # Exactly the durable outbox boundary used before the real node proposal.
        self.controller._save(command)
        assert self.controller._pending().canonical_bytes() == command.canonical_bytes()


@pytest.mark.parametrize("restart", [False, True])
def test_uncertain_proposal_reuses_exact_command_after_quorum_returns(tmp_path: Path, restart: bool) -> None:
    fixture = _PendingFixture(tmp_path)
    operation = _public_command("uncertain-original")
    fixture.save(operation)
    fixture.transport.partition("voter-a")
    with pytest.raises(QuorumUnavailable):
        fixture.nodes["voter-a"].propose(operation, fixture.transport)
    pending = fixture.nodes["voter-a"].store.entry_for_command(operation.command_id)
    assert pending is not None
    assert not transaction_accepted(fixture.nodes["voter-a"].state, operation)
    exact = fixture.controller._pending().canonical_bytes()
    fixture.transport.blocked.clear()
    if restart:
        fixture.restart_node_and_controller()
        assert fixture.nodes["voter-a"].start_election(fixture.transport)
        assert fixture.nodes["voter-a"].store.current_term > pending.log_term
    fixture.controller.reconcile()
    assert fixture.controller._pending() is None
    committed = fixture.nodes["voter-a"].store.entry_for_command(operation.command_id)
    assert committed is not None and committed.command.canonical_bytes() == exact
    assert transaction_accepted(fixture.nodes["voter-a"].state, operation)
    assert len(fixture.nodes["voter-a"].state["product_journal"]["sessions"][SESSION]["rows"]) == 1
    assert fixture.materialized


def test_post_quorum_projection_failure_keeps_exact_pending_until_restart_repair(tmp_path: Path) -> None:
    fixture = _PendingFixture(tmp_path)
    operation = _public_command("accepted-before-local-failure")
    fixture.save(operation)
    fixture.nodes["voter-a"].propose(operation, fixture.transport)
    assert transaction_accepted(fixture.nodes["voter-a"].state, operation)
    fixture.fail_materialization = True
    with pytest.raises(sqlite3.OperationalError, match="projection commit failure"):
        fixture.controller.reconcile()
    assert fixture.controller._pending().canonical_bytes() == operation.canonical_bytes()
    assert fixture.materialized == []
    fixture.restart_node_and_controller()
    fixture.fail_materialization = False
    fixture.controller.reconcile()
    assert fixture.controller._pending() is None
    assert len(fixture.materialized) == 1
    assert transaction_accepted(fixture.nodes["voter-a"].state, operation)


@pytest.mark.parametrize("restart", [False, True])
def test_committed_rejection_receipt_never_becomes_accepted_ack(tmp_path: Path, restart: bool) -> None:
    fixture = _PendingFixture(tmp_path)
    removal = _command("invalid-remove", "SESSION_MEMBER_REMOVE", {
        "session_id": SESSION, "node_id": "voter-c", "occurred_at": STAMP,
    })
    invalid = _command("rejected-product-operation", PRODUCT_TRANSACTION, {
        "authority_commands": [removal.to_dict()], "public_rows": [],
    })
    fixture.save(invalid)
    node = fixture.nodes["voter-a"]
    index = node.store.last_log_index() + 1
    term = node.store.current_term + 1
    # The existing total replay contract permits a committed malformed domain
    # operation to become a rejected no-op. Its ordinary receipt still exists.
    response = node.receive_append_entries(
        leader_id="voter-b", leader_term=term, prev_log_index=index - 1,
        prev_log_term=node.store.term_at(index - 1),
        entries=(LogEntry(index, term, invalid),), leader_commit=index,
        cluster_id=CONFIGURATION.cluster_id,
    )
    assert response.success
    assert node.store.receipt_for_command(invalid.command_id) is not None
    assert not transaction_accepted(node.state, invalid)
    if restart:
        fixture.restart_node_and_controller()
        assert fixture.nodes["voter-a"].rejected_commands == {}
    with pytest.raises(ControlPlaneError):
        fixture.controller.reconcile()
    assert fixture.controller._pending().canonical_bytes() == invalid.canonical_bytes()
    assert fixture.materialized == []
    assert fixture.nodes["voter-a"].state["memberships"][SESSION]["voter-c"] is True


def test_authoritatively_overwritten_pending_public_prefix_can_be_retired(tmp_path: Path) -> None:
    fixture = _PendingFixture(tmp_path)
    pending = _public_command("overwritten-old-leader-operation")
    fixture.save(pending)
    fixture.transport.partition("voter-a")
    with pytest.raises(QuorumUnavailable):
        fixture.nodes["voter-a"].propose(pending, fixture.transport)
    original_entry = fixture.nodes["voter-a"].store.entry_for_command(pending.command_id)
    assert original_entry is not None
    successor = fixture.nodes["voter-b"]
    assert successor.start_election(fixture.transport)
    replacement = _public_command("committed-successor-operation", actor="voter-b")
    successor.propose(replacement, fixture.transport)
    fixture.transport.blocked.clear()
    successor.synchronize(fixture.transport)
    old = fixture.nodes["voter-a"]
    assert old.store.commit_index >= original_entry.log_index
    assert old.store.entry_for_command(pending.command_id) is None
    assert transaction_accepted(old.state, replacement)
    assert not transaction_accepted(old.state, pending)
    # Reporting one explicit aborted outcome is acceptable. Retaining a proven
    # overwritten command forever is not: the next valid writer must be able
    # to stage against the surviving authoritative prefix without reusing it.
    try:
        fixture.controller.reconcile()
    except ControlPlaneError:
        pass
    assert fixture.controller._pending() is None
    assert not transaction_accepted(old.state, pending)
    assert old.state["product_journal"]["sessions"][SESSION]["rows"] == replacement.payload["public_rows"]


def test_expired_private_only_outcome_proof_fails_closed_without_reproposal(tmp_path: Path) -> None:
    fixture = _PendingFixture(tmp_path)
    value = {
        "schema": "fcp.control-plane.private-receipt-row.v1", "table": "enrollment_tokens",
        "key": {"token_hash": "a" * 64},
        "row": {"token_hash": "a" * 64, "token_id": "disposable-token-id",
                "created_at": STAMP, "expires_at": "2026-09-08T12:10:00+00:00",
                "max_uses": 1, "use_count": 0, "created_by": "voter-a", "revoked_at": None},
    }
    private = fixture.controller.private.seal(value, previous_digest=None)
    operation = _command("old-private-only-outcome", PRODUCT_TRANSACTION, {
        "authority_commands": [], "public_rows": [], "private_rows": [private],
    })
    fixture.save(operation)
    node = fixture.nodes["voter-a"]
    node.propose(operation, fixture.transport)
    # Model a valid distant snapshot after the bounded proof window expired.
    # No public row can prove whether this private-only operation was applied.
    state = copy.deepcopy(node.state)
    state["product_journal"]["accepted_transactions"] = {}
    state["product_journal"]["accepted_transaction_order"] = []
    snapshot = Snapshot(
        CONFIGURATION.cluster_id, CONFIGURATION,
        node.store.commit_index + MAX_COMMAND_RECEIPTS + 1,
        node.store.current_term + 1, state, _state_digest(state), (),
    )
    response = node.receive_install_snapshot(
        leader_id="voter-b", leader_term=snapshot.last_included_term,
        snapshot=snapshot, cluster_id=CONFIGURATION.cluster_id,
    )
    assert response.success
    original_index = node.store.last_log_index()
    with pytest.raises((ControlPlaneError, AuthorizationError)):
        fixture.controller.reconcile()
    assert fixture.controller._pending().canonical_bytes() == operation.canonical_bytes()
    assert node.store.last_log_index() == original_index
    assert not transaction_accepted(node.state, operation)
