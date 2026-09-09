"""Atomic local staging and durable retry for the replicated product journal."""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .control_plane_journal import PRODUCT_TRANSACTION
from .control_plane_journal_private import PrivateJournalRows
from .control_plane_journal_store import JournalCoordinatorStore
from .control_plane_product import _secret_file
from .control_plane_replication import (
    AuthorityCommand,
    ControlPlaneError,
    QuorumUnavailable,
)

ROW_COLUMNS = (
    "session_id", "revision", "event_id", "request_id", "event_type",
    "occurred_at", "actor_node_id", "payload_json", "content_hash",
)


def public_rows(database: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in database.execute(
        f"SELECT {','.join(ROW_COLUMNS)} FROM session_events ORDER BY session_id,revision"
    ).fetchall()]


def authority_rows(database: sqlite3.Connection) -> dict[tuple[str, ...], tuple[Any, ...]]:
    """Compare only durable semantic authority; liveness remains relay-local."""
    result = {}
    for row in database.execute("SELECT node_id,display_name,public_key,revoked_at FROM nodes"):
        result[("node", row[0])] = (row[1], row[2], row[3] is not None)
    for row in database.execute("SELECT session_id,display_name,created_by_node_id,state FROM sessions"):
        result[("session", row[0])] = (row[1], row[2], row[3])
    for row in database.execute("SELECT session_id,node_id,removed_at FROM session_memberships"):
        result[("member", row[0], row[1])] = (row[2] is None,)
    for row in database.execute("SELECT session_id,capability_id,node_id,type FROM capabilities"):
        result[("capability", row[0], row[1])] = (row[2], row[3])
    return result


def _verify_authority_changes(before, after, expected):
    for key in before.keys() | after.keys():
        if before.get(key) == after.get(key):
            continue
        kind, identity, *scope = key
        wanted = None
        if kind == "node":
            node = expected["nodes"].get(identity)
            if node is not None:
                wanted = (node["display_name"], node["public_key"], identity in expected["revocations"])
        elif kind == "session":
            session = expected["sessions"].get(identity)
            if session is not None:
                wanted = (session["display_name"], session["creator_node_id"], "active")
        elif kind == "member":
            active = expected["memberships"].get(identity, {}).get(scope[0])
            if active is not None:
                wanted = (active,)
        else:
            capability = expected["capabilities"].get(identity, {}).get(scope[0])
            if capability is not None:
                wanted = (capability["owner_node_id"], capability["capability_type"])
        if after.get(key) != wanted:
            raise ControlPlaneError("local authority mutation lacks matching replicated semantics")


class ProductJournalController:
    """A complete product operation commits to quorum before its SQLite view.

    The small durable outbox stores the exact encrypted command before proposal.
    An uncertain outcome is resolved before another writer can allocate a public
    revision. No retry regenerates event IDs, timestamps or encryption nonces.
    """

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self._context = threading.local()
        self.private = PrivateJournalRows(
            _secret_file(runtime.deployment.transport_secret_file),
            runtime.node.configuration.cluster_id,
        )
        self.pending_path = runtime.deployment.replica_database.with_name("product_journal_pending.sqlite3")
        database = self._outbox()
        try:
            with database:
                database.execute("CREATE TABLE IF NOT EXISTS pending(slot INTEGER PRIMARY KEY CHECK(slot=1), command_json TEXT NOT NULL, reserved_index INTEGER, proposing_term INTEGER)")
                columns = {row[1] for row in database.execute("PRAGMA table_info(pending)")}
                for column in ("reserved_index", "proposing_term"):
                    if column not in columns:
                        database.execute(f"ALTER TABLE pending ADD COLUMN {column} INTEGER")
        finally:
            database.close()

    def _outbox(self) -> sqlite3.Connection:
        database = sqlite3.connect(self.pending_path)
        database.execute("PRAGMA synchronous=FULL")
        return database

    @property
    def active(self) -> bool:
        return getattr(self._context, "commands", None) is not None

    def queue(self, command: AuthorityCommand) -> None:
        if not self.active:
            raise ControlPlaneError("product authority command has no owned operation")
        self._context.commands.append(command)

    def _pending(self) -> AuthorityCommand | None:
        database = self._outbox()
        try:
            row = database.execute("SELECT command_json FROM pending WHERE slot=1").fetchone()
            return None if row is None else AuthorityCommand.from_dict(json.loads(row[0]))
        finally:
            database.close()

    def _save(self, command: AuthorityCommand) -> None:
        database = self._outbox()
        try:
            with database:
                database.execute("INSERT INTO pending VALUES(1,?,?,?)", (
                    command.canonical_bytes().decode("utf-8"),
                    self.runtime.node.store.last_log_index() + 1,
                    self.runtime.node.store.current_term,
                ))
        finally:
            database.close()

    def _clear(self) -> None:
        database = self._outbox()
        try:
            with database:
                database.execute("DELETE FROM pending WHERE slot=1")
        finally:
            database.close()

    def reconcile(self) -> None:
        """Resolve a previous uncertain proposal, then restore committed rows."""
        command = self._pending()
        if command is None:
            return
        node = self.runtime.node
        receipt = node.store.receipt_for_command(command.command_id)
        if receipt is None:
            entry = node.store.entry_for_command(command.command_id)
            if entry is None:
                journals = node.state.get("product_journal", {}).get("sessions", {})
                for row in command.payload["public_rows"]:
                    committed = journals.get(row["session_id"], {}).get("rows", [])
                    if len(committed) >= row["revision"] and committed[row["revision"] - 1] != row:
                        # Exact canonical history is append-only. This different
                        # committed event proves the old proposal did not apply.
                        self.runtime.materialize()
                        self._clear()
                        raise ControlPlaneError("pending product operation was superseded by committed history")
                database = self._outbox()
                try:
                    reserved = database.execute("SELECT reserved_index,proposing_term FROM pending WHERE slot=1").fetchone()
                finally:
                    database.close()
                if reserved is None or reserved[0] is None:
                    raise ControlPlaneError("pending product operation has no durable append provenance")
                snapshot = node.store.snapshot()
                if snapshot is not None and snapshot.last_included_index >= reserved[0]:
                    raise ControlPlaneError("pending product outcome exceeds its retained proof window")
                if node.store.commit_index >= reserved[0]:
                    replacements = node.store.entries(after=reserved[0] - 1, limit=1)
                    if replacements and replacements[0].log_index == reserved[0] and replacements[0].log_term > reserved[1]:
                        self.runtime.materialize()
                        self._clear()
                        raise ControlPlaneError("pending product operation was replaced in a higher committed term")
            self.runtime.require_quorum_leader()
            if entry is not None and entry.log_term < node.store.current_term:
                # A real current-term commit is required to commit an inherited
                # prefix. This existing idempotent enrollment is that barrier.
                own = node.state["nodes"][node.voter_id]
                node.propose(AuthorityCommand(
                    command_id=f"journal-barrier-{node.store.current_term}-{command.command_id}",
                    command_type="NODE_ENROLL", cluster_id=node.configuration.cluster_id,
                    issued_by=node.voter_id, payload={
                        "node_id": node.voter_id, "display_name": own["display_name"],
                        "public_key": own["public_key"],
                    },
                ), self.runtime.transport)
            else:
                node.propose(command, self.runtime.transport)
            receipt = node.store.receipt_for_command(command.command_id)
        if receipt is None or receipt.content_hash != command.content_hash:
            raise QuorumUnavailable("product journal outcome is not durably resolved")
        if node.state.get("product_journal", {}).get("accepted_transactions", {}).get(command.command_id) != command.content_hash:
            raise ControlPlaneError("pending product command has no durable accepted outcome")
        self.runtime.materialize()
        self._clear()

    @contextmanager
    def operation(self) -> Iterator[sqlite3.Connection]:
        """Wrap the entire facade call, including errors after inner SQL exits."""
        with self.runtime.local.store.transaction() as database:
            yield database

    @contextmanager
    def local_connectivity_operation(self) -> Iterator[sqlite3.Connection]:
        """Permit only local liveness; leave any pending authority envelope intact."""
        with self.runtime._lifecycle_lock, self.runtime.local.store.local_connectivity_transaction() as database:
            yield database

    @contextmanager
    def transaction(self, store: JournalCoordinatorStore) -> Iterator[sqlite3.Connection]:
        runtime = self.runtime
        with runtime._lifecycle_lock:
            self.reconcile()
            runtime.materialize()
            self._context.commands = []
            submitted = False
            try:
                with store.raw_transaction() as database:
                    before_rows = public_rows(database)
                    before_private = self.private.capture(database)
                    before_authority = authority_rows(database)
                    yield database
                    # A caught nested write failure poisons the whole operation.
                    # Refuse before saving/proposing anything to quorum, not only
                    # when the surrounding SQLite transaction later unwinds.
                    store.assert_committable()
                    after_rows = public_rows(database)
                    old = {(row["session_id"], row["revision"]): row for row in before_rows}
                    after = {(row["session_id"], row["revision"]): row for row in after_rows}
                    if any(after.get(key) != row for key, row in old.items()):
                        raise ControlPlaneError("product operation rewrote committed public history")
                    added = [row for row in after_rows if (row["session_id"], row["revision"]) not in old]
                    journal = runtime.node.state.get("product_journal", {})
                    private = self.private.changes(before_private, self.private.capture(database), journal.get("private_rows", {}))
                    commands = self._context.commands
                    if added or private or commands:
                        if not runtime.ready:
                            raise ControlPlaneError("product authority has not completed its readiness seal")
                        runtime.require_quorum_leader()
                        envelope = AuthorityCommand(
                            command_id=f"product-transaction-{uuid.uuid4().hex}",
                            command_type=PRODUCT_TRANSACTION,
                            cluster_id=runtime.node.configuration.cluster_id,
                            issued_by=runtime.node.voter_id,
                            payload={"authority_commands": [command.to_dict() for command in commands],
                                     "public_rows": added, "private_rows": private},
                        )
                        # Validate before persisting an outbox that cannot commit.
                        expected, _ = runtime.node.state_machine.apply(runtime.node.pending_state(), envelope)
                        _verify_authority_changes(before_authority, authority_rows(database), expected)
                        self._save(envelope)
                        submitted = True
                        runtime.node.propose(envelope, runtime.transport)
                        if runtime.node.state.get("product_journal", {}).get("accepted_transactions", {}).get(envelope.command_id) != envelope.content_hash:
                            raise ControlPlaneError("committed product command lacks an accepted outcome")
                    else:
                        _verify_authority_changes(before_authority, authority_rows(database), runtime.node.state)
                # SQL has committed. The authoritative projection also restores
                # any command effects and ensures the envelope is fully applied.
                if submitted:
                    runtime.materialize()
                    self._clear()
            finally:
                self._context.commands = None


__all__ = ["ROW_COLUMNS", "ProductJournalController", "public_rows"]
