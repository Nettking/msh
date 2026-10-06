"""Real authenticated voters resume a partly committed witnessed public journal."""

from __future__ import annotations

import hashlib
import json
import socket
import threading
import time
from collections import deque
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.control_plane_bootstrap_proposal import (
    propose_bootstrap_command,
)
from catalog.federation.control_plane_journal import (
    PRODUCT_JOURNAL_INITIALIZE,
    journal_history_digest,
    journal_prefix_digest,
    public_row_content_hash,
)
from catalog.federation.control_plane_legacy_migration import _read_event_journal
from catalog.federation.control_plane_readiness import BOOTSTRAP_SEAL_CAPABILITY_ID
from catalog.federation.control_plane_replication import (
    AuthorityCommand,
    ControlPlaneError,
    QuorumUnavailable,
    ReplicaNode,
)
from catalog.federation.tests.test_c03_offline_creator_migration import (
    CREATOR,
    FEDERATION,
    SESSION,
    _event,
    _legacy_events,
    _runtime,
    _topology,
    _write_member_witness,
)
from catalog.federation.tests.test_control_plane_secure_transport import (
    _close as _close_secure,
)
from catalog.federation.tests.test_control_plane_secure_transport import (
    _cluster as _secure_cluster,
)
from catalog.federation.tests.test_control_plane_secure_transport import (
    _genesis as _secure_genesis,
)


class _WitnessedRecoveryDiagnostics:
    """Bounded public-only observations; never alter a consensus outcome."""

    def __init__(self, runtimes, monkeypatch):
        self.runtimes = []
        self.monkeypatch = monkeypatch
        self.runtime_tickets = {}
        self.records = deque(maxlen=128)
        self.lock = threading.Lock()
        self.dropped = 0
        self.observed_store_results = {}
        for runtime in runtimes:
            self.observe_runtime(runtime)

    def observe_runtime(self, runtime):
        ticket = len(self.runtimes)
        self.runtime_tickets[id(runtime.node)] = ticket
        self.runtimes.append(runtime)
        self._wrap(self.monkeypatch, runtime.transport, "_rpc", "client-rpc",
                       lambda args, _kwargs, runtime=runtime: {
                           "voter": runtime.node.voter_id, "runtime_instance": ticket,
                           "target": args[0], "rpc": args[1],
                       }, self._response)
        self._wrap(self.monkeypatch, runtime.server, "_dispatch", "server-dispatch",
                       lambda args, _kwargs, runtime=runtime: {
                           "voter": runtime.node.voter_id, "runtime_instance": ticket,
                           "sender": args[0].sender_id,
                           "rpc": args[0].rpc,
                       }, self._response)
        self._wrap(self.monkeypatch, runtime.node.store, "append_entries", "follower-store",
                       lambda _args, _kwargs, runtime=runtime: {
                           "voter": runtime.node.voter_id, "runtime_instance": ticket},
                       lambda result, before: self._remember_store(before["runtime_instance"], "append_match_index", result))
        for attribute in ("set_term_and_vote", "set_commit_index", "append_local"):
            self._wrap(self.monkeypatch, runtime.node.store, attribute, "business-store-write",
                           lambda args, _kwargs, runtime=runtime, attribute=attribute: {
                               "voter": runtime.node.voter_id, "runtime_instance": ticket,
                               "operation": attribute,
                               "value": args[0].log_index if attribute == "append_local" else args[0],
                           }, lambda _result, before: self._remember_store(
                               before["runtime_instance"], before["operation"], before["value"]))
        self._wrap(self.monkeypatch, runtime.node, "propose", "proposal",
                       lambda args, _kwargs, runtime=runtime: {
                           "voter": runtime.node.voter_id, "runtime_instance": ticket,
                           "command_type": args[0].command_type,
                           "command_id_sha256": hashlib.sha256(args[0].command_id.encode()).hexdigest(),
                           "command_hash": args[0].content_hash,
                           "before": self._node(runtime.node),
                       }, lambda _result, _before, runtime=runtime: {"after": self._node(runtime.node)})

    @staticmethod
    def _response(result, _before=None):
        return {key: result[key] for key in ("term", "success", "match_index", "conflict_index", "granted")
                if isinstance(result, dict) and key in result and type(result[key]) in (int, bool)}

    @staticmethod
    def _error(error):
        return {"type": type(error).__name__, "reason_sha256": hashlib.sha256(str(error).encode()).hexdigest(),
                **{key: getattr(error, key) for key in ("errno", "winerror", "sqlite_errorcode")
                   if type(getattr(error, key, None)) is int}}

    def _remember_store(self, ticket, operation, value):
        if type(value) is int:
            self.observed_store_results.setdefault(ticket, {})[operation] = value
        return {"last_business_store_results": dict(self.observed_store_results.get(ticket, {}))}

    def _node(self, node):
        return {"voter": node.voter_id, "role": node.role, "leader": node.leader_id,
                "runtime_instance": self.runtime_tickets[id(node)],
                "match_index": dict(node._match_index),
                "last_business_store_results": dict(self.observed_store_results.get(self.runtime_tickets[id(node)], {})),
                "snapshot_kind": "cached-memory-and-last-business-store-results", "atomic_snapshot": False}

    def _safe(self, callback, *args):
        try:
            return callback(*args)
        except Exception as error:  # noqa: BLE001 - failed diagnostics must preserve the business exception.
            return {"diagnostic_unavailable": True, "diagnostic_error_type": type(error).__name__}

    def _record(self, value):
        if not self.lock.acquire(blocking=False):
            self.dropped += 1
            return
        try:
            if len(self.records) == self.records.maxlen:
                self.dropped += 1
            self.records.append(value)
        finally:
            self.lock.release()

    def _wrap(self, monkeypatch, owner, attribute, stage, fields, response):
        original = getattr(owner, attribute)

        def observe(*args, **kwargs):
            started = time.monotonic()
            before = self._safe(fields, args, kwargs)
            try:
                result = original(*args, **kwargs)
            except BaseException as error:
                self._record({"stage": stage, **before, "outcome": "ERROR",
                              "duration_seconds": time.monotonic() - started,
                              "error": self._safe(self._error, error)})
                raise
            self._record({"stage": stage, **before, "outcome": "RETURNED",
                          "duration_seconds": time.monotonic() - started,
                          **self._safe(response, result, before)})
            return result

        monkeypatch.setattr(owner, attribute, observe)

    def attach(self, error):
        if self.lock.acquire(blocking=False):
            try:
                records = list(self.records)
            finally:
                self.lock.release()
        else:
            records = []
            self.dropped += 1
        error.add_note("witnessed-recovery-diagnostics:" + json.dumps({
            "schema": "fcp.test.witnessed-recovery-diagnostics.v1",
            "records": records, "dropped_records": self.dropped,
            "voters": [self._safe(self._node, runtime.node) for runtime in self.runtimes],
        }, sort_keys=True, separators=(",", ":")))


def _wire_events(runtime):
    return tuple(event.to_dict() for event in runtime.local.store.replay_events(
        session_id=SESSION, last_applied_revision=0,
    ))


def _catch_up_returning(successor, returning, diagnostics, *, timeout_seconds=120.0):
    """Observe this voter after normal rounds; quorum alone is insufficient."""
    assert 0 < timeout_seconds <= 120.0
    started = time.monotonic()
    deadline = started + timeout_seconds
    expected_commit = successor.node.store.commit_index
    last = {}
    while time.monotonic() < deadline:
        matched = successor.node.synchronize(successor.transport)
        assert matched + 1 >= successor.node.quorum
        if time.monotonic() >= deadline:
            break
        returning.materialize()
        # These reads implement the catch-up predicate, not diagnostic SQL.
        commit = returning.node.store.commit_index
        applied = returning.node.store.last_applied
        ready = returning.ready
        acknowledged = successor.node._match_index.get(returning.node.voter_id, 0)
        last = {"stage": "returning-catchup", "voter": returning.node.voter_id,
                "expected_commit": expected_commit, "commit_index": commit, "last_applied": applied,
                "acknowledged_match": acknowledged, "ready": ready, "quorum_matched": matched,
                "elapsed_seconds": time.monotonic() - started, "observation_budget_seconds": timeout_seconds}
        diagnostics._record(last)
        if (time.monotonic() < deadline and ready and commit >= expected_commit
                and applied >= expected_commit and acknowledged >= expected_commit):
            return last
        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
    error = AssertionError("returning voter did not acknowledge and apply the sealed witnessed prefix within observation budget")
    error.add_note("returning-catchup-observation:" + json.dumps({
        "expected_commit": expected_commit, "elapsed_seconds": time.monotonic() - started,
        "observation_budget_seconds": timeout_seconds, "last_completed_predicate": last,
    }, sort_keys=True, separators=(",", ":")))
    raise error


def test_different_voter_completes_exact_witnessed_prefix_after_first_real_chunk(
    tmp_path: Path, monkeypatch,
) -> None:
    deployments = _topology(tmp_path)
    voter_ids = tuple(deployment.local_voter_id for deployment in deployments)
    base = _legacy_events(voter_ids)
    # Cross the production 64 KiB chunk limit with a few ordinary public rows;
    # retain the normal payload, command, manifest, and state size limits.
    events = base + tuple(_event(
        revision, "demo.bootstrap.note", voter_ids[0],
        {"sequence": revision, "text": "Synthetic public bootstrap history. " * 350},
    ) for revision in range(len(base) + 1, len(base) + 9))
    witnesses = [_write_member_witness(
        tmp_path, voter_id=voter_id, events=events,
    ) for voter_id in voter_ids]
    runtimes = [_runtime(deployment, *witness) for deployment, witness in zip(
        deployments, witnesses, strict=True,
    )]
    original = runtimes[0]
    original_propose = original._propose_bootstrap_command
    interrupted = {}
    started = []

    def interrupt_after_committed_chunk(command):
        result = original_propose(command)
        if command.command_type == PRODUCT_JOURNAL_INITIALIZE and not command.payload["final"]:
            interrupted["command"] = command
            interrupted["state"] = original.node.state
            original._stop.set()
            raise RuntimeError("interrupted after committed non-final journal chunk")
        return result

    monkeypatch.setattr(original, "_propose_bootstrap_command", interrupt_after_committed_chunk)
    diagnostics = _WitnessedRecoveryDiagnostics(runtimes, monkeypatch)
    try:
        for runtime in runtimes:
            runtime.start()
            started.append(runtime)
        with pytest.raises(RuntimeError, match="committed non-final journal chunk"):
            original._attempt_existing_federation_bootstrap()
        interrupted_state = interrupted["state"]
        staged = interrupted_state["product_journal"]["initializing"][SESSION]
        assert 0 < len(staged["rows"]) < staged["expected_revision"]
        assert SESSION not in interrupted_state["product_journal"]["sessions"]
        assert BOOTSTRAP_SEAL_CAPABILITY_ID not in interrupted_state["capabilities"][SESSION]
        assert not original.ready
        command = interrupted["command"]
        receipt = original.node.store.receipt_for_command(command.command_id)
        assert receipt.content_hash == command.content_hash
        initial_leadership = interrupted_state["leaders"][SESSION]
        assert initial_leadership["leader_node_id"] == voter_ids[0]
        original.close()
        started.remove(original)

        successor, follower = runtimes[1:]
        assert successor.node.state["product_journal"]["initializing"][SESSION] == staged
        # Use the real witnessed recovery entrypoint. Existing migration fixture
        # timers keep the injected process boundary deterministic; all elections,
        # witness attestations, replication and acknowledgements use real sockets.
        successor._attempt_existing_federation_bootstrap()
        successor.materialize()
        follower.materialize()
        assert successor.node.role == ReplicaNode.LEADER
        assert successor.ready and follower.ready
        state = successor.node.state
        journal = state["product_journal"]["sessions"][SESSION]
        rows = journal["rows"]
        assert SESSION not in state["product_journal"]["initializing"]
        assert rows[:len(staged["rows"])] == staged["rows"]
        assert journal_prefix_digest(rows[:staged["expected_revision"]]) == staged["prefix_digest"]
        assert journal["provenance"] == {
            "kind": "witnessed", "source_revision": len(events),
            "history_digest": journal_history_digest(rows[:len(events)]),
        }
        assert [row["revision"] for row in rows] == list(range(1, len(rows) + 1))
        assert len({row["event_id"] for row in rows}) == len(rows)
        leadership = successor.local.session_leadership(session_id=SESSION)
        assert leadership.creator_node_id == CREATOR
        assert leadership.leader_node_id == successor.node.voter_id
        assert leadership.term > initial_leadership["term"]
        assert _wire_events(successor)[:len(events)] == tuple(event.to_dict() for event in events)
        assert _wire_events(follower) == _wire_events(successor)
        assert state["federation_id"] == FEDERATION

        returning = _runtime(deployments[0], *witnesses[0])
        diagnostics.observe_runtime(returning)
        returning.start()
        started.append(returning)
        assert successor.node.synchronize(successor.transport) + 1 >= successor.node.quorum
        _catch_up_returning(successor, returning, diagnostics)
        returning.materialize()
        assert returning.ready
        assert returning.node.role == ReplicaNode.FOLLOWER
        assert _wire_events(returning) == _wire_events(successor)
        for runtime in (successor, follower, returning):
            assert runtime.node.state["leaders"][SESSION]["creator_node_id"] == CREATOR
            assert runtime.node.store.receipt_for_command(command.command_id).content_hash == command.content_hash
        for node_db, _pairing in witnesses:
            assert _read_event_journal(node_db, SESSION) == events
    except BaseException as error:
        diagnostics.attach(error)
        raise
    finally:
        for runtime in reversed(started):
            runtime.close()


@pytest.mark.parametrize("fault", ["response-lost", "authenticated-rejection"])
def test_real_socket_new_chunk_failure_preserves_exact_pending_recovery(tmp_path, monkeypatch, request, fault):
    configuration, registry, _credentials, nodes, _codecs, servers, transports = _secure_cluster(tmp_path)
    original_id, successor_id, follower_id = configuration.voter_ids
    original, successor, follower = (nodes[voter] for voter in configuration.voter_ids)
    session = "session-secure"
    rows = []
    for revision in (1, 2):
        event_type = "session.created" if revision == 1 else "demo.bootstrap.note"
        payload = json.dumps({"session_id": session, "display_name": "Secure Federation"}
                             if revision == 1 else {"sequence": revision},
                             sort_keys=True, separators=(",", ":"))
        rows.append({"session_id": session, "revision": revision, "event_id": f"socket-event-{revision}",
                     "event_type": event_type, "occurred_at": "2026-09-08T09:00:00Z",
                     "actor_node_id": original_id, "payload_json": payload,
                     "request_id": "sha256:" + hashlib.sha256(str(revision).encode()).hexdigest(),
                     "content_hash": public_row_content_hash(event_type, payload)})
    payload = {"session_id": session, "expected_revision": 2, "prefix_digest": journal_prefix_digest(rows)}
    try:
        assert original.start_election(transports[original_id])
        propose_bootstrap_command(original, transports[original_id], _secure_genesis(configuration, registry, original_id))
        first = AuthorityCommand("socket-journal-first", PRODUCT_JOURNAL_INITIALIZE, configuration.cluster_id,
                                 original_id, {**payload, "public_rows": rows[:1], "final": False})
        propose_bootstrap_command(original, transports[original_id], first)
        staged = successor.state["product_journal"]["initializing"][session]
        assert staged["rows"] == rows[:1]
        servers[original_id].close()
        assert successor.start_election(transports[successor_id])
        term = successor.store.current_term
        commit = successor.store.commit_index
        assert term > original.store.current_term
        command = AuthorityCommand("socket-journal-final", PRODUCT_JOURNAL_INITIALIZE, configuration.cluster_id,
                                   successor_id, {**payload, "public_rows": rows[1:], "final": True})
        handler_socket = threading.local()
        handler = servers[follower_id]._server.RequestHandlerClass
        original_handle = handler.handle
        dispatch = servers[follower_id]._dispatch
        faults = []

        def capture_socket(self):
            handler_socket.connection = self.request
            try:
                original_handle(self)
            finally:
                del handler_socket.connection

        def fail_one_new_chunk(opened):
            entries = opened.payload.get("entries", ())
            selected = any(entry["command"]["command_id"] == command.command_id for entry in entries)
            if opened.rpc != "append_entries" or not selected or faults:
                return dispatch(opened)
            faults.append(opened.sender_id)
            assert opened.sender_id == successor_id  # Already authenticated by the original handler.
            if fault == "authenticated-rejection":
                raise ControlPlaneError("synthetic authenticated new-chunk rejection")
            response = dispatch(opened)  # Durable append happens before the real socket loses its response.
            assert response["success"] is True
            handler_socket.connection.shutdown(socket.SHUT_RDWR)
            return response

        monkeypatch.setattr(handler, "handle", capture_socket)
        monkeypatch.setattr(servers[follower_id], "_dispatch", fail_one_new_chunk)
        diagnostics = _WitnessedRecoveryDiagnostics([
            SimpleNamespace(node=nodes[voter], server=servers[voter], transport=transports[voter])
            for voter in configuration.voter_ids
        ], monkeypatch)
        with pytest.raises(QuorumUnavailable, match="authority command was not committed by quorum") as failed:
            propose_bootstrap_command(successor, transports[successor_id], command)
        diagnostics.attach(failed.value)
        observation = json.loads(failed.value.__notes__[-1].split(":", 1)[1])
        follower_errors = [record for record in observation["records"]
                           if record["stage"] == "client-rpc" and record["target"] == follower_id
                           and record["outcome"] == "ERROR"]
        assert len(follower_errors) == 1
        assert follower_errors[0]["error"]["type"] == (
            "OSError" if fault == "response-lost" else "ControlPlaneError"
        )
        server_records = [record for record in observation["records"]
                          if record["stage"] == "server-dispatch" and record["voter"] == follower_id]
        assert server_records[-1]["outcome"] == ("RETURNED" if fault == "response-lost" else "ERROR")
        assert faults == [successor_id]
        pending = successor.store.entry_for_command(command.command_id)
        assert pending is not None and pending.command == command
        assert pending.log_term == term and pending.log_index == commit + 1
        assert successor.store.current_term == term and successor.role == ReplicaNode.LEADER
        assert successor.store.commit_index == commit
        assert successor.state["product_journal"]["initializing"][session] == staged
        assert session not in successor.state["product_journal"]["sessions"]
        for node in (successor, follower):
            assert node.store.receipt_for_command(command.command_id) is None
        follower_pending = follower.store.entry_for_command(command.command_id)
        assert (follower_pending == pending) if fault == "response-lost" else (follower_pending is None)

        entry, _events = propose_bootstrap_command(successor, transports[successor_id], command)
        assert entry == pending
        assert successor.store.last_log_index() == pending.log_index
        assert successor.store.current_term == term
        assert faults == [successor_id]
        for node in (successor, follower):
            assert node.store.receipt_for_command(command.command_id).content_hash == command.content_hash
            assert node.store.entry_for_command(command.command_id) == pending
            assert node.store.commit_index == pending.log_index
            assert node.state["product_journal"]["sessions"][session]["rows"] == rows
            assert session not in node.state["product_journal"]["initializing"]
        request.node.user_properties.append(("controlled_socket_boundary", json.dumps({
            "fault": fault, "old_CI_cause_claimed": False,
            "command_id": command.command_id, "command_hash": command.content_hash,
            "pending_term": term, "pending_index": pending.log_index,
            "first_call_commit_index": commit,
            "follower_stored_before_replay": follower_pending is not None,
            "replay_commit_index": successor.store.commit_index,
            "first_call_diagnostics": observation,
        }, sort_keys=True)))
    finally:
        _close_secure(servers)


def test_witnessed_diagnostics_are_bounded_and_redact_private_errors(monkeypatch):
    diagnostics = _WitnessedRecoveryDiagnostics([], monkeypatch)
    secret = "private-token-and-payload"
    failure = OSError(10061, secret)

    def rejected():
        raise failure

    owner = SimpleNamespace(call=rejected)
    diagnostics._wrap(monkeypatch, owner, "call", "client-rpc", lambda _args, _kwargs: {"rpc": "append_entries"},
                      diagnostics._response)
    with pytest.raises(OSError) as actual:
        owner.call()
    assert actual.value is failure
    diagnostics.attach(failure)
    note = failure.__notes__[-1]
    assert secret not in note
    observation = json.loads(note.split(":", 1)[1])
    assert observation["records"][0]["error"]["errno"] == 10061
    assert observation["records"][0]["error"]["reason_sha256"] == hashlib.sha256(str(failure).encode()).hexdigest()
    for index in range(300):
        diagnostics._record({"stage": "bounded", "index": index})
    diagnostics.lock.acquire()
    try:
        diagnostics._record({"stage": "lost"})
    finally:
        diagnostics.lock.release()
    error = RuntimeError("original failure")
    diagnostics.attach(error)
    value = json.loads(error.__notes__[-1].split(":", 1)[1])
    assert len(value["records"]) == 128 and value["dropped_records"] == 174
    assert value["records"][-1]["index"] == 299
    assert diagnostics._response({"success": True, "term": 2, "payload": secret, "match_index": "secret"}) == {
        "success": True, "term": 2,
    }


def test_witnessed_diagnostic_read_failure_cannot_replace_original_exception(monkeypatch):
    diagnostics = _WitnessedRecoveryDiagnostics([], monkeypatch)
    original = RuntimeError("business-error")

    def failed(*_args):
        raise ValueError("private diagnostic failure")

    def business():
        raise original

    owner = SimpleNamespace(call=business)
    diagnostics._wrap(monkeypatch, owner, "call", "proposal", failed, failed)
    with pytest.raises(RuntimeError) as actual:
        owner.call()
    assert actual.value is original
    diagnostics.attach(original)
    value = json.loads(original.__notes__[-1].split(":", 1)[1])
    assert value["records"][0]["diagnostic_unavailable"] is True
    assert value["records"][0]["diagnostic_error_type"] == "ValueError"
    assert value["records"][0]["error"]["type"] == "RuntimeError"
    assert "private diagnostic failure" not in original.__notes__[-1]


def test_witnessed_diagnostics_never_read_store_for_a_snapshot_or_note(monkeypatch):
    failed_write = RuntimeError("original store failure")

    class BusinessStore:
        def __getattr__(self, name):
            raise AssertionError("diagnostic database read forbidden: " + name)

        @property
        def current_term(self):
            raise AssertionError("diagnostic current_term read forbidden")

        @property
        def commit_index(self):
            raise AssertionError("diagnostic commit_index read forbidden")

        def last_log_index(self):
            raise AssertionError("diagnostic last_log_index read forbidden")

        def append_entries(self, _entries, **_kwargs):
            return 7

        def append_local(self, _entry):
            return None

        def set_term_and_vote(self, _term, _vote):
            return None

        def set_commit_index(self, _index):
            if _index == 9:
                raise failed_write

    original_error = QuorumUnavailable("original quorum failure")

    def propose(command, _transport):
        node.store.append_local(SimpleNamespace(log_index=8))
        if command.command_id == "failure":
            raise original_error
        return "original result"

    node = SimpleNamespace(voter_id="voter", role="LEADER", leader_id="voter", _match_index={"peer": 7},
                           store=BusinessStore(), propose=propose)
    transport = SimpleNamespace(_rpc=lambda *_args: {"term": 3, "success": True})
    server = SimpleNamespace(_dispatch=lambda _opened: {"term": 3, "success": True})
    runtime = SimpleNamespace(node=node, transport=transport, server=server)
    diagnostics = _WitnessedRecoveryDiagnostics([runtime], monkeypatch)
    assert diagnostics._node(node)["last_business_store_results"] == {}
    node.store.set_term_and_vote(3, "voter")
    node.store.set_commit_index(7)
    with pytest.raises(RuntimeError) as actual_write:
        node.store.set_commit_index(9)
    assert actual_write.value is failed_write
    node.store.append_entries(())
    command = SimpleNamespace(command_id="success", command_type=PRODUCT_JOURNAL_INITIALIZE,
                              content_hash="sha256:" + "a" * 64)
    assert node.propose(command, transport) == "original result"
    assert transport._rpc("peer", "append_entries", {}) == {"term": 3, "success": True}
    command.command_id = "failure"
    with pytest.raises(QuorumUnavailable) as actual:
        node.propose(command, transport)
    assert actual.value is original_error
    diagnostics.attach(original_error)
    value = json.loads(original_error.__notes__[-1].split(":", 1)[1])
    snapshot = value["voters"][0]
    assert snapshot["atomic_snapshot"] is False
    assert snapshot["last_business_store_results"] == {
        "set_term_and_vote": 3, "set_commit_index": 7, "append_match_index": 7, "append_local": 8,
    }
    assert not any(record.get("diagnostic_unavailable") for record in value["records"])


def test_returning_diagnostics_do_not_reuse_closed_same_voter_observations(monkeypatch):
    def runtime():
        store = SimpleNamespace(append_entries=lambda *_args: 8, append_local=lambda *_args: None,
                                set_term_and_vote=lambda *_args: None, set_commit_index=lambda *_args: None)
        return SimpleNamespace(node=SimpleNamespace(voter_id="same-voter", role="FOLLOWER", leader_id="leader",
            _match_index={}, store=store, propose=lambda *_args: None),
            transport=SimpleNamespace(_rpc=lambda *_args: {"success": True}),
            server=SimpleNamespace(_dispatch=lambda *_args: {"success": True}))

    original, returning = runtime(), runtime()
    diagnostics = _WitnessedRecoveryDiagnostics([original], monkeypatch)
    original.node.store.set_commit_index(8)
    diagnostics.observe_runtime(returning)
    assert diagnostics._node(original.node)["last_business_store_results"] == {"set_commit_index": 8}
    assert diagnostics._node(returning.node)["last_business_store_results"] == {}
    assert diagnostics._node(original.node)["runtime_instance"] != diagnostics._node(returning.node)["runtime_instance"]
    returning.node.store.set_commit_index(11)
    returning.server._dispatch(SimpleNamespace(sender_id="leader", rpc="append_entries"))
    assert diagnostics._node(original.node)["last_business_store_results"] == {"set_commit_index": 8}
    assert diagnostics._node(returning.node)["last_business_store_results"] == {"set_commit_index": 11}
    assert diagnostics.records[-1]["runtime_instance"] == 1


@pytest.mark.parametrize(("commit", "applied", "ready", "acknowledged"), [
    (8, 8, False, 0),  # Permanently unavailable returning voter, despite surviving quorum.
    (11, 8, True, 11),  # An ACK and claimed readiness cannot replace applied-prefix proof.
])
def test_returning_catchup_fails_closed_for_missing_specific_prefix(monkeypatch, commit, applied, ready, acknowledged):
    clock = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    rounds = []

    def synchronize(_transport):
        rounds.append(True)
        clock[0] += 5.0
        return 1

    successor = SimpleNamespace(node=SimpleNamespace(store=SimpleNamespace(commit_index=11), quorum=2,
        synchronize=synchronize, _match_index={"returning": acknowledged}), transport=None)
    returning = SimpleNamespace(node=SimpleNamespace(voter_id="returning",
        store=SimpleNamespace(commit_index=commit, last_applied=applied)), ready=ready, materialize=lambda: None)
    with pytest.raises(AssertionError, match="did not acknowledge and apply") as failure:
        _catch_up_returning(successor, returning, _WitnessedRecoveryDiagnostics([], monkeypatch), timeout_seconds=10)
    note = json.loads(failure.value.__notes__[-1].split(":", 1)[1])
    assert len(rounds) == 2 and note["observation_budget_seconds"] == 10
    assert note["last_completed_predicate"]["commit_index"] == commit
    assert note["last_completed_predicate"]["last_applied"] == applied
    assert note["last_completed_predicate"]["acknowledged_match"] == acknowledged


def test_returning_catchup_cannot_retry_a_lost_quorum(monkeypatch):
    successor = SimpleNamespace(node=SimpleNamespace(store=SimpleNamespace(commit_index=11), quorum=2,
        synchronize=lambda _transport: 0), transport=None)
    returning = SimpleNamespace()
    with pytest.raises(AssertionError):
        _catch_up_returning(successor, returning, _WitnessedRecoveryDiagnostics([], monkeypatch))


def test_returning_real_reply_loss_observes_exact_catchup_after_normal_round(tmp_path, monkeypatch, request):
    created = []
    entered, release, completed = (threading.Event() for _ in range(3))
    observations = []
    factory = _runtime

    def runtime(*args):
        value = factory(*args)
        created.append(value)
        if len(created) != 4:
            return value
        dispatch, close = value.server._dispatch, value.close
        successor = created[1]
        synchronize = successor.node.synchronize

        def withheld(opened):
            if opened.rpc == "append_entries" and not entered.is_set():
                entered.set()
                assert release.wait(30), "controlled returning request was not released"
                try:
                    return dispatch(opened)
                finally:
                    completed.set()
            return dispatch(opened)

        def normal_round(transport):
            matched = synchronize(transport)
            if entered.is_set() and not release.is_set():
                observations.append({"quorum_matched": matched, "returning_ready": value.ready,
                    "returning_commit": value.node.store.commit_index,
                    "acknowledged_match": successor.node._match_index.get(value.node.voter_id, 0)})
                assert matched + 1 >= successor.node.quorum and not value.ready
                release.set()
                assert completed.wait(10), "controlled returning request did not complete"
            return matched

        def finished():
            release.set()
            close()

        monkeypatch.setattr(value.server, "_dispatch", withheld)
        monkeypatch.setattr(value, "close", finished)
        monkeypatch.setattr(successor.node, "synchronize", normal_round)
        return value

    monkeypatch.setitem(globals(), "_runtime", runtime)
    test_different_voter_completes_exact_witnessed_prefix_after_first_real_chunk(tmp_path, monkeypatch)
    assert entered.is_set() and completed.is_set() and len(observations) == 1
    successor, returning = created[1], created[-1]
    assert observations[0]["returning_commit"] == 8 and observations[0]["acknowledged_match"] == 0
    assert returning.ready and returning.node.store.commit_index == returning.node.store.last_applied == 11
    assert successor.node._match_index[returning.node.voter_id] == 11
    assert returning.transport.timeout_seconds == successor.transport.timeout_seconds == 5.0
    request.node.user_properties.append(("controlled_returning_catchup", json.dumps({
        "first_round": observations[0], "final_commit_applied_ack": 11, "rpc_timeout_seconds": 5.0,
        "observation_budget_seconds": 120.0, "original_ci_timeout_cause": "UNVERIFIED",
    }, sort_keys=True)))


def test_returning_real_unavailable_peer_cannot_pass_quorum_as_catchup(tmp_path, monkeypatch):
    configuration, registry, _credentials, nodes, _codecs, servers, transports = _secure_cluster(tmp_path)
    leader_id, returning_id, healthy_id = configuration.voter_ids
    leader, returning = nodes[leader_id], nodes[returning_id]
    diagnostics = _WitnessedRecoveryDiagnostics([
        SimpleNamespace(node=nodes[voter], server=servers[voter], transport=transports[voter])
        for voter in configuration.voter_ids
    ], monkeypatch)
    try:
        assert leader.start_election(transports[leader_id])
        propose_bootstrap_command(leader, transports[leader_id], _secure_genesis(configuration, registry, leader_id))
        servers[returning_id].close()
        payload_json = json.dumps({"session_id": "session-secure", "display_name": "Secure Federation"},
                                  sort_keys=True, separators=(",", ":"))
        row = {"session_id": "session-secure", "revision": 1, "event_id": "returning-closed-event",
               "event_type": "session.created", "occurred_at": "2026-09-08T09:00:00Z", "actor_node_id": leader_id,
               "payload_json": payload_json, "request_id": "sha256:" + "a" * 64,
               "content_hash": public_row_content_hash("session.created", payload_json)}
        command = AuthorityCommand("closed-returning-journal", PRODUCT_JOURNAL_INITIALIZE, configuration.cluster_id,
            leader_id, {"session_id": "session-secure", "expected_revision": 1,
                        "prefix_digest": journal_prefix_digest([row]), "public_rows": [row], "final": True})
        propose_bootstrap_command(leader, transports[leader_id], command)
        assert leader.store.commit_index == nodes[healthy_id].store.commit_index == 2
        assert returning.store.commit_index == 1
        with pytest.raises(AssertionError, match="did not acknowledge and apply") as failure:
            _catch_up_returning(SimpleNamespace(node=leader, transport=transports[leader_id]),
                SimpleNamespace(node=returning, ready=False, materialize=lambda: None), diagnostics, timeout_seconds=0.25)
        assert json.loads(failure.value.__notes__[-1].split(":", 1)[1])["expected_commit"] == 2
        assert returning.store.commit_index == 1
        assert leader._match_index[returning_id] < leader.store.commit_index
        assert any(record.get("stage") == "client-rpc" and record.get("target") == returning_id
                   and record.get("outcome") == "ERROR" for record in diagnostics.records)
        assert transports[leader_id].timeout_seconds == 5.0
    finally:
        _close_secure(servers)
