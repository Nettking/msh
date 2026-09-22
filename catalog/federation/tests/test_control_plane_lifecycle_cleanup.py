"""A failed lifecycle round may only relinquish the authority it owned.

The scheduling gates below select a concrete interleaving; votes, replication,
bootstrap commands and the post-synchronize term guard remain real. Each voter
uses its persistent store and the existing authenticated socket fixture.
"""

from __future__ import annotations

import threading
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path

import pytest

from catalog.federation.control_plane_journal import PRODUCT_JOURNAL_INITIALIZE
from catalog.federation.control_plane_replication import ReplicaNode, StaleTerm
from catalog.federation.tests.test_control_plane_public_journal import (
    FEDERATION,
    SESSION,
    _Cluster,
)

_SCHEDULING_DEADLINE = 20.0


def _wait(event: threading.Event, description: str) -> None:
    # This bounds a broken test schedule, not a production election/RPC timeout.
    assert event.wait(_SCHEDULING_DEADLINE), description


class _OneRoundStop:
    """Select one lifecycle iteration without sleeping for a heartbeat."""

    def __init__(self) -> None:
        self.stopped = False
        self.entered = False

    def wait(self, _timeout: float) -> bool:
        if self.stopped or self.entered:
            return True
        self.entered = True
        return False

    def set(self) -> None:
        self.stopped = True


class _PauseAfterOutermostRelease:
    """Pause the old round only after it has genuinely released its RLock.

    Counting reentrant ownership matters: holding an additional outer lock
    around the round and its exception cleanup must move this pause after both.
    """

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.depth = threading.local()
        self.background: threading.Thread | None = None
        self.released = threading.Event()
        self.resume = threading.Event()
        self.paused = False

    def __enter__(self):
        self.lock.acquire()
        self.depth.value = getattr(self.depth, "value", 0) + 1
        return self

    def __exit__(self, *_exception) -> None:
        self.depth.value -= 1
        outermost = self.depth.value == 0
        self.lock.release()
        if (
            outermost
            and threading.current_thread() is self.background
            and not self.paused
        ):
            self.paused = True
            self.released.set()
            _wait(self.resume, "explicit bootstrap did not reach the journal gate")


@contextmanager
def _authenticated_runtimes(root: Path):
    cluster = _Cluster(root)
    with ExitStack() as cleanup:
        for runtime in cluster.runtimes:
            cleanup.callback(runtime.close)
        # Do not start free-running lifecycle loops. The test owns exactly the
        # one scheduled background round and all normal authenticated servers.
        for runtime in cluster.runtimes:
            runtime.server.start()
        yield cluster.runtimes


def _at_journal_synchronize(monkeypatch, runtime, callback):
    """Schedule after real sync and immediately before the existing guard."""
    propose = runtime._propose_bootstrap_command
    synchronize = runtime.node.synchronize
    active_command = None
    observed_commands = []

    def observe_command(command):
        nonlocal active_command
        previous = active_command
        active_command = command
        try:
            return propose(command)
        finally:
            active_command = previous

    def synchronize_then_schedule(transport):
        matched = synchronize(transport)
        if (
            active_command is not None
            and active_command.command_type == PRODUCT_JOURNAL_INITIALIZE
            and not observed_commands
        ):
            observed_commands.append(active_command)
            callback(active_command)
        return matched

    monkeypatch.setattr(runtime, "_propose_bootstrap_command", observe_command)
    monkeypatch.setattr(runtime.node, "synchronize", synchronize_then_schedule)
    return observed_commands


def _bootstrap(runtime) -> None:
    runtime.bootstrap_new_federation(
        federation_id=FEDERATION,
        session_id=SESSION,
        creator_node_id=runtime.node.voter_id,
        display_name="Lifecycle cleanup ownership",
    )


def _authority(node) -> tuple[int, str, str | None]:
    return node.store.current_term, node.role, node.leader_id


def test_old_lifecycle_failure_cannot_demote_new_explicit_bootstrap(
    tmp_path: Path, monkeypatch,
) -> None:
    with _authenticated_runtimes(tmp_path) as runtimes:
        creator = runtimes[0]
        gate = _PauseAfterOutermostRelease()
        creator._lifecycle_lock = gate
        creator._stop = _OneRoundStop()
        finished = threading.Event()
        background_errors = []
        observed_authority = []

        def fail_old_round() -> None:
            raise RuntimeError("controlled failure of the preceding lifecycle round")

        monkeypatch.setattr(creator, "_drive_lifecycle_round", fail_old_round)

        def background_round() -> None:
            try:
                creator._lifecycle_loop()
            except BaseException as error:  # noqa: BLE001 - surface background failures on the test thread
                background_errors.append(error)
            finally:
                finished.set()

        def finish_old_cleanup(_command) -> None:
            observed_authority.append(_authority(creator.node))
            gate.resume.set()
            _wait(finished, "old lifecycle exception cleanup did not finish")
            assert not background_errors, repr(background_errors)
            observed_authority.append(_authority(creator.node))

        commands = _at_journal_synchronize(monkeypatch, creator, finish_old_cleanup)
        background = threading.Thread(target=background_round, name="old-lifecycle-round")
        gate.background = background
        background.start()
        try:
            _wait(gate.released, "old lifecycle round did not release its lock")
            try:
                _bootstrap(creator)
            except StaleTerm as error:
                # On the unfixed product this is the exact production guard,
                # with same-term leader loss, not a fabricated higher term.
                error.add_note(f"authority before/after old cleanup: {observed_authority!r}")
                raise
        finally:
            gate.resume.set()
            background.join(_SCHEDULING_DEADLINE)
            assert not background.is_alive(), "old lifecycle thread leaked"

        assert not background_errors, repr(background_errors)
        assert len(commands) == 1
        assert finished.is_set()
        assert observed_authority[0] == observed_authority[1]
        term, role, leader_id = observed_authority[0]
        assert term > 0
        assert role == ReplicaNode.LEADER and leader_id == creator.node.voter_id
        assert _authority(creator.node) == (term, role, leader_id)
        assert creator.lifecycle_error == "RuntimeError"
        for runtime in runtimes:
            receipt = runtime.node.store.receipt_for_command(commands[0].command_id)
            assert receipt is not None
            assert receipt.content_hash == commands[0].content_hash
            assert runtime.node.state["federation_id"] == FEDERATION
            assert runtime.ready


def test_authenticated_higher_term_during_bootstrap_still_fails_closed(
    tmp_path: Path, monkeypatch,
) -> None:
    with _authenticated_runtimes(tmp_path) as runtimes:
        creator, successor = runtimes[:2]
        observed = []

        def elect_real_successor(command) -> None:
            observed.append((
                _authority(creator.node),
                creator.node.store.last_log_index(),
                creator.node.store.commit_index,
                command,
            ))
            assert successor.node.start_election(successor.transport)

        commands = _at_journal_synchronize(monkeypatch, creator, elect_real_successor)
        with pytest.raises(StaleTerm, match="bootstrap proposal lost its leader term"):
            _bootstrap(creator)

        assert len(commands) == 1 and len(observed) == 1
        (term, role, leader_id), last_index, commit_index, command = observed[0]
        assert role == ReplicaNode.LEADER and leader_id == creator.node.voter_id
        assert creator.node.store.current_term == term + 1
        assert creator.node.role == ReplicaNode.FOLLOWER
        assert successor.node.role == ReplicaNode.LEADER
        assert successor.node.leader_id == successor.node.voter_id
        for runtime in runtimes:
            assert runtime.node.store.last_log_index() == last_index
            assert runtime.node.store.commit_index == commit_index
            assert runtime.node.store.entry_for_command(command.command_id) is None
            assert runtime.node.store.receipt_for_command(command.command_id) is None
            assert not runtime.ready


def test_failure_of_the_current_leader_round_still_relinquishes_authority(
    tmp_path: Path, monkeypatch,
) -> None:
    with _authenticated_runtimes(tmp_path) as runtimes:
        leader = runtimes[0]
        assert leader.node.start_election(leader.transport)
        term = leader.node.store.current_term
        assert _authority(leader.node) == (term, ReplicaNode.LEADER, leader.node.voter_id)
        leader._stop = _OneRoundStop()
        previous_deadline = leader._next_election_at

        def fail_owned_round() -> None:
            raise RuntimeError("controlled failure of the current leader round")

        monkeypatch.setattr(leader, "_drive_lifecycle_round", fail_owned_round)
        before = time.monotonic()
        leader._lifecycle_loop()
        after = time.monotonic()

        assert _authority(leader.node) == (term, ReplicaNode.FOLLOWER, None)
        assert leader.node._next_index == {}
        assert leader.node._match_index == {}
        assert leader.lifecycle_error == "RuntimeError"
        rank = leader.node.configuration.voter_ids.index(leader.node.voter_id)
        delay = leader.election_timeout_seconds + rank * leader.election_stagger_seconds
        assert before + delay <= leader._next_election_at <= after + delay
        assert leader._next_election_at >= previous_deadline
        assert leader.node.store.last_log_index() == 0
        assert leader.node.store.commit_index == 0
