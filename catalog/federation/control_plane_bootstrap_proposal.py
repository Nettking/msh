"""Retry source-validated bootstrap commands without changing their identity.

Only trusted bootstrap paths may use this helper, while holding their runtime's
lifecycle lock. The caller still validates the complete fresh/witnessed source
and the resulting domain state. This helper neither grants authority to a
request issuer nor turns a receipt into proof of domain acceptance.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Any

from .control_plane_replication import (
    AuthorityCommand,
    ControlPlaneError,
    DuplicateCommandError,
    LogEntry,
    QuorumUnavailable,
    ReplicaNode,
    ReplicationTransport,
    StaleTerm,
)

_BOOTSTRAP_TYPES = frozenset({
    "FEDERATION_GENESIS", "LEADER_TRANSITION", "CAPABILITY_DECLARE",
    "SESSION_MEMBER_REMOVE", "NODE_REVOKE", "PRODUCT_JOURNAL_INITIALIZE",
})


def _recorded_command(node: ReplicaNode, requested: AuthorityCommand) -> AuthorityCommand:
    """Recover only an exact durable command, never ignore a payload conflict."""
    entry = node.store.entry_for_command(requested.command_id)
    receipt = node.store.receipt_for_command(requested.command_id)
    issuers = {requested.issued_by}
    if requested.issued_by in node.configuration.voter_ids:
        issuers.update(node.configuration.voter_ids)
    if entry is not None:
        recorded = entry.command
        candidate = replace(requested, issued_by=recorded.issued_by)
        if recorded.issued_by not in issuers or candidate.content_hash != recorded.content_hash:
            raise DuplicateCommandError("bootstrap command ID payload conflict")
        if receipt is not None and receipt.content_hash != recorded.content_hash:
            raise ControlPlaneError("bootstrap log and receipt identity disagree")
        return recorded
    if receipt is not None:
        # Receipts survive compaction but retain a digest, not the command body.
        # The closed voter set bounds the only variable migration coordinator.
        for issuer in sorted(issuers):
            candidate = replace(requested, issued_by=issuer)
            if candidate.content_hash == receipt.content_hash:
                return candidate
        raise DuplicateCommandError("bootstrap command ID payload conflict")
    return requested


def propose_bootstrap_command(
    node: ReplicaNode,
    transport: ReplicationTransport,
    command: AuthorityCommand,
) -> tuple[LogEntry, tuple[dict[str, Any], ...]]:
    """Prove current quorum and retry one exact bootstrap command.

    Reconstructing a migration on another configured voter may change its
    coordinator ``issued_by``. Recover that field only when the durable entry
    or receipt proves the complete original envelope. All payload, cluster,
    command-type and command-ID conflicts still fail closed.

    An inherited uncommitted prefix requires a real current-term commit. Enroll
    the identical already-present local voter from the validated pending state
    as an idempotent barrier; never directly advance commit indexes or votes.
    """
    if command.command_type not in _BOOTSTRAP_TYPES:
        raise ControlPlaneError("command is not a supported bootstrap operation")
    if command.cluster_id != node.configuration.cluster_id:
        raise ControlPlaneError("bootstrap command cluster identity mismatch")
    term = node.store.current_term
    if node.role != ReplicaNode.LEADER or node.leader_id != node.voter_id:
        raise StaleTerm("bootstrap proposal requires the current leader")
    matched = node.synchronize(transport)
    if node.store.current_term != term or node.role != ReplicaNode.LEADER or node.leader_id != node.voter_id:
        raise StaleTerm("bootstrap proposal lost its leader term")
    if matched + 1 < node.quorum:
        raise QuorumUnavailable("bootstrap proposal requires current voter quorum")

    recorded = _recorded_command(node, command)
    pending = node.store.entries(after=node.store.commit_index)
    if pending and pending[-1].log_term < term:
        own = node.pending_state()["nodes"].get(node.voter_id)
        if own is None:
            raise ControlPlaneError("bootstrap barrier lacks the existing local voter identity")
        # Even an already-committed genesis retry must settle a different
        # inherited bootstrap promotion before callers derive the next term.
        identity = hashlib.sha256(pending[-1].command.canonical_bytes()).hexdigest()
        node.propose(AuthorityCommand(
            command_id=f"bootstrap-barrier-{term}-{identity}",
            command_type="NODE_ENROLL",
            cluster_id=node.configuration.cluster_id,
            issued_by=node.voter_id,
            payload={key: own[key] for key in ("node_id", "display_name", "public_key")},
        ), transport)
    return node.propose(recorded, transport)


__all__ = ["propose_bootstrap_command"]
