"""Small crash-fault-tolerant authority log for Federation control state.

Phase 1 deliberately stops at the consensus boundary.  It provides durable
replica metadata, command-specific bounded envelopes, a deterministic state
machine, follower progress tracking, and an in-process transport seam for
later authenticated inter-host wiring.  SQLite is only the local durable
store; the replicated log order is the authority.

Safety rules this module enforces, in the order Astra raised them:

1.  A proposal only reports success once the entry is durably committed by a
    quorum.  A retry of a still-uncommitted command re-drives replication and
    raises :class:`QuorumUnavailable` when quorum is still unreachable.
    Committed retries stay idempotent through bounded command receipts that
    survive restart and snapshot compaction.
2.  Snapshot installation refuses anything stale or conflicting relative to
    already applied authority, and leaves the projection, ``last_applied``,
    ``commit_index``, snapshot metadata, and the retained log suffix as one
    consistent history.
3.  A leader tracks per-follower progress, backtracks over prefix mismatches,
    replays missing suffixes, repairs divergent uncommitted suffixes, and
    ships a snapshot when a follower falls behind the compaction boundary.
4.  Domain-invalid commands are rejected against the correct ordered logical
    state before they can be appended, and committed replay is total so
    ``commit_index`` can never outrun ``last_applied`` permanently.
5.  Every protocol response carries a term, higher terms force immediate
    step-down, captured term and role are revalidated after each transport
    call, and one-vote-per-term is decided in a single durable transaction.
6.  Incoming AppendEntries requests must be contiguous and valid for the
    cluster, term, and index space; the prefix check, conflict repair, and
    commit advancement happen in one transaction, and commit advancement is
    bounded by the highest prefix the request actually proves.
7.  Commands carry command-specific schemas, genesis binds the immutable
    voter configuration, membership/revocation/actor/capability-ownership
    invariants are checked, and the public projection is built from
    event-specific allowlists so caller-supplied fields cannot leak.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

SCHEMA = "fcp.control-plane.replication.v1"
COMMAND_SCHEMA = "fcp.control-plane.command.v1"
STATE_SCHEMA = "fcp.control-plane.authority-state.v1"
SNAPSHOT_SCHEMA = "fcp.control-plane.snapshot.v1"
FENCING_SCHEMA = "fcp.control-plane.fencing.v1"
RECEIPT_SCHEMA = "fcp.control-plane.command-receipt.v1"
SCHEMA_VERSION = 1

MAX_ID_BYTES = 512
MAX_COMMAND_BYTES = 128 * 1024
MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024
MAX_VOTERS = 3
QUORUM = 2
INITIAL_TERM = 0
INITIAL_FENCING_EPOCH = 0

#: Bounded dedup window retained across snapshot compaction.  Receipts are
#: evicted lowest-index first; see KNOWN PHASE 1 LIMITATIONS in the tests.
MAX_COMMAND_RECEIPTS = 4096
#: Entries shipped in one AppendEntries request.
MAX_APPEND_BATCH = 64
#: Upper bound on prefix backtracking rounds for one follower.
MAX_CATCHUP_ROUNDS = 128

_ID_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,512}$")


class ControlPlaneError(ValueError):
    """Base error for malformed or unsafe control-plane operations."""


class DuplicateCommandError(ControlPlaneError):
    """A command ID was reused for different canonical content."""


class QuorumUnavailable(ControlPlaneError):
    """A leader could not commit an entry with the configured quorum."""


class StaleTerm(ControlPlaneError):
    """A request used an older consensus term, or leadership was lost."""


class LogConflict(ControlPlaneError):
    """A request attempted to overwrite committed authority."""

    def __init__(self, message: str, *, conflict_index: int = 0) -> None:
        super().__init__(message)
        self.conflict_index = conflict_index


def _canonical(value: object, *, maximum: int) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ControlPlaneError("value is not finite canonical JSON") from exc
    if len(encoded) > maximum:
        raise ControlPlaneError("canonical value exceeds its size bound")
    return encoded


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or not _ID_RE.fullmatch(value):
        raise ControlPlaneError(f"{field} is malformed")
    if len(value.encode("utf-8")) > MAX_ID_BYTES:
        raise ControlPlaneError(f"{field} exceeds its size bound")
    return value


def _uint(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ControlPlaneError(f"{field} must be a non-negative integer")
    return value


# --------------------------------------------------------------------------
# Finding 7: command-specific payload schemas
# --------------------------------------------------------------------------

_TEXT_FIELD = "text"
_UINT_FIELD = "uint"
_IDS_FIELD = "ids"
_NODES_FIELD = "nodes"
_EVENTS_FIELD = "events"

_NODE_FIELDS = ("node_id", "display_name", "public_key")

#: ``command_type -> (required fields, optional fields)``.  Any field outside
#: the union of the two is rejected: a command envelope can never carry an
#: unexpected key, so nothing unknown reaches the state machine or the log.
_COMMAND_SCHEMAS: dict[str, tuple[dict[str, str], dict[str, str]]] = {
    "FEDERATION_GENESIS": (
        {
            "federation_id": _TEXT_FIELD,
            "session_id": _TEXT_FIELD,
            "creator_node_id": _TEXT_FIELD,
            "display_name": _TEXT_FIELD,
            "voter_ids": _IDS_FIELD,
            "nodes": _NODES_FIELD,
        },
        {
            "members": _IDS_FIELD,
            "occurred_at": _TEXT_FIELD,
            "session_events": _EVENTS_FIELD,
        },
    ),
    "NODE_ENROLL": (
        {
            "node_id": _TEXT_FIELD,
            "display_name": _TEXT_FIELD,
            "public_key": _TEXT_FIELD,
        },
        {"occurred_at": _TEXT_FIELD},
    ),
    "NODE_REVOKE": (
        {"node_id": _TEXT_FIELD, "reason": _TEXT_FIELD},
        {"occurred_at": _TEXT_FIELD},
    ),
    "SESSION_CREATE": (
        {
            "session_id": _TEXT_FIELD,
            "creator_node_id": _TEXT_FIELD,
            "display_name": _TEXT_FIELD,
            "occurred_at": _TEXT_FIELD,
        },
        {},
    ),
    "SESSION_MEMBER_ADD": (
        {
            "session_id": _TEXT_FIELD,
            "node_id": _TEXT_FIELD,
            "occurred_at": _TEXT_FIELD,
        },
        {},
    ),
    "SESSION_MEMBER_REMOVE": (
        {
            "session_id": _TEXT_FIELD,
            "node_id": _TEXT_FIELD,
            "occurred_at": _TEXT_FIELD,
        },
        {"reason": _TEXT_FIELD},
    ),
    "LEADER_TRANSITION": (
        {
            "session_id": _TEXT_FIELD,
            "previous_leader_node_id": _TEXT_FIELD,
            "leader_node_id": _TEXT_FIELD,
            "term": _UINT_FIELD,
            "occurred_at": _TEXT_FIELD,
        },
        {"reason": _TEXT_FIELD},
    ),
    "CAPABILITY_DECLARE": (
        {
            "session_id": _TEXT_FIELD,
            "capability_id": _TEXT_FIELD,
            "capability_type": _TEXT_FIELD,
            "owner_node_id": _TEXT_FIELD,
            "occurred_at": _TEXT_FIELD,
        },
        {},
    ),
    "CAPABILITY_WITHDRAW": (
        {
            "session_id": _TEXT_FIELD,
            "capability_id": _TEXT_FIELD,
            "occurred_at": _TEXT_FIELD,
        },
        {"reason": _TEXT_FIELD},
    ),
    # Their nested, closed schemas are validated by control_plane_journal.
    "PRODUCT_TRANSACTION": ({}, {}),
    "PRODUCT_JOURNAL_INITIALIZE": ({}, {}),
}

_COMMAND_TYPES = frozenset(_COMMAND_SCHEMAS)

#: Explicit allowlist of the fields each public session event may expose.
#: The projection is built only from these keys, so private, internal, or
#: injected caller fields cannot reach ``session_events``.
_PUBLIC_EVENT_FIELDS: dict[str, tuple[str, ...]] = {
    "session.created": ("session_id", "display_name", "creator_node_id"),
    "node.joined": ("session_id", "node_id"),
    "node.left": ("session_id", "node_id", "reason"),
    "session.leader.changed": (
        "session_id",
        "previous_leader_node_id",
        "leader_node_id",
        "term",
        "reason",
    ),
    "capability.registered": (
        "session_id",
        "capability_id",
        "capability_type",
        "owner_node_id",
    ),
    "capability.status.changed": ("session_id", "capability_id", "reason"),
}


def _id_list(value: object, field: str) -> list[str]:
    if not isinstance(value, list):
        raise ControlPlaneError(f"{field} must be an array")
    return [_text(item, f"{field} entry") for item in value]


def _node_list(value: object, field: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise ControlPlaneError(f"{field} must be an array")
    nodes: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            raise ControlPlaneError(f"{field} entries must be objects")
        unexpected = set(item) - set(_NODE_FIELDS)
        if unexpected:
            raise ControlPlaneError(f"{field} entry has unexpected fields")
        nodes.append({key: _text(item.get(key), key) for key in _NODE_FIELDS})
    return nodes


def _event_list(value: object, field: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ControlPlaneError(f"{field} must be an array")
    for item in value:
        if not isinstance(item, dict):
            raise ControlPlaneError(f"{field} entries must be objects")
    return list(value)


def _validate_payload(command_type: str, payload: object) -> dict[str, Any]:
    """Reject anything that is not exactly this command type's schema."""
    if command_type in {"PRODUCT_TRANSACTION", "PRODUCT_JOURNAL_INITIALIZE"}:
        from .control_plane_journal import validate_product_payload

        validate_product_payload(command_type, payload)
        return payload
    if not isinstance(payload, dict):
        raise ControlPlaneError("command payload must be an object")
    required, optional = _COMMAND_SCHEMAS[command_type]
    allowed = {**required, **optional}
    unexpected = sorted(set(payload) - set(allowed))
    if unexpected:
        raise ControlPlaneError(
            f"{command_type} payload has unexpected fields: {', '.join(unexpected)}"
        )
    missing = sorted(set(required) - set(payload))
    if missing:
        raise ControlPlaneError(
            f"{command_type} payload is missing: {', '.join(missing)}"
        )
    for name, kind in allowed.items():
        if name not in payload:
            continue
        value = payload[name]
        if kind == _TEXT_FIELD:
            _text(value, name)
        elif kind == _UINT_FIELD:
            _uint(value, name)
        elif kind == _IDS_FIELD:
            _id_list(value, name)
        elif kind == _NODES_FIELD:
            _node_list(value, name)
        elif kind == _EVENTS_FIELD:
            _event_list(value, name)
        else:  # pragma: no cover - schema table is closed
            raise ControlPlaneError(f"unsupported schema kind for {name}")
    _canonical(payload, maximum=MAX_COMMAND_BYTES)
    return payload


def _public_payload(event_type: str, source: Mapping[str, Any]) -> dict[str, Any]:
    """Project only the allowlisted fields for one public event type."""
    allowed = _PUBLIC_EVENT_FIELDS[event_type]
    return {key: source[key] for key in allowed if key in source}


@dataclass(frozen=True)
class VoterConfiguration:
    """The immutable three-voter Phase 1 cluster configuration."""

    cluster_id: str
    voter_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _text(self.cluster_id, "cluster_id")
        voters = tuple(sorted(_text(value, "voter_id") for value in self.voter_ids))
        if len(voters) != MAX_VOTERS or len(set(voters)) != len(voters):
            raise ControlPlaneError("Phase 1 requires exactly three unique voters")
        object.__setattr__(self, "voter_ids", voters)

    @property
    def quorum(self) -> int:
        return QUORUM

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": SCHEMA,
            "cluster_id": self.cluster_id,
            "voter_ids": list(self.voter_ids),
        }

    @classmethod
    def from_dict(cls, value: object) -> VoterConfiguration:
        if not isinstance(value, dict) or value.get("schema") != SCHEMA:
            raise ControlPlaneError("unsupported voter configuration schema")
        voters = value.get("voter_ids")
        if not isinstance(voters, list):
            raise ControlPlaneError("voter_ids must be an array")
        return cls(cluster_id=value.get("cluster_id"), voter_ids=tuple(voters))


@dataclass(frozen=True)
class AuthorityCommand:
    """A bounded deterministic command submitted to the authority log."""

    command_id: str
    command_type: str
    cluster_id: str
    issued_by: str
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        _text(self.command_id, "command_id")
        _text(self.command_type, "command_type")
        _text(self.cluster_id, "cluster_id")
        _text(self.issued_by, "issued_by")
        if self.command_type not in _COMMAND_TYPES:
            if self.command_type == "VOTER_CONFIG_CHANGE":
                raise ControlPlaneError("unsupported-voter-reconfiguration")
            raise ControlPlaneError("unsupported command type")
        _validate_payload(self.command_type, self.payload)
        _canonical(self.to_dict(), maximum=MAX_COMMAND_BYTES)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": COMMAND_SCHEMA,
            "command_id": self.command_id,
            "command_type": self.command_type,
            "cluster_id": self.cluster_id,
            "issued_by": self.issued_by,
            "payload": self.payload,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical(self.to_dict(), maximum=MAX_COMMAND_BYTES)

    @property
    def content_hash(self) -> str:
        return "sha256:" + hashlib.sha256(self.canonical_bytes()).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> AuthorityCommand:
        if not isinstance(value, dict) or value.get("schema") != COMMAND_SCHEMA:
            raise ControlPlaneError("unsupported command schema")
        return cls(
            command_id=value.get("command_id"),
            command_type=value.get("command_type"),
            cluster_id=value.get("cluster_id"),
            issued_by=value.get("issued_by"),
            payload=value.get("payload"),
        )


@dataclass(frozen=True)
class LogEntry:
    log_index: int
    log_term: int
    command: AuthorityCommand

    def __post_init__(self) -> None:
        if self.log_index < 1:
            raise ControlPlaneError("log_index must be positive")
        _uint(self.log_term, "log_term")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": SCHEMA,
            "log_index": self.log_index,
            "log_term": self.log_term,
            "command": self.command.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> LogEntry:
        if not isinstance(value, dict) or value.get("schema") != SCHEMA:
            raise ControlPlaneError("unsupported log-entry schema")
        return cls(
            log_index=_uint(value.get("log_index"), "log_index"),
            log_term=_uint(value.get("log_term"), "log_term"),
            command=AuthorityCommand.from_dict(value.get("command")),
        )


@dataclass(frozen=True)
class CommandReceipt:
    """Durable proof that one command ID was committed at one index.

    Receipts outlive the log entries they describe, so command-ID
    deduplication keeps working after snapshot compaction and restart.
    """

    command_id: str
    content_hash: str
    log_index: int
    log_term: int

    def __post_init__(self) -> None:
        _text(self.command_id, "command_id")
        _text(self.content_hash, "content_hash")
        _uint(self.log_index, "log_index")
        _uint(self.log_term, "log_term")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": RECEIPT_SCHEMA,
            "command_id": self.command_id,
            "content_hash": self.content_hash,
            "log_index": self.log_index,
            "log_term": self.log_term,
        }

    @classmethod
    def from_dict(cls, value: object) -> CommandReceipt:
        if not isinstance(value, dict) or value.get("schema") != RECEIPT_SCHEMA:
            raise ControlPlaneError("unsupported command receipt schema")
        return cls(
            command_id=value.get("command_id"),
            content_hash=value.get("content_hash"),
            log_index=_uint(value.get("log_index"), "log_index"),
            log_term=_uint(value.get("log_term"), "log_term"),
        )


@dataclass(frozen=True)
class FencingToken:
    cluster_id: str
    cluster_term: int
    leader_id: str
    commit_index: int
    fencing_epoch: int

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": FENCING_SCHEMA,
            "cluster_id": self.cluster_id,
            "cluster_term": self.cluster_term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "fencing_epoch": self.fencing_epoch,
        }


@dataclass(frozen=True)
class Snapshot:
    cluster_id: str
    voter_configuration: VoterConfiguration
    last_included_index: int
    last_included_term: int
    state: dict[str, Any]
    digest: str
    command_receipts: tuple[CommandReceipt, ...] = ()

    def __post_init__(self) -> None:
        _uint(self.last_included_index, "last_included_index")
        _uint(self.last_included_term, "last_included_term")
        if self.cluster_id != self.voter_configuration.cluster_id:
            raise ControlPlaneError("snapshot cluster identity mismatch")
        _canonical(self.state, maximum=MAX_SNAPSHOT_BYTES)
        if self.digest != _state_digest(self.state):
            raise ControlPlaneError("snapshot digest mismatch")
        from .control_plane_journal import validate_product_journal_state

        validate_product_journal_state(self.state)
        receipts = tuple(self.command_receipts)
        if len(receipts) > MAX_COMMAND_RECEIPTS:
            raise ControlPlaneError("snapshot receipt window exceeds its bound")
        if len({receipt.command_id for receipt in receipts}) != len(receipts):
            raise ControlPlaneError("snapshot receipts repeat a command ID")
        object.__setattr__(
            self,
            "command_receipts",
            tuple(sorted(receipts, key=lambda item: (item.log_index, item.command_id))),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": SNAPSHOT_SCHEMA,
            "cluster_id": self.cluster_id,
            "voter_configuration": self.voter_configuration.to_dict(),
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "state": self.state,
            "digest": self.digest,
            "command_receipts": [
                receipt.to_dict() for receipt in self.command_receipts
            ],
        }

    @classmethod
    def from_dict(cls, value: object) -> Snapshot:
        if not isinstance(value, dict) or value.get("schema") != SNAPSHOT_SCHEMA:
            raise ControlPlaneError("unsupported snapshot schema")
        state = value.get("state")
        if not isinstance(state, dict):
            raise ControlPlaneError("snapshot state must be an object")
        receipts = value.get("command_receipts", [])
        if not isinstance(receipts, list):
            raise ControlPlaneError("snapshot command_receipts must be an array")
        return cls(
            cluster_id=_text(value.get("cluster_id"), "cluster_id"),
            voter_configuration=VoterConfiguration.from_dict(
                value.get("voter_configuration")
            ),
            last_included_index=_uint(
                value.get("last_included_index"), "last_included_index"
            ),
            last_included_term=_uint(
                value.get("last_included_term"), "last_included_term"
            ),
            state=state,
            digest=_text(value.get("digest"), "digest"),
            command_receipts=tuple(
                CommandReceipt.from_dict(item) for item in receipts
            ),
        )


def _state_digest(state: Mapping[str, object]) -> str:
    return (
        "sha256:"
        + hashlib.sha256(
            _canonical(dict(state), maximum=MAX_SNAPSHOT_BYTES)
        ).hexdigest()
    )


def _empty_state(configuration: VoterConfiguration) -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        "cluster_id": configuration.cluster_id,
        "federation_id": None,
        "voter_ids": [],
        "sessions": {},
        "nodes": {},
        "memberships": {},
        "revocations": {},
        "leaders": {},
        "capabilities": {},
        "session_events": {},
    }


def _copy_state(state: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(_canonical(dict(state), maximum=MAX_SNAPSHOT_BYTES))


def _require_state(state: Mapping[str, Any], configuration: VoterConfiguration) -> None:
    if (
        state.get("schema") != STATE_SCHEMA
        or state.get("cluster_id") != configuration.cluster_id
    ):
        raise ControlPlaneError("authority state identity mismatch")


def _is_revoked(state: Mapping[str, Any], node_id: str) -> bool:
    return node_id in state["revocations"]


def _require_enrolled(state: Mapping[str, Any], node_id: str, field: str) -> None:
    if node_id not in state["nodes"]:
        raise ControlPlaneError(f"{field} is not an enrolled node")
    if _is_revoked(state, node_id):
        raise ControlPlaneError(f"{field} is revoked")


def _require_member(state: Mapping[str, Any], session_id: str, node_id: str, field: str) -> None:
    _require_enrolled(state, node_id, field)
    if state["memberships"].get(session_id, {}).get(node_id) is not True:
        raise ControlPlaneError(f"{field} is not a member of the session")


def _event(
    state: dict[str, Any],
    *,
    session_id: str,
    event_type: str,
    actor_node_id: str,
    occurred_at: str,
    source: Mapping[str, Any],
) -> dict[str, Any]:
    """Append one public event built strictly from this type's allowlist."""
    events = state["session_events"].setdefault(session_id, [])
    revision = len(events) + 1
    projected = {
        "session_id": session_id,
        "revision": revision,
        "event_type": event_type,
        "actor_node_id": actor_node_id,
        "occurred_at": occurred_at,
        "payload": _public_payload(event_type, source),
    }
    events.append(projected)
    return projected


def _import_genesis_events(imported: object, session_id: str) -> list[dict[str, Any]]:
    """Rebuild imported genesis events through the public allowlist."""
    if not isinstance(imported, list):
        raise ControlPlaneError("genesis session_events must be an array")
    events: list[dict[str, Any]] = []
    for expected, item in enumerate(imported, start=1):
        if not isinstance(item, dict) or item.get("revision") != expected:
            raise ControlPlaneError("genesis event revisions are not contiguous")
        event_type = _text(item.get("event_type"), "event_type")
        if event_type not in _PUBLIC_EVENT_FIELDS:
            raise ControlPlaneError("unsupported genesis event type")
        if item.get("session_id") != session_id:
            raise ControlPlaneError("genesis event session mismatch")
        source = item.get("payload")
        if not isinstance(source, dict):
            raise ControlPlaneError("genesis event payload must be an object")
        events.append(
            {
                "session_id": session_id,
                "revision": expected,
                "event_type": event_type,
                "actor_node_id": _text(item.get("actor_node_id"), "actor_node_id"),
                "occurred_at": _text(item.get("occurred_at"), "occurred_at"),
                "payload": _public_payload(event_type, source),
            }
        )
    return events


class AuthorityStateMachine:
    """Pure deterministic application of committed Phase 1 commands."""

    def __init__(self, configuration: VoterConfiguration) -> None:
        self.configuration = configuration

    def initial_state(self) -> dict[str, Any]:
        return _empty_state(self.configuration)

    def apply_committed(
        self, state: Mapping[str, Any], command: AuthorityCommand
    ) -> tuple[dict[str, Any], tuple[dict[str, Any], ...], str | None]:
        """Total application used for replaying a committed log.

        Finding 4: replay must never get stuck.  A committed command that
        fails its domain invariants is a deterministic no-op with a recorded
        rejection reason, so ``last_applied`` always catches up to
        ``commit_index`` and a committed log is always replayable.
        """
        try:
            next_state, events = self.apply(state, command)
        except ControlPlaneError as exc:
            return _copy_state(state), (), str(exc)
        return next_state, events, None

    def apply(
        self, state: Mapping[str, Any], command: AuthorityCommand
    ) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
        """Strict application; raises for domain-invalid commands."""
        return self._apply_command(state, command, product_transaction=False)

    def _apply_command(
        self, state: Mapping[str, Any], command: AuthorityCommand,
        *, product_transaction: bool,
    ) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
        _require_state(state, self.configuration)
        if command.cluster_id != self.configuration.cluster_id:
            raise ControlPlaneError("command cluster identity mismatch")
        next_state = _copy_state(state)
        emitted: list[dict[str, Any]] = []
        payload = command.payload
        kind = command.command_type

        from .control_plane_journal import (
            apply_product_command,
            require_product_envelope,
        )

        if kind in {"PRODUCT_TRANSACTION", "PRODUCT_JOURNAL_INITIALIZE"}:
            result, events = apply_product_command(
                next_state, command, self.configuration,
                lambda current, inner: self._apply_command(
                    current, inner, product_transaction=True,
                ),
            )
            return self._finish(result), events
        if not product_transaction:
            require_product_envelope(next_state, command)

        if kind == "FEDERATION_GENESIS":
            federation_id = payload["federation_id"]
            if next_state["federation_id"] is not None:
                if next_state["federation_id"] != federation_id:
                    raise ControlPlaneError("Federation identity cannot change")
                return next_state, ()
            # Finding 7: the immutable voter set is bound into genesis.
            if tuple(sorted(payload["voter_ids"])) != self.configuration.voter_ids:
                raise ControlPlaneError("genesis voter configuration mismatch")
            session_id = payload["session_id"]
            creator = payload["creator_node_id"]
            nodes = _node_list(payload["nodes"], "nodes")
            enrolled = {node["node_id"]: node for node in nodes}
            if len(enrolled) != len(nodes):
                raise ControlPlaneError("genesis enrolls a node twice")
            if creator not in enrolled:
                raise ControlPlaneError("genesis creator must be an enrolled node")
            if command.issued_by not in enrolled:
                raise ControlPlaneError("issued_by is not an enrolled node")
            members = payload.get("members", [creator])
            member_ids = _id_list(members, "members")
            for node_id in member_ids:
                if node_id not in enrolled:
                    raise ControlPlaneError("genesis member is not enrolled")
            next_state["federation_id"] = federation_id
            next_state["voter_ids"] = list(self.configuration.voter_ids)
            next_state["nodes"] = dict(enrolled)
            next_state["sessions"][session_id] = {
                "display_name": payload["display_name"],
                "creator_node_id": creator,
                "revision": 0,
            }
            next_state["leaders"][session_id] = {
                "creator_node_id": creator,
                "leader_node_id": creator,
                "term": 1,
            }
            next_state["memberships"][session_id] = {creator: True}
            for node_id in member_ids:
                next_state["memberships"][session_id][node_id] = True
            imported = payload.get("session_events", [])
            if imported:
                events = _import_genesis_events(imported, session_id)
                next_state["session_events"][session_id] = events
            return self._finish(next_state), ()

        if next_state["federation_id"] is None:
            raise ControlPlaneError("Federation genesis is required first")

        if kind == "NODE_ENROLL":
            _require_enrolled(next_state, command.issued_by, "issued_by")
            node_id = payload["node_id"]
            candidate = {key: payload[key] for key in _NODE_FIELDS}
            existing = next_state["nodes"].get(node_id)
            if existing is not None and existing != candidate:
                raise ControlPlaneError("node identity conflict")
            if _is_revoked(next_state, node_id):
                raise ControlPlaneError("a revoked node cannot be re-enrolled")
            next_state["nodes"][node_id] = candidate

        elif kind == "NODE_REVOKE":
            _require_enrolled(next_state, command.issued_by, "issued_by")
            node_id = payload["node_id"]
            if node_id not in next_state["nodes"]:
                raise ControlPlaneError("cannot revoke unknown node")
            next_state["revocations"][node_id] = {
                "reason": payload["reason"],
                "occurred_at": payload.get("occurred_at"),
            }

        elif kind == "SESSION_CREATE":
            _require_enrolled(next_state, command.issued_by, "issued_by")
            session_id = payload["session_id"]
            creator = payload["creator_node_id"]
            _require_enrolled(next_state, creator, "creator_node_id")
            candidate = {
                "display_name": payload["display_name"],
                "creator_node_id": creator,
                "revision": 0,
            }
            existing = next_state["sessions"].get(session_id)
            if existing is not None:
                if (
                    existing["display_name"] != candidate["display_name"]
                    or existing["creator_node_id"] != candidate["creator_node_id"]
                ):
                    raise ControlPlaneError("session identity conflict")
                return next_state, ()
            next_state["sessions"][session_id] = candidate
            next_state["memberships"][session_id] = {creator: True}
            next_state["leaders"][session_id] = {
                "creator_node_id": creator,
                "leader_node_id": creator,
                "term": 1,
            }
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="session.created",
                    actor_node_id=command.issued_by,
                    occurred_at=payload["occurred_at"],
                    source=payload,
                )
            )
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="node.joined",
                    actor_node_id=command.issued_by,
                    occurred_at=payload["occurred_at"],
                    source={**payload, "node_id": creator},
                )
            )

        elif kind == "SESSION_MEMBER_ADD":
            session_id = payload["session_id"]
            node_id = payload["node_id"]
            if session_id not in next_state["sessions"]:
                raise ControlPlaneError("membership target is unknown")
            _require_member(next_state, session_id, command.issued_by, "issued_by")
            _require_enrolled(next_state, node_id, "node_id")
            members = next_state["memberships"].setdefault(session_id, {})
            if members.get(node_id) is True:
                return next_state, ()
            members[node_id] = True
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="node.joined",
                    actor_node_id=command.issued_by,
                    occurred_at=payload["occurred_at"],
                    source=payload,
                )
            )

        elif kind == "SESSION_MEMBER_REMOVE":
            session_id = payload["session_id"]
            node_id = payload["node_id"]
            if session_id not in next_state["sessions"]:
                raise ControlPlaneError("membership target is unknown")
            _require_member(next_state, session_id, command.issued_by, "issued_by")
            members = next_state["memberships"].get(session_id, {})
            if members.get(node_id) is not True:
                return next_state, ()
            members[node_id] = False
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="node.left",
                    actor_node_id=command.issued_by,
                    occurred_at=payload["occurred_at"],
                    source=payload,
                )
            )

        elif kind == "LEADER_TRANSITION":
            session_id = payload["session_id"]
            leader = next_state["leaders"].get(session_id)
            if leader is None:
                raise ControlPlaneError("leadership session is unknown")
            _require_member(next_state, session_id, command.issued_by, "issued_by")
            previous = payload["previous_leader_node_id"]
            target = payload["leader_node_id"]
            term = payload["term"]
            if previous != leader["leader_node_id"] or term != leader["term"] + 1:
                raise ControlPlaneError(
                    "leadership transition is not contiguous or eligible"
                )
            _require_member(next_state, session_id, target, "leader_node_id")
            leader.update({"leader_node_id": target, "term": term})
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="session.leader.changed",
                    actor_node_id=command.issued_by,
                    occurred_at=payload["occurred_at"],
                    source=payload,
                )
            )
            if not product_transaction:
                from .control_plane_journal import append_automatic_leadership_row

                append_automatic_leadership_row(next_state, command)

        elif kind == "CAPABILITY_DECLARE":
            session_id = payload["session_id"]
            capability_id = payload["capability_id"]
            owner = payload["owner_node_id"]
            if session_id not in next_state["sessions"]:
                raise ControlPlaneError("capability session is unknown")
            # Finding 7: a capability may only be declared by its own owner.
            if command.issued_by != owner:
                raise ControlPlaneError("issued_by does not own the capability")
            _require_member(next_state, session_id, owner, "owner_node_id")
            from .control_plane_readiness import (
                BOOTSTRAP_SEAL_CAPABILITY_ID,
                BOOTSTRAP_SEAL_CAPABILITY_TYPE,
            )

            if capability_id == BOOTSTRAP_SEAL_CAPABILITY_ID and (
                payload["capability_type"] != BOOTSTRAP_SEAL_CAPABILITY_TYPE
                or owner not in self.configuration.voter_ids
                or owner != next_state["leaders"][session_id]["leader_node_id"]
            ):
                raise ControlPlaneError("bootstrap seal requires the operational voter leader")
            capabilities = next_state["capabilities"].setdefault(session_id, {})
            candidate = {
                "capability_id": capability_id,
                "capability_type": payload["capability_type"],
                "owner_node_id": owner,
            }
            existing = capabilities.get(capability_id)
            if existing is not None:
                if existing != candidate:
                    raise ControlPlaneError("capability identity conflict")
                return next_state, ()
            capabilities[capability_id] = candidate
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="capability.registered",
                    actor_node_id=command.issued_by,
                    occurred_at=payload["occurred_at"],
                    source=payload,
                )
            )

        elif kind == "CAPABILITY_WITHDRAW":
            session_id = payload["session_id"]
            capability_id = payload["capability_id"]
            capabilities = next_state["capabilities"].get(session_id, {})
            existing = capabilities.get(capability_id)
            if existing is None:
                return next_state, ()
            # Finding 7: only the declaring owner may withdraw a capability.
            if command.issued_by != existing["owner_node_id"]:
                raise ControlPlaneError("issued_by does not own the capability")
            _require_member(next_state, session_id, command.issued_by, "issued_by")
            capabilities.pop(capability_id)
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="capability.status.changed",
                    actor_node_id=command.issued_by,
                    occurred_at=payload["occurred_at"],
                    source=payload,
                )
            )

        else:  # pragma: no cover - constructor rejects unknown commands
            raise ControlPlaneError("unsupported command type")

        return self._finish(next_state), tuple(emitted)

    @staticmethod
    def _finish(state: dict[str, Any]) -> dict[str, Any]:
        for session_id, events in state["session_events"].items():
            if session_id in state["sessions"]:
                state["sessions"][session_id]["revision"] = len(events)
        _canonical(state, maximum=MAX_SNAPSHOT_BYTES)
        return state


class PersistentReplicaStore:
    """Atomic local persistence for one replica's consensus metadata and log."""

    def __init__(self, database: Path | str, configuration: VoterConfiguration) -> None:
        self.database = str(database)
        self.configuration = configuration
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    # -- connection and transaction plumbing -------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """One serialized write transaction, committed only on clean exit."""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.rollback()
                raise
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._read() as database:
            database.execute("PRAGMA journal_mode=WAL")
            database.executescript(
                """
                CREATE TABLE IF NOT EXISTS replica_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS replica_voters (
                    voter_id TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS replica_log (
                    log_index INTEGER PRIMARY KEY,
                    log_term INTEGER NOT NULL,
                    command_id TEXT NOT NULL UNIQUE,
                    command_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS replica_receipts (
                    command_id TEXT PRIMARY KEY,
                    content_hash TEXT NOT NULL,
                    log_index INTEGER NOT NULL,
                    log_term INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS replica_projection (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    state_json TEXT NOT NULL,
                    state_digest TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS replica_snapshot (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    snapshot_json TEXT NOT NULL,
                    snapshot_digest TEXT NOT NULL
                );
                """
            )
        with self._write() as database:
            existing = database.execute(
                "SELECT value FROM replica_metadata WHERE key='cluster_id'"
            ).fetchone()
            if existing is None:
                metadata = {
                    "cluster_id": self.configuration.cluster_id,
                    "current_term": INITIAL_TERM,
                    "voted_for": "",
                    "commit_index": 0,
                    "last_applied": 0,
                    "fencing_epoch": INITIAL_FENCING_EPOCH,
                    "last_snapshot_index": 0,
                    "last_snapshot_term": 0,
                }
                database.executemany(
                    "INSERT INTO replica_metadata(key,value) VALUES(?,?)",
                    tuple((key, str(value)) for key, value in metadata.items()),
                )
                database.executemany(
                    "INSERT INTO replica_voters(voter_id) VALUES(?)",
                    ((voter,) for voter in self.configuration.voter_ids),
                )
                state = _empty_state(self.configuration)
                state_json = _canonical(state, maximum=MAX_SNAPSHOT_BYTES).decode()
                database.execute(
                    "INSERT INTO replica_projection(singleton,state_json,state_digest)"
                    " VALUES(1,?,?)",
                    (state_json, _state_digest(state)),
                )
            elif existing["value"] != self.configuration.cluster_id:
                raise ControlPlaneError("replica cluster identity changed")
            voters = tuple(
                row["voter_id"]
                for row in database.execute(
                    "SELECT voter_id FROM replica_voters ORDER BY voter_id"
                ).fetchall()
            )
            if voters != self.configuration.voter_ids:
                raise ControlPlaneError("voter configuration is immutable")

    # -- metadata ----------------------------------------------------------

    @staticmethod
    def _meta(database: sqlite3.Connection, key: str) -> str:
        row = database.execute(
            "SELECT value FROM replica_metadata WHERE key=?", (key,)
        ).fetchone()
        if row is None:
            raise ControlPlaneError(f"missing replica metadata: {key}")
        return str(row["value"])

    def _metadata(self, key: str) -> str:
        with self._read() as database:
            return self._meta(database, key)

    @staticmethod
    def _set_metadata(database: sqlite3.Connection, **values: object) -> None:
        for key, value in values.items():
            database.execute(
                "UPDATE replica_metadata SET value=? WHERE key=?",
                (str(value), key),
            )

    @property
    def current_term(self) -> int:
        return int(self._metadata("current_term"))

    @property
    def voted_for(self) -> str | None:
        value = self._metadata("voted_for")
        return value or None

    @property
    def commit_index(self) -> int:
        return int(self._metadata("commit_index"))

    @property
    def last_applied(self) -> int:
        return int(self._metadata("last_applied"))

    @property
    def fencing_epoch(self) -> int:
        return int(self._metadata("fencing_epoch"))

    @property
    def last_snapshot_index(self) -> int:
        return int(self._metadata("last_snapshot_index"))

    @property
    def last_snapshot_term(self) -> int:
        return int(self._metadata("last_snapshot_term"))

    def set_term_and_vote(self, term: int, voted_for: str | None) -> None:
        _uint(term, "current_term")
        if voted_for is not None:
            _text(voted_for, "voted_for")
        with self._write() as database:
            self._set_metadata(
                database,
                current_term=term,
                voted_for=voted_for or "",
            )

    def observe_term(self, term: int) -> bool:
        """Adopt a strictly higher term and clear the vote, atomically."""
        _uint(term, "term")
        with self._write() as database:
            current = int(self._meta(database, "current_term"))
            if term <= current:
                return False
            self._set_metadata(database, current_term=term, voted_for="")
            return True

    def increment_fencing_epoch(self) -> int:
        with self._write() as database:
            value = int(self._meta(database, "fencing_epoch")) + 1
            self._set_metadata(database, fencing_epoch=value)
            return value

    def try_grant_vote(
        self,
        *,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> bool:
        """Decide and durably record one vote inside a single transaction.

        Finding 5: the term check, the already-voted check, the up-to-date
        check, and the durable write share one ``BEGIN IMMEDIATE``
        transaction, so two competing requests in the same term can never
        both observe an unused vote — whichever transaction commits second
        sees the first one's vote and is refused.
        """
        _uint(term, "term")
        _text(candidate_id, "candidate_id")
        _uint(last_log_index, "last_log_index")
        _uint(last_log_term, "last_log_term")
        with self._write() as database:
            current = int(self._meta(database, "current_term"))
            voted_for = self._meta(database, "voted_for")
            if term < current:
                return False
            if term > current:
                voted_for = ""
            if voted_for not in ("", candidate_id):
                return False
            local_index = self._last_log_index(database)
            local_term = self._term_at_index(database, local_index) or 0
            if (last_log_term, last_log_index) < (local_term, local_index):
                # Refusing still adopts the higher term, but records no vote.
                if term > current:
                    self._set_metadata(database, current_term=term, voted_for="")
                return False
            self._set_metadata(database, current_term=term, voted_for=candidate_id)
            return True

    # -- log reads ---------------------------------------------------------

    def _snapshot_boundary(self, database: sqlite3.Connection) -> tuple[int, int]:
        return (
            int(self._meta(database, "last_snapshot_index")),
            int(self._meta(database, "last_snapshot_term")),
        )

    def _last_log_index(self, database: sqlite3.Connection) -> int:
        row = database.execute("SELECT MAX(log_index) FROM replica_log").fetchone()
        boundary, _term = self._snapshot_boundary(database)
        return max(boundary, int(row[0] or 0))

    def _term_at_index(self, database: sqlite3.Connection, index: int) -> int | None:
        if index == 0:
            return 0
        boundary, boundary_term = self._snapshot_boundary(database)
        if index == boundary and boundary:
            return boundary_term
        row = database.execute(
            "SELECT log_term FROM replica_log WHERE log_index=?", (index,)
        ).fetchone()
        return int(row[0]) if row is not None else None

    def last_log_index(self) -> int:
        with self._read() as database:
            return self._last_log_index(database)

    def last_log_term(self) -> int:
        with self._read() as database:
            return self._term_at_index(database, self._last_log_index(database)) or 0

    def term_at(self, index: int) -> int | None:
        with self._read() as database:
            return self._term_at_index(database, index)

    @staticmethod
    def _entry(row: sqlite3.Row) -> LogEntry:
        return LogEntry(
            log_index=int(row["log_index"]),
            log_term=int(row["log_term"]),
            command=AuthorityCommand.from_dict(json.loads(row["command_json"])),
        )

    def entries(self, *, after: int = 0, limit: int | None = None) -> tuple[LogEntry, ...]:
        query = "SELECT * FROM replica_log WHERE log_index>? ORDER BY log_index"
        parameters: tuple[object, ...] = (after,)
        if limit is not None:
            query += " LIMIT ?"
            parameters = (after, limit)
        with self._read() as database:
            rows = database.execute(query, parameters).fetchall()
        return tuple(self._entry(row) for row in rows)

    def entry_for_command(self, command_id: str) -> LogEntry | None:
        with self._read() as database:
            row = database.execute(
                "SELECT * FROM replica_log WHERE command_id=?", (command_id,)
            ).fetchone()
        return None if row is None else self._entry(row)

    def receipt_for_command(self, command_id: str) -> CommandReceipt | None:
        with self._read() as database:
            row = database.execute(
                "SELECT * FROM replica_receipts WHERE command_id=?", (command_id,)
            ).fetchone()
        if row is None:
            return None
        return CommandReceipt(
            command_id=str(row["command_id"]),
            content_hash=str(row["content_hash"]),
            log_index=int(row["log_index"]),
            log_term=int(row["log_term"]),
        )

    def command_receipts(self) -> tuple[CommandReceipt, ...]:
        with self._read() as database:
            rows = database.execute(
                "SELECT * FROM replica_receipts ORDER BY log_index, command_id"
            ).fetchall()
        return tuple(
            CommandReceipt(
                command_id=str(row["command_id"]),
                content_hash=str(row["content_hash"]),
                log_index=int(row["log_index"]),
                log_term=int(row["log_term"]),
            )
            for row in rows
        )

    # -- log writes --------------------------------------------------------

    @staticmethod
    def _record_receipt(
        database: sqlite3.Connection, receipt: CommandReceipt
    ) -> None:
        database.execute(
            "INSERT INTO replica_receipts(command_id,content_hash,log_index,log_term)"
            " VALUES(?,?,?,?) ON CONFLICT(command_id) DO UPDATE SET"
            " content_hash=excluded.content_hash,log_index=excluded.log_index,"
            "log_term=excluded.log_term",
            (
                receipt.command_id,
                receipt.content_hash,
                receipt.log_index,
                receipt.log_term,
            ),
        )
        database.execute(
            "DELETE FROM replica_receipts WHERE command_id IN ("
            " SELECT command_id FROM replica_receipts"
            " ORDER BY log_index DESC, command_id DESC LIMIT -1 OFFSET ?)",
            (MAX_COMMAND_RECEIPTS,),
        )

    def _conflict_index(self, database: sqlite3.Connection, index: int) -> int:
        """First index the leader should retry from after a prefix mismatch."""
        boundary, _term = self._snapshot_boundary(database)
        last = self._last_log_index(database)
        if index > last:
            return last + 1
        local_term = self._term_at_index(database, index)
        if local_term is None:
            return max(boundary + 1, 1)
        row = database.execute(
            "SELECT MIN(log_index) FROM replica_log WHERE log_term=? AND log_index<=?",
            (local_term, index),
        ).fetchone()
        first = int(row[0]) if row is not None and row[0] is not None else index
        return max(boundary + 1, first, 1)

    @staticmethod
    def _validate_incoming(
        entries: Sequence[LogEntry],
        *,
        prev_log_index: int,
        cluster_id: str,
        leader_term: int | None,
    ) -> None:
        """Finding 6: entries must be contiguous and valid before any write."""
        previous_term = None
        for offset, entry in enumerate(entries):
            expected = prev_log_index + 1 + offset
            if entry.log_index != expected:
                raise ControlPlaneError(
                    "append entries are not contiguous with the request prefix"
                )
            if entry.command.cluster_id != cluster_id:
                raise ControlPlaneError("append entry cluster identity mismatch")
            if leader_term is not None and entry.log_term > leader_term:
                raise ControlPlaneError("append entry term exceeds the leader term")
            if previous_term is not None and entry.log_term < previous_term:
                raise ControlPlaneError("append entry terms are not monotonic")
            previous_term = entry.log_term

    def append_entries(
        self,
        entries: Sequence[LogEntry],
        *,
        prev_log_index: int,
        prev_log_term: int,
        leader_commit: int,
        leader_term: int | None = None,
    ) -> int:
        """Apply one AppendEntries request atomically; return the match index.

        Finding 6: validation, the prefix check, conflict repair, and commit
        advancement all happen inside one transaction, and commit advancement
        is bounded by ``prev_log_index + len(entries)`` — the highest prefix
        this request actually proves — so a short heartbeat can never commit
        a divergent suffix the leader never sent.
        """
        _uint(prev_log_index, "prev_log_index")
        _uint(prev_log_term, "prev_log_term")
        _uint(leader_commit, "leader_commit")
        self._validate_incoming(
            entries,
            prev_log_index=prev_log_index,
            cluster_id=self.configuration.cluster_id,
            leader_term=leader_term,
        )
        entries = tuple(entries)
        proven = prev_log_index + len(entries)
        with self._write() as database:
            boundary, boundary_term = self._snapshot_boundary(database)
            commit_index = int(self._meta(database, "commit_index"))
            if prev_log_index < boundary:
                # We compacted past the prefix the leader assumed.  Everything
                # up to the boundary is committed authority we already hold, so
                # rebase the request onto the boundary rather than refusing it.
                if proven < boundary:
                    if leader_commit > commit_index:
                        self._set_metadata(
                            database,
                            commit_index=max(
                                commit_index, min(leader_commit, boundary)
                            ),
                        )
                    return boundary
                covering = entries[boundary - prev_log_index - 1]
                if covering.log_term != boundary_term:
                    raise LogConflict(
                        "append entries conflict with the snapshot boundary",
                        conflict_index=boundary + 1,
                    )
                entries = entries[boundary - prev_log_index :]
                prev_log_index, prev_log_term = boundary, boundary_term
            if self._term_at_index(database, prev_log_index) != prev_log_term:
                raise LogConflict(
                    "follower log prefix mismatch",
                    conflict_index=self._conflict_index(database, prev_log_index),
                )
            for entry in entries:
                existing = database.execute(
                    "SELECT log_term,command_id,content_hash FROM replica_log"
                    " WHERE log_index=?",
                    (entry.log_index,),
                ).fetchone()
                if existing is not None:
                    if (
                        existing["log_term"] == entry.log_term
                        and existing["content_hash"] == entry.command.content_hash
                    ):
                        continue
                    if entry.log_index <= commit_index:
                        raise LogConflict("committed log entry cannot be overwritten")
                    database.execute(
                        "DELETE FROM replica_log WHERE log_index>=?", (entry.log_index,)
                    )
                duplicate = database.execute(
                    "SELECT log_index,content_hash FROM replica_log WHERE command_id=?",
                    (entry.command.command_id,),
                ).fetchone()
                if duplicate is not None:
                    if duplicate["content_hash"] != entry.command.content_hash:
                        raise DuplicateCommandError("command ID payload conflict")
                    if duplicate["log_index"] != entry.log_index:
                        raise DuplicateCommandError(
                            "command ID already exists at another index"
                        )
                receipt = database.execute(
                    "SELECT content_hash,log_index FROM replica_receipts"
                    " WHERE command_id=?",
                    (entry.command.command_id,),
                ).fetchone()
                if (
                    receipt is not None
                    and receipt["content_hash"] != entry.command.content_hash
                ):
                    raise DuplicateCommandError("command ID payload conflict")
                database.execute(
                    "INSERT INTO replica_log"
                    "(log_index,log_term,command_id,command_json,content_hash)"
                    " VALUES(?,?,?,?,?)",
                    (
                        entry.log_index,
                        entry.log_term,
                        entry.command.command_id,
                        entry.command.canonical_bytes().decode(),
                        entry.command.content_hash,
                    ),
                )
            if leader_commit > commit_index:
                self._set_metadata(
                    database,
                    commit_index=max(commit_index, min(leader_commit, proven)),
                )
            return proven

    def append_local(self, entry: LogEntry) -> None:
        """Append one leader-authored entry at the head of the local log."""
        with self._write() as database:
            last = self._last_log_index(database)
            if entry.log_index != last + 1:
                raise LogConflict("leader append is not at the log head")
            if entry.command.cluster_id != self.configuration.cluster_id:
                raise ControlPlaneError("append entry cluster identity mismatch")
            duplicate = database.execute(
                "SELECT content_hash FROM replica_log WHERE command_id=?",
                (entry.command.command_id,),
            ).fetchone()
            if duplicate is not None:
                raise DuplicateCommandError("command ID already exists in the log")
            receipt = database.execute(
                "SELECT content_hash FROM replica_receipts WHERE command_id=?",
                (entry.command.command_id,),
            ).fetchone()
            if receipt is not None:
                raise DuplicateCommandError("command ID was already committed")
            database.execute(
                "INSERT INTO replica_log"
                "(log_index,log_term,command_id,command_json,content_hash)"
                " VALUES(?,?,?,?,?)",
                (
                    entry.log_index,
                    entry.log_term,
                    entry.command.command_id,
                    entry.command.canonical_bytes().decode(),
                    entry.command.content_hash,
                ),
            )

    def set_commit_index(self, index: int) -> None:
        with self._write() as database:
            commit_index = int(self._meta(database, "commit_index"))
            if index < commit_index or index > self._last_log_index(database):
                raise ControlPlaneError("invalid commit index")
            self._set_metadata(database, commit_index=index)

    # -- projection --------------------------------------------------------

    def projection(self) -> dict[str, Any]:
        with self._read() as database:
            row = database.execute(
                "SELECT state_json,state_digest FROM replica_projection"
                " WHERE singleton=1"
            ).fetchone()
        state = json.loads(row["state_json"])
        if row["state_digest"] != _state_digest(state):
            raise ControlPlaneError("local projection digest mismatch")
        return state

    def save_projection(
        self,
        state: Mapping[str, Any],
        last_applied: int,
        *,
        receipt: CommandReceipt | None = None,
    ) -> None:
        """Advance the projection, ``last_applied``, and the dedup receipt as one."""
        encoded = _canonical(dict(state), maximum=MAX_SNAPSHOT_BYTES).decode()
        digest = _state_digest(state)
        with self._write() as database:
            commit_index = int(self._meta(database, "commit_index"))
            if last_applied > commit_index:
                raise ControlPlaneError("last_applied cannot exceed commit_index")
            database.execute(
                "UPDATE replica_projection SET state_json=?,state_digest=?"
                " WHERE singleton=1",
                (encoded, digest),
            )
            self._set_metadata(database, last_applied=last_applied)
            if receipt is not None:
                self._record_receipt(database, receipt)

    # -- snapshots ---------------------------------------------------------

    def install_snapshot(self, snapshot: Snapshot) -> None:
        """Install a snapshot without ever losing committed authority.

        Finding 2.  The snapshot is refused when it is older than the local
        compaction boundary, conflicts with it at an equal index, is stale
        relative to already applied authority, or disagrees with committed
        local history.  A compatible local suffix is retained and replayed
        from the snapshot; an incompatible uncommitted suffix is discarded.
        """
        if (
            snapshot.cluster_id != self.configuration.cluster_id
            or snapshot.voter_configuration != self.configuration
        ):
            raise ControlPlaneError("incompatible authority snapshot")
        index = snapshot.last_included_index
        term = snapshot.last_included_term
        encoded = _canonical(snapshot.to_dict(), maximum=MAX_SNAPSHOT_BYTES).decode()
        with self._write() as database:
            boundary, boundary_term = self._snapshot_boundary(database)
            commit_index = int(self._meta(database, "commit_index"))
            last_applied = int(self._meta(database, "last_applied"))
            if index < boundary:
                raise ControlPlaneError("older authority snapshot rejected")
            if index == boundary and boundary and term != boundary_term:
                raise ControlPlaneError("conflicting authority snapshot rejected")
            if index < last_applied:
                raise ControlPlaneError(
                    "older authority snapshot rejected: applied authority is ahead"
                )
            local_term = self._term_at_index(database, index)
            compatible = local_term == term
            if local_term is not None and not compatible and index <= commit_index:
                raise ControlPlaneError("snapshot conflicts with committed authority")
            if not compatible and commit_index > index:
                raise ControlPlaneError("snapshot would discard committed authority")
            if compatible:
                database.execute(
                    "DELETE FROM replica_log WHERE log_index<=?", (index,)
                )
                new_commit = max(commit_index, index)
            else:
                database.execute("DELETE FROM replica_log")
                new_commit = index
            database.execute(
                "INSERT INTO replica_snapshot(singleton,snapshot_json,snapshot_digest)"
                " VALUES(1,?,?) ON CONFLICT(singleton) DO UPDATE SET"
                " snapshot_json=excluded.snapshot_json,"
                "snapshot_digest=excluded.snapshot_digest",
                (encoded, snapshot.digest),
            )
            database.execute(
                "UPDATE replica_projection SET state_json=?,state_digest=?"
                " WHERE singleton=1",
                (
                    _canonical(snapshot.state, maximum=MAX_SNAPSHOT_BYTES).decode(),
                    snapshot.digest,
                ),
            )
            for receipt in snapshot.command_receipts:
                self._record_receipt(database, receipt)
            # The projection now *is* the snapshot state, so last_applied is
            # exactly the snapshot index; any retained committed suffix is
            # replayed afterwards by ReplicaNode.apply_committed().
            self._set_metadata(
                database,
                last_snapshot_index=index,
                last_snapshot_term=term,
                last_applied=index,
                commit_index=new_commit,
            )

    def snapshot(self) -> Snapshot | None:
        with self._read() as database:
            row = database.execute(
                "SELECT snapshot_json FROM replica_snapshot WHERE singleton=1"
            ).fetchone()
        return Snapshot.from_dict(json.loads(row[0])) if row is not None else None


# --------------------------------------------------------------------------
# Finding 5: structured term-bearing protocol responses
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class VoteResponse:
    term: int
    granted: bool
    voter_id: str = ""


@dataclass(frozen=True)
class AppendResponse:
    term: int
    success: bool
    match_index: int = 0
    conflict_index: int = 0


@dataclass(frozen=True)
class SnapshotResponse:
    term: int
    success: bool
    match_index: int = 0


class ReplicationTransport(Protocol):
    def request_vote(
        self,
        target: str,
        *,
        candidate_id: str,
        term: int,
        last_log_index: int,
        last_log_term: int,
        cluster_id: str,
    ) -> VoteResponse: ...

    def append_entries(
        self,
        target: str,
        *,
        leader_id: str,
        leader_term: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: tuple[LogEntry, ...],
        leader_commit: int,
        cluster_id: str,
    ) -> AppendResponse: ...

    def install_snapshot(
        self,
        target: str,
        *,
        leader_id: str,
        leader_term: int,
        snapshot: Snapshot,
        cluster_id: str,
    ) -> SnapshotResponse: ...


_TRANSPORT_FAILURES = (ControlPlaneError, OSError, TimeoutError)


class ReplicaNode:
    """One fixed-configuration consensus participant."""

    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

    def __init__(
        self,
        voter_id: str,
        store: PersistentReplicaStore,
        *,
        state_machine: AuthorityStateMachine | None = None,
    ) -> None:
        _text(voter_id, "voter_id")
        if voter_id not in store.configuration.voter_ids:
            raise ControlPlaneError("replica is not in the static voter set")
        self.voter_id = voter_id
        self.store = store
        self.configuration = store.configuration
        self.state_machine = state_machine or AuthorityStateMachine(self.configuration)
        self.role = self.FOLLOWER
        self.leader_id: str | None = None
        self.rejected_commands: dict[int, str] = {}
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}
        # Finding 5.  ``_state_lock`` serializes local consensus transitions
        # and is never held across a transport call.  ``_drive_lock``
        # serializes this node's own outbound rounds and is never taken by an
        # inbound handler, so two replicas calling each other cannot deadlock.
        self._state_lock = threading.RLock()
        self._drive_lock = threading.RLock()

    @property
    def quorum(self) -> int:
        return self.configuration.quorum

    @property
    def state(self) -> dict[str, Any]:
        return self.store.projection()

    @property
    def peers(self) -> tuple[str, ...]:
        return tuple(
            voter for voter in self.configuration.voter_ids if voter != self.voter_id
        )

    @property
    def fencing_token(self) -> FencingToken:
        return FencingToken(
            cluster_id=self.configuration.cluster_id,
            cluster_term=self.store.current_term,
            leader_id=self.leader_id or "",
            commit_index=self.store.commit_index,
            fencing_epoch=self.store.fencing_epoch,
        )

    # -- local consensus transitions --------------------------------------

    def _step_down(self, term: int) -> None:
        with self._state_lock:
            self.store.observe_term(term)
            self.role = self.FOLLOWER
            self.leader_id = None
            self._next_index.clear()
            self._match_index.clear()

    def _observe_response_term(self, term: int) -> bool:
        """Finding 5: any higher term in a response steps us down at once."""
        if term > self.store.current_term:
            self._step_down(term)
            return True
        return False

    def _still_leader(self, term: int) -> bool:
        with self._state_lock:
            return (
                self.role == self.LEADER
                and self.leader_id == self.voter_id
                and self.store.current_term == term
            )

    # -- inbound RPCs ------------------------------------------------------

    def receive_vote_request(
        self,
        *,
        candidate_id: str,
        term: int,
        last_log_index: int,
        last_log_term: int,
        cluster_id: str,
    ) -> VoteResponse:
        with self._state_lock:
            current = self.store.current_term
            if (
                cluster_id != self.configuration.cluster_id
                or candidate_id not in self.configuration.voter_ids
            ):
                return VoteResponse(current, False, self.voter_id)
            if term < current:
                return VoteResponse(current, False, self.voter_id)
            if term > current:
                self._step_down(term)
            granted = self.store.try_grant_vote(
                term=term,
                candidate_id=candidate_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            )
            if granted:
                self.role = self.FOLLOWER
                self.leader_id = None
            return VoteResponse(self.store.current_term, granted, self.voter_id)

    def receive_append_entries(
        self,
        *,
        leader_id: str,
        leader_term: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: tuple[LogEntry, ...],
        leader_commit: int,
        cluster_id: str,
    ) -> AppendResponse:
        with self._state_lock:
            current = self.store.current_term
            if (
                cluster_id != self.configuration.cluster_id
                or leader_id not in self.configuration.voter_ids
            ):
                return AppendResponse(current, False)
            if leader_term < current:
                return AppendResponse(current, False)
            if leader_term > current:
                self._step_down(leader_term)
            self.role = self.FOLLOWER
            self.leader_id = leader_id
            try:
                match_index = self.store.append_entries(
                    entries,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    leader_commit=leader_commit,
                    leader_term=leader_term,
                )
            except LogConflict as exc:
                return AppendResponse(
                    self.store.current_term,
                    False,
                    conflict_index=exc.conflict_index,
                )
            except (ControlPlaneError, sqlite3.Error):
                return AppendResponse(self.store.current_term, False)
            self.apply_committed()
            return AppendResponse(self.store.current_term, True, match_index)

    def receive_install_snapshot(
        self,
        *,
        leader_id: str,
        leader_term: int,
        snapshot: Snapshot,
        cluster_id: str,
    ) -> SnapshotResponse:
        with self._state_lock:
            current = self.store.current_term
            if (
                cluster_id != self.configuration.cluster_id
                or leader_id not in self.configuration.voter_ids
            ):
                return SnapshotResponse(current, False)
            if leader_term < current:
                return SnapshotResponse(current, False)
            if leader_term > current:
                self._step_down(leader_term)
            self.role = self.FOLLOWER
            self.leader_id = leader_id
            try:
                self.store.install_snapshot(snapshot)
            except (ControlPlaneError, sqlite3.Error):
                return SnapshotResponse(self.store.current_term, False)
            self.apply_committed()
            return SnapshotResponse(
                self.store.current_term, True, snapshot.last_included_index
            )

    # -- committed replay --------------------------------------------------

    def apply_committed(self) -> tuple[dict[str, Any], ...]:
        """Replay committed entries; never gets stuck on an invalid command."""
        with self._state_lock:
            emitted: list[dict[str, Any]] = []
            commit_index = self.store.commit_index
            for entry in self.store.entries(after=self.store.last_applied):
                if entry.log_index > commit_index:
                    break
                state = self.store.projection()
                state, events, rejection = self.state_machine.apply_committed(
                    state, entry.command
                )
                if rejection is not None:
                    self.rejected_commands[entry.log_index] = rejection
                emitted.extend(events)
                self.store.save_projection(
                    state,
                    entry.log_index,
                    receipt=CommandReceipt(
                        command_id=entry.command.command_id,
                        content_hash=entry.command.content_hash,
                        log_index=entry.log_index,
                        log_term=entry.log_term,
                    ),
                )
            return tuple(emitted)

    # -- elections ---------------------------------------------------------

    def start_election(self, transport: ReplicationTransport) -> bool:
        with self._drive_lock:
            with self._state_lock:
                self.role = self.CANDIDATE
                term = self.store.current_term + 1
                self.store.set_term_and_vote(term, self.voter_id)
                last_index = self.store.last_log_index()
                last_term = self.store.term_at(last_index) or 0
            votes = 1
            for target in self.peers:
                try:
                    response = transport.request_vote(
                        target,
                        candidate_id=self.voter_id,
                        term=term,
                        last_log_index=last_index,
                        last_log_term=last_term,
                        cluster_id=self.configuration.cluster_id,
                    )
                except _TRANSPORT_FAILURES:
                    continue
                if self._observe_response_term(response.term):
                    return False
                if response.granted:
                    votes += 1
            with self._state_lock:
                # Finding 5: a delayed election that completes after the term
                # moved on, or after we were stepped down, cannot take office.
                if self.role != self.CANDIDATE or self.store.current_term != term:
                    if self.role == self.CANDIDATE:
                        self.role = self.FOLLOWER
                    return False
                if votes < self.quorum:
                    self.role = self.FOLLOWER
                    return False
                self.role = self.LEADER
                self.leader_id = self.voter_id
                head = self.store.last_log_index()
                self._next_index = {target: head + 1 for target in self.peers}
                self._match_index = {target: 0 for target in self.peers}
                self.store.increment_fencing_epoch()
                return True

    # -- leader replication ------------------------------------------------

    def _replicate_to(
        self, target: str, transport: ReplicationTransport, *, term: int
    ) -> bool:
        """Bring one follower up to our log head, backtracking as needed.

        Finding 3: handles a follower that missed one or many entries, one
        that carries a divergent uncommitted suffix, and one that has fallen
        behind the compaction boundary and needs a snapshot first.
        """
        for _round in range(MAX_CATCHUP_ROUNDS):
            if not self._still_leader(term):
                return False
            head = self.store.last_log_index()
            boundary = self.store.last_snapshot_index
            next_index = max(1, self._next_index.get(target, head + 1))
            if next_index <= boundary:
                if not self._send_snapshot(target, transport, term=term):
                    return False
                continue
            prev_index = next_index - 1
            prev_term = self.store.term_at(prev_index)
            if prev_term is None:
                if not self._send_snapshot(target, transport, term=term):
                    return False
                continue
            entries = self.store.entries(after=prev_index, limit=MAX_APPEND_BATCH)
            try:
                response = transport.append_entries(
                    target,
                    leader_id=self.voter_id,
                    leader_term=term,
                    prev_log_index=prev_index,
                    prev_log_term=prev_term,
                    entries=entries,
                    leader_commit=self.store.commit_index,
                    cluster_id=self.configuration.cluster_id,
                )
            except _TRANSPORT_FAILURES:
                return False
            if self._observe_response_term(response.term):
                return False
            if response.success:
                match = max(response.match_index, prev_index + len(entries))
                self._match_index[target] = match
                self._next_index[target] = match + 1
                if match >= head:
                    return True
                continue
            # Backtrack, honouring the follower's hint but always making
            # progress so a lying hint cannot spin the loop.  When no smaller
            # prefix is left to try, fail closed rather than retry forever.
            hint = response.conflict_index or prev_index
            candidate = max(1, min(hint, prev_index))
            if candidate >= next_index:
                return False
            self._next_index[target] = candidate
        return False

    def _send_snapshot(
        self, target: str, transport: ReplicationTransport, *, term: int
    ) -> bool:
        snapshot = self.store.snapshot()
        if snapshot is None:
            return False
        try:
            response = transport.install_snapshot(
                target,
                leader_id=self.voter_id,
                leader_term=term,
                snapshot=snapshot,
                cluster_id=self.configuration.cluster_id,
            )
        except _TRANSPORT_FAILURES:
            return False
        if self._observe_response_term(response.term):
            return False
        if not response.success:
            return False
        match = max(response.match_index, snapshot.last_included_index)
        self._match_index[target] = match
        self._next_index[target] = match + 1
        return True

    def synchronize(self, transport: ReplicationTransport) -> int:
        """Bring every reachable follower up to date; return how many matched."""
        with self._drive_lock:
            term = self.store.current_term
            if not self._still_leader(term):
                raise StaleTerm("only the current leader may replicate authority")
            matched = []
            for target in self.peers:
                if self._replicate_to(target, transport, term=term):
                    matched.append(target)
            self._advance_commit(term)
            self.apply_committed()
            self._publish_commit(transport, term=term, targets=matched)
            return len(matched)

    def _advance_commit(self, term: int) -> int:
        """Commit the highest index a quorum stores from the current term."""
        with self._state_lock:
            if not self._still_leader(term):
                return self.store.commit_index
            matches = [self.store.last_log_index()]
            matches.extend(self._match_index.get(target, 0) for target in self.peers)
            matches.sort(reverse=True)
            candidate = matches[self.quorum - 1]
            commit_index = self.store.commit_index
            # Raft safety: a leader only commits an entry from its own term.
            if candidate > commit_index and self.store.term_at(candidate) == term:
                self.store.set_commit_index(candidate)
                return candidate
            return commit_index

    def _publish_commit(
        self, transport: ReplicationTransport, *, term: int, targets: list[str]
    ) -> None:
        commit_index = self.store.commit_index
        # A peer that failed replication in this round needs catch-up on the
        # next round, not a second blocking attempt before healthy peers learn
        # the commit index.
        for target in targets:
            match = self._match_index.get(target, 0)
            if match <= 0:
                continue
            prev_term = self.store.term_at(match)
            if prev_term is None:
                continue
            try:
                response = transport.append_entries(
                    target,
                    leader_id=self.voter_id,
                    leader_term=term,
                    prev_log_index=match,
                    prev_log_term=prev_term,
                    entries=(),
                    leader_commit=commit_index,
                    cluster_id=self.configuration.cluster_id,
                )
            except _TRANSPORT_FAILURES:
                continue
            self._observe_response_term(response.term)

    # -- proposals ---------------------------------------------------------

    def pending_state(self) -> dict[str, Any]:
        """The logical state after every entry currently in the local log.

        Finding 4: a new command is validated against this ordered state, not
        against the last applied projection, so validation matches the order
        the command would actually be applied in.
        """
        state = self.store.projection()
        for entry in self.store.entries(after=self.store.last_applied):
            state, _events, _rejection = self.state_machine.apply_committed(
                state, entry.command
            )
        return state

    def propose(
        self, command: AuthorityCommand, transport: ReplicationTransport
    ) -> tuple[LogEntry, tuple[dict[str, Any], ...]]:
        """Append and commit one command, or raise.

        Finding 1: this returns successfully only when the entry is durably
        committed.  A retry of a command whose entry exists but is still
        uncommitted re-drives replication and raises
        :class:`QuorumUnavailable` if quorum is still out of reach.
        """
        with self._drive_lock:
            term = self.store.current_term
            if not self._still_leader(term):
                raise StaleTerm("only the current leader may append authority")
            if command.cluster_id != self.configuration.cluster_id:
                raise ControlPlaneError("command cluster identity mismatch")

            # A committed receipt makes the retry idempotent, and outlives
            # both restart and snapshot compaction.
            receipt = self.store.receipt_for_command(command.command_id)
            if receipt is not None:
                if receipt.content_hash != command.content_hash:
                    raise DuplicateCommandError("command ID payload conflict")
                existing = self.store.entry_for_command(command.command_id)
                committed = existing or LogEntry(
                    receipt.log_index, receipt.log_term, command
                )
                return committed, self.apply_committed()

            entry = self.store.entry_for_command(command.command_id)
            if entry is not None:
                if entry.command.content_hash != command.content_hash:
                    raise DuplicateCommandError("command ID payload conflict")
                if entry.log_index <= self.store.commit_index:
                    return entry, self.apply_committed()
                if entry.log_term != term:
                    # Committing a prior term's entry directly would violate
                    # Raft's leader-completeness rule; fail closed instead of
                    # reporting a success we cannot prove.
                    raise QuorumUnavailable(
                        "pending authority predates the current leader term"
                    )
            else:
                # Finding 4: reject domain-invalid commands before they can
                # ever reach the log, validated against the ordered state the
                # command would actually be applied to.
                self.state_machine.apply(self.pending_state(), command)
                entry = LogEntry(self.store.last_log_index() + 1, term, command)
                self.store.append_local(entry)

            matched = []
            for target in self.peers:
                if self._replicate_to(target, transport, term=term):
                    matched.append(target)

            # Finding 5: revalidate the captured term and role after every
            # transport call before treating anything as committed.
            if not self._still_leader(term):
                raise StaleTerm("leadership was lost during the proposal")

            committed_to = self._advance_commit(term)
            if committed_to < entry.log_index:
                raise QuorumUnavailable(
                    "authority command was not committed by quorum"
                )
            emitted = self.apply_committed()
            self._publish_commit(transport, term=term, targets=matched)
            return entry, emitted

    # -- snapshots ---------------------------------------------------------

    def create_snapshot(self) -> Snapshot:
        if self.store.last_applied != self.store.commit_index:
            raise ControlPlaneError("snapshot requires all committed entries applied")
        index = self.store.last_applied
        term = self.store.term_at(index) or 0
        state = self.state
        return Snapshot(
            cluster_id=self.configuration.cluster_id,
            voter_configuration=self.configuration,
            last_included_index=index,
            last_included_term=term,
            state=state,
            digest=_state_digest(state),
            command_receipts=self.store.command_receipts(),
        )

    def compact(self) -> Snapshot:
        """Snapshot the applied state and discard the log it covers."""
        snapshot = self.create_snapshot()
        self.store.install_snapshot(snapshot)
        return snapshot

    def install_snapshot(self, snapshot: Snapshot) -> None:
        with self._state_lock:
            self.store.install_snapshot(snapshot)
            self.apply_committed()


class InProcessTransport:
    """Deterministic test transport; intentionally has no network security."""

    def __init__(self, replicas: Mapping[str, ReplicaNode]) -> None:
        self.replicas = dict(replicas)
        self.blocked: set[tuple[str, str]] = set()

    def _reachable(self, source: str, target: str) -> bool:
        return (source, target) not in self.blocked

    def partition(self, voter_id: str) -> None:
        for other in self.replicas:
            if other == voter_id:
                continue
            self.blocked.add((voter_id, other))
            self.blocked.add((other, voter_id))

    def heal(self, voter_id: str | None = None) -> None:
        if voter_id is None:
            self.blocked.clear()
            return
        self.blocked = {
            link for link in self.blocked if voter_id not in link
        }

    def request_vote(self, target: str, **request: Any) -> VoteResponse:
        source = str(request["candidate_id"])
        if not self._reachable(source, target):
            raise TimeoutError(f"{source} cannot reach {target}")
        return self.replicas[target].receive_vote_request(**request)

    def append_entries(self, target: str, **request: Any) -> AppendResponse:
        source = str(request["leader_id"])
        if not self._reachable(source, target):
            raise TimeoutError(f"{source} cannot reach {target}")
        return self.replicas[target].receive_append_entries(**request)

    def install_snapshot(self, target: str, **request: Any) -> SnapshotResponse:
        source = str(request["leader_id"])
        if not self._reachable(source, target):
            raise TimeoutError(f"{source} cannot reach {target}")
        return self.replicas[target].receive_install_snapshot(**request)


__all__ = [
    "INITIAL_TERM",
    "MAX_COMMAND_RECEIPTS",
    "SNAPSHOT_SCHEMA",
    "AppendResponse",
    "AuthorityCommand",
    "AuthorityStateMachine",
    "CommandReceipt",
    "ControlPlaneError",
    "DuplicateCommandError",
    "FencingToken",
    "InProcessTransport",
    "LogConflict",
    "LogEntry",
    "PersistentReplicaStore",
    "QuorumUnavailable",
    "ReplicaNode",
    "ReplicationTransport",
    "Snapshot",
    "SnapshotResponse",
    "StaleTerm",
    "VoteResponse",
    "VoterConfiguration",
]
