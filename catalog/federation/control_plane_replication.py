"""Small crash-fault-tolerant authority log for Federation control state.

Phase 1 deliberately stops at the consensus boundary.  It provides durable
replica metadata, a bounded command envelope, a deterministic state machine,
and an in-process transport seam for later authenticated inter-host wiring.
SQLite is only the local durable store; the replicated log order is the
authority.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from catalog.federation.redaction import redact_nonpublic_data

SCHEMA = "fcp.control-plane.replication.v1"
COMMAND_SCHEMA = "fcp.control-plane.command.v1"
STATE_SCHEMA = "fcp.control-plane.authority-state.v1"
SNAPSHOT_SCHEMA = "fcp.control-plane.snapshot.v1"
FENCING_SCHEMA = "fcp.control-plane.fencing.v1"
SCHEMA_VERSION = 1

MAX_ID_BYTES = 512
MAX_COMMAND_BYTES = 128 * 1024
MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024
MAX_VOTERS = 3
MAX_COMMAND_RECEIPTS = 4096
QUORUM = 2
INITIAL_TERM = 0
INITIAL_FENCING_EPOCH = 0

_ID_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,512}$")
_COMMAND_TYPES = frozenset(
    {
        "FEDERATION_GENESIS",
        "NODE_ENROLL",
        "NODE_REVOKE",
        "SESSION_CREATE",
        "SESSION_MEMBER_ADD",
        "SESSION_MEMBER_REMOVE",
        "LEADER_TRANSITION",
        "CAPABILITY_DECLARE",
        "CAPABILITY_WITHDRAW",
    }
)


class ControlPlaneError(ValueError):
    """Base error for malformed or unsafe control-plane operations."""


class DuplicateCommandError(ControlPlaneError):
    """A command ID was reused for different canonical content."""


class QuorumUnavailable(ControlPlaneError):
    """A leader could not commit an entry with the configured quorum."""


class StaleTerm(ControlPlaneError):
    """A request used an older consensus term."""


class LogConflict(ControlPlaneError):
    """A request attempted to overwrite committed authority."""


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
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 2**63:
        raise ControlPlaneError(f"{field} must be a non-negative integer")
    return value


def _payload(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ControlPlaneError("command payload must be an object")
    _canonical(value, maximum=MAX_COMMAND_BYTES)
    return value


_FIELDS = {
    "FEDERATION_GENESIS": "federation_id session_id creator_node_id display_name nodes members voter_configuration",
    "NODE_ENROLL": "node_id display_name public_key",
    "NODE_REVOKE": "node_id reason occurred_at",
    "SESSION_CREATE": "session_id creator_node_id display_name occurred_at",
    "SESSION_MEMBER_ADD": "session_id node_id occurred_at",
    "SESSION_MEMBER_REMOVE": "session_id node_id occurred_at",
    "LEADER_TRANSITION": "session_id previous_leader_node_id leader_node_id term reason occurred_at",
    "CAPABILITY_DECLARE": "session_id capability_id node_id capability_type display_name occurred_at",
    "CAPABILITY_WITHDRAW": "session_id capability_id node_id occurred_at",
}


def _validate_payload(kind: str, payload: dict[str, Any]) -> None:
    if set(payload) != set(_FIELDS[kind].split()):
        raise ControlPlaneError("command payload has missing or unknown fields")
    for key, value in payload.items():
        if key in {"nodes", "members", "voter_configuration"}:
            continue
        if key == "term":
            if _uint(value, key) == 0:
                raise ControlPlaneError("leadership term must be positive")
        else:
            _text(value, key)
        if key == "occurred_at":
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if (
                    parsed.utcoffset() is None
                    or parsed.utcoffset().total_seconds() != 0
                ):
                    raise ValueError
            except ValueError as exc:
                raise ControlPlaneError("occurred_at must be a UTC timestamp") from exc
    if kind == "FEDERATION_GENESIS":
        VoterConfiguration.from_dict(payload["voter_configuration"])
        if not isinstance(payload["nodes"], list) or not isinstance(
            payload["members"], list
        ):
            raise ControlPlaneError("genesis nodes/members must be arrays")
        for node in payload["nodes"]:
            if not isinstance(node, dict):
                raise ControlPlaneError("genesis node must be an object")
            _validate_payload("NODE_ENROLL", node)
        for member in payload["members"]:
            _text(member, "member")


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
        if set(value) != {"schema", "cluster_id", "voter_ids"}:
            raise ControlPlaneError("unknown voter configuration fields")
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
        _payload(self.payload)
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
        if set(value) != {
            "schema",
            "command_id",
            "command_type",
            "cluster_id",
            "issued_by",
            "payload",
        }:
            raise ControlPlaneError("unknown command envelope fields")
        return cls(
            command_id=value.get("command_id"),
            command_type=value.get("command_type"),
            cluster_id=value.get("cluster_id"),
            issued_by=value.get("issued_by"),
            payload=_payload(value.get("payload")),
        )


@dataclass(frozen=True)
class LogEntry:
    log_index: int
    log_term: int
    command: AuthorityCommand

    def __post_init__(self) -> None:
        if _uint(self.log_index, "log_index") < 1:
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

    def __post_init__(self) -> None:
        _uint(self.last_included_index, "last_included_index")
        _uint(self.last_included_term, "last_included_term")
        if self.cluster_id != self.voter_configuration.cluster_id:
            raise ControlPlaneError("snapshot cluster identity mismatch")
        _canonical(self.state, maximum=MAX_SNAPSHOT_BYTES)
        if self.digest != _state_digest(self.state):
            raise ControlPlaneError("snapshot digest mismatch")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": SNAPSHOT_SCHEMA,
            "cluster_id": self.cluster_id,
            "voter_configuration": self.voter_configuration.to_dict(),
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "state": self.state,
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> Snapshot:
        if not isinstance(value, dict) or value.get("schema") != SNAPSHOT_SCHEMA:
            raise ControlPlaneError("unsupported snapshot schema")
        state = value.get("state")
        if not isinstance(state, dict):
            raise ControlPlaneError("snapshot state must be an object")
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
        "sessions": {},
        "nodes": {},
        "memberships": {},
        "revocations": {},
        "leaders": {},
        "capabilities": {},
        "session_events": {},
        "command_receipts": {},
        "voter_configuration": configuration.to_dict(),
    }


def _copy_state(state: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(_canonical(dict(state), maximum=MAX_SNAPSHOT_BYTES))


def _require_state(state: Mapping[str, Any], configuration: VoterConfiguration) -> None:
    if (
        state.get("schema") != STATE_SCHEMA
        or state.get("cluster_id") != configuration.cluster_id
    ):
        raise ControlPlaneError("authority state identity mismatch")


def _event(
    state: dict[str, Any],
    *,
    session_id: str,
    event_type: str,
    actor_node_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    occurred_at = payload.get("occurred_at")
    if not isinstance(occurred_at, str) or not occurred_at:
        raise ControlPlaneError("projected event requires committed occurred_at")
    events = state["session_events"].setdefault(session_id, [])
    revision = len(events) + 1
    projected = {
        "session_id": session_id,
        "revision": revision,
        "event_type": event_type,
        "actor_node_id": actor_node_id,
        "occurred_at": occurred_at,
        "payload": {
            key: payload[key] for key in _PUBLIC_FIELDS[event_type] if key in payload
        },
    }
    if redact_nonpublic_data(projected) != projected:
        raise ControlPlaneError("nonpublic public-event projection")
    events.append(projected)
    return projected


_PUBLIC_FIELDS = {
    "session.created": ("session_id", "creator_node_id", "display_name"),
    "node.joined": ("node_id",),
    "node.left": ("node_id",),
    "session.leader.changed": (
        "previous_leader_node_id",
        "leader_node_id",
        "term",
        "reason",
    ),
    "capability.registered": (
        "capability_id",
        "node_id",
        "capability_type",
        "display_name",
    ),
    "capability.status.changed": ("capability_id", "node_id"),
}


class AuthorityStateMachine:
    """Pure deterministic application of committed Phase 1 commands."""

    def __init__(self, configuration: VoterConfiguration) -> None:
        self.configuration = configuration

    def initial_state(self) -> dict[str, Any]:
        return _empty_state(self.configuration)

    def apply(
        self,
        state: Mapping[str, Any],
        command: AuthorityCommand,
        *,
        log_index: int | None = None,
        log_term: int = 0,
    ) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
        # Revalidate at the trust boundary even if a caller mutated a nested dict.
        command = AuthorityCommand.from_dict(json.loads(command.canonical_bytes()))
        receipt = state["command_receipts"].get(command.command_id)
        if receipt is not None:
            if receipt["digest"] != command.content_hash:
                raise DuplicateCommandError("command ID payload conflict")
            return _copy_state(state), ()
        if len(state["command_receipts"]) >= MAX_COMMAND_RECEIPTS:
            raise ControlPlaneError("Phase 1 command receipt capacity exhausted")
        result, events = self._apply_domain(state, command)
        result["command_receipts"][command.command_id] = {
            "digest": command.content_hash,
            "index": log_index
            if log_index is not None
            else len(state["command_receipts"]) + 1,
            "term": log_term,
        }
        _canonical(result, maximum=MAX_SNAPSHOT_BYTES)
        return result, events

    def _apply_domain(
        self, state: Mapping[str, Any], command: AuthorityCommand
    ) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
        _require_state(state, self.configuration)
        if command.cluster_id != self.configuration.cluster_id:
            raise ControlPlaneError("command cluster identity mismatch")
        next_state = _copy_state(state)
        emitted: list[dict[str, Any]] = []
        payload = command.payload
        kind = command.command_type

        if kind == "FEDERATION_GENESIS":
            if next_state["federation_id"] is not None:
                raise ControlPlaneError("conflicting second genesis")
            if (
                VoterConfiguration.from_dict(payload["voter_configuration"])
                != self.configuration
            ):
                raise ControlPlaneError("genesis voter configuration mismatch")
            federation_id = _text(payload.get("federation_id"), "federation_id")
            session_id = _text(payload.get("session_id"), "session_id")
            creator = _text(payload.get("creator_node_id"), "creator_node_id")
            node_ids = [node["node_id"] for node in payload["nodes"]]
            keys = [node["public_key"] for node in payload["nodes"]]
            members = payload["members"]
            if (
                creator != command.issued_by
                or creator not in node_ids
                or len(set(node_ids)) != len(node_ids)
                or len(set(keys)) != len(keys)
                or len(set(members)) != len(members)
                or creator not in members
                or not set(self.configuration.voter_ids).issubset(node_ids)
            ):
                raise ControlPlaneError("invalid genesis identities/members")
            next_state["federation_id"] = federation_id
            next_state["sessions"][session_id] = {
                "display_name": _text(payload.get("display_name"), "display_name"),
                "creator_node_id": creator,
                "revision": 0,
            }
            next_state["leaders"][session_id] = {
                "creator_node_id": creator,
                "leader_node_id": creator,
                "term": 1,
            }
            next_state["memberships"][session_id] = {creator: True}
            for node in payload.get("nodes", []):
                if not isinstance(node, dict):
                    raise ControlPlaneError("genesis nodes must be objects")
                node_id = _text(node.get("node_id"), "node_id")
                next_state["nodes"][node_id] = dict(node)
            for node_id in payload.get("members", [creator]):
                node_id = _text(node_id, "member_node_id")
                if node_id not in next_state["nodes"]:
                    raise ControlPlaneError("genesis member is not enrolled")
                next_state["memberships"][session_id][node_id] = True
            imported = payload.get("session_events", [])
            if imported:
                if not isinstance(imported, list):
                    raise ControlPlaneError("genesis session_events must be an array")
                events = []
                for expected, item in enumerate(imported, start=1):
                    if not isinstance(item, dict) or item.get("revision") != expected:
                        raise ControlPlaneError(
                            "genesis event revisions are not contiguous"
                        )
                    events.append(dict(item))
                next_state["session_events"][session_id] = events
                next_state["sessions"][session_id]["revision"] = len(events)
            return next_state, ()

        if next_state["federation_id"] is None:
            raise ControlPlaneError("Federation genesis is required first")

        actor = command.issued_by
        if actor not in next_state["nodes"] or actor in next_state["revocations"]:
            raise ControlPlaneError("issuing actor is not eligible")
        session = payload.get("session_id")
        if (
            kind in {"NODE_ENROLL", "NODE_REVOKE"}
            and actor not in self.configuration.voter_ids
        ):
            raise ControlPlaneError("node authority requires a configured voter")
        if kind == "SESSION_CREATE" and payload["creator_node_id"] != actor:
            raise ControlPlaneError("session creator must be issuing actor")
        if session is not None and kind != "SESSION_CREATE":
            if next_state["memberships"].get(session, {}).get(actor) is not True:
                raise ControlPlaneError("actor is not an active session member")
            if (
                kind in {"SESSION_MEMBER_ADD", "SESSION_MEMBER_REMOVE"}
                and next_state["leaders"][session]["leader_node_id"] != actor
            ):
                raise ControlPlaneError("membership mutation requires session leader")
        target = payload.get("node_id")
        if kind in {"SESSION_MEMBER_ADD", "CAPABILITY_DECLARE", "CAPABILITY_WITHDRAW"}:
            if target not in next_state["nodes"] or target in next_state["revocations"]:
                raise ControlPlaneError("target node is not eligible")
        if kind in {"CAPABILITY_DECLARE", "CAPABILITY_WITHDRAW"}:
            if (
                target != actor
                or next_state["memberships"].get(session, {}).get(target) is not True
            ):
                raise ControlPlaneError("capability mutation requires eligible owner")
            existing_cap = (
                next_state["capabilities"]
                .get(session, {})
                .get(payload["capability_id"])
            )
            if existing_cap is not None and existing_cap["node_id"] != actor:
                raise ControlPlaneError("capability owner conflict")

        if kind == "NODE_ENROLL":
            node_id = _text(payload.get("node_id"), "node_id")
            existing = next_state["nodes"].get(node_id)
            candidate = dict(payload)
            if node_id in next_state["revocations"] or any(
                node["public_key"] == payload["public_key"] and other != node_id
                for other, node in next_state["nodes"].items()
            ):
                raise ControlPlaneError("node identity is revoked or contradictory")
            if existing is not None and existing != candidate:
                raise ControlPlaneError("node identity conflict")
            next_state["nodes"][node_id] = candidate

        elif kind == "NODE_REVOKE":
            node_id = _text(payload.get("node_id"), "node_id")
            if node_id not in next_state["nodes"]:
                raise ControlPlaneError("cannot revoke unknown node")
            next_state["revocations"][node_id] = {
                "reason": str(payload.get("reason") or "revoked"),
                "occurred_at": payload.get("occurred_at"),
            }

        elif kind == "SESSION_CREATE":
            session_id = _text(payload.get("session_id"), "session_id")
            creator = _text(payload.get("creator_node_id"), "creator_node_id")
            if creator not in next_state["nodes"]:
                raise ControlPlaneError("session creator is not enrolled")
            existing = next_state["sessions"].get(session_id)
            candidate = {
                "display_name": _text(payload.get("display_name"), "display_name"),
                "creator_node_id": creator,
                "revision": 0,
            }
            if existing is not None:
                if existing != candidate:
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
                    payload=payload,
                )
            )
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="node.joined",
                    actor_node_id=command.issued_by,
                    payload={**payload, "node_id": creator},
                )
            )

        elif kind == "SESSION_MEMBER_ADD":
            session_id = _text(payload.get("session_id"), "session_id")
            node_id = _text(payload.get("node_id"), "node_id")
            if (
                session_id not in next_state["sessions"]
                or node_id not in next_state["nodes"]
            ):
                raise ControlPlaneError("membership target is unknown")
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
                    payload=payload,
                )
            )

        elif kind == "SESSION_MEMBER_REMOVE":
            session_id = _text(payload.get("session_id"), "session_id")
            node_id = _text(payload.get("node_id"), "node_id")
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
                    payload=payload,
                )
            )

        elif kind == "LEADER_TRANSITION":
            session_id = _text(payload.get("session_id"), "session_id")
            leader = next_state["leaders"].get(session_id)
            if leader is None:
                raise ControlPlaneError("leadership session is unknown")
            previous = _text(
                payload.get("previous_leader_node_id"), "previous_leader_node_id"
            )
            target = _text(payload.get("leader_node_id"), "leader_node_id")
            term = _uint(payload.get("term"), "term")
            if (
                previous != leader["leader_node_id"]
                or term != leader["term"] + 1
                or next_state["memberships"].get(session_id, {}).get(target) is not True
                or target in next_state["revocations"]
            ):
                raise ControlPlaneError(
                    "leadership transition is not contiguous or eligible"
                )
            leader.update({"leader_node_id": target, "term": term})
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="session.leader.changed",
                    actor_node_id=command.issued_by,
                    payload=payload,
                )
            )

        elif kind == "CAPABILITY_DECLARE":
            session_id = _text(payload.get("session_id"), "session_id")
            capability_id = _text(payload.get("capability_id"), "capability_id")
            if session_id not in next_state["sessions"]:
                raise ControlPlaneError("capability session is unknown")
            capabilities = next_state["capabilities"].setdefault(session_id, {})
            existing = capabilities.get(capability_id)
            if existing is not None and existing != payload:
                raise ControlPlaneError("capability identity conflict")
            if existing == payload:
                return next_state, ()
            capabilities[capability_id] = dict(payload)
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="capability.registered",
                    actor_node_id=command.issued_by,
                    payload=payload,
                )
            )

        elif kind == "CAPABILITY_WITHDRAW":
            session_id = _text(payload.get("session_id"), "session_id")
            capability_id = _text(payload.get("capability_id"), "capability_id")
            capabilities = next_state["capabilities"].setdefault(session_id, {})
            if capability_id not in capabilities:
                return next_state, ()
            capabilities.pop(capability_id)
            emitted.append(
                _event(
                    next_state,
                    session_id=session_id,
                    event_type="capability.status.changed",
                    actor_node_id=command.issued_by,
                    payload=payload,
                )
            )

        else:  # pragma: no cover - constructor rejects unknown commands
            raise ControlPlaneError("unsupported command type")

        for session_id, events in next_state["session_events"].items():
            if session_id in next_state["sessions"]:
                next_state["sessions"][session_id]["revision"] = len(events)
        _canonical(next_state, maximum=MAX_SNAPSHOT_BYTES)
        return next_state, tuple(emitted)


class PersistentReplicaStore:
    """Atomic local persistence for one replica's consensus metadata and log."""

    def __init__(self, database: Path | str, configuration: VoterConfiguration) -> None:
        self.database = str(database)
        self.configuration = configuration
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as database:
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
                    tuple(metadata.items()),
                )
                database.executemany(
                    "INSERT INTO replica_voters(voter_id) VALUES(?)",
                    ((voter,) for voter in self.configuration.voter_ids),
                )
                state = _empty_state(self.configuration)
                state_json = _canonical(state, maximum=MAX_SNAPSHOT_BYTES).decode()
                database.execute(
                    "INSERT INTO replica_projection(singleton,state_json,state_digest) VALUES(1,?,?)",
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

    def _metadata(self, key: str) -> str:
        with self._connect() as database:
            row = database.execute(
                "SELECT value FROM replica_metadata WHERE key=?", (key,)
            ).fetchone()
        if row is None:
            raise ControlPlaneError(f"missing replica metadata: {key}")
        return str(row["value"])

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

    def _set_metadata(self, database: sqlite3.Connection, **values: object) -> None:
        for key, value in values.items():
            database.execute(
                "UPDATE replica_metadata SET value=? WHERE key=?",
                (str(value), key),
            )

    def set_term_and_vote(self, term: int, voted_for: str | None) -> None:
        _uint(term, "current_term")
        if voted_for is not None:
            _text(voted_for, "voted_for")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            current = int(
                database.execute(
                    "SELECT value FROM replica_metadata WHERE key='current_term'"
                ).fetchone()[0]
            )
            vote = database.execute(
                "SELECT value FROM replica_metadata WHERE key='voted_for'"
            ).fetchone()[0]
            if term < current:
                raise StaleTerm("term cannot decrease")
            if voted_for is not None and voted_for not in self.configuration.voter_ids:
                raise ControlPlaneError("vote target not configured")
            if term == current and vote and vote != (voted_for or ""):
                raise StaleTerm("one durable vote per term")
            self._set_metadata(
                database,
                current_term=term,
                voted_for=voted_for or "",
            )
            database.commit()

    def increment_fencing_epoch(self) -> int:
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            value = (
                int(
                    database.execute(
                        "SELECT value FROM replica_metadata WHERE key='fencing_epoch'"
                    ).fetchone()[0]
                )
                + 1
            )
            self._set_metadata(database, fencing_epoch=value)
            database.commit()
        return value

    def last_log_index(self) -> int:
        with self._connect() as database:
            row = database.execute("SELECT MAX(log_index) FROM replica_log").fetchone()
        return max(self.last_snapshot_index, int(row[0] or 0))

    def term_at(self, index: int) -> int | None:
        if index == 0:
            return 0
        if index == self.last_snapshot_index and index:
            return int(self._metadata("last_snapshot_term"))
        with self._connect() as database:
            row = database.execute(
                "SELECT log_term FROM replica_log WHERE log_index=?", (index,)
            ).fetchone()
        return int(row[0]) if row is not None else None

    def entries(self, *, after: int = 0) -> tuple[LogEntry, ...]:
        with self._connect() as database:
            rows = database.execute(
                "SELECT * FROM replica_log WHERE log_index>? ORDER BY log_index",
                (after,),
            ).fetchall()
        return tuple(
            LogEntry(
                log_index=int(row["log_index"]),
                log_term=int(row["log_term"]),
                command=AuthorityCommand.from_dict(json.loads(row["command_json"])),
            )
            for row in rows
        )

    def entry_for_command(self, command_id: str) -> LogEntry | None:
        with self._connect() as database:
            row = database.execute(
                "SELECT * FROM replica_log WHERE command_id=?", (command_id,)
            ).fetchone()
        if row is None:
            return None
        return LogEntry(
            log_index=int(row["log_index"]),
            log_term=int(row["log_term"]),
            command=AuthorityCommand.from_dict(json.loads(row["command_json"])),
        )

    def append_entries(
        self,
        entries: Sequence[LogEntry],
        *,
        prev_log_index: int,
        prev_log_term: int,
        leader_commit: int,
        leader_term: int | None = None,
    ) -> None:
        _uint(prev_log_index, "prev_log_index")
        _uint(prev_log_term, "prev_log_term")
        _uint(leader_commit, "leader_commit")
        if (prev_log_index == 0) != (prev_log_term == 0):
            raise LogConflict("invalid prefix index/term")
        previous_term = prev_log_term
        for index, entry in enumerate(entries, start=prev_log_index + 1):
            if (
                entry.log_index != index
                or entry.command.cluster_id != self.configuration.cluster_id
                or entry.log_term < previous_term
                or entry.log_term == 0
                or (leader_term is not None and entry.log_term > leader_term)
            ):
                raise LogConflict("invalid contiguous log index/term/cluster")
            previous_term = entry.log_term
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            metadata = {
                row[0]: row[1]
                for row in database.execute("SELECT key,value FROM replica_metadata")
            }
            snapshot_index = int(metadata["last_snapshot_index"])
            commit_index = int(metadata["commit_index"])
            if prev_log_index == snapshot_index:
                local_term = int(metadata["last_snapshot_term"])
            else:
                row = database.execute(
                    "SELECT log_term FROM replica_log WHERE log_index=?",
                    (prev_log_index,),
                ).fetchone()
                local_term = row[0] if row else None
            if local_term != prev_log_term:
                raise LogConflict("follower log prefix mismatch")
            for entry in entries:
                existing = database.execute(
                    "SELECT log_term,command_id,content_hash FROM replica_log WHERE log_index=?",
                    (entry.log_index,),
                ).fetchone()
                encoded = entry.command.canonical_bytes().decode()
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
                database.execute(
                    "INSERT INTO replica_log(log_index,log_term,command_id,command_json,content_hash) VALUES(?,?,?,?,?)",
                    (
                        entry.log_index,
                        entry.log_term,
                        entry.command.command_id,
                        encoded,
                        entry.command.content_hash,
                    ),
                )
            # Preview the entire retained ordered history before accepting it.
            # This occurs inside the same transaction, so a rejected command
            # cannot leave a partial append or conflicting-suffix deletion.
            projection = json.loads(
                database.execute(
                    "SELECT state_json FROM replica_projection WHERE singleton=1"
                ).fetchone()[0]
            )
            applied = int(metadata["last_applied"])
            for row in database.execute(
                "SELECT * FROM replica_log WHERE log_index>? ORDER BY log_index",
                (applied,),
            ):
                if row["command_id"] in projection["command_receipts"]:
                    raise DuplicateCommandError(
                        "command ID already applied at another index"
                    )
                projection, _ = AuthorityStateMachine(self.configuration).apply(
                    projection,
                    AuthorityCommand.from_dict(json.loads(row["command_json"])),
                    log_index=row["log_index"],
                    log_term=row["log_term"],
                )
            if leader_commit > commit_index:
                self._set_metadata(
                    database,
                    commit_index=max(
                        commit_index, min(leader_commit, prev_log_index + len(entries))
                    ),
                )
            database.commit()

    def append_local(self, entry: LogEntry) -> None:
        self.append_entries(
            (entry,),
            prev_log_index=entry.log_index - 1,
            prev_log_term=self.term_at(entry.log_index - 1) or 0,
            leader_commit=self.commit_index,
        )

    def set_commit_index(self, index: int) -> None:
        if index < self.commit_index or index > self.last_log_index():
            raise ControlPlaneError("invalid commit index")
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            self._set_metadata(database, commit_index=index)
            database.commit()

    def projection(self) -> dict[str, Any]:
        with self._connect() as database:
            row = database.execute(
                "SELECT state_json,state_digest FROM replica_projection WHERE singleton=1"
            ).fetchone()
        state = json.loads(row["state_json"])
        if row["state_digest"] != _state_digest(state):
            raise ControlPlaneError("local projection digest mismatch")
        return state

    def save_projection(self, state: Mapping[str, Any], last_applied: int) -> None:
        encoded = _canonical(dict(state), maximum=MAX_SNAPSHOT_BYTES).decode()
        digest = _state_digest(state)
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            database.execute(
                "UPDATE replica_projection SET state_json=?,state_digest=? WHERE singleton=1",
                (encoded, digest),
            )
            self._set_metadata(database, last_applied=last_applied)
            database.commit()

    def install_snapshot(self, snapshot: Snapshot) -> None:
        snapshot = Snapshot.from_dict(
            json.loads(_canonical(snapshot.to_dict(), maximum=MAX_SNAPSHOT_BYTES))
        )
        if (
            snapshot.cluster_id != self.configuration.cluster_id
            or snapshot.voter_configuration != self.configuration
        ):
            raise ControlPlaneError("incompatible authority snapshot")
        encoded = _canonical(snapshot.to_dict(), maximum=MAX_SNAPSHOT_BYTES).decode()
        with self._connect() as database:
            database.execute("BEGIN IMMEDIATE")
            metadata = {
                row[0]: int(row[1])
                for row in database.execute("SELECT key,value FROM replica_metadata")
                if row[0] not in {"cluster_id", "voted_for"}
            }
            index = snapshot.last_included_index
            if index < max(metadata["last_applied"], metadata["last_snapshot_index"]):
                raise ControlPlaneError("older authority snapshot rejected")
            row = database.execute(
                "SELECT log_term FROM replica_log WHERE log_index=?", (index,)
            ).fetchone()
            local_term = (
                metadata["last_snapshot_term"]
                if index == metadata["last_snapshot_index"]
                else row[0]
                if row
                else None
            )
            compatible = local_term == snapshot.last_included_term
            if index <= metadata["commit_index"] and not compatible:
                raise LogConflict("snapshot conflicts with committed term")
            current_state = json.loads(
                database.execute(
                    "SELECT state_json FROM replica_projection WHERE singleton=1"
                ).fetchone()[0]
            )
            if index == metadata["last_applied"] and snapshot.state != current_state:
                raise LogConflict("snapshot conflicts with applied state")
            _require_state(snapshot.state, self.configuration)
            if (
                snapshot.state.get("voter_configuration")
                != self.configuration.to_dict()
            ):
                raise ControlPlaneError("snapshot genesis voters mismatch")
            receipts = snapshot.state.get("command_receipts", {})
            if len(receipts) > MAX_COMMAND_RECEIPTS:
                raise ControlPlaneError("snapshot receipt capacity exceeded")
            for command_id, receipt in current_state["command_receipts"].items():
                if receipts.get(command_id) != receipt:
                    raise LogConflict("snapshot loses committed command identity")
            if not compatible:
                if metadata["commit_index"] > index:
                    raise LogConflict("snapshot would discard committed suffix")
                database.execute("DELETE FROM replica_log WHERE log_index>?", (index,))
            database.execute(
                "INSERT INTO replica_snapshot(singleton,snapshot_json,snapshot_digest) VALUES(1,?,?) ON CONFLICT(singleton) DO UPDATE SET snapshot_json=excluded.snapshot_json,snapshot_digest=excluded.snapshot_digest",
                (encoded, snapshot.digest),
            )
            database.execute(
                "DELETE FROM replica_log WHERE log_index<=?",
                (snapshot.last_included_index,),
            )
            database.execute(
                "UPDATE replica_projection SET state_json=?,state_digest=? WHERE singleton=1",
                (
                    _canonical(snapshot.state, maximum=MAX_SNAPSHOT_BYTES).decode(),
                    snapshot.digest,
                ),
            )
            self._set_metadata(
                database,
                last_snapshot_index=snapshot.last_included_index,
                last_snapshot_term=snapshot.last_included_term,
                last_applied=index,
                commit_index=max(metadata["commit_index"], index),
            )
            database.commit()

    def snapshot(self) -> Snapshot | None:
        with self._connect() as database:
            row = database.execute(
                "SELECT snapshot_json FROM replica_snapshot WHERE singleton=1"
            ).fetchone()
        return Snapshot.from_dict(json.loads(row[0])) if row is not None else None


@dataclass(frozen=True)
class VoteResponse:
    term: int
    granted: bool


@dataclass(frozen=True)
class AppendResponse:
    term: int
    success: bool
    match_index: int


def _serialized(method):
    @wraps(method)
    def invoke(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return invoke


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
    ) -> AppendResponse: ...


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
        self._lock = threading.RLock()
        self._proposal_lock = threading.RLock()
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.apply_committed()

    @property
    def quorum(self) -> int:
        return self.configuration.quorum

    @property
    def state(self) -> dict[str, Any]:
        return self.store.projection()

    @property
    def fencing_token(self) -> FencingToken:
        return FencingToken(
            cluster_id=self.configuration.cluster_id,
            cluster_term=self.store.current_term,
            leader_id=self.leader_id or "",
            commit_index=self.store.commit_index,
            fencing_epoch=self.store.fencing_epoch,
        )

    def _step_down(self, term: int) -> None:
        if term > self.store.current_term:
            self.store.set_term_and_vote(term, None)
        self.role = self.FOLLOWER
        self.leader_id = None

    @_serialized
    def receive_vote_request(
        self,
        *,
        candidate_id: str,
        term: int,
        last_log_index: int,
        last_log_term: int,
        cluster_id: str,
    ) -> VoteResponse:
        _uint(term, "term")
        _uint(last_log_index, "last_log_index")
        _uint(last_log_term, "last_log_term")
        if (
            cluster_id != self.configuration.cluster_id
            or candidate_id not in self.configuration.voter_ids
        ):
            return VoteResponse(self.store.current_term, False)
        if term < self.store.current_term:
            return VoteResponse(self.store.current_term, False)
        if term > self.store.current_term:
            self._step_down(term)
        local_last_term = self.store.term_at(self.store.last_log_index()) or 0
        up_to_date = (last_log_term, last_log_index) >= (
            local_last_term,
            self.store.last_log_index(),
        )
        if (
            not up_to_date
            or last_log_term > term
            or ((last_log_index == 0) != (last_log_term == 0))
        ):
            return VoteResponse(self.store.current_term, False)
        if self.store.voted_for not in (None, candidate_id):
            return VoteResponse(self.store.current_term, False)
        # Durable before returning the vote is a protocol invariant.
        self.store.set_term_and_vote(term, candidate_id)
        self.role = self.FOLLOWER
        self.leader_id = None
        return VoteResponse(self.store.current_term, True)

    def start_election(self, transport: ReplicationTransport) -> bool:
        with self._lock:
            self.role = self.CANDIDATE
            term = self.store.current_term + 1
            self.store.set_term_and_vote(term, self.voter_id)
            last_index = self.store.last_log_index()
            last_term = self.store.term_at(last_index) or 0
        votes = 1
        for target in self.configuration.voter_ids:
            if target == self.voter_id:
                continue
            try:
                response = transport.request_vote(
                    target,
                    candidate_id=self.voter_id,
                    term=term,
                    last_log_index=last_index,
                    last_log_term=last_term,
                    cluster_id=self.configuration.cluster_id,
                )
            except (ControlPlaneError, OSError, TimeoutError):  # lost vote
                continue
            with self._lock:
                if response.term > self.store.current_term:
                    self._step_down(response.term)
                if self.store.current_term != term or self.role != self.CANDIDATE:
                    return False
                if response.term == term and response.granted:
                    votes += 1
        with self._lock:
            if self.store.current_term != term or self.role != self.CANDIDATE:
                return False
            if votes < self.quorum:
                self.role = self.FOLLOWER
                return False
            self.role = self.LEADER
            self.leader_id = self.voter_id
            self.store.increment_fencing_epoch()
            self.next_index = {
                v: self.store.last_log_index() + 1
                for v in self.configuration.voter_ids
                if v != self.voter_id
            }
            self.match_index = dict.fromkeys(self.next_index, 0)
            return True

    @_serialized
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
        _uint(leader_term, "leader_term")
        if (
            cluster_id != self.configuration.cluster_id
            or leader_id not in self.configuration.voter_ids
        ):
            return AppendResponse(self.store.current_term, False, 0)
        if leader_term < self.store.current_term:
            return AppendResponse(self.store.current_term, False, 0)
        self._step_down(leader_term)
        self.leader_id = leader_id
        try:
            self.store.append_entries(
                entries,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                leader_commit=leader_commit,
                leader_term=leader_term,
            )
        except (ControlPlaneError, sqlite3.Error):
            return AppendResponse(self.store.current_term, False, 0)
        self.role = self.FOLLOWER
        self.leader_id = leader_id
        self.apply_committed()
        return AppendResponse(
            self.store.current_term, True, prev_log_index + len(entries)
        )

    @_serialized
    def apply_committed(self) -> tuple[dict[str, Any], ...]:
        state = self.state
        emitted: list[dict[str, Any]] = []
        for entry in self.store.entries(after=self.store.last_applied):
            if entry.log_index > self.store.commit_index:
                break
            state, events = self.state_machine.apply(
                state, entry.command, log_index=entry.log_index, log_term=entry.log_term
            )
            emitted.extend(events)
            self.store.save_projection(state, entry.log_index)
        return tuple(emitted)

    def propose(
        self, command: AuthorityCommand, transport: ReplicationTransport
    ) -> tuple[LogEntry, tuple[dict[str, Any], ...]]:
        with self._proposal_lock:
            with self._lock:
                term = self.store.current_term
                self._require_leader(term)
                command = AuthorityCommand.from_dict(
                    json.loads(command.canonical_bytes())
                )
                if command.cluster_id != self.configuration.cluster_id:
                    raise ControlPlaneError("command cluster identity mismatch")
                self.apply_committed()
                receipt = self.state["command_receipts"].get(command.command_id)
                if receipt is not None:
                    if receipt["digest"] != command.content_hash:
                        raise DuplicateCommandError("command ID payload conflict")
                    return LogEntry(receipt["index"], receipt["term"], command), ()
                existing = self.store.entry_for_command(command.command_id)
                if (
                    existing is not None
                    and existing.command.content_hash != command.content_hash
                ):
                    raise DuplicateCommandError("command ID payload conflict")
                if existing is None:
                    entry = LogEntry(self.store.last_log_index() + 1, term, command)
                    self.store.append_local(entry)
                else:
                    entry = existing
            # A prior-term entry cannot be committed by counting its replicas.
            # Phase 1 fails closed until a valid current-term command commits
            # that prefix (no synthetic authority command is invented here).
            self.replicate(transport, term=term)
            with self._lock:
                self._require_leader(term)
                if self.store.commit_index < entry.log_index:
                    raise QuorumUnavailable(
                        "authority command was not committed by quorum"
                    )
                return entry, self.apply_committed()

    def _require_leader(self, term: int) -> None:
        if (
            self.role != self.LEADER
            or self.leader_id != self.voter_id
            or self.store.current_term != term
        ):
            raise StaleTerm("only the current leader may append authority")

    def _replicate_follower(
        self, target: str, transport: ReplicationTransport, term: int
    ) -> None:
        while True:
            with self._lock:
                self._require_leader(term)
                next_index = self.next_index[target]
                snapshot = (
                    self.store.snapshot()
                    if next_index <= self.store.last_snapshot_index
                    else None
                )
                previous = next_index - 1
                entries = self.store.entries(after=previous) if snapshot is None else ()
                request = dict(
                    leader_id=self.voter_id,
                    leader_term=term,
                    cluster_id=self.configuration.cluster_id,
                )
                if snapshot is None:
                    request.update(
                        prev_log_index=previous,
                        prev_log_term=self.store.term_at(previous) or 0,
                        entries=entries,
                        leader_commit=self.store.commit_index,
                    )
                else:
                    request["snapshot"] = snapshot
            try:
                response = (
                    transport.install_snapshot(target, **request)
                    if snapshot is not None
                    else transport.append_entries(target, **request)
                )
            except (OSError, TimeoutError):
                return
            with self._lock:
                if response.term > self.store.current_term:
                    self._step_down(response.term)
                self._require_leader(term)
                if response.term != term:
                    return
                if response.success:
                    matched = (
                        snapshot.last_included_index
                        if snapshot
                        else previous + len(entries)
                    )
                    if response.match_index != matched:
                        raise ControlPlaneError("invalid follower progress response")
                    self.match_index[target] = matched
                    self.next_index[target] = matched + 1
                    if snapshot is not None:
                        continue
                    return
                if snapshot is not None or next_index <= 1:
                    return
                self.next_index[target] = next_index - 1

    def replicate(
        self, transport: ReplicationTransport, *, term: int | None = None
    ) -> None:
        with self._proposal_lock:
            with self._lock:
                term = self.store.current_term if term is None else term
                self._require_leader(term)
                targets = tuple(self.next_index)
            for target in targets:
                self._replicate_follower(target, transport, term)
            with self._lock:
                self._require_leader(term)
                indexes = sorted(
                    [self.store.last_log_index(), *self.match_index.values()],
                    reverse=True,
                )
                quorum_index = indexes[self.quorum - 1]
                if (
                    quorum_index > self.store.commit_index
                    and self.store.term_at(quorum_index) == term
                ):
                    self.store.set_commit_index(quorum_index)
                    self.apply_committed()
            for target in targets:
                self._replicate_follower(target, transport, term)
            with self._lock:
                self._require_leader(term)

    @_serialized
    def receive_install_snapshot(
        self, *, leader_id: str, leader_term: int, snapshot: Snapshot, cluster_id: str
    ) -> AppendResponse:
        if (
            cluster_id != self.configuration.cluster_id
            or leader_id not in self.configuration.voter_ids
        ):
            return AppendResponse(self.store.current_term, False, 0)
        if leader_term < self.store.current_term:
            return AppendResponse(self.store.current_term, False, 0)
        self._step_down(leader_term)
        self.leader_id = leader_id
        try:
            if snapshot.last_included_term > leader_term:
                raise ControlPlaneError("snapshot term exceeds leader term")
            self.install_snapshot(snapshot)
        except ControlPlaneError:
            return AppendResponse(self.store.current_term, False, 0)
        return AppendResponse(
            self.store.current_term, True, snapshot.last_included_index
        )

    @_serialized
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
        )

    @_serialized
    def install_snapshot(self, snapshot: Snapshot) -> None:
        self.store.install_snapshot(snapshot)
        self.apply_committed()


class InProcessTransport:
    """Deterministic test transport; intentionally has no network security."""

    def __init__(self, replicas: Mapping[str, ReplicaNode]) -> None:
        self.replicas = dict(replicas)
        self.blocked: set[tuple[str, str]] = set()

    def _reachable(self, source: str, target: str) -> bool:
        return (source, target) not in self.blocked

    def request_vote(self, target: str, **request: object) -> VoteResponse:
        source = str(request["candidate_id"])
        if not self._reachable(source, target):
            raise TimeoutError("in-process link blocked")
        return self.replicas[target].receive_vote_request(**request)  # type: ignore[arg-type]

    def append_entries(self, target: str, **request: object) -> AppendResponse:
        source = str(request["leader_id"])
        if not self._reachable(source, target):
            raise TimeoutError("in-process link blocked")
        return self.replicas[target].receive_append_entries(**request)  # type: ignore[arg-type]

    def install_snapshot(self, target: str, **request: object) -> AppendResponse:
        if not self._reachable(str(request["leader_id"]), target):
            raise TimeoutError("in-process link blocked")
        return self.replicas[target].receive_install_snapshot(**request)


__all__ = [
    "INITIAL_TERM",
    "SNAPSHOT_SCHEMA",
    "AuthorityCommand",
    "AuthorityStateMachine",
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
    "StaleTerm",
    "VoterConfiguration",
]
