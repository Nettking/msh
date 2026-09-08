"""Canonical product journal carried atomically by the authority log.

The narrow C03 ``session_events`` projection is deliberately separate.  It has
never been the client replay journal and contains internal bootstrap events.
This module preserves the actual public database rows, including their request
digests, without putting private enrollment or invitation rows in public JSON.

The runtime authenticates migration witnesses and encrypts private receipt rows.
The deterministic core checks complete-prefix identity and opaque-row CAS only;
neither an initialization command nor base64 encoding is a witness or encryption.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from .control_plane_replication import (
    MAX_COMMAND_BYTES,
    MAX_COMMAND_RECEIPTS,
    MAX_SNAPSHOT_BYTES,
    AuthorityCommand,
    ControlPlaneError,
    VoterConfiguration,
    _canonical,
    _require_enrolled,
    _require_member,
    _text,
    _uint,
)
from .recorder_control_events import (
    SCAN_EVENTS as RECORDER_CONTROL_SCAN_EVENTS,
)
from .recorder_control_events import (
    mask_recorder_control_scan_event_payload,
)
from .redaction import redact_secrets
from .shared_file_storage import mask_public_jsonl_chunk_paths

PRODUCT_JOURNAL_SCHEMA = "fcp.control-plane.product-journal.v1"
PRODUCT_TRANSACTION = "PRODUCT_TRANSACTION"
PRODUCT_JOURNAL_INITIALIZE = "PRODUCT_JOURNAL_INITIALIZE"
PUBLIC_ROW_FIELDS = frozenset(
    {
        "session_id", "revision", "event_id", "request_id", "event_type",
        "occurred_at", "actor_node_id", "payload_json", "content_hash",
    }
)
MAX_PUBLIC_PAYLOAD_BYTES = 48_000
MAX_PRODUCT_COMMANDS = 32
MAX_PRODUCT_ROWS = 256
MAX_PRIVATE_ROW_BYTES = 64 * 1024
# Existing quorum-witnessed legacy manifest/history limit; do not widen the
# import boundary merely because the containing consensus snapshot is larger.
MAX_WITNESSED_HISTORY_BYTES = 2 * 1024 * 1024
REPLICATED_COORDINATOR_ID = "fcp-replicated-coordinator-v1"
LEGACY_COORDINATOR_ID = "fcp-relay-coordinator"
LEADERSHIP_SCHEMA = "fcp.session-leadership.v1"
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_HMAC_SHA256 = re.compile(r"hmac-sha256:[0-9a-f]{64}\Z")
_INNER_TYPES = frozenset(
    {
        "NODE_ENROLL", "NODE_REVOKE", "SESSION_CREATE", "SESSION_MEMBER_ADD",
        "SESSION_MEMBER_REMOVE", "LEADER_TRANSITION", "CAPABILITY_DECLARE",
        "CAPABILITY_WITHDRAW",
    }
)
_CAPABILITY_EVENTS = frozenset({"capability.registered", "capability.status.changed"})


def _closed(value: object, fields: set[str] | frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ControlPlaneError(f"{label} has unexpected or missing fields")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ControlPlaneError(f"{label} must be a SHA-256 digest")
    return value


def _rows(value: object, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > MAX_PRODUCT_ROWS:
        raise ControlPlaneError(f"{label} must be a bounded array")
    return value


def _public_payload(payload: object, event_type: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ControlPlaneError("public event payload must be an object")
    _canonical(payload, maximum=MAX_PUBLIC_PAYLOAD_BYTES)
    # These are the existing relay public-payload rules, including its two
    # schema-bound allowances.  Do not replace them with the narrow C03 fields.
    redacted = redact_secrets(payload)
    if redacted == payload or redacted == mask_public_jsonl_chunk_paths(payload):
        return payload
    if (
        event_type in RECORDER_CONTROL_SCAN_EVENTS
        and redacted == mask_recorder_control_scan_event_payload(payload)
    ):
        return payload
    raise ControlPlaneError("public event contains nonpublic material")


def public_row_content_hash(event_type: str, payload_json: str) -> str:
    """Use CoordinatorStore's existing idempotency content-hash algorithm."""
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    return hashlib.sha256(f"{event_type}\0{payload_hash}".encode()).hexdigest()


def _validate_capability_payload(event_type: str, payload: dict[str, Any]) -> None:
    if event_type not in _CAPABILITY_EVENTS:
        return
    from .errors import FederationValidationError
    from .models import CapabilityAnnouncement

    try:
        CapabilityAnnouncement.from_dict(payload)
    except FederationValidationError as exc:
        raise ControlPlaneError("public capability payload is malformed") from exc


def validate_public_row(row: object, *, imported: bool = False) -> dict[str, Any]:
    value = _closed(row, PUBLIC_ROW_FIELDS, "public event row")
    for field in PUBLIC_ROW_FIELDS - {"revision", "payload_json", "content_hash"}:
        _text(value[field], field)
        if redact_secrets(value[field]) != value[field]:
            raise ControlPlaneError("public event metadata contains nonpublic material")
    if _uint(value["revision"], "revision") < 1:
        raise ControlPlaneError("public event revision must be positive")
    _digest(value["request_id"], "request_id")
    try:
        stamp = datetime.fromisoformat(value["occurred_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise ControlPlaneError("public event timestamp is malformed") from exc
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ControlPlaneError("public event timestamp must include its timezone")
    encoded = value["payload_json"]
    if not isinstance(encoded, str):
        raise ControlPlaneError("payload_json must be canonical JSON text")
    try:
        payload = json.loads(encoded)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise ControlPlaneError("payload_json is malformed") from exc
    if _canonical(payload, maximum=MAX_PUBLIC_PAYLOAD_BYTES).decode("utf-8") != encoded:
        raise ControlPlaneError("payload_json is not canonical")
    _public_payload(payload, value["event_type"])
    if not imported:
        _validate_capability_payload(value["event_type"], payload)
    expected = public_row_content_hash(value["event_type"], encoded)
    legacy_digest = hashlib.sha256(
        f"c03-leadership:{value['session_id']}:{payload.get('term')}:{payload.get('leader_node_id')}".encode()
    ).hexdigest()
    old_c03 = (
        imported
        and value["event_type"] == "session.leader.changed"
        and value["event_id"] == "c03-" + legacy_digest[:32]
        and value["request_id"] == "sha256:" + legacy_digest
        and value["actor_node_id"] == REPLICATED_COORDINATOR_ID
        and payload.get("schema") == LEADERSHIP_SCHEMA
        and value["content_hash"]
        == "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    )
    if value["content_hash"] != expected and not old_c03:
        raise ControlPlaneError("public event content hash mismatch")
    return value


def public_rows_digest(rows: list[dict[str, Any]]) -> str:
    """Digest the complete exact row prefix, never the narrow C03 projection."""
    return "sha256:" + hashlib.sha256(
        _canonical(rows, maximum=MAX_SNAPSHOT_BYTES)
    ).hexdigest()


journal_prefix_digest = public_rows_digest


def journal_history_digest(rows: list[dict[str, Any]]) -> str:
    """Bind witnessed rows to the original canonical public wire history.

    SQL stores UTC as +00:00, whereas SessionEvent.to_dict uses Z. Reusing that
    model serializer preserves exactly the witness manifest's representation.
    Request/content digests are internal database fields and are not wire fields.
    """
    from .errors import FederationValidationError
    from .models import SessionEvent

    events = []
    for row in rows:
        try:
            event = SessionEvent.from_dict({
                "schema": SessionEvent.SCHEMA,
                **{field: row[field] for field in (
                    "session_id", "revision", "event_id", "event_type",
                    "occurred_at", "actor_node_id",
                )},
                "payload": json.loads(row["payload_json"]),
            })
        except (FederationValidationError, json.JSONDecodeError) as exc:
            raise ControlPlaneError("witnessed history contains an invalid public event") from exc
        events.append(event.to_dict())
    return "sha256:" + hashlib.sha256(
        _canonical(events, maximum=MAX_WITNESSED_HISTORY_BYTES)
    ).hexdigest()


def private_ciphertext_digest(ciphertext: str) -> str:
    try:
        decoded = base64.b64decode(ciphertext, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ControlPlaneError("private receipt must be canonical base64 ciphertext") from exc
    if (
        len(decoded) < 28
        or len(decoded) > MAX_PRIVATE_ROW_BYTES
        or base64.b64encode(decoded).decode("ascii") != ciphertext
    ):
        raise ControlPlaneError("private receipt ciphertext exceeds its bounds")
    return "sha256:" + hashlib.sha256(decoded).hexdigest()


def _validate_provenance(value: object, maximum_revision: int) -> dict[str, Any]:
    provenance = _closed(value, {"kind", "history_digest", "source_revision"}, "journal provenance")
    if provenance["kind"] != "witnessed":
        raise ControlPlaneError("journal provenance must identify a witnessed prefix")
    _digest(provenance["history_digest"], "history_digest")
    revision = _uint(provenance["source_revision"], "source_revision")
    if not 1 <= revision <= maximum_revision:
        raise ControlPlaneError("witnessed journal source revision exceeds its complete prefix")
    return provenance


def _is_witnessed_row(journal: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
    provenance = journal.get("provenance")
    return provenance is not None and row["revision"] <= provenance["source_revision"]


def _validate_witness_binding(journal: Mapping[str, Any]) -> None:
    provenance = journal.get("provenance")
    if provenance is None:
        return
    revision = provenance["source_revision"]
    rows = journal["rows"]
    if len(rows) >= revision and journal_history_digest(rows[:revision]) != provenance["history_digest"]:
        raise ControlPlaneError("canonical public history differs from witnessed provenance")


def validate_product_payload(command_type: str, payload: object) -> None:
    if command_type == PRODUCT_TRANSACTION:
        if not isinstance(payload, dict):
            raise ControlPlaneError("product transaction payload must be an object")
        allowed = {"authority_commands", "public_rows", "private_rows"}
        if set(payload) - allowed or not {"authority_commands", "public_rows"} <= set(payload):
            raise ControlPlaneError("product transaction has unexpected or missing fields")
        commands = payload["authority_commands"]
        if not isinstance(commands, list) or len(commands) > MAX_PRODUCT_COMMANDS:
            raise ControlPlaneError("product authority commands must be a bounded array")
        identities: set[str] = set()
        for raw in commands:
            _closed(
                raw,
                {"schema", "command_id", "command_type", "cluster_id", "issued_by", "payload"},
                "nested authority command",
            )
            if raw["command_type"] not in _INNER_TYPES:
                raise ControlPlaneError("product transaction forbids genesis and nested product commands")
            inner = AuthorityCommand.from_dict(raw)
            if inner.command_id in identities:
                raise ControlPlaneError("product transaction repeats an authority command ID")
            identities.add(inner.command_id)
        for row in _rows(payload["public_rows"], "public_rows"):
            validate_public_row(row)
        keys: set[str] = set()
        for row in _rows(payload.get("private_rows", []), "private_rows"):
            _closed(row, {"key", "previous_digest", "ciphertext"}, "private receipt row")
            key = row["key"]
            if not isinstance(key, str) or not _HMAC_SHA256.fullmatch(key) or key in keys:
                raise ControlPlaneError("private receipt key must be a unique HMAC-SHA256 digest")
            keys.add(key)
            if row["previous_digest"] is not None:
                _digest(row["previous_digest"], "previous_digest")
            if not isinstance(row["ciphertext"], str):
                raise ControlPlaneError("private receipt ciphertext must be base64 text")
            private_ciphertext_digest(row["ciphertext"])
    else:
        if not isinstance(payload, dict):
            raise ControlPlaneError("product journal initialization must be an object")
        fields = {"session_id", "expected_revision", "prefix_digest", "public_rows", "final"}
        if "provenance" in payload:
            fields.add("provenance")
        value = _closed(
            payload, fields, "product journal initialization",
        )
        _text(value["session_id"], "session_id")
        _uint(value["expected_revision"], "expected_revision")
        _digest(value["prefix_digest"], "prefix_digest")
        if not isinstance(value["final"], bool):
            raise ControlPlaneError("initialization final must be boolean")
        if "provenance" in value:
            _validate_provenance(value["provenance"], value["expected_revision"])
        for row in _rows(value["public_rows"], "public_rows"):
            validate_public_row(row, imported=True)
            if not _is_witnessed_row(value, row):
                _validate_capability_payload(row["event_type"], json.loads(row["payload_json"]))
    _canonical(payload, maximum=MAX_COMMAND_BYTES)


def _namespace(state: dict[str, Any]) -> dict[str, Any]:
    namespace = state.setdefault(
        "product_journal",
        {"schema": PRODUCT_JOURNAL_SCHEMA, "sessions": {}, "initializing": {}, "private_rows": {}},
    )
    namespace.setdefault("accepted_transactions", {})
    namespace.setdefault("accepted_transaction_order", [])
    return namespace


def journal_session(state: Mapping[str, Any], session_id: str) -> dict[str, Any] | None:
    return state.get("product_journal", {}).get("sessions", {}).get(session_id)


def transaction_accepted(state: Mapping[str, Any], command: AuthorityCommand) -> bool:
    """A consensus receipt alone also exists for rejected committed commands."""
    return (
        command.command_type == PRODUCT_TRANSACTION
        and state.get("product_journal", {}).get("accepted_transactions", {}).get(command.command_id)
        == command.content_hash
    )


def _record_accepted(namespace: dict[str, Any], command: AuthorityCommand) -> None:
    accepted, order = namespace["accepted_transactions"], namespace["accepted_transaction_order"]
    while len(order) >= MAX_COMMAND_RECEIPTS:
        del accepted[order.pop(0)]
    accepted[command.command_id] = command.content_hash
    order.append(command.command_id)


def _append_exact(rows: list[dict[str, Any]], batch: list[dict[str, Any]], session_id: str) -> None:
    ids = {row["event_id"] for row in rows}
    requests = {(row["actor_node_id"], row["request_id"]) for row in rows}
    for row in batch:
        if row["session_id"] != session_id:
            raise ControlPlaneError("public event session mismatch")
        revision = row["revision"]
        if revision <= len(rows):
            if rows[revision - 1] != row:
                raise ControlPlaneError("public journal prefix conflict")
            continue
        if revision != len(rows) + 1:
            raise ControlPlaneError("public journal revision gap")
        if row["event_id"] in ids or (row["actor_node_id"], row["request_id"]) in requests:
            raise ControlPlaneError("public journal event or request identity conflict")
        rows.append(dict(row))
        ids.add(row["event_id"])
        requests.add((row["actor_node_id"], row["request_id"]))


def _leadership_prefix(state: Mapping[str, Any], session_id: str, rows: list[dict[str, Any]]) -> None:
    creator = state["sessions"][session_id]["creator_node_id"]
    leader, term = creator, 1
    for row in rows:
        payload = json.loads(row["payload_json"])
        if row["event_type"] == "session.created" and (
            payload.get("session_id") != session_id
            or row["actor_node_id"] != creator
            or payload.get("creator_node_id", creator) != creator
        ):
            raise ControlPlaneError("public journal immutable creator mismatch")
        if row["event_type"] != "session.leader.changed" or row["actor_node_id"] not in {
            REPLICATED_COORDINATOR_ID, LEGACY_COORDINATOR_ID,
        }:
            continue
        if (
            payload.get("schema") != LEADERSHIP_SCHEMA
            or payload.get("session_id") != session_id
            or payload.get("previous_leader_node_id") != leader
            or isinstance(payload.get("term"), bool)
            or payload.get("term") != term + 1
        ):
            raise ControlPlaneError("canonical leadership history is not contiguous")
        leader = _text(payload.get("leader_node_id"), "leader_node_id")
        term += 1
    authority = state["leaders"][session_id]
    if (leader, term) != (authority["leader_node_id"], authority["term"]):
        raise ControlPlaneError("public journal disagrees with committed leadership")


def _initialize(state: dict[str, Any], command: AuthorityCommand) -> None:
    payload = command.payload
    session_id = payload["session_id"]
    if session_id not in state["sessions"]:
        raise ControlPlaneError("journal initialization session is unknown")
    _require_member(state, session_id, command.issued_by, "initialization actor")
    namespace = _namespace(state)
    if session_id in namespace["sessions"]:
        raise ControlPlaneError("an active canonical journal cannot be reinitialized")
    candidate = {
        "expected_revision": payload["expected_revision"],
        "prefix_digest": payload["prefix_digest"],
        "rows": [],
    }
    if "provenance" in payload:
        candidate["provenance"] = dict(payload["provenance"])
    staging = namespace["initializing"].setdefault(session_id, candidate)
    if (
        any(staging[key] != candidate[key] for key in ("expected_revision", "prefix_digest"))
        or staging.get("provenance") != candidate.get("provenance")
    ):
        raise ControlPlaneError("journal initialization identity conflict")
    _append_exact(staging["rows"], payload["public_rows"], session_id)
    if len(staging["rows"]) > staging["expected_revision"]:
        raise ControlPlaneError("journal initialization exceeds its complete prefix")
    _validate_witness_binding(staging)
    _check_global_event_ids(namespace)
    if not payload["final"]:
        return
    rows = staging["rows"]
    if len(rows) != staging["expected_revision"] or public_rows_digest(rows) != staging["prefix_digest"]:
        raise ControlPlaneError("journal initialization does not match its complete prefix")
    if rows and rows[0]["event_type"] != "session.created":
        raise ControlPlaneError("a migrated complete journal must begin with session.created")
    _leadership_prefix(state, session_id, rows)
    namespace["sessions"][session_id] = {
        "revision": len(rows), "prefix_digest": staging["prefix_digest"], "rows": rows,
    }
    if "provenance" in staging:
        namespace["sessions"][session_id]["provenance"] = dict(staging["provenance"])
    del namespace["initializing"][session_id]
    _check_global_event_ids(namespace)


def _matches(row: Mapping[str, Any], session_id: str, event_type: str, fields: Mapping[str, Any]) -> bool:
    if row["session_id"] != session_id or row["event_type"] != event_type:
        return False
    payload = json.loads(row["payload_json"])
    return all(payload.get(key) == value for key, value in fields.items())


def _require_effect_rows(
    before: Mapping[str, Any], command: AuthorityCommand,
    rows: list[dict[str, Any]], cursors: dict[str, int],
) -> None:
    payload, kind = command.payload, command.command_type
    session_id = payload.get("session_id")
    required: list[tuple[str, str, dict[str, Any]]] = []
    if kind == "SESSION_CREATE" and session_id not in before["sessions"]:
        required += [
            (session_id, "session.created", {"session_id": session_id}),
            (session_id, "node.joined", {"node_id": payload["creator_node_id"]}),
        ]
    elif kind == "SESSION_MEMBER_ADD" and not before["memberships"].get(session_id, {}).get(payload["node_id"]):
        required.append((session_id, "node.joined", {"node_id": payload["node_id"]}))
    elif kind == "SESSION_MEMBER_REMOVE" and before["memberships"].get(session_id, {}).get(payload["node_id"]):
        required.append((session_id, "node.left", {"node_id": payload["node_id"]}))
    elif kind == "CAPABILITY_DECLARE" and payload["capability_id"] not in before["capabilities"].get(session_id, {}):
        required.append((session_id, "capability.registered", {
            "capability_id": payload["capability_id"], "node_id": payload["owner_node_id"],
            "type": payload["capability_type"],
        }))
    elif kind == "CAPABILITY_WITHDRAW" and payload["capability_id"] in before["capabilities"].get(session_id, {}):
        required.append((session_id, "capability.status.changed", {"capability_id": payload["capability_id"]}))
    elif kind == "LEADER_TRANSITION":
        required.append((session_id, "session.leader.changed", {
            "schema": LEADERSHIP_SCHEMA,
            "previous_leader_node_id": payload["previous_leader_node_id"],
            "leader_node_id": payload["leader_node_id"], "term": payload["term"],
        }))
    elif kind == "NODE_REVOKE" and payload["node_id"] not in before["revocations"]:
        required.extend(
            (session, "node.revoked", {"node_id": payload["node_id"]})
            for session, members in before["memberships"].items() if members.get(payload["node_id"])
        )
    for session, event_type, fields in required:
        matching = [
            row for row in rows
            if row["revision"] > cursors.get(session, 0)
            and _matches(row, session, event_type, fields)
        ]
        if not matching:
            raise ControlPlaneError("product authority effect is missing its public event")
        cursors[session] = min(row["revision"] for row in matching)
    # A command whose C03 fields are unchanged may still carry a public
    # capability metadata change; its full public row is checked separately.


def _authorize_row(
    before: Mapping[str, Any], after: Mapping[str, Any], row: Mapping[str, Any],
    commands: list[AuthorityCommand],
) -> None:
    session_id, actor = row["session_id"], row["actor_node_id"]
    payload, event_type = json.loads(row["payload_json"]), row["event_type"]
    if session_id not in after["sessions"]:
        raise ControlPlaneError("public event session is unknown")
    if actor == REPLICATED_COORDINATOR_ID:
        if event_type == "session.leader.changed":
            if not any(
                command.command_type == "LEADER_TRANSITION"
                and command.payload["session_id"] == session_id
                and all(payload.get(key) == command.payload[key] for key in (
                    "previous_leader_node_id", "leader_node_id", "term",
                )) for command in commands
            ):
                raise ControlPlaneError("coordinator leadership event lacks its authority transition")
        elif event_type == "node.revoked":
            if not any(command.command_type == "NODE_REVOKE" and command.payload["node_id"] == payload.get("node_id") for command in commands):
                raise ControlPlaneError("coordinator revocation event lacks its authority command")
        elif event_type == "capability.health.changed":
            _require_member(before, session_id, payload.get("node_id"), "health node")
        else:
            raise ControlPlaneError("unsupported coordinator-authored public event")
    else:
        # Self-removal is authorized in the pre-operation state; successful join
        # is authorized in the post-operation state. Neither admits a stranger.
        try:
            _require_member(before, session_id, actor, "event actor")
        except ControlPlaneError:
            _require_member(after, session_id, actor, "event actor")
    if event_type in _CAPABILITY_EVENTS:
        _validate_capability_payload(event_type, payload)
        owner, capability_id = payload["node_id"], payload.get("capability_id")
        existing = after["capabilities"].get(session_id, {}).get(capability_id)
        if existing is None:
            existing = before["capabilities"].get(session_id, {}).get(capability_id)
        if (
            existing is None or actor != owner or existing["owner_node_id"] != owner
            or payload.get("session_id") != session_id
            or payload.get("type") != existing["capability_type"]
        ):
            raise ControlPlaneError("public capability row disagrees with owner authority")


def apply_product_command(
    state: dict[str, Any], command: AuthorityCommand, configuration: VoterConfiguration,
    apply_semantic: Callable[[Mapping[str, Any], AuthorityCommand], tuple[dict[str, Any], tuple[dict[str, Any], ...]]],
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    if state["federation_id"] is None:
        raise ControlPlaneError("Federation genesis is required first")
    if command.issued_by not in configuration.voter_ids:
        raise ControlPlaneError("product transaction requires a configured voter")
    _require_enrolled(state, command.issued_by, "issued_by")
    if command.command_type == PRODUCT_JOURNAL_INITIALIZE:
        _initialize(state, command)
        return state, ()
    accepted = state.get("product_journal", {}).get("accepted_transactions", {})
    if command.command_id in accepted:
        if accepted[command.command_id] != command.content_hash:
            raise ControlPlaneError("product transaction command identity conflict")
        return state, ()
    before = json.loads(_canonical(state, maximum=MAX_SNAPSHOT_BYTES))
    commands = [AuthorityCommand.from_dict(raw) for raw in command.payload["authority_commands"]]
    rows = command.payload["public_rows"]
    if not state.get("product_journal", {}).get("sessions"):
        raise ControlPlaneError("canonical journal initialization is required first")
    effect_cursors = {
        session: journal["revision"]
        for session, journal in state["product_journal"]["sessions"].items()
    }
    for inner in commands:
        if inner.cluster_id != command.cluster_id:
            raise ControlPlaneError("product authority cluster mismatch")
        from .control_plane_readiness import BOOTSTRAP_SEAL_CAPABILITY_ID

        if inner.payload.get("capability_id") == BOOTSTRAP_SEAL_CAPABILITY_ID:
            raise ControlPlaneError("internal bootstrap seal is not a product transaction")
        preceding = state
        state, _ = apply_semantic(state, inner)
        _require_effect_rows(preceding, inner, rows, effect_cursors)
    namespace = _namespace(state)
    for row in rows:
        session_id = row["session_id"]
        if session_id not in namespace["sessions"]:
            if session_id in before["sessions"] or session_id in namespace["initializing"]:
                raise ControlPlaneError("canonical journal initialization is required first")
            namespace["sessions"][session_id] = {"revision": 0, "prefix_digest": public_rows_digest([]), "rows": []}
        _authorize_row(before, state, row, commands)
        existing = namespace["sessions"][session_id]
        _append_exact(existing["rows"], [row], session_id)
        existing.update(revision=len(existing["rows"]), prefix_digest=public_rows_digest(existing["rows"]))
    for row in command.payload.get("private_rows", []):
        existing = namespace["private_rows"].get(row["key"])
        digest = private_ciphertext_digest(row["ciphertext"])
        if existing is not None and existing["ciphertext"] == row["ciphertext"]:
            continue
        if (existing["digest"] if existing is not None else None) != row["previous_digest"]:
            raise ControlPlaneError("private receipt compare-and-set conflict")
        namespace["private_rows"][row["key"]] = {"ciphertext": row["ciphertext"], "digest": digest}
    for session_id, journal in namespace["sessions"].items():
        _leadership_prefix(state, session_id, journal["rows"])
    _check_global_event_ids(namespace)
    _record_accepted(namespace, command)
    _canonical(state, maximum=MAX_SNAPSHOT_BYTES)
    return state, ()


def append_automatic_leadership_row(state: dict[str, Any], command: AuthorityCommand) -> None:
    """Allocate the release leadership row in the same committed state change."""
    source = command.payload
    session_id = source["session_id"]
    journal = journal_session(state, session_id)
    if journal is None:
        if session_id in state.get("product_journal", {}).get("initializing", {}):
            raise ControlPlaneError("leadership cannot change during journal initialization")
        return
    digest = hashlib.sha256((command.command_id + "\0" + command.content_hash).encode()).hexdigest()
    payload = {
        "schema": LEADERSHIP_SCHEMA, "session_id": session_id,
        "previous_leader_node_id": source["previous_leader_node_id"],
        "leader_node_id": source["leader_node_id"], "term": source["term"],
        "reason": source.get("reason", "replicated-authority-materialization"),
    }
    encoded = _canonical(payload, maximum=MAX_PUBLIC_PAYLOAD_BYTES).decode("utf-8")
    row = {
        "session_id": session_id, "revision": journal["revision"] + 1,
        "event_id": "c03-" + digest[:32], "request_id": "sha256:" + digest,
        "event_type": "session.leader.changed", "occurred_at": source["occurred_at"],
        "actor_node_id": REPLICATED_COORDINATOR_ID, "payload_json": encoded,
        "content_hash": public_row_content_hash("session.leader.changed", encoded),
    }
    validate_public_row(row)
    _append_exact(journal["rows"], [row], session_id)
    journal.update(revision=len(journal["rows"]), prefix_digest=public_rows_digest(journal["rows"]))
    _leadership_prefix(state, session_id, journal["rows"])
    _check_global_event_ids(state["product_journal"])


def require_product_envelope(state: Mapping[str, Any], command: AuthorityCommand) -> None:
    """Once a session is canonical, ordinary public effects need one envelope."""
    namespace = state.get("product_journal")
    if not namespace:
        return
    kind, payload = command.command_type, command.payload
    if kind in {"LEADER_TRANSITION", "FEDERATION_GENESIS"}:
        return
    if kind == "NODE_ENROLL" and state["nodes"].get(payload["node_id"]) == {
        key: payload[key] for key in ("node_id", "display_name", "public_key")
    }:
        return  # The existing current-term barrier is an unchanged voter row.
    if kind == "CAPABILITY_DECLARE":
        from .control_plane_readiness import BOOTSTRAP_SEAL_CAPABILITY_ID

        if payload["capability_id"] == BOOTSTRAP_SEAL_CAPABILITY_ID:
            if journal_session(state, payload["session_id"]) is None:
                raise ControlPlaneError("bootstrap seal requires complete canonical journal initialization")
            return
    if kind in _INNER_TYPES:
        raise ControlPlaneError("release authority mutation requires PRODUCT_TRANSACTION")


def _check_global_event_ids(namespace: Mapping[str, Any]) -> None:
    identities: set[str] = set()
    for collection in (namespace["sessions"], namespace["initializing"]):
        for journal in collection.values():
            for row in journal["rows"]:
                if row["event_id"] in identities:
                    raise ControlPlaneError("public journal event ID is not globally unique")
                identities.add(row["event_id"])


def validate_product_journal_state(state: Mapping[str, Any]) -> None:
    """Check journal identity before installing or restoring a snapshot."""
    namespace = state.get("product_journal")
    if namespace is None:
        return
    required = {"schema", "sessions", "initializing", "private_rows"}
    outcome_fields = {"accepted_transactions", "accepted_transaction_order"}
    if not isinstance(namespace, dict) or set(namespace) not in (required, required | outcome_fields):
        raise ControlPlaneError("product journal has unexpected or missing fields")
    if namespace["schema"] != PRODUCT_JOURNAL_SCHEMA:
        raise ControlPlaneError("unsupported product journal schema")
    for name in ("sessions", "initializing", "private_rows"):
        if not isinstance(namespace[name], dict):
            raise ControlPlaneError("product journal collections must be objects")
    if set(namespace["sessions"]) & set(namespace["initializing"]):
        raise ControlPlaneError("an active journal cannot also be initializing")
    for name in ("sessions", "initializing"):
        for session_id, journal in namespace[name].items():
            if session_id not in state["sessions"]:
                raise ControlPlaneError("product journal session is unknown")
            count_key = "revision" if name == "sessions" else "expected_revision"
            fields = {"rows", count_key, "prefix_digest"}
            if isinstance(journal, dict) and "provenance" in journal:
                fields.add("provenance")
            _closed(journal, fields, "session journal")
            count = _uint(journal[count_key], count_key)
            _digest(journal["prefix_digest"], "prefix_digest")
            if "provenance" in journal:
                _validate_provenance(journal["provenance"], count)
            rows = journal["rows"]
            if not isinstance(rows, list):
                raise ControlPlaneError("session journal rows must be an array")
            checked: list[dict[str, Any]] = []
            for row in rows:
                validate_public_row(row, imported=True)
                if not _is_witnessed_row(journal, row):
                    _validate_capability_payload(row["event_type"], json.loads(row["payload_json"]))
                if row["revision"] != len(checked) + 1:
                    raise ControlPlaneError("snapshot public journal is not contiguous")
                _append_exact(checked, [row], session_id)
            if len(rows) > count:
                raise ControlPlaneError("snapshot public journal exceeds its revision")
            _validate_witness_binding(journal)
            if name == "sessions":
                if len(rows) != count or public_rows_digest(rows) != journal["prefix_digest"]:
                    raise ControlPlaneError("snapshot public journal prefix mismatch")
                _leadership_prefix(state, session_id, rows)
    for key, row in namespace["private_rows"].items():
        if not isinstance(key, str) or not _HMAC_SHA256.fullmatch(key):
            raise ControlPlaneError("snapshot private receipt key is malformed")
        _closed(row, {"ciphertext", "digest"}, "snapshot private receipt")
        if not isinstance(row["ciphertext"], str) or private_ciphertext_digest(row["ciphertext"]) != row["digest"]:
            raise ControlPlaneError("snapshot private receipt digest mismatch")
    accepted = namespace.get("accepted_transactions", {})
    order = namespace.get("accepted_transaction_order", [])
    if (
        not isinstance(accepted, dict) or not isinstance(order, list)
        or len(accepted) > MAX_COMMAND_RECEIPTS or len(order) != len(accepted)
        or any(not isinstance(identity, str) for identity in order)
        or len(set(order)) != len(order) or set(order) != set(accepted)
    ):
        raise ControlPlaneError("snapshot product outcome window is malformed or exceeds its bound")
    for identity, digest in accepted.items():
        _text(identity, "accepted transaction ID")
        _digest(digest, "accepted transaction digest")
    _check_global_event_ids(namespace)
    _canonical(namespace, maximum=MAX_SNAPSHOT_BYTES)
