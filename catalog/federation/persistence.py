"""Versioned transactional SQLite authority for the Phase 2 control plane.

This database is deliberately separate from :mod:`catalog.federation.outbox`.
The outbox tracks delivery attempts; the tables here are authoritative session
history and coordinator state.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
import sqlite3
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    RevisionGapError,
)
from .models import (
    CapabilityAnnouncement,
    CapabilityStatus,
    NodeIdentity,
    Session,
    SessionEvent,
    SessionState,
)
from .redaction import redact_secrets

SCHEMA_VERSION = 1
COORDINATOR_ID = "msh-relay-coordinator"
MAX_EVENT_PAYLOAD_BYTES = 48_000
MAX_AUDIT_DETAILS_BYTES = 8_192
MAX_AUDIT_ROWS = 100_000
MAX_AUDIT_READ_LIMIT = 10_000
DEFAULT_AUDIT_READ_LIMIT = 1_000
MAX_REPLAY_EVENTS = 10_000
MAX_TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_TOKEN_USES = 100
STATUS_PAGE_SCHEMA = "msh.coordinator_status.pagination.v1"
STATUS_PAGE_MAX_BYTES = 60_000
STATUS_PAGE_MAX_ITEMS = 64
STATUS_CURSOR_MAX_BYTES = 512
_STATUS_CURSOR_VERSION = 1


def _time(value: datetime, field: str = "now") -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-timestamp", field, "must be timezone-aware"
        )
    return value.astimezone(timezone.utc).isoformat()


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FederationOperationError(
            "malformed-coordinator-state", "persisted timestamp is not timezone-aware"
        )
    return parsed.astimezone(timezone.utc)


def _canonical_json(
    value: Any, *, field: str, max_bytes: int
) -> tuple[str, str]:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-json", field, "must contain JSON-compatible values"
        ) from exc
    if len(encoded.encode("utf-8")) > max_bytes:
        raise FederationValidationError(
            "payload-too-large", field, f"exceeds {max_bytes} bytes"
        )
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _encode_status_cursor(*, offset: int, snapshot: str) -> str:
    encoded = json.dumps(
        {
            "offset": offset,
            "snapshot": snapshot,
            "version": _STATUS_CURSOR_VERSION,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return base64.urlsafe_b64encode(encoded).rstrip(b"=").decode("ascii")


def _decode_status_cursor(value: str) -> tuple[int, str]:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > STATUS_CURSOR_MAX_BYTES
        or not value.isascii()
    ):
        raise FederationValidationError(
            "invalid-status-cursor",
            "cursor",
            "must be a bounded opaque ASCII cursor",
        )
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.b64decode(
            value + padding,
            altchars=b"-_",
            validate=True,
        )
        payload = json.loads(decoded.decode("ascii"))
    except (
        binascii.Error,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise FederationValidationError(
            "invalid-status-cursor",
            "cursor",
            "must be a valid coordinator-issued cursor",
        ) from exc
    if not isinstance(payload, dict):
        raise FederationValidationError(
            "invalid-status-cursor",
            "cursor",
            "must be a valid coordinator-issued cursor",
        )
    offset = payload.get("offset")
    snapshot = payload.get("snapshot")
    version = payload.get("version")
    if (
        isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
        or version != _STATUS_CURSOR_VERSION
        or not isinstance(snapshot, str)
        or len(snapshot) != 64
        or any(character not in "0123456789abcdef" for character in snapshot)
    ):
        raise FederationValidationError(
            "invalid-status-cursor",
            "cursor",
            "must be a valid coordinator-issued cursor",
        )
    return offset, snapshot


def _status_payload_size(value: dict[str, Any]) -> int:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FederationOperationError(
            "malformed-coordinator-state",
            "coordinator status contains invalid JSON",
        ) from exc
    return len(encoded)


def _update_status_snapshot(
    digest: Any,
    *,
    section: str,
    values: tuple[Any, ...],
) -> None:
    encoded = json.dumps(
        [section, *values],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    digest.update(len(encoded).to_bytes(8, byteorder="big"))
    digest.update(encoded)


def _validate_token_bounds(ttl_seconds: int, max_uses: int) -> None:
    if (
        isinstance(ttl_seconds, bool)
        or not isinstance(ttl_seconds, int)
        or not 1 <= ttl_seconds <= MAX_TOKEN_TTL_SECONDS
    ):
        raise FederationValidationError(
            "invalid-token-ttl",
            "ttl_seconds",
            f"must be between 1 and {MAX_TOKEN_TTL_SECONDS}",
        )
    if (
        isinstance(max_uses, bool)
        or not isinstance(max_uses, int)
        or not 1 <= max_uses <= MAX_TOKEN_USES
    ):
        raise FederationValidationError(
            "invalid-token-use-limit",
            "max_uses",
            f"must be between 1 and {MAX_TOKEN_USES}",
        )


def _token_hash(raw_token: str) -> str:
    if not isinstance(raw_token, str) or len(raw_token) < 32:
        # Use a fixed digest path even for malformed input so lookup behavior
        # does not reveal whether a similarly prefixed token exists.
        raw_token = str(raw_token)
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def _request_key(request_id: str | None) -> str | None:
    """Persist only a stable digest of caller-controlled request identifiers."""

    if request_id is None:
        return None
    if (
        not isinstance(request_id, str)
        or not request_id
        or len(request_id) > 512
        or any(ord(character) < 32 for character in request_id)
    ):
        raise FederationValidationError(
            "invalid-request-id",
            "request_id",
            "must be bounded non-empty text without control characters",
        )
    return f"sha256:{hashlib.sha256(request_id.encode('utf-8')).hexdigest()}"


class CoordinatorStore:
    """Durable coordinator authority; every mutation uses ``BEGIN IMMEDIATE``."""

    def __init__(
        self,
        database: Path | str,
        *,
        coordinator_id: str = COORDINATOR_ID,
        token_factory: Callable[[int], str] = secrets.token_urlsafe,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.database = str(database)
        self.coordinator_id = str(coordinator_id)
        self._token_factory = token_factory
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        database = self._connect()
        try:
            database.execute("BEGIN IMMEDIATE")
            yield database
            database.commit()
        except BaseException:
            database.rollback()
            raise
        finally:
            database.close()

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        """Keep related authority reads on one SQLite snapshot."""

        database = self._connect()
        try:
            database.execute("BEGIN")
            yield database
            database.commit()
        except BaseException:
            database.rollback()
            raise
        finally:
            database.close()

    def initialize(self) -> None:
        with self._connect() as database:
            database.execute("PRAGMA journal_mode=WAL")
        with self.transaction() as database:
            database.executescript(
                """
                BEGIN IMMEDIATE;

                CREATE TABLE IF NOT EXISTS coordinator_schema (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    version INTEGER NOT NULL CHECK(version > 0)
                );
                INSERT OR IGNORE INTO coordinator_schema(singleton, version)
                    VALUES(1, 1);

                CREATE TABLE IF NOT EXISTS enrollment_tokens (
                    token_hash TEXT PRIMARY KEY,
                    token_id TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    max_uses INTEGER NOT NULL CHECK(max_uses > 0),
                    use_count INTEGER NOT NULL DEFAULT 0
                        CHECK(use_count >= 0 AND use_count <= max_uses),
                    created_by TEXT NOT NULL,
                    revoked_at TEXT
                );

                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    public_key TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    identity_version INTEGER NOT NULL CHECK(identity_version >= 0),
                    enrolled_at TEXT NOT NULL,
                    revoked_at TEXT,
                    revoked_by TEXT,
                    revocation_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
                    created_at TEXT NOT NULL,
                    created_by_node_id TEXT NOT NULL REFERENCES nodes(node_id),
                    coordinator_id TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS session_memberships (
                    session_id TEXT NOT NULL REFERENCES sessions(session_id),
                    node_id TEXT NOT NULL REFERENCES nodes(node_id),
                    joined_at TEXT NOT NULL,
                    removed_at TEXT,
                    removed_by TEXT,
                    removal_reason TEXT,
                    PRIMARY KEY(session_id, node_id)
                );
                CREATE INDEX IF NOT EXISTS memberships_by_node
                    ON session_memberships(node_id, session_id);

                CREATE TABLE IF NOT EXISTS session_invitations (
                    token_hash TEXT PRIMARY KEY,
                    invitation_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL REFERENCES sessions(session_id),
                    created_by_node_id TEXT NOT NULL REFERENCES nodes(node_id),
                    request_id TEXT NOT NULL,
                    requested_ttl_seconds INTEGER NOT NULL CHECK(
                        requested_ttl_seconds > 0
                    ),
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    max_uses INTEGER NOT NULL CHECK(max_uses > 0),
                    use_count INTEGER NOT NULL DEFAULT 0
                        CHECK(use_count >= 0 AND use_count <= max_uses),
                    revoked_at TEXT,
                    UNIQUE(created_by_node_id, request_id)
                );

                CREATE TABLE IF NOT EXISTS session_join_requests (
                    node_id TEXT NOT NULL REFERENCES nodes(node_id),
                    request_id TEXT NOT NULL,
                    token_hash TEXT NOT NULL,
                    session_id TEXT NOT NULL REFERENCES sessions(session_id),
                    accepted_at TEXT NOT NULL,
                    PRIMARY KEY(node_id, request_id)
                );

                CREATE TABLE IF NOT EXISTS capabilities (
                    session_id TEXT NOT NULL,
                    capability_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    protocol_version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    properties_json TEXT NOT NULL CHECK(json_valid(properties_json)),
                    announced_at TEXT NOT NULL,
                    last_heartbeat_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, capability_id),
                    FOREIGN KEY(session_id, node_id)
                        REFERENCES session_memberships(session_id, node_id)
                );
                CREATE INDEX IF NOT EXISTS capabilities_deterministic
                    ON capabilities(session_id, type, node_id, capability_id);

                CREATE TABLE IF NOT EXISTS node_connectivity (
                    node_id TEXT PRIMARY KEY REFERENCES nodes(node_id),
                    state TEXT NOT NULL CHECK(
                        state IN ('disconnected','connected','replaying','error','revoked')
                    ),
                    connection_id TEXT,
                    connected_at TEXT,
                    disconnected_at TEXT,
                    last_heartbeat_at TEXT,
                    last_error TEXT CHECK(last_error IS NULL OR length(last_error) <= 2048)
                );

                CREATE TABLE IF NOT EXISTS session_events (
                    session_id TEXT NOT NULL REFERENCES sessions(session_id),
                    revision INTEGER NOT NULL CHECK(revision > 0),
                    event_id TEXT NOT NULL UNIQUE,
                    request_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    actor_node_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
                    content_hash TEXT NOT NULL,
                    PRIMARY KEY(session_id, revision),
                    UNIQUE(session_id, actor_node_id, request_id)
                );
                CREATE INDEX IF NOT EXISTS events_replay
                    ON session_events(session_id, revision);

                CREATE TABLE IF NOT EXISTS accepted_requests (
                    actor_node_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    message_type TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    accepted_at TEXT NOT NULL,
                    PRIMARY KEY(actor_node_id, request_id)
                );

                CREATE TABLE IF NOT EXISTS audit_log (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at TEXT NOT NULL,
                    actor_node_id TEXT,
                    session_id TEXT,
                    request_id TEXT,
                    action TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    details_json TEXT NOT NULL CHECK(json_valid(details_json))
                );
                CREATE INDEX IF NOT EXISTS audit_order
                    ON audit_log(audit_id);
                """
            )
            version = database.execute(
                "SELECT version FROM coordinator_schema WHERE singleton=1"
            ).fetchone()[0]
            if version != SCHEMA_VERSION:
                raise FederationOperationError(
                    "unsupported-coordinator-schema",
                    f"expected {SCHEMA_VERSION}, found {version}",
                )

    def _audit(
        self,
        database: sqlite3.Connection,
        *,
        now: datetime,
        action: str,
        outcome: str,
        reason: str,
        actor_node_id: str | None = None,
        session_id: str | None = None,
        request_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details_json, _ = _canonical_json(
            redact_secrets(details or {}),
            field="audit.details",
            max_bytes=MAX_AUDIT_DETAILS_BYTES,
        )
        database.execute(
            """
            INSERT INTO audit_log(
                occurred_at,actor_node_id,session_id,request_id,
                action,outcome,reason,details_json
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                _time(now),
                redact_secrets(actor_node_id),
                redact_secrets(session_id),
                _request_key(request_id),
                redact_secrets(action),
                outcome,
                redact_secrets(reason),
                details_json,
            ),
        )
        database.execute(
            """
            DELETE FROM audit_log
            WHERE audit_id <= COALESCE(
                (
                    SELECT audit_id FROM audit_log
                    ORDER BY audit_id DESC
                    LIMIT 1 OFFSET ?
                ),
                -1
            )
            """,
            (MAX_AUDIT_ROWS,),
        )

    def audit_rejection(
        self,
        *,
        now: datetime,
        action: str,
        reason: str,
        actor_node_id: str | None = None,
        session_id: str | None = None,
        request_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.transaction() as database:
            self._audit(
                database,
                now=now,
                action=action,
                outcome="rejected",
                reason=reason,
                actor_node_id=actor_node_id,
                session_id=session_id,
                request_id=request_id,
                details=details,
            )

    def audit_entries(
        self,
        *,
        limit: int = DEFAULT_AUDIT_READ_LIMIT,
        actor_node_id: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_AUDIT_READ_LIMIT
        ):
            raise FederationValidationError(
                "invalid-audit-limit",
                "limit",
                f"must be between 1 and {MAX_AUDIT_READ_LIMIT}",
            )
        where = ""
        arguments: tuple[Any, ...]
        if actor_node_id is None:
            arguments = (limit,)
        else:
            where = "WHERE actor_node_id=?"
            arguments = (actor_node_id, limit)
        with self._connect() as database:
            rows = database.execute(
                f"""
                SELECT * FROM (
                    SELECT * FROM audit_log
                    {where}
                    ORDER BY audit_id DESC
                    LIMIT ?
                )
                ORDER BY audit_id
                """,
                arguments,
            ).fetchall()
        return tuple(
            {
                "audit_id": row["audit_id"],
                "occurred_at": row["occurred_at"],
                "actor_node_id": row["actor_node_id"],
                "session_id": row["session_id"],
                "request_id": row["request_id"],
                "action": row["action"],
                "outcome": row["outcome"],
                "reason": row["reason"],
                "details": json.loads(row["details_json"]),
            }
            for row in rows
        )

    def create_enrollment_token(
        self,
        *,
        now: datetime,
        ttl_seconds: int = 600,
        max_uses: int = 1,
        created_by: str = "local-admin",
    ) -> dict[str, Any]:
        _validate_token_bounds(ttl_seconds, max_uses)
        token_id = f"enroll-{self._id_factory()}"
        raw_token = f"msh_enroll_{self._token_factory(32)}"
        expires_at = now + timedelta(seconds=ttl_seconds)
        with self.transaction() as database:
            database.execute(
                """
                INSERT INTO enrollment_tokens(
                    token_hash,token_id,created_at,expires_at,max_uses,created_by
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    _token_hash(raw_token),
                    token_id,
                    _time(now),
                    _time(expires_at),
                    max_uses,
                    created_by,
                ),
            )
            self._audit(
                database,
                now=now,
                action="enrollment-token.created",
                outcome="accepted",
                reason="bounded-token-created",
                details={
                    "token_id": token_id,
                    "expires_at": _time(expires_at),
                    "max_uses": max_uses,
                },
            )
        return {
            "token": raw_token,
            "token_id": token_id,
            "expires_at": _time(expires_at),
            "max_uses": max_uses,
        }

    def enroll_node(
        self, identity: NodeIdentity, *, raw_token: str, now: datetime
    ) -> NodeIdentity:
        digest = _token_hash(raw_token)
        rejection: AuthenticationError | None = None
        with self.transaction() as database:
            token = database.execute(
                "SELECT * FROM enrollment_tokens WHERE token_hash=?", (digest,)
            ).fetchone()
            reason: str | None = None
            if token is None:
                reason = "unknown-enrollment-token"
            elif token["revoked_at"] is not None:
                reason = "revoked-enrollment-token"
            elif now >= _parse_time(token["expires_at"]):
                reason = "expired-enrollment-token"
            elif token["use_count"] >= token["max_uses"]:
                reason = "reused-enrollment-token"
            if reason is not None:
                self._audit(
                    database,
                    now=now,
                    action="node.enroll",
                    outcome="rejected",
                    reason=reason,
                    actor_node_id=identity.node_id,
                )
                rejection = AuthenticationError(
                    reason, "enrollment token was rejected", "token"
                )
            else:
                existing = database.execute(
                    "SELECT * FROM nodes WHERE node_id=? OR public_key=?",
                    (identity.node_id, identity.public_key),
                ).fetchone()
                if existing is not None:
                    self._audit(
                        database,
                        now=now,
                        action="node.enroll",
                        outcome="rejected",
                        reason="identity-already-enrolled",
                        actor_node_id=identity.node_id,
                    )
                    rejection = AuthenticationError(
                        "identity-already-enrolled",
                        "node identity is already enrolled",
                        "node_id",
                    )
                else:
                    database.execute(
                        """
                        INSERT INTO nodes(
                            node_id,display_name,public_key,created_at,
                            identity_version,enrolled_at
                        ) VALUES(?,?,?,?,?,?)
                        """,
                        (
                            identity.node_id,
                            identity.display_name,
                            identity.public_key,
                            _time(identity.created_at, "created_at"),
                            identity.identity_version,
                            _time(now),
                        ),
                    )
                    database.execute(
                        """
                        UPDATE enrollment_tokens SET use_count=use_count+1
                        WHERE token_hash=?
                        """,
                        (digest,),
                    )
                    database.execute(
                        """
                        INSERT INTO node_connectivity(node_id,state)
                            VALUES(?, 'disconnected')
                        """,
                        (identity.node_id,),
                    )
                    self._audit(
                        database,
                        now=now,
                        action="node.enroll",
                        outcome="accepted",
                        reason="identity-enrolled",
                        actor_node_id=identity.node_id,
                        details={"token_id": token["token_id"]},
                    )
        if rejection is not None:
            raise rejection
        return identity

    @staticmethod
    def _node_from_row(row: sqlite3.Row) -> NodeIdentity:
        return NodeIdentity(
            node_id=row["node_id"],
            display_name=row["display_name"],
            public_key=row["public_key"],
            created_at=_parse_time(row["created_at"]),
            identity_version=row["identity_version"],
        )

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        with self._connect() as database:
            row = database.execute(
                "SELECT * FROM nodes WHERE node_id=?", (node_id,)
            ).fetchone()
        if row is None:
            return None
        return {
            "identity": self._node_from_row(row),
            "enrolled_at": _parse_time(row["enrolled_at"]),
            "revoked_at": (
                _parse_time(row["revoked_at"]) if row["revoked_at"] else None
            ),
            "revoked_by": row["revoked_by"],
            "revocation_reason": row["revocation_reason"],
        }

    def require_active_node(
        self, node_id: str, *, database: sqlite3.Connection | None = None
    ) -> sqlite3.Row:
        owns_connection = database is None
        connection = database or self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM nodes WHERE node_id=?", (node_id,)
            ).fetchone()
            if row is None:
                raise AuthenticationError(
                    "unknown-node", "node is not enrolled", "actor_node_id"
                )
            if row["revoked_at"] is not None:
                raise AuthenticationError(
                    "revoked-node", "node identity is revoked", "actor_node_id"
                )
            return row
        finally:
            if owns_connection:
                connection.close()

    def public_identity(self, node_id: str) -> NodeIdentity:
        node = self.get_node(node_id)
        if node is None:
            raise AuthenticationError(
                "unknown-node", "node is not enrolled", "actor_node_id"
            )
        if node["revoked_at"] is not None:
            raise AuthenticationError(
                "revoked-node", "node identity is revoked", "actor_node_id"
            )
        return node["identity"]

    def _require_membership(
        self,
        database: sqlite3.Connection,
        *,
        session_id: str,
        node_id: str,
    ) -> sqlite3.Row:
        self.require_active_node(node_id, database=database)
        row = database.execute(
            """
            SELECT membership.*, session.state AS session_state,
                   session.created_by_node_id
            FROM session_memberships AS membership
            JOIN sessions AS session USING(session_id)
            WHERE membership.session_id=? AND membership.node_id=?
            """,
            (session_id, node_id),
        ).fetchone()
        if row is None or row["removed_at"] is not None:
            raise AuthorizationError(
                "not-session-member",
                "node has not joined the target session",
                "session_id",
            )
        if row["session_state"] not in {
            SessionState.ACTIVE.value,
            SessionState.PAUSED.value,
            SessionState.DEGRADED.value,
        }:
            raise AuthorizationError(
                "session-not-active",
                "session does not accept traffic in its current state",
                "session_id",
            )
        return row

    def require_membership(self, *, session_id: str, node_id: str) -> None:
        with self._connect() as database:
            self._require_membership(
                database, session_id=session_id, node_id=node_id
            )

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> SessionEvent:
        return SessionEvent(
            session_id=row["session_id"],
            revision=row["revision"],
            event_id=row["event_id"],
            event_type=row["event_type"],
            occurred_at=_parse_time(row["occurred_at"]),
            actor_node_id=row["actor_node_id"],
            payload=json.loads(row["payload_json"]),
        )

    def _append_event_tx(
        self,
        database: sqlite3.Connection,
        *,
        session_id: str,
        actor_node_id: str,
        request_id: str,
        event_type: str,
        payload: dict[str, Any],
        now: datetime,
        authorize_actor: bool,
    ) -> tuple[SessionEvent, bool]:
        request_key = _request_key(request_id)
        payload_json, payload_hash = _canonical_json(
            payload, field="payload", max_bytes=MAX_EVENT_PAYLOAD_BYTES
        )
        content_hash = hashlib.sha256(
            f"{event_type}\0{payload_hash}".encode()
        ).hexdigest()
        if authorize_actor:
            # Authorization and idempotent lookup share one transaction. A
            # removed member must not recover an old accepted response.
            self._require_membership(
                database, session_id=session_id, node_id=actor_node_id
            )
        existing = database.execute(
            """
            SELECT * FROM session_events
            WHERE session_id=? AND actor_node_id=? AND request_id=?
            """,
            (session_id, actor_node_id, request_key),
        ).fetchone()
        if existing is not None:
            if (
                existing["event_type"] != event_type
                or existing["content_hash"] != content_hash
            ):
                raise FederationOperationError(
                    "idempotency-conflict",
                    "event request ID was reused with different content",
                    "request_id",
                )
            return self._event_from_row(existing), False
        session = database.execute(
            "SELECT revision FROM sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if session is None:
            raise AuthorizationError(
                "unknown-session", "target session does not exist", "session_id"
            )
        revision = session["revision"] + 1
        event_id = f"event-{self._id_factory()}"
        database.execute(
            """
            INSERT INTO session_events(
                session_id,revision,event_id,request_id,event_type,occurred_at,
                actor_node_id,payload_json,content_hash
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                session_id,
                revision,
                event_id,
                request_key,
                event_type,
                _time(now),
                actor_node_id,
                payload_json,
                content_hash,
            ),
        )
        database.execute(
            "UPDATE sessions SET revision=? WHERE session_id=?",
            (revision, session_id),
        )
        return (
            SessionEvent(
                session_id=session_id,
                revision=revision,
                event_id=event_id,
                event_type=event_type,
                occurred_at=now,
                actor_node_id=actor_node_id,
                payload=payload,
            ),
            True,
        )

    def append_event(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        request_id: str,
        event_type: str,
        payload: dict[str, Any],
        now: datetime,
    ) -> tuple[SessionEvent, bool]:
        with self.transaction() as database:
            event, created = self._append_event_tx(
                database,
                session_id=session_id,
                actor_node_id=actor_node_id,
                request_id=request_id,
                event_type=event_type,
                payload=payload,
                now=now,
                authorize_actor=True,
            )
            if created:
                self._audit(
                    database,
                    now=now,
                    action="session-event.append",
                    outcome="accepted",
                    reason="event-appended",
                    actor_node_id=actor_node_id,
                    session_id=session_id,
                    request_id=request_id,
                    details={
                        "event_id": event.event_id,
                        "revision": event.revision,
                        "event_type": event.event_type,
                    },
                )
        return event, created

    def replay_events(
        self,
        *,
        session_id: str,
        last_applied_revision: int,
        actor_node_id: str | None = None,
    ) -> tuple[SessionEvent, ...]:
        events, current_revision = self.replay_event_page(
            session_id=session_id,
            last_applied_revision=last_applied_revision,
            actor_node_id=actor_node_id,
            limit=MAX_REPLAY_EVENTS,
        )
        if current_revision - last_applied_revision > MAX_REPLAY_EVENTS:
            raise RevisionGapError(
                "replay-window-too-large",
                f"use paged replay for more than {MAX_REPLAY_EVENTS} events",
                "last_applied_revision",
            )
        return events

    def replay_event_page(
        self,
        *,
        session_id: str,
        last_applied_revision: int,
        actor_node_id: str | None,
        limit: int,
    ) -> tuple[tuple[SessionEvent, ...], int]:
        """Read one exact, contiguous replay page and its snapshot revision."""

        if (
            isinstance(last_applied_revision, bool)
            or not isinstance(last_applied_revision, int)
            or last_applied_revision < 0
        ):
            raise FederationValidationError(
                "invalid-non-negative-integer",
                "last_applied_revision",
                "must be a non-negative integer",
            )
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_REPLAY_EVENTS
        ):
            raise FederationValidationError(
                "invalid-replay-limit",
                "limit",
                f"must be between 1 and {MAX_REPLAY_EVENTS}",
            )
        with self.read_transaction() as database:
            if actor_node_id is not None:
                # Membership and event rows are observed from one SQLite
                # snapshot so removal cannot race an authorized replay.
                self._require_membership(
                    database,
                    session_id=session_id,
                    node_id=actor_node_id,
                )
            session = database.execute(
                "SELECT revision FROM sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if session is None:
                raise AuthorizationError(
                    "unknown-session", "target session does not exist", "session_id"
                )
            current_revision = session["revision"]
            if last_applied_revision > current_revision:
                raise RevisionGapError(
                    "revision-ahead",
                    "last applied revision exceeds coordinator revision",
                    "last_applied_revision",
                )
            rows = database.execute(
                """
                SELECT * FROM session_events
                WHERE session_id=? AND revision>?
                ORDER BY revision
                LIMIT ?
                """,
                (session_id, last_applied_revision, limit),
            ).fetchall()
        expected = last_applied_revision + 1
        events: list[SessionEvent] = []
        for row in rows:
            if row["revision"] != expected:
                raise RevisionGapError(
                    "authoritative-revision-gap",
                    f"expected revision {expected}, found {row['revision']}",
                    "revision",
                )
            events.append(self._event_from_row(row))
            expected += 1
        expected_page_end = min(
            current_revision,
            last_applied_revision + limit,
        )
        if expected != expected_page_end + 1:
            raise RevisionGapError(
                "authoritative-revision-gap",
                f"expected revision {expected}, page ends at {expected_page_end}",
                "revision",
            )
        return tuple(events), current_revision

    def event_for_request(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        request_id: str,
    ) -> SessionEvent | None:
        """Return one authoritative event by its intrinsic idempotency key."""

        with self.read_transaction() as database:
            row = database.execute(
                """
                SELECT * FROM session_events
                WHERE session_id=? AND actor_node_id=? AND request_id=?
                """,
                (
                    session_id,
                    actor_node_id,
                    _request_key(request_id),
                ),
            ).fetchone()
        return self._event_from_row(row) if row is not None else None

    def session_ids_for_node(self, node_id: str) -> tuple[str, ...]:
        with self.read_transaction() as database:
            rows = database.execute(
                """
                SELECT session_id FROM session_memberships
                WHERE node_id=? AND removed_at IS NULL
                ORDER BY session_id
                """,
                (node_id,),
            ).fetchall()
        return tuple(row["session_id"] for row in rows)

    def revocation_events(self, node_id: str) -> tuple[SessionEvent, ...]:
        """Return durable revocation events for externally revoked nodes."""

        with self.read_transaction() as database:
            rows = database.execute(
                """
                SELECT * FROM session_events
                WHERE event_type='node.revoked'
                  AND json_extract(payload_json, '$.node_id')=?
                ORDER BY session_id,revision
                """,
                (node_id,),
            ).fetchall()
        return tuple(self._event_from_row(row) for row in rows)

    def create_session(
        self,
        *,
        actor_node_id: str,
        display_name: str,
        request_id: str,
        now: datetime,
        session_id: str | None = None,
    ) -> Session:
        if not isinstance(display_name, str) or not display_name.strip():
            raise FederationValidationError(
                "invalid-id", "display_name", "must be non-empty text"
            )
        requested_session_id = session_id
        session_id = session_id or f"session-{self._id_factory()}"
        with self.transaction() as database:
            self.require_active_node(actor_node_id, database=database)
            existing_event = database.execute(
                """
                SELECT payload_json FROM session_events
                WHERE actor_node_id=? AND request_id=?
                  AND event_type='session.created'
                """,
                (
                    actor_node_id,
                    _request_key(f"{request_id}:created"),
                ),
            ).fetchone()
            if existing_event is not None:
                existing_payload = json.loads(existing_event["payload_json"])
                existing_session_id = existing_payload["session_id"]
                if (
                    existing_payload.get("display_name") != display_name.strip()
                    or (
                        requested_session_id is not None
                        and existing_session_id != session_id
                    )
                ):
                    raise FederationOperationError(
                        "idempotency-conflict",
                        "session request ID was reused with different content",
                        "request_id",
                    )
                return self._session_tx(database, existing_session_id)
            database.execute(
                """
                INSERT INTO sessions(
                    session_id,display_name,state,revision,created_at,
                    created_by_node_id,coordinator_id
                ) VALUES(?,?,?,0,?,?,?)
                """,
                (
                    session_id,
                    display_name.strip(),
                    SessionState.ACTIVE.value,
                    _time(now),
                    actor_node_id,
                    self.coordinator_id,
                ),
            )
            database.execute(
                """
                INSERT INTO session_memberships(session_id,node_id,joined_at)
                    VALUES(?,?,?)
                """,
                (session_id, actor_node_id, _time(now)),
            )
            self._append_event_tx(
                database,
                session_id=session_id,
                actor_node_id=actor_node_id,
                request_id=f"{request_id}:created",
                event_type="session.created",
                payload={"session_id": session_id, "display_name": display_name.strip()},
                now=now,
                authorize_actor=False,
            )
            self._append_event_tx(
                database,
                session_id=session_id,
                actor_node_id=actor_node_id,
                request_id=f"{request_id}:creator-joined",
                event_type="node.joined",
                payload={"node_id": actor_node_id},
                now=now,
                authorize_actor=False,
            )
            self._audit(
                database,
                now=now,
                action="session.create",
                outcome="accepted",
                reason="session-created",
                actor_node_id=actor_node_id,
                session_id=session_id,
                request_id=request_id,
            )
            return self._session_tx(database, session_id)

    def _session_tx(
        self, database: sqlite3.Connection, session_id: str
    ) -> Session:
        row = database.execute(
            "SELECT * FROM sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if row is None:
            raise AuthorizationError(
                "unknown-session", "target session does not exist", "session_id"
            )
        return Session(
            session_id=row["session_id"],
            display_name=row["display_name"],
            state=row["state"],
            revision=row["revision"],
            created_at=_parse_time(row["created_at"]),
            created_by_node_id=row["created_by_node_id"],
            coordinator_id=row["coordinator_id"],
        )

    def get_session(self, session_id: str) -> Session | None:
        with self._connect() as database:
            row = database.execute(
                "SELECT * FROM sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            return self._session_tx(database, session_id) if row is not None else None

    def create_invitation(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        now: datetime,
        ttl_seconds: int = 600,
        max_uses: int = 1,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        _validate_token_bounds(ttl_seconds, max_uses)
        invitation_id: str | None = None
        command_request_id = request_id or f"invite-command-{self._id_factory()}"
        request_key = _request_key(command_request_id)
        if request_key is None:  # pragma: no cover - non-optional invariant
            raise FederationValidationError(
                "invalid-request-id", "request_id", "must be non-empty text"
            )
        raw_token: str | None = None
        expires_at: datetime | None = None
        replaced = False
        with self.transaction() as database:
            membership = self._require_membership(
                database, session_id=session_id, node_id=actor_node_id
            )
            if membership["created_by_node_id"] != actor_node_id:
                raise AuthorizationError(
                    "invitation-not-authorized",
                    "only the session creator may issue invitations in Phase 2",
                    "actor_node_id",
                )
            existing = database.execute(
                """
                SELECT * FROM session_invitations
                WHERE created_by_node_id=? AND request_id=?
                """,
                (actor_node_id, request_key),
            ).fetchone()
            if existing is not None and (
                existing["session_id"] != session_id
                or existing["requested_ttl_seconds"] != ttl_seconds
                or existing["max_uses"] != max_uses
            ):
                raise FederationOperationError(
                    "idempotency-conflict",
                    "invitation request ID was reused with different bounds",
                    "request_id",
                )
            if existing is not None and existing["use_count"] > 0:
                raise FederationOperationError(
                    "invitation-already-used",
                    "an invitation from this request has already been used",
                    "request_id",
                )
            raw_token = f"msh_join_{self._token_factory(32)}"
            expires_at = now + timedelta(seconds=ttl_seconds)
            if existing is None:
                invitation_id = f"invite-{self._id_factory()}"
                database.execute(
                    """
                    INSERT INTO session_invitations(
                        token_hash,invitation_id,session_id,created_by_node_id,
                        request_id,requested_ttl_seconds,created_at,expires_at,
                        max_uses
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        _token_hash(raw_token),
                        invitation_id,
                        session_id,
                        actor_node_id,
                        request_key,
                        ttl_seconds,
                        _time(now),
                        _time(expires_at),
                        max_uses,
                    ),
                )
            else:
                # Raw tokens are deliberately unrecoverable. On an exact
                # retry before use, atomically revoke the unreachable value by
                # rotating its digest and return one replacement token.
                invitation_id = existing["invitation_id"]
                replaced = True
                database.execute(
                    """
                    UPDATE session_invitations
                    SET token_hash=?,created_at=?,expires_at=?,revoked_at=NULL
                    WHERE invitation_id=?
                    """,
                    (
                        _token_hash(raw_token),
                        _time(now),
                        _time(expires_at),
                        invitation_id,
                    ),
                )
            self._audit(
                database,
                now=now,
                action="session-invitation.created",
                outcome="accepted",
                reason=(
                    "idempotent-token-rotated"
                    if replaced
                    else "bounded-token-created"
                ),
                actor_node_id=actor_node_id,
                session_id=session_id,
                request_id=command_request_id,
                details={
                    "invitation_id": invitation_id,
                    "expires_at": _time(expires_at),
                    "max_uses": max_uses,
                },
            )
        if (
            raw_token is None
            or invitation_id is None
            or expires_at is None
        ):  # pragma: no cover - transactional invariant
            raise FederationOperationError(
                "invitation-state-error",
                "invitation creation produced no result",
            )
        return {
            "token": raw_token,
            "invitation_id": invitation_id,
            "session_id": session_id,
            "expires_at": _time(expires_at),
            "max_uses": max_uses,
            "replaced": replaced,
        }

    def join_session(
        self,
        *,
        node_id: str,
        raw_token: str,
        request_id: str,
        now: datetime,
        expected_session_id: str | None = None,
    ) -> Session:
        if expected_session_id is not None and (
            not isinstance(expected_session_id, str)
            or not expected_session_id.strip()
        ):
            raise FederationValidationError(
                "invalid-id",
                "expected_session_id",
                "must be non-empty opaque text",
            )
        digest = _token_hash(raw_token)
        request_key = _request_key(request_id)
        if request_key is None:  # pragma: no cover - non-optional API invariant
            raise FederationValidationError(
                "invalid-request-id", "request_id", "must be non-empty text"
            )
        rejection: AuthorizationError | FederationOperationError | None = None
        result: Session | None = None
        with self.transaction() as database:
            self.require_active_node(node_id, database=database)
            accepted_join = database.execute(
                """
                SELECT * FROM session_join_requests
                WHERE node_id=? AND request_id=?
                """,
                (node_id, request_key),
            ).fetchone()
            if accepted_join is not None:
                same_request = hmac.compare_digest(
                    accepted_join["token_hash"], digest
                ) and expected_session_id in {
                    None,
                    accepted_join["session_id"],
                }
                if not same_request:
                    self._audit(
                        database,
                        now=now,
                        action="session.join",
                        outcome="rejected",
                        reason="idempotency-conflict",
                        actor_node_id=node_id,
                        session_id=expected_session_id,
                        request_id=request_id,
                    )
                    rejection = FederationOperationError(
                        "idempotency-conflict",
                        "session join request ID was reused with different content",
                        "request_id",
                    )
                else:
                    membership = database.execute(
                        """
                        SELECT * FROM session_memberships
                        WHERE session_id=? AND node_id=?
                        """,
                        (accepted_join["session_id"], node_id),
                    ).fetchone()
                    if membership is None or membership["removed_at"] is not None:
                        self._audit(
                            database,
                            now=now,
                            action="session.join",
                            outcome="rejected",
                            reason="not-session-member",
                            actor_node_id=node_id,
                            session_id=accepted_join["session_id"],
                            request_id=request_id,
                        )
                        rejection = AuthorizationError(
                            "not-session-member",
                            "node is no longer a member of the joined session",
                            "session_id",
                        )
                    else:
                        self._audit(
                            database,
                            now=now,
                            action="session.join",
                            outcome="accepted",
                            reason="idempotent-request-replayed",
                            actor_node_id=node_id,
                            session_id=accepted_join["session_id"],
                            request_id=request_id,
                        )
                        result = self._session_tx(
                            database, accepted_join["session_id"]
                        )
            else:
                invitation = database.execute(
                    "SELECT * FROM session_invitations WHERE token_hash=?",
                    (digest,),
                ).fetchone()
                reason: str | None = None
                if invitation is None:
                    reason = "unknown-session-invitation"
                elif invitation["revoked_at"] is not None:
                    reason = "revoked-session-invitation"
                elif now >= _parse_time(invitation["expires_at"]):
                    reason = "expired-session-invitation"
                elif invitation["use_count"] >= invitation["max_uses"]:
                    reason = "reused-session-invitation"
                elif (
                    expected_session_id is not None
                    and invitation["session_id"] != expected_session_id
                ):
                    reason = "wrong-session-invitation"
                if reason is not None:
                    self._audit(
                        database,
                        now=now,
                        action="session.join",
                        outcome="rejected",
                        reason=reason,
                        actor_node_id=node_id,
                        session_id=expected_session_id,
                        request_id=request_id,
                    )
                    rejection = AuthorizationError(
                        reason,
                        "session invitation was rejected",
                        (
                            "session_id"
                            if reason == "wrong-session-invitation"
                            else "token"
                        ),
                    )
                else:
                    session_id = invitation["session_id"]
                    membership = database.execute(
                        """
                        SELECT * FROM session_memberships
                        WHERE session_id=? AND node_id=?
                        """,
                        (session_id, node_id),
                    ).fetchone()
                    if membership is not None and membership["removed_at"] is None:
                        self._audit(
                            database,
                            now=now,
                            action="session.join",
                            outcome="rejected",
                            reason="already-session-member",
                            actor_node_id=node_id,
                            session_id=session_id,
                            request_id=request_id,
                        )
                        rejection = AuthorizationError(
                            "already-session-member",
                            "node has already joined this session",
                            "node_id",
                        )
                    else:
                        database.execute(
                            """
                            UPDATE session_invitations SET use_count=use_count+1
                            WHERE token_hash=?
                            """,
                            (digest,),
                        )
                        if membership is None:
                            database.execute(
                                """
                                INSERT INTO session_memberships(
                                    session_id,node_id,joined_at
                                ) VALUES(?,?,?)
                                """,
                                (session_id, node_id, _time(now)),
                            )
                        else:
                            database.execute(
                                """
                                UPDATE session_memberships
                                SET joined_at=?,removed_at=NULL,removed_by=NULL,
                                    removal_reason=NULL
                                WHERE session_id=? AND node_id=?
                                """,
                                (_time(now), session_id, node_id),
                            )
                        self._append_event_tx(
                            database,
                            session_id=session_id,
                            actor_node_id=node_id,
                            request_id=f"{request_id}:joined",
                            event_type="node.joined",
                            payload={"node_id": node_id},
                            now=now,
                            authorize_actor=False,
                        )
                        database.execute(
                            """
                            INSERT INTO session_join_requests(
                                node_id,request_id,token_hash,session_id,accepted_at
                            ) VALUES(?,?,?,?,?)
                            """,
                            (
                                node_id,
                                request_key,
                                digest,
                                session_id,
                                _time(now),
                            ),
                        )
                        self._audit(
                            database,
                            now=now,
                            action="session.join",
                            outcome="accepted",
                            reason="membership-created",
                            actor_node_id=node_id,
                            session_id=session_id,
                            request_id=request_id,
                            details={
                                "invitation_id": invitation["invitation_id"]
                            },
                        )
                        result = self._session_tx(database, session_id)
        if rejection is not None:
            raise rejection
        if result is None:  # pragma: no cover - defensive invariant
            raise FederationOperationError(
                "join-state-error", "session join produced no result"
            )
        return result

    def remove_member(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        target_node_id: str,
        request_id: str,
        reason: str,
        now: datetime,
    ) -> bool:
        summary = str(reason).strip()[:512] or "removed-by-session-owner"
        if redact_secrets(summary) != summary:
            raise FederationValidationError(
                "nonpublic-removal-reason",
                "reason",
                "must not contain credentials, backend paths, or physical addresses",
            )
        with self.transaction() as database:
            actor_membership = self._require_membership(
                database, session_id=session_id, node_id=actor_node_id
            )
            if (
                actor_node_id != target_node_id
                and actor_membership["created_by_node_id"] != actor_node_id
            ):
                raise AuthorizationError(
                    "membership-removal-not-authorized",
                    "only the session creator may remove another node",
                    "actor_node_id",
                )
            target = database.execute(
                """
                SELECT * FROM session_memberships
                WHERE session_id=? AND node_id=?
                """,
                (session_id, target_node_id),
            ).fetchone()
            prior_removal = database.execute(
                """
                SELECT payload_json FROM session_events
                WHERE session_id=? AND actor_node_id=? AND request_id=?
                  AND event_type='node.left'
                """,
                (
                    session_id,
                    actor_node_id,
                    _request_key(f"{request_id}:left"),
                ),
            ).fetchone()
            if prior_removal is not None:
                prior_payload = json.loads(prior_removal["payload_json"])
                if prior_payload != {
                    "node_id": target_node_id,
                    "reason": summary,
                }:
                    raise FederationOperationError(
                        "idempotency-conflict",
                        "membership removal request ID was reused with different content",
                        "request_id",
                    )
                if target is not None and target["removed_at"] is None:
                    raise FederationOperationError(
                        "idempotency-conflict",
                        "membership removal request belongs to an earlier membership lifecycle",
                        "request_id",
                    )
                return False
            if target is None or target["removed_at"] is not None:
                return False
            database.execute(
                """
                UPDATE session_memberships
                SET removed_at=?,removed_by=?,removal_reason=?
                WHERE session_id=? AND node_id=?
                """,
                (
                    _time(now),
                    actor_node_id,
                    summary,
                    session_id,
                    target_node_id,
                ),
            )
            database.execute(
                """
                UPDATE capabilities SET status=?
                WHERE session_id=? AND node_id=?
                """,
                (CapabilityStatus.REVOKED.value, session_id, target_node_id),
            )
            self._append_event_tx(
                database,
                session_id=session_id,
                actor_node_id=actor_node_id,
                request_id=f"{request_id}:left",
                event_type="node.left",
                payload={"node_id": target_node_id, "reason": summary},
                now=now,
                authorize_actor=False,
            )
            self._audit(
                database,
                now=now,
                action="session.member.remove",
                outcome="accepted",
                reason="membership-removed",
                actor_node_id=actor_node_id,
                session_id=session_id,
                request_id=request_id,
                details={"target_node_id": target_node_id},
            )
            return True

    def revoke_node(
        self,
        *,
        node_id: str,
        revoked_by: str,
        reason: str,
        request_id: str,
        now: datetime,
    ) -> bool:
        summary = str(reason).strip()[:512] or "revoked-by-local-admin"
        if redact_secrets(summary) != summary:
            raise FederationValidationError(
                "nonpublic-revocation-reason",
                "reason",
                "must not contain credentials, backend paths, or physical addresses",
            )
        with self.transaction() as database:
            row = database.execute(
                "SELECT * FROM nodes WHERE node_id=?", (node_id,)
            ).fetchone()
            if row is None:
                raise FederationOperationError(
                    "unknown-node", "node is not enrolled", "node_id"
                )
            if row["revoked_at"] is not None:
                return False
            database.execute(
                """
                UPDATE nodes
                SET revoked_at=?,revoked_by=?,revocation_reason=?
                WHERE node_id=?
                """,
                (_time(now), revoked_by, summary, node_id),
            )
            database.execute(
                """
                UPDATE node_connectivity
                SET state='revoked',disconnected_at=?,connection_id=NULL,
                    last_error='node identity revoked'
                WHERE node_id=?
                """,
                (_time(now), node_id),
            )
            database.execute(
                "UPDATE capabilities SET status=? WHERE node_id=?",
                (CapabilityStatus.REVOKED.value, node_id),
            )
            memberships = database.execute(
                """
                SELECT session_id FROM session_memberships
                WHERE node_id=? AND removed_at IS NULL
                ORDER BY session_id
                """,
                (node_id,),
            ).fetchall()
            for membership in memberships:
                session_id = membership["session_id"]
                self._append_event_tx(
                    database,
                    session_id=session_id,
                    actor_node_id=self.coordinator_id,
                    request_id=f"{request_id}:{session_id}",
                    event_type="node.revoked",
                    payload={"node_id": node_id, "reason": summary},
                    now=now,
                    authorize_actor=False,
                )
            self._audit(
                database,
                now=now,
                action="node.revoke",
                outcome="accepted",
                reason="node-revoked",
                actor_node_id=node_id,
                request_id=request_id,
                details={"revoked_by": revoked_by, "sessions": len(memberships)},
            )
            return True

    def upsert_capability(
        self,
        capability: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        request_id: str,
        now: datetime,
    ) -> tuple[CapabilityAnnouncement, bool]:
        if not isinstance(capability, CapabilityAnnouncement):
            raise FederationValidationError(
                "invalid-capability",
                "capability",
                "must be a CapabilityAnnouncement",
            )
        capability = CapabilityAnnouncement.from_dict(capability.to_dict())
        if capability.node_id != actor_node_id:
            raise AuthorizationError(
                "capability-node-mismatch",
                "capability node ID must match the authenticated actor",
                "node_id",
            )
        properties_json, _ = _canonical_json(
            capability.properties, field="properties", max_bytes=MAX_EVENT_PAYLOAD_BYTES
        )
        with self.transaction() as database:
            self._require_membership(
                database,
                session_id=capability.session_id,
                node_id=actor_node_id,
            )
            prior_request = database.execute(
                """
                SELECT event_type,payload_json FROM session_events
                WHERE session_id=? AND actor_node_id=? AND request_id=?
                """,
                (
                    capability.session_id,
                    actor_node_id,
                    _request_key(f"{request_id}:capability"),
                ),
            ).fetchone()
            if prior_request is not None:
                prior_capability = CapabilityAnnouncement.from_dict(
                    json.loads(prior_request["payload_json"])
                )
                if prior_capability != capability:
                    raise FederationOperationError(
                        "idempotency-conflict",
                        "capability request ID was reused with different content",
                        "request_id",
                    )
                return (
                    prior_capability,
                    prior_request["event_type"] == "capability.registered",
                )
            existing = database.execute(
                """
                SELECT * FROM capabilities
                WHERE session_id=? AND capability_id=?
                """,
                (capability.session_id, capability.capability_id),
            ).fetchone()
            created = existing is None
            if existing is not None and (
                existing["node_id"] != actor_node_id
                or existing["type"] != capability.type
            ):
                raise FederationOperationError(
                    "capability-identity-conflict",
                    "scoped capability identity belongs to another node or type",
                    "capability_id",
                )
            if created:
                database.execute(
                    """
                    INSERT INTO capabilities(
                        session_id,capability_id,node_id,type,protocol,
                        protocol_version,status,properties_json,
                        announced_at,last_heartbeat_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        capability.session_id,
                        capability.capability_id,
                        capability.node_id,
                        capability.type,
                        capability.protocol,
                        capability.protocol_version,
                        capability.status.value,
                        properties_json,
                        _time(capability.announced_at, "announced_at"),
                        _time(now),
                    ),
                )
            else:
                database.execute(
                    """
                    UPDATE capabilities
                    SET protocol=?,protocol_version=?,status=?,properties_json=?,
                        announced_at=?,last_heartbeat_at=?
                    WHERE session_id=? AND capability_id=?
                    """,
                    (
                        capability.protocol,
                        capability.protocol_version,
                        capability.status.value,
                        properties_json,
                        _time(capability.announced_at, "announced_at"),
                        _time(now),
                        capability.session_id,
                        capability.capability_id,
                    ),
                )
            event_type = (
                "capability.registered" if created else "capability.status.changed"
            )
            _, event_created = self._append_event_tx(
                database,
                session_id=capability.session_id,
                actor_node_id=actor_node_id,
                request_id=f"{request_id}:capability",
                event_type=event_type,
                payload=capability.to_dict(),
                now=now,
                authorize_actor=False,
            )
            self._audit(
                database,
                now=now,
                action="capability.announce",
                outcome="accepted",
                reason="capability-created" if created else "capability-updated",
                actor_node_id=actor_node_id,
                session_id=capability.session_id,
                request_id=request_id,
                details={
                    "capability_id": capability.capability_id,
                    "type": capability.type,
                    "event_created": event_created,
                },
            )
        return capability, created

    @staticmethod
    def _capability_from_row(row: sqlite3.Row) -> CapabilityAnnouncement:
        return CapabilityAnnouncement(
            capability_id=row["capability_id"],
            node_id=row["node_id"],
            session_id=row["session_id"],
            type=row["type"],
            protocol=row["protocol"],
            protocol_version=row["protocol_version"],
            status=row["status"],
            properties=json.loads(row["properties_json"]),
            announced_at=_parse_time(row["announced_at"]),
        )

    def list_capabilities(
        self,
        *,
        session_id: str | None = None,
        node_id: str | None = None,
        capability_type: str | None = None,
    ) -> tuple[CapabilityAnnouncement, ...]:
        clauses: list[str] = []
        arguments: list[str] = []
        for column, value in (
            ("session_id", session_id),
            ("node_id", node_id),
            ("type", capability_type),
        ):
            if value is not None:
                clauses.append(f"{column}=?")
                arguments.append(value)
        query = "SELECT * FROM capabilities"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY session_id,type,node_id,capability_id"
        with self._connect() as database:
            rows = database.execute(query, arguments).fetchall()
        return tuple(self._capability_from_row(row) for row in rows)

    def mark_connected(
        self,
        *,
        node_id: str,
        connection_id: str,
        now: datetime,
    ) -> None:
        with self.transaction() as database:
            self.require_active_node(node_id, database=database)
            database.execute(
                """
                UPDATE node_connectivity
                SET state='connected',connection_id=?,connected_at=?,
                    disconnected_at=NULL,last_heartbeat_at=?,last_error=NULL
                WHERE node_id=?
                """,
                (connection_id, _time(now), _time(now), node_id),
            )
            self._audit(
                database,
                now=now,
                action="node.connect",
                outcome="accepted",
                reason="identity-authenticated",
                actor_node_id=node_id,
                details={"connection_id": connection_id},
            )

    def heartbeat(self, *, node_id: str, now: datetime) -> None:
        with self.transaction() as database:
            self.require_active_node(node_id, database=database)
            connectivity = database.execute(
                "SELECT state FROM node_connectivity WHERE node_id=?", (node_id,)
            ).fetchone()
            if connectivity is None or connectivity["state"] != "connected":
                raise AuthenticationError(
                    "connection-not-active",
                    "heartbeat requires an authenticated active connection",
                    "node_id",
                )
            database.execute(
                """
                UPDATE node_connectivity
                SET last_heartbeat_at=?,last_error=NULL
                WHERE node_id=?
                """,
                (_time(now), node_id),
            )
            database.execute(
                "UPDATE capabilities SET last_heartbeat_at=? WHERE node_id=?",
                (_time(now), node_id),
            )

    def mark_disconnected(
        self, *, node_id: str, now: datetime, error: str | None = None
    ) -> tuple[SessionEvent, ...]:
        summary = str(error)[:2048] if error else None
        events: list[SessionEvent] = []
        with self.transaction() as database:
            row = database.execute(
                "SELECT state FROM node_connectivity WHERE node_id=?", (node_id,)
            ).fetchone()
            if row is None or row["state"] == "revoked":
                return ()
            database.execute(
                """
                UPDATE node_connectivity
                SET state=?,connection_id=NULL,disconnected_at=?,last_error=?
                WHERE node_id=?
                """,
                ("error" if summary else "disconnected", _time(now), summary, node_id),
            )
            capabilities = database.execute(
                """
                SELECT * FROM capabilities
                WHERE node_id=? AND status IN (?,?)
                ORDER BY session_id,capability_id
                """,
                (
                    node_id,
                    CapabilityStatus.READY.value,
                    CapabilityStatus.REGISTERING.value,
                ),
            ).fetchall()
            for capability in capabilities:
                database.execute(
                    """
                    UPDATE capabilities SET status=?
                    WHERE session_id=? AND capability_id=?
                    """,
                    (
                        CapabilityStatus.UNAVAILABLE.value,
                        capability["session_id"],
                        capability["capability_id"],
                    ),
                )
                event, created = self._append_event_tx(
                    database,
                    session_id=capability["session_id"],
                    actor_node_id=self.coordinator_id,
                    request_id=(
                        f"disconnect:{node_id}:"
                        f"{capability['capability_id']}:{_time(now)}"
                    ),
                    event_type="capability.health.changed",
                    payload={
                        "capability_id": capability["capability_id"],
                        "node_id": node_id,
                        "status": CapabilityStatus.UNAVAILABLE.value,
                        "reason": "node-disconnected",
                    },
                    now=now,
                    authorize_actor=False,
                )
                if created:
                    events.append(event)
            self._audit(
                database,
                now=now,
                action="node.disconnect",
                outcome="accepted",
                reason="connection-closed",
                actor_node_id=node_id,
                details={"capabilities_changed": len(capabilities)},
            )
        return tuple(events)

    def mark_all_disconnected(self, *, now: datetime) -> None:
        with self.transaction() as database:
            connected_nodes = database.execute(
                """
                SELECT node_id FROM node_connectivity
                WHERE state='connected'
                ORDER BY node_id
                """
            ).fetchall()
            database.execute(
                """
                UPDATE node_connectivity
                SET state='disconnected',connection_id=NULL,disconnected_at=?,
                    last_error=NULL
                WHERE state!='revoked'
                """,
                (_time(now),),
            )
            for node in connected_nodes:
                node_id = node["node_id"]
                capabilities = database.execute(
                    """
                    SELECT * FROM capabilities
                    WHERE node_id=? AND status IN (?,?)
                    ORDER BY session_id,capability_id
                    """,
                    (
                        node_id,
                        CapabilityStatus.READY.value,
                        CapabilityStatus.REGISTERING.value,
                    ),
                ).fetchall()
                for capability in capabilities:
                    database.execute(
                        """
                        UPDATE capabilities SET status=?
                        WHERE session_id=? AND capability_id=?
                        """,
                        (
                            CapabilityStatus.UNAVAILABLE.value,
                            capability["session_id"],
                            capability["capability_id"],
                        ),
                    )
                    self._append_event_tx(
                        database,
                        session_id=capability["session_id"],
                        actor_node_id=self.coordinator_id,
                        request_id=(
                            f"relay-restart:{node_id}:"
                            f"{capability['capability_id']}:{_time(now)}"
                        ),
                        event_type="capability.health.changed",
                        payload={
                            "capability_id": capability["capability_id"],
                            "node_id": node_id,
                            "status": CapabilityStatus.UNAVAILABLE.value,
                            "reason": "relay-restart",
                        },
                        now=now,
                        authorize_actor=False,
                    )

    def sweep_stale(
        self, *, now: datetime, heartbeat_timeout_seconds: float
    ) -> tuple[str, ...]:
        stale_nodes, _ = self.sweep_stale_with_events(
            now=now,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        )
        return stale_nodes

    def sweep_stale_with_events(
        self, *, now: datetime, heartbeat_timeout_seconds: float
    ) -> tuple[tuple[str, ...], tuple[SessionEvent, ...]]:
        if heartbeat_timeout_seconds <= 0:
            raise FederationValidationError(
                "invalid-heartbeat-timeout",
                "heartbeat_timeout_seconds",
                "must be positive",
            )
        stale_nodes: list[str] = []
        events: list[SessionEvent] = []
        with self.transaction() as database:
            rows = database.execute(
                """
                SELECT node_id,last_heartbeat_at FROM node_connectivity
                WHERE state='connected' AND last_heartbeat_at IS NOT NULL
                ORDER BY node_id
                """
            ).fetchall()
            for row in rows:
                elapsed = (now - _parse_time(row["last_heartbeat_at"])).total_seconds()
                if elapsed <= heartbeat_timeout_seconds:
                    continue
                node_id = row["node_id"]
                stale_nodes.append(node_id)
                database.execute(
                    """
                    UPDATE node_connectivity
                    SET state='disconnected',connection_id=NULL,disconnected_at=?,
                        last_error='stale heartbeat'
                    WHERE node_id=?
                    """,
                    (_time(now), node_id),
                )
                capabilities = database.execute(
                    """
                    SELECT * FROM capabilities
                    WHERE node_id=? AND status IN (?,?)
                    ORDER BY session_id,capability_id
                    """,
                    (
                        node_id,
                        CapabilityStatus.READY.value,
                        CapabilityStatus.REGISTERING.value,
                    ),
                ).fetchall()
                for capability in capabilities:
                    database.execute(
                        """
                        UPDATE capabilities SET status=?
                        WHERE session_id=? AND capability_id=?
                        """,
                        (
                            CapabilityStatus.UNAVAILABLE.value,
                            capability["session_id"],
                            capability["capability_id"],
                        ),
                    )
                    event, created = self._append_event_tx(
                        database,
                        session_id=capability["session_id"],
                        actor_node_id=self.coordinator_id,
                        request_id=(
                            f"heartbeat-expired:{node_id}:"
                            f"{capability['capability_id']}:{_time(now)}"
                        ),
                        event_type="capability.health.changed",
                        payload={
                            "capability_id": capability["capability_id"],
                            "node_id": node_id,
                            "status": CapabilityStatus.UNAVAILABLE.value,
                            "reason": "stale-heartbeat",
                        },
                        now=now,
                        authorize_actor=False,
                    )
                    if created:
                        events.append(event)
                self._audit(
                    database,
                    now=now,
                    action="heartbeat.expire",
                    outcome="accepted",
                    reason="stale-heartbeat",
                    actor_node_id=node_id,
                    details={"capabilities_changed": len(capabilities)},
                )
        return tuple(stale_nodes), tuple(events)

    def accept_request(
        self,
        *,
        actor_node_id: str,
        request_id: str,
        message_type: str,
        request_hash: str,
        now: datetime,
        session_id: str | None = None,
        target_node_id: str | None = None,
    ) -> bool:
        """Atomically deduplicate non-event relay requests.

        Repeats with identical content are acknowledged as duplicates; reuse
        with different content is a conflict. Only hashes are retained, so raw
        invitations, enrollment credentials, and payload secrets are not stored.
        """

        request_key = _request_key(request_id)
        with self.transaction() as database:
            self.require_active_node(actor_node_id, database=database)
            if session_id is not None:
                self._require_membership(
                    database,
                    session_id=session_id,
                    node_id=actor_node_id,
                )
                if target_node_id is not None:
                    try:
                        self._require_membership(
                            database,
                            session_id=session_id,
                            node_id=target_node_id,
                        )
                    except AuthenticationError as exc:
                        raise AuthorizationError(
                            "target-unavailable",
                            "target node is not available for session routing",
                            "target_node_id",
                        ) from exc
            existing = database.execute(
                """
                SELECT * FROM accepted_requests
                WHERE actor_node_id=? AND request_id=?
                """,
                (actor_node_id, request_key),
            ).fetchone()
            if existing is not None:
                if (
                    existing["message_type"] != message_type
                    or not hmac.compare_digest(existing["request_hash"], request_hash)
                ):
                    raise FederationOperationError(
                        "request-id-conflict",
                        "request ID was reused with different content",
                        "request_id",
                    )
                return False
            database.execute(
                """
                INSERT INTO accepted_requests(
                    actor_node_id,request_id,message_type,request_hash,accepted_at
                ) VALUES(?,?,?,?,?)
                """,
                (
                    actor_node_id,
                    request_key,
                    message_type,
                    request_hash,
                    _time(now),
                ),
            )
            return True

    def authorized_status(
        self,
        *,
        actor_node_id: str,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        requested_cursor = (
            _decode_status_cursor(cursor) if cursor is not None else None
        )
        with self.read_transaction() as database:
            self.require_active_node(actor_node_id, database=database)

            snapshot_digest = hashlib.sha256()
            session_count = 0
            for row in database.execute(
                """
                SELECT session.session_id,session.display_name,
                       session.state,session.revision
                FROM sessions AS session
                JOIN session_memberships AS membership USING(session_id)
                WHERE membership.node_id=? AND membership.removed_at IS NULL
                ORDER BY session.session_id
                """,
                (actor_node_id,),
            ):
                session_count += 1
                _update_status_snapshot(
                    snapshot_digest,
                    section="sessions",
                    values=(
                        row["session_id"],
                        row["display_name"],
                        row["state"],
                        row["revision"],
                    ),
                )

            node_count = 0
            for row in database.execute(
                """
                SELECT DISTINCT
                       node.node_id,node.display_name,node.revoked_at,
                       connectivity.state,connectivity.last_heartbeat_at
                FROM nodes AS node
                JOIN session_memberships AS shared_membership
                  ON shared_membership.node_id=node.node_id
                LEFT JOIN node_connectivity AS connectivity
                  ON connectivity.node_id=node.node_id
                WHERE shared_membership.removed_at IS NULL
                  AND EXISTS (
                    SELECT 1 FROM session_memberships AS actor_membership
                    WHERE actor_membership.session_id=
                          shared_membership.session_id
                      AND actor_membership.node_id=?
                      AND actor_membership.removed_at IS NULL
                  )
                ORDER BY node.node_id
                """,
                (actor_node_id,),
            ):
                node_count += 1
                _update_status_snapshot(
                    snapshot_digest,
                    section="nodes",
                    values=(
                        row["node_id"],
                        row["display_name"],
                        row["revoked_at"],
                        row["state"],
                        row["last_heartbeat_at"],
                    ),
                )

            capability_count = 0
            for row in database.execute(
                """
                SELECT capability.session_id,capability.type,
                       capability.node_id,capability.capability_id,
                       capability.protocol,capability.protocol_version,
                       capability.status,capability.properties_json,
                       capability.last_heartbeat_at
                FROM capabilities AS capability
                WHERE EXISTS (
                    SELECT 1 FROM session_memberships AS actor_membership
                    WHERE actor_membership.session_id=capability.session_id
                      AND actor_membership.node_id=?
                      AND actor_membership.removed_at IS NULL
                )
                ORDER BY capability.session_id,capability.type,
                         capability.node_id,capability.capability_id
                """,
                (actor_node_id,),
            ):
                capability_count += 1
                _update_status_snapshot(
                    snapshot_digest,
                    section="capabilities",
                    values=(
                        row["session_id"],
                        row["type"],
                        row["node_id"],
                        row["capability_id"],
                        row["protocol"],
                        row["protocol_version"],
                        row["status"],
                        row["properties_json"],
                        row["last_heartbeat_at"],
                    ),
                )

            snapshot = snapshot_digest.hexdigest()
            total_items = session_count + node_count + capability_count
            if requested_cursor is None:
                offset = 0
            else:
                offset, requested_snapshot = requested_cursor
                if not hmac.compare_digest(snapshot, requested_snapshot):
                    raise FederationOperationError(
                        "status-snapshot-changed",
                        "authorized status changed while pages were being read",
                        "cursor",
                    )
                if offset > total_items:
                    raise FederationValidationError(
                        "invalid-status-cursor",
                        "cursor",
                        "cursor offset is outside the authorized status",
                    )

            candidates: list[tuple[str, dict[str, Any]]] = []
            remaining = STATUS_PAGE_MAX_ITEMS

            if remaining and offset < session_count:
                local_offset = offset
                limit = min(remaining, session_count - local_offset)
                rows = database.execute(
                    """
                    SELECT session.* FROM sessions AS session
                    JOIN session_memberships AS membership USING(session_id)
                    WHERE membership.node_id=?
                      AND membership.removed_at IS NULL
                    ORDER BY session.session_id
                    LIMIT ? OFFSET ?
                    """,
                    (actor_node_id, limit, local_offset),
                ).fetchall()
                candidates.extend(
                    (
                        "sessions",
                        {
                            "session_id": redact_secrets(row["session_id"]),
                            "display_name": redact_secrets(
                                row["display_name"]
                            ),
                            "state": row["state"],
                            "revision": row["revision"],
                        },
                    )
                    for row in rows
                )
                remaining -= len(rows)

            node_start = session_count
            if (
                remaining
                and offset + len(candidates) < node_start + node_count
                and offset + len(candidates) >= node_start
            ):
                local_offset = offset + len(candidates) - node_start
                limit = min(remaining, node_count - local_offset)
                rows = database.execute(
                    """
                    SELECT DISTINCT
                           node.node_id,node.display_name,node.revoked_at,
                           connectivity.state,connectivity.last_heartbeat_at
                    FROM nodes AS node
                    JOIN session_memberships AS shared_membership
                      ON shared_membership.node_id=node.node_id
                    LEFT JOIN node_connectivity AS connectivity
                      ON connectivity.node_id=node.node_id
                    WHERE shared_membership.removed_at IS NULL
                      AND EXISTS (
                        SELECT 1
                        FROM session_memberships AS actor_membership
                        WHERE actor_membership.session_id=
                              shared_membership.session_id
                          AND actor_membership.node_id=?
                          AND actor_membership.removed_at IS NULL
                      )
                    ORDER BY node.node_id
                    LIMIT ? OFFSET ?
                    """,
                    (actor_node_id, limit, local_offset),
                ).fetchall()
                candidates.extend(
                    (
                        "nodes",
                        {
                            "node_id": redact_secrets(row["node_id"]),
                            "display_name": redact_secrets(
                                row["display_name"]
                            ),
                            "revoked": row["revoked_at"] is not None,
                            "connection_state": (
                                row["state"] or "disconnected"
                            ),
                            "last_heartbeat": row["last_heartbeat_at"],
                        },
                    )
                    for row in rows
                )
                remaining -= len(rows)

            capability_start = session_count + node_count
            if (
                remaining
                and offset + len(candidates)
                < capability_start + capability_count
                and offset + len(candidates) >= capability_start
            ):
                local_offset = offset + len(candidates) - capability_start
                limit = min(remaining, capability_count - local_offset)
                rows = database.execute(
                    """
                    SELECT capability.* FROM capabilities AS capability
                    WHERE EXISTS (
                        SELECT 1
                        FROM session_memberships AS actor_membership
                        WHERE actor_membership.session_id=
                              capability.session_id
                          AND actor_membership.node_id=?
                          AND actor_membership.removed_at IS NULL
                    )
                    ORDER BY capability.session_id,capability.type,
                             capability.node_id,capability.capability_id
                    LIMIT ? OFFSET ?
                    """,
                    (actor_node_id, limit, local_offset),
                ).fetchall()
                candidates.extend(
                    (
                        "capabilities",
                        {
                            "session_id": redact_secrets(row["session_id"]),
                            "capability_id": redact_secrets(
                                row["capability_id"]
                            ),
                            "node_id": redact_secrets(row["node_id"]),
                            "type": redact_secrets(row["type"]),
                            "protocol": redact_secrets(row["protocol"]),
                            "protocol_version": row["protocol_version"],
                            "status": row["status"],
                            "properties": redact_secrets(
                                json.loads(row["properties_json"])
                            ),
                            "last_heartbeat": row["last_heartbeat_at"],
                        },
                    )
                    for row in rows
                )

        page: dict[str, Any] = {
            "schema": "msh.coordinator_status.v1",
            "coordinator_id": self.coordinator_id,
            "sessions": [],
            "nodes": [],
            "capabilities": [],
        }

        def finalize_page(item_count: int) -> dict[str, Any]:
            next_offset = offset + item_count
            has_more = next_offset < total_items
            return {
                **page,
                "pagination": {
                    "schema": STATUS_PAGE_SCHEMA,
                    "page_start": offset,
                    "item_count": item_count,
                    "total_items": total_items,
                    "has_more": has_more,
                    "next_cursor": (
                        _encode_status_cursor(
                            offset=next_offset,
                            snapshot=snapshot,
                        )
                        if has_more
                        else None
                    ),
                },
            }

        accepted_count = 0
        for section, item in candidates:
            page[section].append(item)
            candidate_page = finalize_page(accepted_count + 1)
            if _status_payload_size(candidate_page) > STATUS_PAGE_MAX_BYTES:
                page[section].pop()
                break
            accepted_count += 1

        if accepted_count == 0 and offset < total_items:
            raise FederationOperationError(
                "status-entry-too-large",
                "one authorized status entry exceeds the bounded page size",
            )
        result = finalize_page(accepted_count)
        if _status_payload_size(result) > STATUS_PAGE_MAX_BYTES:
            raise FederationOperationError(
                "status-page-too-large",
                "coordinator status metadata exceeds the bounded page size",
            )
        return result
