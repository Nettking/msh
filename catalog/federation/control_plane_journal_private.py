"""Encrypted logical receipt rows accompanying committed public journal writes.

These rows contain token hashes and accepted-request receipts, never raw grants.
They are separate from the member-visible session journal. Only the fixed trusted
voters possessing the deployment secret can restore them. SQL identifiers come
only from the closed table specification below; no SQL or database pages cross
the replication transport.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import sqlite3
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from .control_plane_replication import ControlPlaneError

PRIVATE_ROW_SCHEMA = "fcp.control-plane.private-receipt-row.v1"
MAX_PRIVATE_ROW_BYTES = 16 * 1024
MAX_PRIVATE_CIPHERTEXT_CHARACTERS = 4 * ((MAX_PRIVATE_ROW_BYTES + 28 + 2) // 3)
MAX_PRIVATE_TEXT_BYTES = 4096
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_HMAC_SHA256 = re.compile(r"hmac-sha256:[0-9a-f]{64}\Z")
_TOKEN_HASH = re.compile(r"[0-9a-f]{64}\Z")
_INTEGER_COLUMNS = frozenset({"max_uses", "use_count", "requested_ttl_seconds"})
_TIMESTAMP_COLUMNS = frozenset({"created_at", "expires_at", "accepted_at", "revoked_at"})
PRIVATE_TABLES = {
    "enrollment_tokens": (
        ("token_hash",),
        ("token_hash", "token_id", "created_at", "expires_at", "max_uses", "use_count", "created_by", "revoked_at"),
    ),
    "session_invitations": (
        ("token_hash",),
        ("token_hash", "invitation_id", "session_id", "created_by_node_id", "request_id", "requested_ttl_seconds", "created_at", "expires_at", "max_uses", "use_count", "revoked_at"),
    ),
    "session_join_requests": (
        ("node_id", "request_id"),
        ("node_id", "request_id", "token_hash", "session_id", "accepted_at"),
    ),
    "accepted_requests": (
        ("actor_node_id", "request_id"),
        ("actor_node_id", "request_id", "message_type", "request_hash", "accepted_at"),
    ),
}


def _canonical(value: object) -> bytes:
    try:
        result = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ControlPlaneError("private receipt row is not canonical JSON") from None
    if len(result) > MAX_PRIVATE_ROW_BYTES:
        raise ControlPlaneError("private receipt row exceeds its bound")
    return result


def _text(value: object, *, maximum: int = MAX_PRIVATE_TEXT_BYTES) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= maximum:
        raise ControlPlaneError("private receipt text is malformed")
    if any(ord(char) < 32 for char in value):
        raise ControlPlaneError("private receipt text is malformed")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError:
        raise ControlPlaneError("private receipt text is malformed") from None
    if size > maximum:
        raise ControlPlaneError("private receipt text exceeds its bound")
    return value


def _digest(value: object) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ControlPlaneError("private receipt digest is malformed")
    return value


def _identity(value: object) -> str:
    if not isinstance(value, str) or _HMAC_SHA256.fullmatch(value) is None:
        raise ControlPlaneError("private receipt identity is malformed")
    return value


def _ciphertext(value: object) -> bytes:
    if not isinstance(value, str) or not 40 <= len(value) <= MAX_PRIVATE_CIPHERTEXT_CHARACTERS:
        raise ControlPlaneError("private receipt ciphertext exceeds its bound")
    try:
        encoded = base64.b64decode(value, validate=True)
    except (ValueError, UnicodeError):
        raise ControlPlaneError("private receipt ciphertext is not canonical base64") from None
    if (
        not 28 <= len(encoded) <= MAX_PRIVATE_ROW_BYTES + 28
        or base64.b64encode(encoded).decode("ascii") != value
    ):
        raise ControlPlaneError("private receipt ciphertext is not canonical base64")
    return encoded


def _timestamp(value: str) -> None:
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise ValueError("timezone required")
    except (ValueError, OverflowError):
        raise ControlPlaneError("private receipt timestamp is malformed") from None


def _validated(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or len(value) != 4 or set(value) != {"schema", "table", "key", "row"}:
        raise ControlPlaneError("private receipt envelope fields are invalid")
    if (
        value["schema"] != PRIVATE_ROW_SCHEMA
        or not isinstance(value["table"], str)
        or value["table"] not in PRIVATE_TABLES
    ):
        raise ControlPlaneError("private receipt table or schema is invalid")
    primary, columns = PRIVATE_TABLES[value["table"]]
    key = value["key"]
    if not isinstance(key, dict) or len(key) != len(primary) or set(key) != set(primary):
        raise ControlPlaneError("private receipt key fields are invalid")
    for name, item in key.items():
        _text(item, maximum=512)
        if name == "token_hash" and _TOKEN_HASH.fullmatch(item) is None:
            raise ControlPlaneError("private receipt token hash is malformed")
    row = value["row"]
    if row is not None:
        if not isinstance(row, dict) or len(row) != len(columns) or set(row) != set(columns):
            raise ControlPlaneError("private receipt row fields are invalid")
        if any(row[name] != key[name] for name in primary):
            raise ControlPlaneError("private receipt primary key changed")
        for name, item in row.items():
            if name in _INTEGER_COLUMNS:
                if isinstance(item, bool) or not isinstance(item, int) or not 0 <= item < 2**63:
                    raise ControlPlaneError("private receipt integer is malformed")
            elif name == "revoked_at" and item is None:
                continue
            else:
                _text(item)
                if name == "token_hash" and _TOKEN_HASH.fullmatch(item) is None:
                    raise ControlPlaneError("private receipt token hash is malformed")
                if name in _TIMESTAMP_COLUMNS:
                    _timestamp(item)
        if "use_count" in row:
            used, maximum = row["use_count"], row["max_uses"]
            if not 1 <= maximum <= 100 or not 0 <= used <= maximum:
                raise ControlPlaneError("private receipt consumption bounds are invalid")
        if "requested_ttl_seconds" in row and not 1 <= row["requested_ttl_seconds"] <= 7 * 24 * 60 * 60:
            raise ControlPlaneError("private receipt invitation lifetime is invalid")
    _canonical(value)
    return value


def _committed_rows(value: object) -> list[tuple[str, dict[str, Any]]]:
    if not isinstance(value, Mapping):
        raise ControlPlaneError("private committed receipts must be a mapping")
    result = []
    for key, envelope in value.items():
        _identity(key)
        if not isinstance(envelope, dict) or len(envelope) != 2 or set(envelope) != {"ciphertext", "digest"}:
            raise ControlPlaneError("private committed receipt fields are invalid")
        expected = "sha256:" + hashlib.sha256(_ciphertext(envelope["ciphertext"])).hexdigest()
        if not hmac.compare_digest(_digest(envelope["digest"]), expected):
            raise ControlPlaneError("private committed receipt digest mismatch")
        result.append((key, envelope))
    return sorted(result)


class PrivateJournalRows:
    """Seal exact absolute rows or tombstones using domain-separated keys."""

    def __init__(self, secret: bytes, cluster_id: str) -> None:
        if not isinstance(secret, bytes) or not 32 <= len(secret) <= 4096:
            raise ControlPlaneError("private receipt secret length is invalid")
        _text(cluster_id)
        self._domain = (PRIVATE_ROW_SCHEMA + ":" + cluster_id).encode("utf-8")
        derived = HKDF(
            algorithm=hashes.SHA256(), length=64,
            salt=hashlib.sha256(self._domain).digest(),
            info=b"fcp-product-journal-encryption-and-row-identity-v1",
        ).derive(secret)
        self._cipher = AESGCM(derived[:32])
        self._identity_key = derived[32:]

    def identity(self, value: Mapping[str, Any]) -> str:
        _validated(value)
        message = _canonical({"table": value["table"], "key": value["key"]})
        return "hmac-sha256:" + hmac.new(self._identity_key, message, hashlib.sha256).hexdigest()

    def capture(self, database: sqlite3.Connection) -> dict[str, dict[str, Any]]:
        captured = {}
        for table, (primary, columns) in PRIVATE_TABLES.items():
            rows = database.execute(f"SELECT {','.join(columns)} FROM {table}").fetchall()
            for entry in rows:
                row = dict(zip(columns, entry, strict=True))
                value = _validated({
                    "schema": PRIVATE_ROW_SCHEMA, "table": table,
                    "key": {name: row[name] for name in primary}, "row": row,
                })
                captured[self.identity(value)] = value
        return captured

    def seal(self, value: dict[str, Any], *, previous_digest: str | None) -> dict[str, Any]:
        _validated(value)
        if previous_digest is not None:
            _digest(previous_digest)
        key = self.identity(value)
        nonce = os.urandom(12)
        encrypted = nonce + self._cipher.encrypt(nonce, _canonical(value), self._domain + key.encode("ascii"))
        return {"key": key, "previous_digest": previous_digest, "ciphertext": base64.b64encode(encrypted).decode("ascii")}

    def open(self, key: str, ciphertext: str) -> dict[str, Any]:
        _identity(key)
        encoded = _ciphertext(ciphertext)
        try:
            plaintext = self._cipher.decrypt(encoded[:12], encoded[12:], self._domain + key.encode("ascii"))
            value = _validated(json.loads(plaintext))
            if _canonical(value) != plaintext:
                raise ValueError("canonical encoding required")
        except (ValueError, TypeError, UnicodeError, RecursionError, InvalidTag):
            # Never attach decoder exceptions containing private plaintext bytes.
            raise ControlPlaneError("private receipt envelope verification failed") from None
        if not hmac.compare_digest(self.identity(value), key):
            raise ControlPlaneError("private receipt envelope identity mismatch")
        return value

    def changes(
        self,
        before: Mapping[str, dict[str, Any]],
        after: Mapping[str, dict[str, Any]],
        committed: Mapping[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        for captured in (before, after):
            if not isinstance(captured, Mapping):
                raise ControlPlaneError("captured private receipts must be a mapping")
            for key, value in captured.items():
                if not hmac.compare_digest(_identity(key), self.identity(value)):
                    raise ControlPlaneError("captured private receipt identity mismatch")
        current = dict(_committed_rows(committed))
        changes = []
        for key in sorted(before.keys() | after.keys()):
            if before.get(key) == after.get(key):
                continue
            value = after.get(key)
            if value is None:
                value = {**before[key], "row": None}
            previous_digest = current[key]["digest"] if key in current else None
            changes.append(self.seal(value, previous_digest=previous_digest))
        return changes

    def apply(
        self, database: sqlite3.Connection, committed: Mapping[str, dict[str, Any]],
        *, complete: bool = False,
    ) -> None:
        """Replay exact absolute rows atomically inside the caller's transaction.

        The consensus core owns ciphertext CAS ordering. This method authenticates
        the complete committed snapshot and does not increment token consumption
        or commit the caller's outer operation. Tombstones precede upserts so an
        invitation token rotation can retain its secondary unique request key.

        ``complete=True`` is reserved for a certified initialized journal. Its
        private namespace is then the complete authority for these four tables:
        unrepresented legacy grants and request receipts are invalidated on
        every projection, including a returning old coordinator database. The
        cutover requires fresh grants/new request IDs; public witnesses cannot
        certify private legacy receipts. Human credential stores, Recorder data,
        and every table outside PRIVATE_TABLES are outside this operation.
        """
        if type(complete) is not bool:
            raise ControlPlaneError("private receipt completeness flag must be boolean")
        if not database.in_transaction:
            raise ControlPlaneError("private receipt replay requires an existing transaction")
        # Decrypt and validate the complete input before executing any SQL.
        values = [
            self.open(key, envelope["ciphertext"])
            for key, envelope in _committed_rows(committed)
        ]
        rank = {table: index for index, table in enumerate(PRIVATE_TABLES)}
        deletions = sorted(
            (value for value in values if value["row"] is None),
            key=lambda value: -rank[value["table"]],
        )
        upserts = sorted(
            (value for value in values if value["row"] is not None),
            key=lambda value: rank[value["table"]],
        )
        database.execute("SAVEPOINT fcp_c03_private_receipt_apply")
        try:
            if complete:
                retained = {table: set() for table in PRIVATE_TABLES}
                for value in upserts:
                    table = value["table"]
                    primary, _columns = PRIVATE_TABLES[table]
                    retained[table].add(tuple(value["key"][name] for name in primary))
                # Remove dependent receipts before their invitation/enrollment
                # targets. Parameterized keys avoid unbounded SQL IN clauses.
                for table in reversed(PRIVATE_TABLES):
                    primary, _columns = PRIVATE_TABLES[table]
                    where = " AND ".join(f"{name}=?" for name in primary)
                    keys = database.execute(f"SELECT {','.join(primary)} FROM {table}").fetchall()
                    for row in keys:
                        key = tuple(row)
                        if key not in retained[table]:
                            database.execute(f"DELETE FROM {table} WHERE {where}", key)
            for value in deletions:
                table = value["table"]
                primary, _columns = PRIVATE_TABLES[table]
                where = " AND ".join(f"{name}=?" for name in primary)
                database.execute(
                    f"DELETE FROM {table} WHERE {where}",
                    tuple(value["key"][name] for name in primary),
                )
            for value in upserts:
                table = value["table"]
                primary, columns = PRIVATE_TABLES[table]
                updates = ",".join(f"{name}=excluded.{name}" for name in columns if name not in primary)
                database.execute(
                    f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)}) "
                    f"ON CONFLICT({','.join(primary)}) DO UPDATE SET {updates}",
                    tuple(value["row"][name] for name in columns),
                )
        except BaseException:
            try:
                database.execute("ROLLBACK TO SAVEPOINT fcp_c03_private_receipt_apply")
            finally:
                database.execute("RELEASE SAVEPOINT fcp_c03_private_receipt_apply")
            raise
        else:
            database.execute("RELEASE SAVEPOINT fcp_c03_private_receipt_apply")


__all__ = ["PRIVATE_ROW_SCHEMA", "PRIVATE_TABLES", "PrivateJournalRows"]
