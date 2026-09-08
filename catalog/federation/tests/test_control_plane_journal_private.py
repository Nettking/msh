"""Encrypted private receipts replay absolute state without reviving grants."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import sqlite3
from datetime import datetime, timezone

import pytest

from catalog.federation.control_plane_journal_private import (
    MAX_PRIVATE_CIPHERTEXT_CHARACTERS,
    PRIVATE_ROW_SCHEMA,
    PRIVATE_TABLES,
    PrivateJournalRows,
)
from catalog.federation.control_plane_journal_store import JournalCoordinatorStore
from catalog.federation.control_plane_replication import ControlPlaneError
from catalog.federation.errors import AuthenticationError
from catalog.federation.persistence import CoordinatorStore
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)
STAMP = NOW.isoformat()
SECRET = b"private-receipt-test-key-material!"


@pytest.fixture
def codec():
    return PrivateJournalRows(SECRET, "private-receipt-cluster")


def _enrollment(token_hash="1" * 64, *, use_count=0):
    return {
        "schema": PRIVATE_ROW_SCHEMA,
        "table": "enrollment_tokens",
        "key": {"token_hash": token_hash},
        "row": {
            "token_hash": token_hash,
            "token_id": "enrollment-private-fixture-identity",
            "created_at": STAMP,
            "expires_at": "2026-09-08T01:00:00+00:00",
            "max_uses": 1,
            "use_count": use_count,
            "created_by": "private-fixture-issuer",
            "revoked_at": None,
        },
    }


def _invitation(token_hash):
    return {
        "schema": PRIVATE_ROW_SCHEMA,
        "table": "session_invitations",
        "key": {"token_hash": token_hash},
        "row": {
            "token_hash": token_hash,
            "invitation_id": "invitation-fixed-request",
            "session_id": "session",
            "created_by_node_id": "member",
            "request_id": "sha256:" + "a" * 64,
            "requested_ttl_seconds": 3600,
            "created_at": STAMP,
            "expires_at": "2026-09-08T01:00:00+00:00",
            "max_uses": 1,
            "use_count": 0,
            "revoked_at": None,
        },
    }


def _committed(codec, values):
    # This is the core's committed ciphertext shape, not a simulated quorum.
    # Consensus CAS/certification is covered by the separate journal/core tests.
    result = {}
    for value in values:
        sealed = codec.seal(value, previous_digest=None)
        result[sealed["key"]] = {
            "ciphertext": sealed["ciphertext"],
            "digest": "sha256:" + hashlib.sha256(base64.b64decode(sealed["ciphertext"])).hexdigest(),
        }
    return result


def _seed_session(database):
    database.execute(
        """
        INSERT INTO nodes(
            node_id,display_name,public_key,created_at,identity_version,enrolled_at
        ) VALUES('member','Member','public-fixture-key',?,1,?)
        """,
        (STAMP, STAMP),
    )
    database.execute(
        """
        INSERT INTO sessions(
            session_id,display_name,state,created_at,created_by_node_id,coordinator_id
        ) VALUES('session','Session','active',?,'member','coordinator')
        """,
        (STAMP,),
    )
    database.execute(
        "INSERT INTO session_memberships(session_id,node_id,joined_at) VALUES('session','member',?)",
        (STAMP,),
    )


def test_encryption_round_trip_has_opaque_keyed_identity_and_fresh_nonce(codec):
    value = _enrollment()
    first = codec.seal(value, previous_digest=None)
    second = codec.seal(value, previous_digest=None)
    assert codec.open(first["key"], first["ciphertext"]) == value
    assert first["key"] == second["key"]
    assert first["ciphertext"] != second["ciphertext"]
    identity_input = json.dumps(
        {"table": value["table"], "key": value["key"]},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert first["key"] != "hmac-sha256:" + hashlib.sha256(identity_input).hexdigest()
    wire = json.dumps(first)
    for private_value in (value["table"], value["row"]["token_hash"], value["row"]["token_id"]):
        assert private_value not in wire
    assert SECRET.decode() not in wire
    assert first["key"] != PrivateJournalRows(b"z" * 32, "private-receipt-cluster").identity(value)
    assert first["key"] != PrivateJournalRows(SECRET, "another-cluster").identity(value)


@pytest.mark.parametrize("failure", ["tamper", "wrong-key", "wrong-cluster", "wrong-identity"])
def test_encrypted_receipt_refuses_tamper_or_another_security_context(codec, failure):
    sealed = codec.seal(_enrollment(), previous_digest=None)
    decoder = codec
    if failure == "tamper":
        raw = bytearray(base64.b64decode(sealed["ciphertext"]))
        raw[-1] ^= 1
        sealed["ciphertext"] = base64.b64encode(raw).decode()
    elif failure == "wrong-key":
        decoder = PrivateJournalRows(b"z" * 32, "private-receipt-cluster")
    elif failure == "wrong-cluster":
        decoder = PrivateJournalRows(SECRET, "another-cluster")
    else:
        sealed["key"] = codec.identity(_enrollment("2" * 64))
    with pytest.raises(ControlPlaneError, match="verification failed"):
        decoder.open(sealed["key"], sealed["ciphertext"])


@pytest.mark.parametrize(
    "failure",
    [
        "extra-envelope-field", "missing-row", "unknown-table", "unhashable-table",
        "extra-row-field", "missing-column", "key-mismatch", "extra-key-field",
        "raw-token-key", "null-required-text", "integer-text", "boolean-count",
        "zero-maximum", "over-consumed", "oversized-integer", "negative-count",
        "naive-timestamp", "invalid-timestamp", "oversized-text", "surrogate-text",
    ],
)
def test_private_rows_have_closed_bounded_typed_fields(codec, failure):
    value = _enrollment()
    if failure == "extra-envelope-field":
        value["private_key"] = "forbidden-field"
    elif failure == "missing-row":
        del value["row"]
    elif failure == "unknown-table":
        value["table"] = "identity_private_keys"
    elif failure == "unhashable-table":
        value["table"] = []
    elif failure == "extra-row-field":
        value["row"]["private_key"] = "forbidden-field"
    elif failure == "missing-column":
        del value["row"]["created_by"]
    elif failure == "key-mismatch":
        value["key"]["token_hash"] = "2" * 64
    elif failure == "extra-key-field":
        value["key"]["table"] = "nodes"
    elif failure == "raw-token-key":
        value["key"]["token_hash"] = value["row"]["token_hash"] = "fcp_enroll_raw-grant"
    elif failure == "null-required-text":
        value["row"]["created_by"] = None
    elif failure == "integer-text":
        value["row"]["created_by"] = 12
    elif failure == "boolean-count":
        value["row"]["use_count"] = True
    elif failure == "zero-maximum":
        value["row"]["max_uses"] = 0
    elif failure == "over-consumed":
        value["row"]["use_count"] = 2
    elif failure == "oversized-integer":
        value["row"]["max_uses"] = 2**80
    elif failure == "negative-count":
        value["row"]["use_count"] = -1
    elif failure == "naive-timestamp":
        value["row"]["created_at"] = "2026-09-08T00:00:00"
    elif failure == "invalid-timestamp":
        value["row"]["revoked_at"] = "not-a-timestamp"
    elif failure == "oversized-text":
        value["row"]["created_by"] = "x" * 4097
    else:
        value["row"]["created_by"] = "\ud800"
    with pytest.raises(ControlPlaneError):
        codec.seal(value, previous_digest=None)


@pytest.mark.parametrize("digest", ["", "0" * 64, "sha256:short", 1, []])
def test_previous_digest_must_match_the_core_digest_format(codec, digest):
    with pytest.raises(ControlPlaneError, match="digest is malformed"):
        codec.seal(_enrollment(), previous_digest=digest)


@pytest.mark.parametrize("ciphertext", ["!" * 40, "A" * (MAX_PRIVATE_CIPHERTEXT_CHARACTERS + 1), None])
def test_ciphertext_is_bounded_before_decode(codec, ciphertext):
    with pytest.raises(ControlPlaneError):
        codec.open(codec.identity(_enrollment()), ciphertext)


def test_consumed_enrollment_replay_is_absolute_and_does_not_restore_a_grant(tmp_path, codec):
    source = CoordinatorStore(tmp_path / "source.sqlite3")
    token = source.create_enrollment_token(now=NOW, max_uses=1)
    with source.read_transaction() as database:
        before = codec.capture(database)
    identity = IdentityStore(tmp_path / "first-identity", display_name="first").create(now=NOW)
    source.enroll_node(identity.identity, raw_token=token["token"], now=NOW)
    with source.read_transaction() as database:
        consumed = codec.capture(database)
    initial = _committed(codec, before.values())
    changes = codec.changes(before, consumed, initial)
    assert len(changes) == 1
    changed = changes[0]
    assert changed["previous_digest"] == initial[changed["key"]]["digest"]
    assert codec.open(changed["key"], changed["ciphertext"])["row"]["use_count"] == 1
    assert token["token"] not in json.dumps(consumed)
    assert token["token"] not in json.dumps(changes)

    committed = _committed(codec, consumed.values())
    target = CoordinatorStore(tmp_path / "target.sqlite3")
    for _ in range(2):
        with target.transaction() as database:
            codec.apply(database, committed)
            assert database.execute("SELECT use_count FROM enrollment_tokens").fetchone()[0] == 1
    another = IdentityStore(tmp_path / "second-identity", display_name="second").create(now=NOW)
    with pytest.raises(AuthenticationError) as refused:
        target.enroll_node(another.identity, raw_token=token["token"], now=NOW)
    assert refused.value.code == "reused-enrollment-token"
    tombstone = codec.changes(consumed, {}, committed)
    assert len(tombstone) == 1
    assert tombstone[0]["key"] == changed["key"]
    assert tombstone[0]["previous_digest"] == committed[changed["key"]]["digest"]
    assert codec.open(tombstone[0]["key"], tombstone[0]["ciphertext"])["row"] is None


def test_invitation_rotation_deletes_old_token_before_same_request_upsert(tmp_path, codec):
    target = CoordinatorStore(tmp_path / "target.sqlite3")
    # Force the new identity to sort first, exposing naive upsert-before-delete
    # replay against UNIQUE(created_by_node_id, request_id).
    new, old = sorted((_invitation("1" * 64), _invitation("2" * 64)), key=codec.identity)
    with target.transaction() as database:
        _seed_session(database)
        codec.apply(database, _committed(codec, [old]))
    tombstone = {**old, "row": None}
    committed = _committed(codec, [new, tombstone])
    for _ in range(2):
        with target.transaction() as database:
            codec.apply(database, committed)
            rows = database.execute("SELECT token_hash,use_count FROM session_invitations").fetchall()
            assert [tuple(row) for row in rows] == [(new["key"]["token_hash"], 0)]


@pytest.mark.parametrize("failure", ["tampered", "extra-field", "wrong-digest"])
def test_all_envelopes_are_validated_before_any_row_is_applied(tmp_path, codec, failure):
    target = CoordinatorStore(tmp_path / "target.sqlite3")
    committed = _committed(codec, [_enrollment("1" * 64), _enrollment("2" * 64)])
    last = committed[max(committed)]
    if failure == "tampered":
        raw = bytearray(base64.b64decode(last["ciphertext"]))
        raw[-1] ^= 1
        last["ciphertext"] = base64.b64encode(raw).decode()
        last["digest"] = "sha256:" + hashlib.sha256(raw).hexdigest()
    elif failure == "extra-field":
        last["plaintext"] = "forbidden-field"
    else:
        last["digest"] = "sha256:" + "0" * 64
    with target.transaction() as database:
        with pytest.raises(ControlPlaneError):
            codec.apply(database, committed)
        assert database.execute("SELECT count(*) FROM enrollment_tokens").fetchone()[0] == 0


def test_sql_failure_rolls_back_private_apply_savepoint_without_committing_outer(tmp_path, codec):
    target = CoordinatorStore(tmp_path / "target.sqlite3")
    # The invitation is well formed but its real FK target is absent. The
    # preceding enrollment upsert must roll back even if the caller catches it.
    committed = _committed(codec, [_enrollment(), _invitation("2" * 64)])
    with target.transaction() as database:
        with pytest.raises(sqlite3.IntegrityError):
            codec.apply(database, committed)
        assert database.in_transaction
        assert database.execute("SELECT count(*) FROM enrollment_tokens").fetchone()[0] == 0


def test_successful_private_apply_is_rolled_back_with_whole_outer_operation(tmp_path, codec):
    target = CoordinatorStore(tmp_path / "target.sqlite3")
    with pytest.raises(RuntimeError, match="outer refusal"), target.transaction() as database:
        codec.apply(database, _committed(codec, [_enrollment()]))
        assert database.execute("SELECT count(*) FROM enrollment_tokens").fetchone()[0] == 1
        raise RuntimeError("outer refusal")
    with target.read_transaction() as database:
        assert database.execute("SELECT count(*) FROM enrollment_tokens").fetchone()[0] == 0


def test_receipt_apply_requires_an_explicit_transaction(tmp_path, codec):
    target = CoordinatorStore(tmp_path / "target.sqlite3")
    database = target._connect()
    try:
        with pytest.raises(ControlPlaneError, match="requires an existing transaction"):
            codec.apply(database, _committed(codec, [_enrollment()]))
        assert not database.in_transaction
    finally:
        database.close()


def test_staged_authority_read_helpers_see_new_rows_without_committing(tmp_path):
    target = JournalCoordinatorStore(tmp_path / "target.sqlite3")
    with pytest.raises(RuntimeError, match="outer refusal"), target.raw_transaction() as database:
        _seed_session(database)
        assert target.get_session("session").display_name == "Session"
        assert target.get_node("member")["identity"].node_id == "member"
        target.require_membership(session_id="session", node_id="member")
        assert target.require_active_node("member")["node_id"] == "member"
        assert database.in_transaction
        raise RuntimeError("outer refusal")
    assert target.get_session("session") is None
    assert target.get_node("member") is None


def test_captured_identity_cannot_be_substituted(codec):
    value = _enrollment()
    wrong = codec.identity(_enrollment("2" * 64))
    with pytest.raises(ControlPlaneError, match="identity mismatch"):
        codec.changes({}, {wrong: value}, {})


def test_only_receipt_tables_are_captured_and_no_private_key_column_is_permitted(tmp_path, codec):
    target = CoordinatorStore(tmp_path / "target.sqlite3")
    with target.transaction() as database:
        database.execute("CREATE TABLE unrelated_private_keys(private_key TEXT)")
        database.execute("INSERT INTO unrelated_private_keys VALUES('must-remain-local')")
        target_capture = codec.capture(database)
    assert target_capture == {}
    assert set(PRIVATE_TABLES) == {
        "enrollment_tokens", "session_invitations", "session_join_requests", "accepted_requests"
    }
    value = copy.deepcopy(_enrollment())
    value["row"]["private_key"] = "must-remain-local"
    with pytest.raises(ControlPlaneError, match="row fields are invalid"):
        codec.seal(value, previous_digest=None)


def test_certified_cutover_and_returning_old_database_cannot_resurrect_legacy_grants(tmp_path, codec):
    target = CoordinatorStore(tmp_path / "target.sqlite3")
    old_token = target.create_enrollment_token(now=NOW)
    with target.transaction() as database:
        _seed_session(database)
        # These unrelated sentinel tables test the SQL scope, not human-auth
        # replication. That subsystem has its own credential recovery tests.
        database.execute("CREATE TABLE human_credentials(password_hash TEXT, salt TEXT)")
        database.execute("INSERT INTO human_credentials VALUES('private-verifier', 'private-salt')")
        database.execute("CREATE TABLE recorder_observations(payload BLOB)")
        database.execute("INSERT INTO recorder_observations VALUES(?)", (b"retained-observation",))
    invitation = target.create_invitation(
        session_id="session", actor_node_id="member", now=NOW, request_id="legacy-invite",
    )
    request_key = "sha256:" + "b" * 64
    with target.transaction() as database:
        database.execute(
            "INSERT INTO session_join_requests(node_id,request_id,token_hash,session_id,accepted_at) "
            "VALUES(?,?,?,?,?)",
            ("member", request_key, hashlib.sha256(invitation["token"].encode()).hexdigest(), "session", STAMP),
        )
        database.execute(
            "INSERT INTO accepted_requests(actor_node_id,request_id,message_type,request_hash,accepted_at) "
            "VALUES(?,?,?,?,?)",
            ("member", request_key, "capability.announce", "sha256:" + "c" * 64, STAMP),
        )
        legacy = codec.capture(database)
        assert {value["table"] for value in legacy.values()} == set(PRIVATE_TABLES)
        codec.apply(database, {})  # No certified initialization: no cutover.
        assert codec.capture(database) == legacy
    old_path = tmp_path / "returning-old.sqlite3"
    source = target._connect()
    backup = sqlite3.connect(old_path)
    try:
        source.backup(backup)
    finally:
        backup.close()
        source.close()
    with target.transaction() as database:
        codec.apply(database, {}, complete=True)
        assert codec.capture(database) == {}
    new_token = target.create_enrollment_token(now=NOW)
    with target.read_transaction() as database:
        current = codec.capture(database)
    committed = _committed(codec, current.values())
    returning = CoordinatorStore(old_path)
    for store in (target, returning):
        with store.transaction() as database:
            codec.apply(database, committed, complete=True)
            assert codec.capture(database) == current
            assert tuple(database.execute("SELECT * FROM human_credentials").fetchone()) == (
                "private-verifier", "private-salt",
            )
            assert database.execute("SELECT payload FROM recorder_observations").fetchone()[0] == b"retained-observation"
            assert database.execute("SELECT count(*) FROM sessions").fetchone()[0] == 1
            assert database.execute("SELECT count(*) FROM session_memberships").fetchone()[0] == 1
    identity = IdentityStore(tmp_path / "fresh-identity", display_name="Fresh").create(now=NOW).identity
    with pytest.raises(AuthenticationError) as refused:
        returning.enroll_node(identity, raw_token=old_token["token"], now=NOW)
    assert refused.value.code == "unknown-enrollment-token"
    assert returning.enroll_node(identity, raw_token=new_token["token"], now=NOW) == identity


@pytest.mark.parametrize("failure", ["invalid-envelope", "sql-failure", "outer-failure"])
def test_complete_projection_cannot_partially_invalidate_old_receipts(tmp_path, codec, failure):
    target = CoordinatorStore(tmp_path / "target.sqlite3")
    target.create_enrollment_token(now=NOW)
    with target.read_transaction() as database:
        before = codec.capture(database)
    if failure == "outer-failure":
        with pytest.raises(RuntimeError, match="outer refusal"), target.transaction() as database:
            codec.apply(database, {}, complete=True)
            assert codec.capture(database) == {}
            raise RuntimeError("outer refusal")
    else:
        committed = _committed(codec, [_invitation("2" * 64)])
        if failure == "invalid-envelope":
            committed[next(iter(committed))]["digest"] = "sha256:" + "0" * 64
        expected = ControlPlaneError if failure == "invalid-envelope" else sqlite3.IntegrityError
        with target.transaction() as database:
            with pytest.raises(expected):
                codec.apply(database, committed, complete=True)
            assert codec.capture(database) == before
            assert database.in_transaction
    with target.read_transaction() as database:
        assert codec.capture(database) == before
