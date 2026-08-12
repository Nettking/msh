from __future__ import annotations

import sqlite3

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.sqlite_schema import (
    SQLITE_SCHEMA_VERSION_TABLE,
    SQLiteMigration,
    ensure_sqlite_schema,
)


def _create_example_v1(connection: sqlite3.Connection) -> None:
    connection.execute("CREATE TABLE example (id INTEGER PRIMARY KEY)")


def _upgrade_example_v2(connection: sqlite3.Connection) -> None:
    connection.execute("ALTER TABLE example ADD COLUMN value TEXT NOT NULL DEFAULT ''")


def _validate_example_v2(connection: sqlite3.Connection) -> None:
    columns = {row[1] for row in connection.execute("PRAGMA table_info(example)")}
    if columns != {"id", "value"}:
        raise RuntimeError("example schema is incomplete")


def _fresh(_: sqlite3.Connection) -> int:
    return 0


def test_fresh_schema_runs_consecutive_migrations_and_is_idempotent() -> None:
    connection = sqlite3.connect(":memory:")
    migrations = (
        SQLiteMigration(1, _create_example_v1),
        SQLiteMigration(2, _upgrade_example_v2),
    )

    assert (
        ensure_sqlite_schema(
            connection,
            schema_name="test.example",
            target_version=2,
            migrations=migrations,
            detect_legacy_version=_fresh,
            validate=_validate_example_v2,
        )
        == 2
    )
    assert connection.execute(
        f"SELECT version FROM {SQLITE_SCHEMA_VERSION_TABLE} WHERE schema_name='test.example'"
    ).fetchone() == (2,)

    def unexpected_detection(_: sqlite3.Connection) -> int:
        raise AssertionError("legacy detection must not run after the marker exists")

    assert (
        ensure_sqlite_schema(
            connection,
            schema_name="test.example",
            target_version=2,
            migrations=migrations,
            detect_legacy_version=unexpected_detection,
            validate=_validate_example_v2,
        )
        == 2
    )


def test_current_unversioned_schema_is_registered_without_replaying_migrations() -> None:
    connection = sqlite3.connect(":memory:")
    _create_example_v1(connection)
    _upgrade_example_v2(connection)
    connection.commit()

    def current(_: sqlite3.Connection) -> int:
        return 2

    def must_not_run(_: sqlite3.Connection) -> None:
        raise AssertionError("current legacy schema must not be rewritten")

    assert (
        ensure_sqlite_schema(
            connection,
            schema_name="test.current",
            target_version=2,
            migrations=(SQLiteMigration(1, must_not_run), SQLiteMigration(2, must_not_run)),
            detect_legacy_version=current,
            validate=_validate_example_v2,
        )
        == 2
    )


def test_failed_migration_rolls_back_schema_and_version_marker() -> None:
    connection = sqlite3.connect(":memory:")

    def fail_v2(db: sqlite3.Connection) -> None:
        db.execute("ALTER TABLE example ADD COLUMN temporary TEXT")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        ensure_sqlite_schema(
            connection,
            schema_name="test.rollback",
            target_version=2,
            migrations=(
                SQLiteMigration(1, _create_example_v1),
                SQLiteMigration(2, fail_v2),
            ),
            detect_legacy_version=_fresh,
            validate=lambda _: None,
        )

    assert connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='example'"
    ).fetchone() is None
    assert connection.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{SQLITE_SCHEMA_VERSION_TABLE}'"
    ).fetchone() is None


def test_newer_schema_version_is_rejected_without_mutation() -> None:
    connection = sqlite3.connect(":memory:")
    migrations = (SQLiteMigration(1, _create_example_v1),)
    ensure_sqlite_schema(
        connection,
        schema_name="test.future",
        target_version=1,
        migrations=migrations,
        detect_legacy_version=_fresh,
        validate=lambda _: None,
    )
    connection.execute(
        f"UPDATE {SQLITE_SCHEMA_VERSION_TABLE} SET version=7 WHERE schema_name='test.future'"
    )
    connection.commit()

    with pytest.raises(FederationValidationError) as unsupported:
        ensure_sqlite_schema(
            connection,
            schema_name="test.future",
            target_version=1,
            migrations=migrations,
            detect_legacy_version=_fresh,
            validate=lambda _: None,
        )
    assert unsupported.value.code == "unsupported-sqlite-schema"
    assert connection.execute(
        f"SELECT version FROM {SQLITE_SCHEMA_VERSION_TABLE} WHERE schema_name='test.future'"
    ).fetchone() == (7,)


def test_components_share_metadata_table_without_sharing_versions() -> None:
    connection = sqlite3.connect(":memory:")

    def create_a(db: sqlite3.Connection) -> None:
        db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")

    def create_b(db: sqlite3.Connection) -> None:
        db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")

    ensure_sqlite_schema(
        connection,
        schema_name="component.a",
        target_version=1,
        migrations=(SQLiteMigration(1, create_a),),
        detect_legacy_version=_fresh,
        validate=lambda _: None,
    )
    ensure_sqlite_schema(
        connection,
        schema_name="component.b",
        target_version=1,
        migrations=(SQLiteMigration(1, create_b),),
        detect_legacy_version=_fresh,
        validate=lambda _: None,
    )

    assert connection.execute(
        f"SELECT schema_name, version FROM {SQLITE_SCHEMA_VERSION_TABLE} ORDER BY schema_name"
    ).fetchall() == [("component.a", 1), ("component.b", 1)]
