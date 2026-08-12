"""Transactional schema versioning for Federation-owned SQLite databases.

The helper is intentionally small: it serializes schema upgrades with
``BEGIN IMMEDIATE``, records a component-scoped version in one shared metadata
table, applies only explicit consecutive migrations, and validates the final
shape before commit.  It does not own connections, application data, backup
policy, or store-specific schema definitions.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from .errors import FederationValidationError

SQLITE_SCHEMA_VERSION_TABLE = "fcp_schema_versions"
MAX_SQLITE_SCHEMA_NAME_LENGTH = 128


@dataclass(frozen=True)
class SQLiteMigration:
    """One explicit upgrade step ending at ``to_version``."""

    to_version: int
    upgrade: Callable[[sqlite3.Connection], None]


def _unsupported(schema_name: str, version: object, target_version: int) -> FederationValidationError:
    return FederationValidationError(
        "unsupported-sqlite-schema",
        schema_name,
        f"database schema version {version!r} is not supported; expected 0..{target_version}",
    )


def _validate_plan(
    schema_name: str,
    target_version: int,
    migrations: Sequence[SQLiteMigration],
) -> dict[int, SQLiteMigration]:
    if (
        not isinstance(schema_name, str)
        or not schema_name
        or len(schema_name) > MAX_SQLITE_SCHEMA_NAME_LENGTH
    ):
        raise ValueError("schema_name must be bounded non-empty text")
    if isinstance(target_version, bool) or not isinstance(target_version, int) or target_version < 1:
        raise ValueError("target_version must be a positive integer")
    by_target = {migration.to_version: migration for migration in migrations}
    expected = tuple(range(1, target_version + 1))
    if len(by_target) != len(migrations) or tuple(sorted(by_target)) != expected:
        raise ValueError("migrations must contain exactly one consecutive step for each target version")
    return by_target


def ensure_sqlite_schema(
    connection: sqlite3.Connection,
    *,
    schema_name: str,
    target_version: int,
    migrations: Sequence[SQLiteMigration],
    detect_legacy_version: Callable[[sqlite3.Connection], int],
    validate: Callable[[sqlite3.Connection], None],
) -> int:
    """Upgrade one logical SQLite schema atomically and return its version.

    ``detect_legacy_version`` is consulted only once for databases that predate
    the shared version marker.  Version ``0`` means the component schema is
    absent.  A detected current version is simply registered; older versions
    are upgraded through every explicit consecutive migration.
    """

    by_target = _validate_plan(schema_name, target_version, migrations)
    if connection.in_transaction:
        raise RuntimeError("SQLite schema migration requires an idle connection")

    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SQLITE_SCHEMA_VERSION_TABLE} (
                schema_name TEXT PRIMARY KEY
                    CHECK(length(schema_name) > 0 AND length(schema_name) <= {MAX_SQLITE_SCHEMA_NAME_LENGTH}),
                version INTEGER NOT NULL CHECK(version >= 0)
            )
            """
        )
        row = connection.execute(
            f"SELECT version FROM {SQLITE_SCHEMA_VERSION_TABLE} WHERE schema_name = ?",
            (schema_name,),
        ).fetchone()
        if row is None:
            version = detect_legacy_version(connection)
            if isinstance(version, bool) or not isinstance(version, int) or not 0 <= version <= target_version:
                raise _unsupported(schema_name, version, target_version)
            connection.execute(
                f"INSERT INTO {SQLITE_SCHEMA_VERSION_TABLE}(schema_name, version) VALUES (?, ?)",
                (schema_name, version),
            )
        else:
            version = row[0]
            if isinstance(version, bool) or not isinstance(version, int) or not 0 <= version <= target_version:
                raise _unsupported(schema_name, version, target_version)

        while version < target_version:
            next_version = version + 1
            by_target[next_version].upgrade(connection)
            connection.execute(
                f"UPDATE {SQLITE_SCHEMA_VERSION_TABLE} SET version = ? WHERE schema_name = ?",
                (next_version, schema_name),
            )
            version = next_version

        validate(connection)
        connection.commit()
        return version
    except Exception:
        connection.rollback()
        raise


__all__ = [
    "MAX_SQLITE_SCHEMA_NAME_LENGTH",
    "SQLITE_SCHEMA_VERSION_TABLE",
    "SQLiteMigration",
    "ensure_sqlite_schema",
]
