"""Read existing storage control-plane events without mutating coordinator state."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path

from catalog.federation.models import SessionEvent
from catalog.federation.storage_control_plane import StorageControlPlaneSnapshot


class ReadOnlyStorageAuthorityStore:
    """Minimal ``snapshot`` facade over an existing coordinator SQLite database.

    Federation GET projections must not create storage tables merely because the
    operator opened the Storage page. This facade opens the coordinator database
    in SQLite ``mode=ro`` and replays an existing ``storage_control_events`` table.
    A coordinator that has never created storage authority projects an empty
    snapshot rather than causing DDL on the read path.
    """

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)

    def _connect(self) -> sqlite3.Connection:
        uri = Path(self.database).resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def snapshot(self, session_id: str) -> StorageControlPlaneSnapshot:
        snapshot = StorageControlPlaneSnapshot(session_id)
        with closing(self._connect()) as connection, connection:
            table = connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='storage_control_events'
                """
            ).fetchone()
            if table is None:
                return snapshot
            rows = connection.execute(
                """
                SELECT session_id, revision, event_id, event_type, occurred_at,
                       actor_node_id, payload_json
                FROM storage_control_events
                WHERE session_id=?
                ORDER BY revision
                """,
                (session_id,),
            ).fetchall()

        for row in rows:
            snapshot.apply(
                SessionEvent(
                    session_id=row["session_id"],
                    revision=row["revision"],
                    event_id=row["event_id"],
                    event_type=row["event_type"],
                    occurred_at=datetime.fromisoformat(row["occurred_at"]),
                    actor_node_id=row["actor_node_id"],
                    payload=json.loads(row["payload_json"]),
                )
            )
        return snapshot


__all__ = ["ReadOnlyStorageAuthorityStore"]
