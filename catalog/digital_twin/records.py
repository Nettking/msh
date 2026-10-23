"""Private manifests, command fencing and an independently rebuildable index.

Initialization is an explicit installation operation, never a GET side effect.
Attached rollback-journal databases provide one SQLite commit boundary for
primary records, audit and derived rows. Sources are never attached or written.
"""

from __future__ import annotations

import json
import secrets
import shutil
import sqlite3
import time
from contextlib import closing, contextmanager
from pathlib import Path

from .contracts import (
    LEASE_SECONDS,
    MIN_FREE,
    Budget,
    TwinError,
    encode,
    identifier,
    now,
    require,
)

PRIMARY_SQL = """
CREATE TABLE metadata(version INTEGER NOT NULL, cursor_key TEXT NOT NULL);
CREATE TABLE objects(id TEXT PRIMARY KEY, kind TEXT NOT NULL, revision TEXT NOT NULL,
 parent TEXT, payload TEXT NOT NULL, scopes TEXT NOT NULL);
CREATE INDEX object_parent ON objects(kind,parent);
CREATE TABLE commands(id TEXT PRIMARY KEY, actor TEXT NOT NULL, operation TEXT NOT NULL,
 key TEXT NOT NULL, digest TEXT NOT NULL, status TEXT NOT NULL, created TEXT NOT NULL,
 wall REAL NOT NULL, mono REAL NOT NULL, finished TEXT, scopes TEXT NOT NULL,
 response TEXT, http INTEGER, location TEXT,
 UNIQUE(actor,operation,key));
CREATE TABLE audit(command_id TEXT PRIMARY KEY, actor TEXT NOT NULL,
 operation TEXT NOT NULL, outcome TEXT NOT NULL, recorded_at TEXT NOT NULL);
"""
INDEX_SQL = """CREATE TABLE IF NOT EXISTS views.rows(
 snapshot TEXT NOT NULL, view TEXT NOT NULL, ordinal INTEGER NOT NULL,
 id TEXT NOT NULL, payload TEXT NOT NULL,
 PRIMARY KEY(snapshot,view,ordinal));
CREATE INDEX IF NOT EXISTS views.rows_id ON rows(snapshot,id);
CREATE TABLE IF NOT EXISTS views.manifests(
 snapshot TEXT PRIMARY KEY, revision TEXT NOT NULL, counts TEXT NOT NULL);"""


class TwinStore:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.primary = self.directory / "primary.sqlite3"
        self.index = self.directory / "views.sqlite3"

    def initialize(self):
        """Called only by the explicit process-twin installation CLI."""
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.primary.exists():
            with self.connection() as c:
                self._validate(c)
            return
        with closing(sqlite3.connect(self.primary)) as c, c:
            c.executescript(PRIMARY_SQL)
            c.execute("INSERT INTO metadata VALUES(1,?)", (secrets.token_hex(32),))
            c.execute("ATTACH DATABASE ? AS views", (str(self.index),))
            c.executescript(INDEX_SQL)

    @staticmethod
    def _validate(c):
        row = c.execute("SELECT version FROM metadata").fetchone()
        require(row is not None and row[0] == 1, "STORE_UNAVAILABLE", 503)

    @contextmanager
    def connection(self, write=False, *, index=False, rebuild=False):
        c = None
        try:
            require(self.primary.is_file(), "STORE_UNAVAILABLE", 503)
            c = sqlite3.connect(
                self.primary.resolve().as_uri() + ("?mode=rw" if write else "?mode=ro"),
                uri=True,
                timeout=1,
                # SQLite otherwise retains the last large bound JSON value in
                # its statement cache until connection close, alongside the
                # immutable Python payload and serialized staging text.
                cached_statements=0,
            )
            c.row_factory = sqlite3.Row
            self._validate(c)
            if not write:
                c.execute("PRAGMA query_only=ON")
            if index:
                require(
                    self.index.is_file() or (write and rebuild),
                    "VIEW_INDEX_UNAVAILABLE",
                    503,
                )
                uri = self.index.resolve().as_uri() + (
                    "?mode=rwc" if rebuild else "?mode=rw" if write else "?mode=ro"
                )
                c.execute("ATTACH DATABASE ? AS views", (uri,))
                if rebuild:
                    c.executescript(INDEX_SQL)
            if write:
                c.execute("BEGIN IMMEDIATE")
            yield c
            if write:
                c.commit()
        except sqlite3.Error as exc:
            if c is not None:
                c.rollback()
            if getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL:
                raise TwinError("INSUFFICIENT_STORAGE", 507) from None
            raise TwinError("STORE_UNAVAILABLE", 503) from None
        finally:
            if c is not None:
                c.close()

    def object(self, object_id, kind=None):
        with self.connection() as c:
            row = c.execute("SELECT * FROM objects WHERE id=?", (object_id,)).fetchone()
        require(
            row is not None and (kind is None or row["kind"] == kind),
            "OBJECT_NOT_FOUND",
            404,
        )
        return json.loads(row["payload"]), json.loads(row["scopes"])

    def objects(self, kind, parent=None):
        with self.connection() as c:
            query = "SELECT * FROM objects WHERE kind=?"
            args = [kind]
            if parent is not None:
                query += " AND parent=?"
                args.append(parent)
            rows = c.execute(query + " ORDER BY rowid", args).fetchall()
        return [(json.loads(r["payload"]), json.loads(r["scopes"])) for r in rows]

    def latest_revision(self, parent):
        with self.connection() as c:
            row = c.execute(
                "SELECT id FROM objects WHERE kind='definition' AND parent=? ORDER BY rowid DESC LIMIT 1",
                (parent,),
            ).fetchone()
        require(row is not None, "OBJECT_NOT_FOUND", 404)
        return row[0]

    def command(self, *, actor, operation=None, key=None, command_id=None):
        with self.connection() as c:
            if command_id:
                row = c.execute(
                    "SELECT * FROM commands WHERE id=? AND actor=?", (command_id, actor)
                ).fetchone()
            else:
                row = c.execute(
                    "SELECT * FROM commands WHERE actor=? AND operation=? AND key=?",
                    (actor, operation, key),
                ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["scopes"] = json.loads(result["scopes"])
        if result["response"] is not None:
            result["response"] = json.loads(result["response"])
        if result["status"] == "RUNNING" and self.expired(result):
            result.update(
                status="FAILED",
                http=503,
                response={"code": "BUILD_ABANDONED"},
                location=None,
            )
        return result

    @staticmethod
    def expired(command):
        wall, mono = time.time() - command["wall"], time.monotonic() - command["mono"]
        require(wall >= 0 and mono >= 0 and abs(wall - mono) < 2, "CLOCK_UNSAFE", 503)
        return mono >= LEASE_SECONDS

    def reserve(self, actor, operation, key, body_digest, scopes, heavy):
        require(
            shutil.disk_usage(self.directory).free >= MIN_FREE,
            "INSUFFICIENT_STORAGE",
            507,
        )
        with self.connection(write=True) as c:
            duplicate = c.execute(
                "SELECT id FROM commands WHERE actor=? AND operation=? AND key=?",
                (actor, operation, key),
            ).fetchone()
            require(duplicate is None, "COMMAND_RUNNING", 409)
            active = c.execute(
                "SELECT * FROM commands WHERE status='RUNNING'"
            ).fetchall()
            heavy_ops = ("PREPARE_DATASET", "BUILD_SNAPSHOT", "REBUILD_INDEX")
            for item in active:
                if self.expired(item):
                    failure = encode({"code": "BUILD_ABANDONED"})
                    c.execute(
                        "UPDATE commands SET status='FAILED',http=503,response=?,finished=? WHERE id=? AND status='RUNNING'",
                        (failure, now(), item["id"]),
                    )
                    c.execute(
                        "INSERT OR IGNORE INTO audit VALUES(?,?,?,?,?)",
                        (
                            item["id"],
                            item["actor"],
                            item["operation"],
                            "BUILD_ABANDONED",
                            now(),
                        ),
                    )
                elif heavy and item["operation"] in heavy_ops:
                    raise TwinError("BUILD_BUSY", 409)
            command_id = identifier("cmd")
            c.execute(
                "INSERT INTO commands(id,actor,operation,key,digest,status,created,wall,mono,scopes) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    command_id,
                    actor,
                    operation,
                    key,
                    body_digest,
                    "RUNNING",
                    now(),
                    time.time(),
                    time.monotonic(),
                    encode(scopes),
                ),
            )
        return command_id

    def finish(
        self,
        command_id,
        actor,
        data,
        http,
        location,
        objects=(),
        views=None,
        *,
        budget: Budget,
        before_commit,
        scopes,
        rebuild=False,
        expected_latest=None,
    ):
        serialized = [
            (oid, kind, rev, parent, budget.encoded(payload), encode(scopes))
            for oid, kind, rev, parent, payload, scopes in objects
        ]
        serialized_views = (
            []
            if views is None
            else [
                (snapshot, view, n, row["row_id"], budget.encoded(row))
                for snapshot, by_view in views.items()
                for view, rows in by_view.items()
                for n, row in enumerate(rows)
            ]
        )
        # Enforce the aggregate artifact, not only each row's individual size.
        require(
            sum(len(r[4].encode("utf-8")) for r in serialized)
            + sum(len(r[4].encode("utf-8")) for r in serialized_views)
            <= 536_870_912,
            "ARTIFACT_LIMIT_EXCEEDED",
            422,
        )
        with self.connection(write=True, index=views is not None, rebuild=rebuild) as c:
            command = c.execute(
                "SELECT * FROM commands WHERE id=? AND actor=?", (command_id, actor)
            ).fetchone()
            require(
                command is not None
                and command["status"] == "RUNNING"
                and not self.expired(command),
                "BUILD_ABANDONED",
                503,
            )
            if expected_latest:
                parent, expected = expected_latest
                latest = c.execute(
                    "SELECT id FROM objects WHERE kind='definition' AND parent=? ORDER BY rowid DESC LIMIT 1",
                    (parent,),
                ).fetchone()
                require(
                    latest is not None and latest[0] == expected,
                    "DEFINITION_CHANGED",
                    409,
                )
            for item in serialized:
                c.execute("INSERT INTO objects VALUES(?,?,?,?,?,?)", item)
            if rebuild:
                # Replace only the selected derived rows, never primary evidence.
                for snapshot in views:
                    c.execute("DELETE FROM views.rows WHERE snapshot=?", (snapshot,))
            if views is not None:
                c.executemany(
                    "INSERT INTO views.rows VALUES(?,?,?,?,?)", serialized_views
                )
                for snapshot, by_view in views.items():
                    c.execute(
                        "INSERT OR REPLACE INTO views.manifests VALUES(?,?,?)",
                        (
                            snapshot,
                            data["content_revision"],
                            encode({view: len(rows) for view, rows in by_view.items()}),
                        ),
                    )
            c.execute(
                "UPDATE commands SET status='SUCCEEDED',response=?,http=?,location=?,finished=?,scopes=? WHERE id=?",
                (
                    budget.encoded(data),
                    http,
                    location,
                    now(),
                    encode(scopes),
                    command_id,
                ),
            )
            c.execute(
                "INSERT INTO audit VALUES(?,?,?,?,?)",
                (command_id, actor, command["operation"], "SUCCEEDED", now()),
            )
            before_commit()
            budget.check()
            require(not self.expired(command), "BUILD_ABANDONED", 503)

    def fail(self, command_id, actor, error):
        with self.connection(write=True) as c:
            updated = c.execute(
                "UPDATE commands SET status='FAILED',http=?,response=?,finished=? WHERE id=? AND actor=? AND status='RUNNING'",
                (
                    error.status,
                    encode({"code": error.code, "field": error.field}),
                    now(),
                    command_id,
                    actor,
                ),
            )
            if updated.rowcount:
                c.execute(
                    "INSERT INTO audit SELECT id,actor,operation,?,? FROM commands WHERE id=?",
                    (error.code, now(), command_id),
                )

    def rows(self, snapshot, view, offset, limit):
        try:
            with self.connection(index=True) as c:
                manifest = c.execute(
                    "SELECT revision,counts FROM views.manifests WHERE snapshot=?",
                    (snapshot,),
                ).fetchone()
                require(manifest is not None, "VIEW_INDEX_UNAVAILABLE", 503)
                primary = c.execute(
                    "SELECT revision FROM objects WHERE id=?", (snapshot,)
                ).fetchone()
                require(
                    primary is not None and primary[0] == manifest[0],
                    "VIEW_INDEX_UNAVAILABLE",
                    503,
                )
                count = c.execute(
                    "SELECT count(*) FROM views.rows WHERE snapshot=? AND view=?",
                    (snapshot, view),
                ).fetchone()[0]
                require(
                    json.loads(manifest[1]).get(view) == count,
                    "VIEW_INDEX_UNAVAILABLE",
                    503,
                )
                rows = c.execute(
                    "SELECT payload FROM views.rows WHERE snapshot=? AND view=? ORDER BY ordinal LIMIT ? OFFSET ?",
                    (snapshot, view, limit, offset),
                ).fetchall()
                return [json.loads(r[0]) for r in rows]
        except TwinError as exc:
            if exc.code == "STORE_UNAVAILABLE":
                raise TwinError("VIEW_INDEX_UNAVAILABLE", 503) from None
            raise

    def index_available(self, snapshot):
        try:
            self.rows(snapshot, "episodes", 0, 1)
            return True
        except TwinError as exc:
            if exc.code == "VIEW_INDEX_UNAVAILABLE":
                return False
            raise

    def cursor_key(self):
        with self.connection() as c:
            return c.execute("SELECT cursor_key FROM metadata").fetchone()[0]
