from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from catalog.federation.outbox import (
    _OUTBOX_DELIVERY_DATASET_KEY_SQL,
    SQLiteOutbox,
)


class _TracingOutbox(SQLiteOutbox):
    def __init__(self, *args, **kwargs):
        self.statements: list[str] = []
        super().__init__(*args, **kwargs)

    def _connect(self):
        connection = super()._connect()
        connection.set_trace_callback(self.statements.append)
        return connection


def test_existing_v3_outbox_rebuilds_fair_delivery_index_and_uses_it(tmp_path):
    database = tmp_path / "outbox.sqlite3"
    outbox = _TracingOutbox(database)
    timestamp = datetime(2026, 10, 6, tzinfo=UTC)
    completed, _ = outbox.enqueue(
        session_id="session-a",
        destination_id="group-a",
        schema_id="fcp.recorder.storage_delivery.v1",
        payload={"dataset_id": "completed-dataset", "batch_id": "completed"},
        idempotency_key="completed-key",
        content_hash="sha256:" + "f" * 64,
        now=timestamp,
    )
    outbox.acknowledge(completed.outbox_id, now=timestamp)

    # Model an existing schema-v3 database created before the derived index
    # existed. Reinitialization must add the index without changing the schema
    # version or any durable outbox row.
    with sqlite3.connect(database) as connection:
        connection.execute("DROP INDEX outbox_pending_delivery_dataset")
        connection.commit()
    outbox.initialize()
    preserved = outbox.get(completed.outbox_id)
    assert preserved is not None
    assert preserved.state == "completed"
    assert preserved.content_hash == completed.content_hash

    for index in range(4):
        outbox.enqueue(
            session_id="session-a",
            destination_id="group-a",
            schema_id="fcp.recorder.storage_delivery.v1",
            payload={
                "dataset_id": f"dataset-{index}",
                "batch_id": f"batch-{index}",
            },
            idempotency_key=f"key-{index}",
            content_hash=f"sha256:{index:064x}",
            now=timestamp,
        )

    outbox.statements.clear()
    entries = outbox.pending_for_delivery(
        session_id="session-a",
        destination_id="group-a",
        schema_id="fcp.recorder.storage_delivery.v1",
        limit=4,
    )

    assert [entry.outbox_id for entry in entries] == [2, 3, 4, 5]
    outbox.record_failure(2, error="TimeoutError", now=timestamp)
    pending_count, pending_has_error = outbox.pending_summary(
        session_id="session-a",
        destination_id="group-a",
        schema_id="fcp.recorder.storage_delivery.v1",
    )
    assert pending_count == 4
    assert pending_has_error is True
    count_queries = [
        statement
        for statement in outbox.statements
        if statement.lstrip().startswith("SELECT COUNT(*) FROM (")
    ]
    delivery_queries = [
        statement
        for statement in outbox.statements
        if statement.lstrip().startswith("WITH ranked_outbox AS (")
    ]
    summary_queries = [
        statement
        for statement in outbox.statements
        if statement.lstrip().startswith("SELECT COUNT(*) AS pending_count,")
    ]
    assert len(count_queries) == len(delivery_queries) == len(summary_queries) == 1
    count_query = count_queries[0]
    delivery_query = delivery_queries[0]
    summary_query = summary_queries[0]
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        count_plan = connection.execute(f"EXPLAIN QUERY PLAN {count_query}").fetchall()
        delivery_plan = connection.execute(
            f"EXPLAIN QUERY PLAN {delivery_query}"
        ).fetchall()
        summary_plan = connection.execute(
            f"EXPLAIN QUERY PLAN {summary_query}"
        ).fetchall()
        version = connection.execute(
            "SELECT version FROM outbox_schema WHERE singleton=1"
        ).fetchone()[0]

    assert version == 3
    assert "outbox_pending_delivery_dataset" in repr(count_plan)
    assert "outbox_pending_delivery_dataset" in repr(delivery_plan)
    # SQLite versions differ on whether EXPLAIN labels the window CTE's
    # expression-index scan as covering. The invariant is that it searches
    # the route index and ranks IDs before joining payload-bearing rows.
    delivery_plan_text = repr(delivery_plan)
    assert "SEARCH outbox USING" in delivery_plan_text
    assert "INDEX outbox_pending_delivery_dataset" in delivery_plan_text
    assert "USE TEMP B-TREE FOR LAST 2 TERMS OF ORDER BY" not in repr(delivery_plan)
    assert "COVERING INDEX outbox_pending_delivery_dataset" in repr(summary_plan)
    normalized_delivery_query = " ".join(delivery_query.split())
    ranked_ids = normalized_delivery_query.split(") SELECT entry.*", 1)[0]
    assert "SELECT outbox_id," in ranked_ids
    assert "SELECT outbox.*" not in ranked_ids
    assert "JOIN outbox AS entry ON entry.outbox_id = ranked.outbox_id" in (
        normalized_delivery_query
    )
    assert "WHERE ranked.delivery_rank <= " in normalized_delivery_query
    assert normalized_delivery_query.endswith("LIMIT 4")


def test_existing_v3_outbox_rebuilds_older_noncovering_delivery_index(tmp_path):
    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    timestamp = datetime(2026, 10, 6, tzinfo=UTC)
    entry, _ = outbox.enqueue(
        session_id="session-a",
        destination_id="group-a",
        schema_id="fcp.recorder.storage_delivery.v1",
        payload={"dataset_id": "dataset-a", "batch_id": "batch-a"},
        idempotency_key="key-a",
        content_hash="sha256:" + "a" * 64,
        now=timestamp,
    )
    outbox.record_failure(entry.outbox_id, error="TimeoutError", now=timestamp)
    before = outbox.get(entry.outbox_id)

    # The first version of this derived index did not include last_error.
    # CREATE INDEX IF NOT EXISTS alone would leave it in place after upgrade.
    with sqlite3.connect(database) as connection:
        connection.execute("DROP INDEX outbox_pending_delivery_dataset")
        connection.execute(
            f"""CREATE INDEX outbox_pending_delivery_dataset
                ON outbox(
                    state, session_id, destination_id, schema_id,
                    {_OUTBOX_DELIVERY_DATASET_KEY_SQL}, outbox_id
                ) WHERE state='pending'"""
        )
        connection.commit()

    outbox.initialize()

    after = outbox.get(entry.outbox_id)
    assert after == before
    assert outbox.pending_summary(
        session_id="session-a",
        destination_id="group-a",
        schema_id="fcp.recorder.storage_delivery.v1",
    ) == (1, True)
    with sqlite3.connect(database) as connection:
        indexed_columns = tuple(
            row[2]
            for row in connection.execute(
                "PRAGMA index_xinfo('outbox_pending_delivery_dataset')"
            ).fetchall()
            if row[5]
        )
        plan = connection.execute(
            """EXPLAIN QUERY PLAN
               SELECT COUNT(*), MAX(last_error IS NOT NULL)
               FROM outbox INDEXED BY outbox_pending_delivery_dataset
               WHERE state='pending' AND session_id='session-a'
                 AND destination_id='group-a'
                 AND schema_id='fcp.recorder.storage_delivery.v1'"""
        ).fetchall()
        version = connection.execute(
            "SELECT version FROM outbox_schema WHERE singleton=1"
        ).fetchone()[0]

    assert indexed_columns == (
        "state",
        "session_id",
        "destination_id",
        "schema_id",
        None,
        "outbox_id",
        "last_error",
    )
    assert "COVERING INDEX outbox_pending_delivery_dataset" in repr(plan)
    assert version == 3
