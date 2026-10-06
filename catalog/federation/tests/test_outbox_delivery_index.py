from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from catalog.federation.outbox import SQLiteOutbox


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
    assert len(count_queries) == len(delivery_queries) == 1
    count_query = count_queries[0]
    delivery_query = delivery_queries[0]
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        count_plan = connection.execute(f"EXPLAIN QUERY PLAN {count_query}").fetchall()
        delivery_plan = connection.execute(
            f"EXPLAIN QUERY PLAN {delivery_query}"
        ).fetchall()
        version = connection.execute(
            "SELECT version FROM outbox_schema WHERE singleton=1"
        ).fetchone()[0]

    assert version == 3
    assert "outbox_pending_delivery_dataset" in repr(count_plan)
    assert "outbox_pending_delivery_dataset" in repr(delivery_plan)
    assert "COVERING INDEX outbox_pending_delivery_dataset" in repr(delivery_plan)
    assert "USE TEMP B-TREE FOR LAST 2 TERMS OF ORDER BY" not in repr(delivery_plan)
