from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

import pytest

from catalog.federation.outbox import (
    _OUTBOX_DELIVERY_DATASET_KEY_SQL,
    _OUTBOX_RETIRED_SUMMARY_INDEX_KEY_COLUMNS,
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
    bounded_ids = normalized_delivery_query.split("bounded_outbox_ids AS (", 1)[
        1
    ].split(") SELECT entry.*", 1)[0]
    assert "SELECT outbox_id FROM ranked_outbox" in bounded_ids
    assert "WHERE delivery_rank <= 1" in bounded_ids
    assert "ORDER BY outbox_id LIMIT 4" in bounded_ids
    assert "JOIN outbox AS entry ON entry.outbox_id = bounded.outbox_id" in (
        normalized_delivery_query
    )
    assert normalized_delivery_query.index("LIMIT 4") < normalized_delivery_query.index(
        "SELECT entry.*"
    )
    assert normalized_delivery_query.endswith("ORDER BY bounded.outbox_id")


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
        "next_attempt_at",
        "last_error",
    )
    assert "COVERING INDEX outbox_pending_delivery_dataset" in repr(plan)
    assert version == 3


def test_pending_delivery_bounds_payload_join_when_groups_exceed_limit(tmp_path):
    database = tmp_path / "outbox.sqlite3"
    outbox = _TracingOutbox(database)
    timestamp = datetime(2026, 10, 6, tzinfo=UTC)
    for index in range(12):
        outbox.enqueue(
            session_id="session-a",
            destination_id="group-a",
            schema_id="fcp.recorder.storage_delivery.v1",
            payload={"dataset_id": f"dataset-{index}", "batch_id": f"batch-{index}"},
            idempotency_key=f"key-{index}",
            content_hash=f"sha256:{index:064x}",
            now=timestamp,
        )

    outbox.statements.clear()
    entries = outbox.pending_for_delivery(
        session_id="session-a",
        destination_id="group-a",
        schema_id="fcp.recorder.storage_delivery.v1",
        limit=3,
    )

    assert [entry.outbox_id for entry in entries] == [1, 2, 3]
    delivery_query = next(
        statement
        for statement in outbox.statements
        if statement.lstrip().startswith("WITH ranked_outbox AS (")
    )
    normalized = " ".join(delivery_query.split())
    bounded_ids = normalized.split("bounded_outbox_ids AS (", 1)[1].split(
        ") SELECT entry.*", 1
    )[0]
    assert "WHERE delivery_rank <= 1" in bounded_ids
    assert "ORDER BY outbox_id LIMIT 3" in bounded_ids
    assert normalized.index("LIMIT 3") < normalized.index("SELECT entry.*")


def test_retired_summary_uses_bounded_covering_index_without_temp_sort(tmp_path):
    database = tmp_path / "outbox.sqlite3"
    outbox = _TracingOutbox(database)
    timestamp = datetime(2026, 10, 6, tzinfo=UTC)
    for index in range(256):
        entry, _created = outbox.enqueue(
            session_id="session-a",
            destination_id="group-a",
            schema_id="fcp.recorder.storage_delivery.v1",
            payload={"dataset_id": f"dataset-{index:03d}"},
            idempotency_key=f"key-{index}",
            content_hash=f"sha256:{index:064x}",
            now=timestamp,
        )
        if index % 64 == 0:
            outbox.retire(
                entry.outbox_id,
                reason="payload-field-missing",
                dataset_id=None if index == 0 else f"dataset-{index:03d}",
                now=timestamp,
            )

    outbox.statements.clear()
    summary = outbox.retired_summary(
        session_id="session-a",
        destination_id="group-a",
        schema_id="fcp.recorder.storage_delivery.v1",
    )
    assert summary.total == 4
    assert [item.dataset_id for item in summary.datasets] == [
        "dataset-064",
        "dataset-128",
        "dataset-192",
        None,
    ]

    count_queries = [
        statement
        for statement in outbox.statements
        if statement.lstrip().startswith("SELECT COUNT(*) AS total FROM outbox")
    ]
    grouped_queries = [
        statement
        for statement in outbox.statements
        if statement.lstrip().startswith(
            "SELECT retirement_dataset_id AS dataset_id"
        )
    ]
    assert len(count_queries) == len(grouped_queries) == 1
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        count_plan = connection.execute(
            f"EXPLAIN QUERY PLAN {count_queries[0]}"
        ).fetchall()
        grouped_plan = connection.execute(
            f"EXPLAIN QUERY PLAN {grouped_queries[0]}"
        ).fetchall()

    assert "COVERING INDEX outbox_retired_summary" in repr(count_plan)
    assert "COVERING INDEX outbox_retired_summary" in repr(grouped_plan)
    assert "USE TEMP B-TREE" not in repr(grouped_plan)


@pytest.mark.parametrize("wrong_state", ["pending", "RETIRED"])
def test_existing_v3_outbox_replaces_incompatible_retired_summary_index(
    tmp_path, wrong_state
):
    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    timestamp = datetime(2026, 10, 6, tzinfo=UTC)
    entry, _created = outbox.enqueue(
        session_id="session-a",
        destination_id="group-a",
        schema_id="fcp.recorder.storage_delivery.v1",
        payload={"dataset_id": "dataset-a"},
        idempotency_key="key-a",
        content_hash="sha256:" + "a" * 64,
        now=timestamp,
    )
    outbox.retire(
        entry.outbox_id,
        reason="payload-field-missing",
        dataset_id="dataset-a",
        now=timestamp,
    )
    before = outbox.get(entry.outbox_id)

    with sqlite3.connect(database) as connection:
        connection.execute("DROP INDEX outbox_retired_summary")
        # Same name and key columns, but the wrong partial predicate. A
        # name-only IF NOT EXISTS migration would silently preserve this.
        connection.execute(
            f"""CREATE INDEX outbox_retired_summary
               ON outbox(
                   session_id, destination_id, schema_id,
                   (retirement_dataset_id IS NULL), retirement_dataset_id
               ) WHERE state='{wrong_state}'"""
        )
        inserted_index_sql = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='index' AND name='outbox_retired_summary'"
        ).fetchone()[0]
        assert f"WHERE state='{wrong_state}'" in inserted_index_sql
        connection.commit()

    outbox.initialize()

    assert outbox.get(entry.outbox_id) == before
    assert outbox.retired_summary(
        session_id="session-a",
        destination_id="group-a",
        schema_id="fcp.recorder.storage_delivery.v1",
    ).total == 1
    with sqlite3.connect(database) as connection:
        index_columns = tuple(
            row[2]
            for row in connection.execute(
                "PRAGMA index_xinfo('outbox_retired_summary')"
            ).fetchall()
            if row[5]
        )
        index_sql = connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='index' AND name='outbox_retired_summary'"
        ).fetchone()[0]
        version = connection.execute(
            "SELECT version FROM outbox_schema WHERE singleton=1"
        ).fetchone()[0]

    assert index_columns == _OUTBOX_RETIRED_SUMMARY_INDEX_KEY_COLUMNS
    assert "WHERE state = 'retired'" in index_sql
    assert version == 3
