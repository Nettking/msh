from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from catalog.common.telemetry_cache import TelemetryCache, cache_status, discover_jsonl_files, rebuild_cache


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _require_cache_dependencies() -> None:
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")


def test_nested_jsonl_files_are_discovered(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    top = data_dir / "2026-03-23.jsonl"
    nested = data_dir / "machines" / "QuickTurn" / "2026-03-24.jsonl"
    ignored = data_dir / "cache" / "internal.jsonl"
    _write_jsonl(top, [{"timestamp": "2026-03-23T00:00:00Z"}])
    _write_jsonl(nested, [{"timestamp": "2026-03-24T00:00:00Z"}])
    _write_jsonl(ignored, [{"timestamp": "2026-03-25T00:00:00Z"}])

    discovered = discover_jsonl_files(data_dir)

    assert discovered == [top, nested]


def test_rebuild_creates_partitioned_parquet_and_missing_columns_are_null(tmp_path: Path) -> None:
    _require_cache_dependencies()
    data_dir = tmp_path / "data"
    _write_jsonl(
        data_dir / "nested" / "sample.jsonl",
        [
            {
                "timestamp": "2026-03-23T12:00:00Z",
                "machine_id": "QuickTurn",
                "execution": "ACTIVE",
                "Srpm": 800,
            }
        ],
    )

    result = rebuild_cache(data_dir)

    parquet_file = result.cache_path / "machine_id=QuickTurn" / "date=2026-03-23" / "part.parquet"
    assert result.row_count == 1
    assert parquet_file.exists()

    rows = TelemetryCache(result.cache_path).samples_by_date_range("2026-03-23", "2026-03-23")
    assert len(rows) == 1
    assert rows[0]["machine_id"] == "QuickTurn"
    assert rows[0]["execution"] == "ACTIVE"
    assert pd.isna(rows[0]["mode"])
    assert pd.isna(rows[0]["Tool_number"])


def test_latest_sample_per_machine_works(tmp_path: Path) -> None:
    _require_cache_dependencies()
    data_dir = tmp_path / "data"
    _write_jsonl(
        data_dir / "telemetry.jsonl",
        [
            {"timestamp": "2026-03-23T08:00:00Z", "machine_id": "QuickTurn", "execution": "READY", "Srpm": 0},
            {"timestamp": "2026-03-23T08:05:00Z", "machine_id": "QuickTurn", "execution": "ACTIVE", "Srpm": 900},
            {"timestamp": "2026-03-23T08:03:00Z", "machine_id": "IG500", "execution": "STOPPED", "Srpm": 0},
        ],
    )
    result = rebuild_cache(data_dir)

    latest = TelemetryCache(result.cache_path).latest_sample_per_machine()

    by_machine = {row["machine_id"]: row for row in latest}
    assert by_machine["QuickTurn"]["execution"] == "ACTIVE"
    assert by_machine["IG500"]["execution"] == "STOPPED"


def test_repeated_rebuild_does_not_duplicate_rows(tmp_path: Path) -> None:
    _require_cache_dependencies()
    data_dir = tmp_path / "data"
    _write_jsonl(
        data_dir / "telemetry.jsonl",
        [
            {"timestamp": "2026-03-23T08:00:00Z", "machine_id": "QuickTurn"},
            {"timestamp": "2026-03-23T08:01:00Z", "machine_id": "QuickTurn"},
        ],
    )

    first = rebuild_cache(data_dir)
    second = rebuild_cache(data_dir)
    summary = TelemetryCache(second.cache_path).machine_activity_summary()

    assert first.row_count == 2
    assert second.row_count == 2
    assert summary[0]["sample_count"] == 2
    assert len(list(second.cache_path.rglob("*.parquet"))) == 1


def test_cache_status_uses_manifest_source_set_for_freshness(tmp_path: Path) -> None:
    _require_cache_dependencies()
    data_dir = tmp_path / "data"
    source = data_dir / "telemetry.jsonl"
    _write_jsonl(source, [{"timestamp": "2026-03-23T08:00:00Z", "machine_id": "QuickTurn"}])

    result = rebuild_cache(data_dir)
    manifest_path = result.cache_path / "_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    fresh_status = cache_status(data_dir, result.cache_path)
    assert manifest_path.exists()
    assert manifest["row_count"] == 1
    assert manifest["sources"][0]["path"] == "telemetry.jsonl"
    assert fresh_status.exists is True
    assert fresh_status.fresh is True
    assert fresh_status.manifest_exists is True
    assert fresh_status.manifest_row_count == 1

    renamed = data_dir / "renamed.jsonl"
    source.rename(renamed)

    stale_status = cache_status(data_dir, result.cache_path)
    assert stale_status.exists is True
    assert stale_status.fresh is False
    assert stale_status.source_file_count == 1
    assert stale_status.manifest_source_file_count == 1

    renamed.unlink()

    deleted_status = cache_status(data_dir, result.cache_path)
    assert deleted_status.exists is True
    assert deleted_status.fresh is False
    assert deleted_status.source_file_count == 0
    assert deleted_status.manifest_source_file_count == 1


def test_cache_absent_queries_are_empty_and_status_reports_missing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _write_jsonl(data_dir / "telemetry.jsonl", [{"timestamp": "2026-03-23T08:00:00Z", "machine_id": "QuickTurn"}])
    cache_dir = data_dir / "cache" / "parquet"

    status = cache_status(data_dir, cache_dir)
    latest = TelemetryCache(cache_dir).latest_sample_per_machine()

    assert status.exists is False
    assert status.fresh is False
    assert latest == []


def test_cached_cache_status_uses_ttl_and_can_be_invalidated(tmp_path: Path) -> None:
    from catalog.common.telemetry_cache import cached_cache_status, invalidate_cache_status

    _require_cache_dependencies()
    data_dir = tmp_path / "data"
    source = data_dir / "telemetry.jsonl"
    _write_jsonl(source, [{"timestamp": "2026-03-23T08:00:00Z", "machine_id": "QuickTurn"}])
    result = rebuild_cache(data_dir)

    first = cached_cache_status(data_dir, result.cache_path, ttl_seconds=60, force=True)
    source.write_text(
        source.read_text(encoding="utf-8")
        + json.dumps({"timestamp": "2026-03-23T08:01:00Z", "machine_id": "QuickTurn"})
        + "\n",
        encoding="utf-8",
    )

    within_ttl = cached_cache_status(data_dir, result.cache_path, ttl_seconds=60)
    assert within_ttl is first
    assert within_ttl.fresh is True

    invalidate_cache_status(data_dir, result.cache_path)
    after_invalidate = cached_cache_status(data_dir, result.cache_path, ttl_seconds=60)
    assert after_invalidate is not first
    assert after_invalidate.fresh is False
