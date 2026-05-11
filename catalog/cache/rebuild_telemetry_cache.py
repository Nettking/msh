"""CLI entry point for rebuilding the Parquet telemetry cache."""

from __future__ import annotations

import argparse
from pathlib import Path

from catalog.common.telemetry_cache import default_cache_dir, rebuild_cache


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild the MSH telemetry Parquet cache from raw JSONL files.")
    parser.add_argument("--data-dir", default="data", help="Directory to scan recursively for raw *.jsonl telemetry files.")
    parser.add_argument("--cache-dir", default=None, help="Output directory for partitioned Parquet cache files.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)
    cache_dir = Path(args.cache_dir) if args.cache_dir else default_cache_dir(data_dir)
    result = rebuild_cache(data_dir=data_dir, cache_dir=cache_dir)
    print(f"Imported rows: {result.row_count}")
    print(f"Source JSONL files: {result.source_file_count}")
    print(f"Output cache path: {result.cache_path}")


if __name__ == "__main__":
    main()
