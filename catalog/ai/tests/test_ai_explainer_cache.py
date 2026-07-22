from __future__ import annotations

import os
from pathlib import Path

from catalog.ai.repo_index import build_chunks, load_cached_chunks, load_or_build_chunks, save_cached_chunks


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_cached_chunks_round_trip_and_invalidate_when_source_changes(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    source = _write(root / "README.md", "Initial overview\n")
    chunks = build_chunks(root)

    cache_path = save_cached_chunks(root, chunks)
    original_stat = source.stat()

    assert cache_path.exists()
    assert load_cached_chunks(root) == chunks

    source.write_text("Changed overview\n", encoding="utf-8")
    os.utime(source, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

    assert load_cached_chunks(root) is None


def test_load_or_build_chunks_uses_cache_when_valid(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write(root / "README.md", "Overview\n")

    first = load_or_build_chunks(root, use_cache=True, rebuild=True)
    second = load_or_build_chunks(root, use_cache=True)

    assert second == first
