from __future__ import annotations

from pathlib import Path

import pytest

from catalog.common.managed_temporary import (
    ManagedTemporaryError,
    ManagedTemporaryRoot,
    scavenge_managed_temporary_root,
)


def test_scavenger_reclaims_abandoned_owned_file_but_not_unrelated_file(
    tmp_path: Path,
) -> None:
    root = ManagedTemporaryRoot(tmp_path / "managed", namespace="test-files")
    temporary = root.allocate(prefix="fcp-chunk-", suffix=".partial")
    temporary.write(b"abandoned")
    temporary.flush()
    temporary.handle.close()
    temporary.owner_handle.close()
    unrelated = root.root / "fcp-chunk-unrelated.partial"
    unrelated.write_bytes(b"keep")

    report = scavenge_managed_temporary_root(
        root.root,
        namespace="test-files",
        max_entries=16,
    )

    assert report.reclaimed_files == 1
    assert report.reclaimed_owner_records == 1
    assert not temporary.path.exists()
    assert not temporary.owner_path.exists()
    assert unrelated.read_bytes() == b"keep"


def test_scavenger_skips_live_owned_file(tmp_path: Path) -> None:
    root = ManagedTemporaryRoot(tmp_path / "managed", namespace="test-live")
    temporary = root.allocate(prefix="fcp-raw-", suffix=".partial")
    temporary.write(b"live")
    temporary.flush()

    report = scavenge_managed_temporary_root(
        root.root,
        namespace="test-live",
        max_entries=16,
    )

    assert report.reclaimed_files == 0
    assert report.skipped_active == 1
    assert temporary.path.read_bytes() == b"live"
    temporary.close()


def test_scavenger_reclaims_owned_directory_and_rejects_symlink_child(
    tmp_path: Path,
) -> None:
    root = ManagedTemporaryRoot(tmp_path / "managed", namespace="test-directories")
    temporary = root.allocate_directory(prefix="cache-temp-")
    (temporary.path / "part.parquet").write_bytes(b"cache")
    temporary.owner_handle.close()

    report = scavenge_managed_temporary_root(
        root.root,
        namespace="test-directories",
        max_entries=16,
    )

    assert report.reclaimed_files == 1
    assert not temporary.path.exists()
    assert not temporary.owner_path.exists()

    guarded = root.allocate_directory(prefix="cache-temp-")
    outside = tmp_path / "outside.txt"
    outside.write_text("do not delete", encoding="utf-8")
    try:
        (guarded.path / "escape").symlink_to(outside)
    except OSError as exc:
        guarded.close()
        pytest.skip(f"symlink creation unavailable: {exc}")
    guarded.owner_handle.close()

    report = scavenge_managed_temporary_root(
        root.root,
        namespace="test-directories",
        max_entries=16,
    )

    assert report.reclaimed_files == 0
    assert report.skipped_ambiguous == 1
    assert guarded.path.is_dir()
    assert outside.read_text(encoding="utf-8") == "do not delete"


def test_managed_root_rejects_symlink_root(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "managed"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with pytest.raises(ManagedTemporaryError):
        ManagedTemporaryRoot(link, namespace="test-root").ensure()
