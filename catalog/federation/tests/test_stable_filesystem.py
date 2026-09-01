from __future__ import annotations

import os
from pathlib import Path

import pytest

from catalog.federation.host_resources import measure_filesystem
from catalog.federation.stable_filesystem import StableFilesystemError, stable_directory


def test_stable_directory_temp_replace_round_trip(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with stable_directory(root, Path("nested/leaf"), create=True) as directory:
        with directory.temporary_file(prefix="stage-") as (name, handle):
            handle.write(b"payload")
            handle.flush()
            os.fsync(handle.fileno())
            directory.replace(name, "result.bin")
            directory.fsync()
        assert directory.stat("result.bin").st_size == 7
        assert directory.sha256("result.bin").startswith("sha256:")
    assert (root / "nested" / "leaf" / "result.bin").read_bytes() == b"payload"


def test_stable_directory_resource_id_matches_admission_measurement(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with stable_directory(root, Path("nested/leaf"), create=True) as directory:
        measurement = measure_filesystem(root / "nested" / "leaf")
        assert measurement.available is True
        assert directory.resource_id == measurement.resource_id


@pytest.mark.skipif(os.name == "nt", reason="POSIX descriptor-relative race consequence")
def test_posix_parent_symlink_rebind_cannot_redirect_replace(tmp_path: Path) -> None:
    root = tmp_path / "root"
    parent = root / "managed"
    outside = tmp_path / "outside"
    parent.mkdir(parents=True)
    outside.mkdir()
    detached = root / "detached"

    with stable_directory(root, Path("managed")) as directory:
        parent.rename(detached)
        parent.symlink_to(outside, target_is_directory=True)
        with directory.temporary_file(prefix="race-") as (name, handle):
            handle.write(b"inside")
            handle.flush()
            os.fsync(handle.fileno())
            directory.replace(name, "result.bin")
            directory.fsync()

    assert not (outside / "result.bin").exists()
    assert (detached / "result.bin").read_bytes() == b"inside"


def test_stable_directory_rejects_redirect_component(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    link = root / "redirect"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
    with pytest.raises((OSError, StableFilesystemError)), stable_directory(
        root, Path("redirect")
    ):
        pass


def test_owned_temporary_scavenger_reclaims_abandoned_file_only(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with stable_directory(root) as directory:
        manager = directory.temporary_file(prefix="fcp-chunk-")
        name, handle = manager.__enter__()
        owner = manager.gen.gi_frame.f_locals["owner"]
        handle.close()
        # Simulate a process that died after the durable owner record was ready:
        # closing the owner handle releases its OS lock, but the context manager
        # itself remains suspended until after scavenging.
        owner.close()
        with stable_directory(root) as reentry:
            report = reentry.scavenge_temporary_files(
                prefixes=("fcp-chunk-",),
            )
            assert report.reclaimed_files == 1
            assert report.reclaimed_owner_records == 1
            assert not (root / name).exists()
            assert not (root / f".{name}.fcp-stable-owner.json").exists()
        manager.__exit__(None, None, None)


def test_owned_temporary_scavenger_skips_live_file_and_unrelated_files(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    unrelated = root / "fcp-chunk-unrelated"
    unrelated.write_bytes(b"keep")
    with stable_directory(root) as directory:
        manager = directory.temporary_file(prefix="fcp-encoded-")
        name, handle = manager.__enter__()
        handle.write(b"live")
        handle.flush()
        with stable_directory(root) as reentry:
            report = reentry.scavenge_temporary_files(
                prefixes=("fcp-encoded-",),
            )
            assert report.reclaimed_files == 0
            assert report.skipped_active == 1
            assert (root / name).exists()
        manager.__exit__(None, None, None)
    assert unrelated.read_bytes() == b"keep"


def test_owned_temporary_scavenger_never_deletes_symlink_substitution(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside.bin"
    root.mkdir()
    outside.write_bytes(b"outside")
    with stable_directory(root) as directory:
        manager = directory.temporary_file(prefix="fcp-raw-")
        name, handle = manager.__enter__()
        handle.write(b"owned")
        handle.close()
        owner = manager.gen.gi_frame.f_locals["owner"]
        owner.close()
        (root / name).unlink()
        try:
            (root / name).symlink_to(outside)
        except OSError as exc:
            manager.__exit__(None, None, None)
            pytest.skip(f"symlink creation unavailable: {exc}")
        with stable_directory(root) as reentry:
            report = reentry.scavenge_temporary_files(prefixes=("fcp-raw-",))
            assert report.reclaimed_files == 0
            assert report.skipped_ambiguous == 1
            assert (root / name).is_symlink()
        manager.__exit__(None, None, None)
    assert outside.read_bytes() == b"outside"
