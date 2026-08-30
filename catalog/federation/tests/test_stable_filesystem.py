from __future__ import annotations

import os
from pathlib import Path

import pytest

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
