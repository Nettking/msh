"""Logical artifact paths stay confined while their file objects are replaced."""

from __future__ import annotations

import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

import pytest

from catalog.capabilities.analysis.content_store import LocalArtifactContentStore
from catalog.federation import stable_filesystem
from catalog.federation.errors import FederationValidationError


@pytest.mark.skipif(os.name != "nt", reason="Windows deleted-file final name")
@pytest.mark.parametrize("extended_root", [False, True])
@pytest.mark.parametrize("key", ["x", "analysis/session/work/plan.json"])
def test_resolution_survives_native_leaf_replacement(
    tmp_path, monkeypatch, extended_root, key
):
    import ctypes
    import msvcrt
    import ntpath
    from ctypes import wintypes

    root = Path(str(tmp_path / "artifacts").removeprefix("\\\\?\\"))
    if extended_root:
        root = Path("\\\\?\\" + str(root))
    store = LocalArtifactContentStore(root)
    payload = b"same registered content"
    identity = store.write_bytes(key, payload)
    destination = store.root / key
    replacement = tmp_path / "replacement"
    replacement.write_bytes(payload)
    original_final_name = ntpath._getfinalpathname
    final_name = ctypes.WinDLL(
        "kernel32", use_last_error=True
    ).GetFinalPathNameByHandleW
    final_name.argtypes = (
        wintypes.HANDLE,
        wintypes.LPWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
    )
    final_name.restype = wintypes.DWORD
    interleaved = False

    def replace_during_final_name(path):
        nonlocal interleaved
        if Path(path) != destination or interleaved:
            return original_final_name(path)
        interleaved = True
        # Reproduce the real Windows API interleaving: the final-name query's
        # file object is opened before publication, but named after publication.
        with store._open_read(destination) as old:
            stable_filesystem.replace_preserving_readers(replacement, destination)
            handle = msvcrt.get_osfhandle(old.fileno())
            size = final_name(handle, None, 0, 0)
            assert size, ctypes.get_last_error()
            buffer = ctypes.create_unicode_buffer(size + 1)
            written = final_name(handle, buffer, len(buffer), 0)
            assert 0 < written < len(buffer), ctypes.get_last_error()
            assert "$Deleted" in buffer.value
            return buffer.value

    monkeypatch.setattr(ntpath, "_getfinalpathname", replace_during_final_name)
    assert store.resolve(key) == destination
    assert store.read_bytes(key, **vars(identity)) == payload


def test_concurrent_publication_keeps_logical_resolution_inside_root(tmp_path):
    store = LocalArtifactContentStore(tmp_path / "artifacts", chunk_size=4)
    key = "analysis/session/work/plan.json"
    payload = b"stable reader snapshot" * 100
    identity = store.write_bytes(key, payload)
    start = threading.Barrier(3)

    def publish():
        start.wait(timeout=10)
        for _ in range(25):
            assert store.write_bytes(key, payload) == identity

    def resolve():
        start.wait(timeout=10)
        for _ in range(2000):
            assert store.resolve(key) == store.root / key

    with closing(store.stream(key, **vars(identity))) as reader:
        first = next(reader)
        with ThreadPoolExecutor(max_workers=3) as pool:
            pending = [pool.submit(publish), pool.submit(resolve), pool.submit(resolve)]
            for future in pending:
                future.result(timeout=30)
        assert first + b"".join(reader) == payload
    assert store.read_bytes(key, **vars(identity)) == payload


@pytest.mark.parametrize("inside", [False, True])
@pytest.mark.parametrize("parent_link", [False, True])
def test_resolution_preserves_real_link_containment(tmp_path, inside, parent_link):
    store = LocalArtifactContentStore(tmp_path / "artifacts")
    target_parent = store.root / "target" if inside else tmp_path / "outside"
    target_parent.mkdir()
    target = target_parent / "body"
    target.write_bytes(b"unchanged target")
    link = store.root / "alias"
    try:
        link.symlink_to(
            target_parent if parent_link else target, target_is_directory=parent_link
        )
    except OSError as error:
        if os.name == "nt" and error.winerror == 1314:
            pytest.skip("Windows symlink privilege unavailable")
        raise
    key = "alias/body" if parent_link else "alias"
    if inside:
        assert store.resolve(key) == target
    else:
        with pytest.raises(FederationValidationError) as error:
            store.resolve(key)
        assert error.value.code == "artifact-object-key-escape"
    assert target.read_bytes() == b"unchanged target"


@pytest.mark.skipif(os.name != "nt", reason="Windows junction containment")
@pytest.mark.parametrize("inside", [False, True])
@pytest.mark.parametrize("parent_link", [False, True])
def test_resolution_preserves_real_junction_containment(tmp_path, inside, parent_link):
    store = LocalArtifactContentStore(tmp_path / "artifacts")
    target = store.root / "target" if inside else tmp_path / "outside"
    target.mkdir()
    (target / "body").write_bytes(b"unchanged target")
    link = store.root / "alias"
    # Directory junctions exercise real reparse targets without granting symlink
    # privileges or changing the runner account. Remove only the owned junction.
    subprocess.run(
        ["cmd", "/d", "/c", "mklink", "/J", str(link), str(target)],
        check=True,
        capture_output=True,
    )
    try:
        key = "alias/body" if parent_link else "alias"
        expected = target / "body" if parent_link else target
        if inside:
            assert store.resolve(key) == expected
        else:
            with pytest.raises(FederationValidationError) as error:
                store.resolve(key)
            assert error.value.code == "artifact-object-key-escape"
        assert (target / "body").read_bytes() == b"unchanged target"
    finally:
        os.rmdir(link)


@pytest.mark.parametrize("key", ["new", "missing/parent/new", ".", "./new"])
def test_resolution_retains_missing_paths_and_root_identity(tmp_path, key):
    store = LocalArtifactContentStore(tmp_path / "artifacts")
    assert store.resolve(key) == store.root / key
