from __future__ import annotations

import errno
import threading
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


def test_owner_record_is_never_authenticatable_while_unlocked(tmp_path: Path) -> None:
    """The scavenger's safety rests on live records being locked, not on timing.

    ``scavenge_managed_temporary_root`` deletes any owner record it can parse,
    authenticate, and lock. So a record must never be parseable before its
    owner holds the lock: allocation writes the payload through the handle it
    already locked, and an empty sidecar parses as nothing.
    """

    root = ManagedTemporaryRoot(tmp_path / "root", namespace="probe")
    temporary = root.allocate(prefix="live-", suffix=".partial")
    try:
        temporary.write(b"payload")
        temporary.flush()

        report = scavenge_managed_temporary_root(tmp_path / "root", namespace="probe")

        assert report.skipped_active == 1
        assert report.reclaimed_files == 0
        assert temporary.path.exists()
        assert temporary.owner_path.exists()
    finally:
        temporary.close()


def test_concurrent_allocation_beside_scavenging_keeps_every_live_temporary(
    tmp_path: Path,
) -> None:
    """Reproduces the analysis-publication strand at its actual seam.

    ``ContentStore._atomic_write`` scavenges the shared temporary root and then
    allocates into it, so concurrent artifact writes interleave one thread's
    scavenge with another's allocation. When the owner record was written
    through a handle that was closed before a second handle took the lock, the
    scavenge could authenticate and reclaim a record whose owner was still
    mid-allocation: the allocation then failed reopening its own sidecar, and
    the submission was stranded.
    """

    root = tmp_path / "root"
    workers = 8
    rounds = 60
    barrier = threading.Barrier(workers)
    failures: list[str] = []

    def _worker(index: int) -> None:
        try:
            barrier.wait(timeout=30)
            for _ in range(rounds):
                scavenge_managed_temporary_root(root, namespace="probe")
                temporary = ManagedTemporaryRoot(root, namespace="probe").allocate(
                    prefix="probe-",
                    suffix=".partial",
                )
                try:
                    temporary.write(b"payload" * 100)
                    temporary.flush()
                    if not temporary.path.exists():
                        failures.append(
                            f"worker {index}: live temporary {temporary.path.name} "
                            "was reclaimed while still owned"
                        )
                finally:
                    temporary.close()
        except BaseException as exc:  # noqa: BLE001 - report, do not mask
            failures.append(f"worker {index}: {type(exc).__name__}: {exc}")

    threads = [
        threading.Thread(target=_worker, args=(index,)) for index in range(workers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)

    assert failures == []
    # Every allocation closed cleanly, so nothing owned is left behind.
    assert sorted(entry.name for entry in root.iterdir()) == [".fcp-managed-root.json"]


def test_first_use_from_several_threads_agrees_on_one_root_marker(
    tmp_path: Path,
) -> None:
    """A root marker made visible before it is written reads back as corrupt.

    ``open("x")`` publishes an empty file first, so a thread that inspected the
    marker in that window rejected the whole root as unreadable and failed the
    write it was admitting.
    """

    root = tmp_path / "root"
    workers = 8
    barrier = threading.Barrier(workers)
    identities: list[tuple[int, int]] = []
    failures: list[str] = []
    guard = threading.Lock()

    def _worker(index: int) -> None:
        try:
            barrier.wait(timeout=30)
            identity = ManagedTemporaryRoot(root, namespace="probe").ensure()
            with guard:
                identities.append(identity)
        except BaseException as exc:  # noqa: BLE001 - report, do not mask
            with guard:
                failures.append(f"worker {index}: {type(exc).__name__}: {exc}")

    threads = [
        threading.Thread(target=_worker, args=(index,)) for index in range(workers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert failures == []
    assert len(identities) == workers
    assert len(set(identities)) == 1
    # One marker won the race; no staging file is left behind.
    assert sorted(entry.name for entry in root.iterdir()) == [".fcp-managed-root.json"]


def test_root_marker_publication_survives_a_filesystem_without_hard_links(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The atomic publication must degrade, not fail, where link() is missing.

    Markers are published by hard-linking a fully written staging file into
    place, which is atomic and refuses to clobber. Some bind-mounted and
    network filesystems -- including Windows bind mounts under some Docker
    backends -- do not implement ``link``. Falling back to the direct exclusive
    create keeps those deployments working, and the reader's zero-length wait
    covers the window it reopens.
    """

    import os as _os

    def _no_link(*_args: object, **_kwargs: object) -> None:
        raise OSError(errno.EPERM, "link not supported on this filesystem")

    monkeypatch.setattr(_os, "link", _no_link)
    root = tmp_path / "root"
    workers = 8
    barrier = threading.Barrier(workers)
    identities: list[tuple[int, int]] = []
    failures: list[str] = []
    guard = threading.Lock()

    def _worker(index: int) -> None:
        try:
            barrier.wait(timeout=30)
            identity = ManagedTemporaryRoot(root, namespace="probe").ensure()
            with guard:
                identities.append(identity)
        except BaseException as exc:  # noqa: BLE001 - report, do not mask
            with guard:
                failures.append(f"worker {index}: {type(exc).__name__}: {exc}")

    threads = [
        threading.Thread(target=_worker, args=(index,)) for index in range(workers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert failures == []
    assert len(set(identities)) == 1
    assert sorted(entry.name for entry in root.iterdir()) == [".fcp-managed-root.json"]

    temporary = ManagedTemporaryRoot(root, namespace="probe").allocate(prefix="probe-")
    try:
        temporary.write(b"payload")
        temporary.flush()
        assert temporary.path.exists()
    finally:
        temporary.close()
