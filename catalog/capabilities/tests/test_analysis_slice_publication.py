"""Crash-correct publication tests for deterministic analysis slice archives."""

from __future__ import annotations

import tarfile
from pathlib import Path

import pytest

from catalog.capabilities.analysis import packaging
from catalog.capabilities.analysis.contracts import slice_artifact_id
from catalog.capabilities.tests.analysis_harness import (
    build_stack,
    source_files,
    work_slice,
)
from catalog.federation.errors import FederationValidationError


def test_failed_slice_pack_does_not_replace_the_published_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "input"
    root.mkdir()
    source = root / "day.jsonl"
    source.write_text('{"timestamp":"2026-08-13T10:00:00Z"}\n', encoding="utf-8")
    destination = tmp_path / "artifacts" / "slice.tar.gz"
    destination.parent.mkdir()
    previously_published = b"previously-published-content"
    destination.write_bytes(previously_published)

    def fail_during_pack(*_args, **_kwargs) -> None:
        raise OSError("injected archive write failure")

    monkeypatch.setattr(packaging.tarfile.TarFile, "addfile", fail_during_pack)

    with pytest.raises(OSError, match="injected archive write failure"):
        packaging.write_slice_archive(destination, files=(source,), root=root)

    assert destination.read_bytes() == previously_published
    assert list(destination.parent.glob(f".{destination.name}.*.partial")) == []


def test_retry_rebuilds_a_partial_slice_before_authority_registration(
    tmp_path: Path,
) -> None:
    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    object_key = f"{work.object_key_prefix}/slice.tar.gz"
    destination = stack.content_store.resolve(object_key)
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = b"\x1f\x8b\x08\x00hard-killed-partial"
    destination.write_bytes(partial)

    outcome = stack.submit(work)

    descriptor = stack.authority.artifact(slice_artifact_id(work))
    assert outcome.created is True
    assert destination.read_bytes() != partial
    assert descriptor.size_bytes == destination.stat().st_size
    assert stack.content_store.identity(object_key).content_hash == descriptor.content_hash
    with tarfile.open(destination, mode="r:gz") as archive:
        assert archive.getnames() == ["day.jsonl"]
        extracted = archive.extractfile("day.jsonl")
        assert extracted is not None
        assert b'"machine":"A"' in extracted.read()


def test_retry_rebuilds_a_complete_but_stale_unregistered_slice(
    tmp_path: Path,
) -> None:
    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    object_key = f"{work.object_key_prefix}/slice.tar.gz"
    destination = stack.content_store.resolve(object_key)
    stale_root = tmp_path / "stale-input"
    stale_files = source_files(
        stale_root,
        body='{"timestamp":"2026-08-13T09:00:00Z","machine":"stale"}\n',
    )
    packaging.write_slice_archive(destination, files=stale_files, root=stale_root)

    stack.submit(work)

    with tarfile.open(destination, mode="r:gz") as archive:
        extracted = archive.extractfile("day.jsonl")
        assert extracted is not None
        body = extracted.read()
    assert b'"machine":"A"' in body
    assert b'"machine":"stale"' not in body


def test_retry_never_overwrites_an_immutable_registered_slice(tmp_path: Path) -> None:
    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    stack.submit(work)
    object_key = f"{work.object_key_prefix}/slice.tar.gz"
    destination = stack.content_store.resolve(object_key)
    registered_bytes = destination.read_bytes()
    changed_files = source_files(
        stack.data_dir,
        body='{"timestamp":"2026-08-13T11:00:00Z","machine":"B"}\n',
    )

    with pytest.raises(FederationValidationError) as error:
        stack.scheduler.submit(
            work,
            slice_files=changed_files,
            slice_root=stack.data_dir,
        )

    assert error.value.code == "analysis-slice-registered-content-invalid"
    assert destination.read_bytes() == registered_bytes
