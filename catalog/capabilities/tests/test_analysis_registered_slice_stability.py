"""Regression coverage for registered analysis slices beside live source writes."""

from __future__ import annotations

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


def _registered(stack, work):
    stack.submit(work)
    object_key = f"{work.object_key_prefix}/slice.tar.gz"
    destination = stack.content_store.resolve(object_key)
    descriptor = stack.authority.artifact(slice_artifact_id(work))
    return destination, descriptor, destination.read_bytes()


def test_transient_source_change_is_retried_not_reported_as_corruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    destination, descriptor, registered_bytes = _registered(stack, work)
    real_compare = packaging._compare_slice_archive_once
    calls = {"count": 0}

    def _one_transient_change(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return packaging.SliceArchiveComparison.SOURCE_CHANGED
        return real_compare(*args, **kwargs)

    monkeypatch.setattr(
        packaging, "_compare_slice_archive_once", _one_transient_change
    )
    outcome = stack.scheduler.submit(
        work,
        slice_files=source_files(stack.data_dir),
        slice_root=stack.data_dir,
    )

    assert calls["count"] == 2
    assert outcome.created is False
    assert destination.read_bytes() == registered_bytes
    reread = stack.authority.artifact(slice_artifact_id(work))
    assert reread.content_hash == descriptor.content_hash
    assert reread.size_bytes == descriptor.size_bytes


def test_stable_source_mismatch_under_same_work_identity_fails_closed(
    tmp_path: Path,
) -> None:
    """A metadata/source-signature collision must never silently reuse old bytes."""

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    destination, _descriptor, registered_bytes = _registered(stack, work)
    changed_files = source_files(
        stack.data_dir,
        body='{"timestamp":"2026-08-13T10:00:00Z","machine":"B"}\n',
    )

    with pytest.raises(FederationValidationError) as error:
        stack.scheduler.submit(
            work,
            slice_files=changed_files,
            slice_root=stack.data_dir,
        )

    assert error.value.code == "analysis-slice-registered-content-invalid"
    assert destination.read_bytes() == registered_bytes


def test_source_that_never_stabilizes_has_a_bounded_distinct_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    destination, _descriptor, registered_bytes = _registered(stack, work)
    calls = {"count": 0}

    def _always_changing(*_args, **_kwargs):
        calls["count"] += 1
        return packaging.SliceArchiveComparison.SOURCE_CHANGED

    monkeypatch.setattr(packaging, "_compare_slice_archive_once", _always_changing)

    with pytest.raises(FederationValidationError) as error:
        stack.scheduler.submit(
            work,
            slice_files=source_files(stack.data_dir),
            slice_root=stack.data_dir,
        )

    assert error.value.code == "analysis-slice-source-changing"
    assert calls["count"] == packaging._SOURCE_STABILITY_ATTEMPTS
    assert destination.read_bytes() == registered_bytes


def test_missing_registered_body_fails_closed_without_republishing(
    tmp_path: Path,
) -> None:
    """Recovery must not publish an unverified reconstruction at the bound key."""

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    destination, _descriptor, _registered_bytes = _registered(stack, work)
    destination.unlink()

    with pytest.raises(FederationValidationError) as error:
        stack.scheduler.submit(
            work,
            slice_files=source_files(stack.data_dir),
            slice_root=stack.data_dir,
        )

    assert error.value.code == "analysis-slice-registered-content-invalid"
    assert not destination.exists()


def test_real_registered_body_corruption_still_fails_closed(
    tmp_path: Path,
) -> None:
    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    destination, _descriptor, registered_bytes = _registered(stack, work)
    corrupt = bytearray(registered_bytes)
    corrupt[-1] ^= 0xFF
    destination.write_bytes(corrupt)

    with pytest.raises(FederationValidationError) as error:
        stack.scheduler.submit(
            work,
            slice_files=source_files(stack.data_dir),
            slice_root=stack.data_dir,
        )

    assert error.value.code == "analysis-slice-registered-content-invalid"
    assert destination.read_bytes() == bytes(corrupt)
