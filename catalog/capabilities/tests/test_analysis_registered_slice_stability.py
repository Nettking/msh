"""Regression coverage for registered analysis slices beside live source writes."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from catalog.capabilities.analysis import packaging
from catalog.capabilities.analysis.content_store import ContentIdentity
from catalog.capabilities.analysis.contracts import plan_artifact_id, slice_artifact_id
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


def test_retry_later_preserves_both_registered_input_identities(tmp_path: Path) -> None:
    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    first = stack.submit(work)
    artifact_ids = (plan_artifact_id(work), slice_artifact_id(work))
    original = tuple(stack.authority.artifact(item) for item in artifact_ids)
    stack.clock.advance(seconds=45)

    retry = stack.scheduler.submit(
        work,
        slice_files=source_files(stack.data_dir),
        slice_root=stack.data_dir,
    )

    assert retry.job_id == first.job_id
    assert retry.created is False
    assert tuple(stack.authority.artifact(item) for item in artifact_ids) == original


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("session_id", "session-other"),
        ("job_id", "job-other"),
        ("schema_id", "fcp.other.v1"),
        ("media_type", "application/octet-stream"),
        ("object_key", "other/input.json"),
        ("transfer_id", "transfer-other"),
        ("content_hash", "sha256:" + "f" * 64),
        ("size_bytes", 99999),
        ("endpoint_id", "analysis-other"),
    ),
)
def test_retry_later_still_rejects_changed_input_binding(
    tmp_path: Path, field: str, value: str | int
) -> None:
    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    stack.submit(work)
    original = stack.authority.artifact(plan_artifact_id(work))
    values = {
        name: getattr(original, name)
        for name in (
            "artifact_id", "session_id", "job_id", "schema_id", "media_type",
            "object_key", "transfer_id",
        )
    }
    identity = ContentIdentity(original.content_hash, original.size_bytes)
    if field in ("content_hash", "size_bytes"):
        identity = replace(identity, **{field: value})
    elif field == "endpoint_id":
        stack.gateway.endpoint_id = value
    else:
        values[field] = value
    stack.clock.advance(seconds=45)

    with pytest.raises(FederationValidationError) as error:
        stack.gateway.register_input(
            **values,
            identity=identity,
            authority_node_id=stack.node_id,
            now=stack.clock(),
        )

    assert error.value.code == "artifact-identity-conflict"
    assert stack.authority.artifact(original.artifact_id) == original


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
