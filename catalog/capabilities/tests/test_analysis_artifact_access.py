"""Security boundary tests for analysis job input retrieval.

Authorization is the F7.6 artifact authority; the bytes move on the F6 object
transfer contract. Both are the repository's existing components.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import tarfile

import pytest

from catalog.capabilities.analysis.contracts import (
    ANALYSIS_DATA_SLICE_SCHEMA,
    ANALYSIS_ENDPOINT_ID,
    analysis_grant_id,
)
from catalog.capabilities.analysis.packaging import extract_slice_archive
from catalog.capabilities.artifact_contracts import ArtifactInputReference
from catalog.capabilities.tests.analysis_harness import build_stack
from catalog.federation.errors import FederationValidationError


def _dispatched(tmp_path, **kwargs):
    """Submit and run a job so a completed attempt and its grant exist."""

    stack = build_stack(tmp_path, **kwargs)
    job_id = stack.submit().job_id
    hold: dict[str, object] = {}
    original = stack.transport.request

    async def capture(*, target_node_id, request):
        hold["snapshot"] = stack.store.snapshot(request.job.job_id)
        return await original(target_node_id=target_node_id, request=request)

    stack.transport.request = capture
    asyncio.run(stack.scheduler.schedule(job_id))
    return stack, job_id, hold


def _while_owned(tmp_path, check, **kwargs):
    """Run ``check(stack, job_id, snapshot)`` while ownership is still live."""

    stack = build_stack(tmp_path, **kwargs)
    job_id = stack.submit().job_id
    original = stack.transport.request
    observed: list[object] = []

    async def capture(*, target_node_id, request):
        snapshot = stack.store.snapshot(request.job.job_id)
        observed.append(check(stack, request.job.job_id, snapshot))
        return await original(target_node_id=target_node_id, request=request)

    stack.transport.request = capture
    asyncio.run(stack.scheduler.schedule(job_id))
    assert observed, "the dispatch hook never ran"
    return stack, job_id


def _reference(stack, job_id, snapshot, *, schema=ANALYSIS_DATA_SLICE_SCHEMA):
    reference = next(item for item in snapshot.job.inputs if item.schema_name == schema)
    return ArtifactInputReference(
        artifact_id=reference.reference_id,
        session_id=reference.session_id,
        job_id=job_id,
        content_hash=reference.content_hash,
        schema_id=reference.schema_name,
        endpoint_id=ANALYSIS_ENDPOINT_ID,
    )


def _manifest(stack, grant_id, reference, **overrides):
    parameters = {
        "grant_id": grant_id,
        "reference": reference,
        "authenticated_session_id": stack.session_id,
        "authenticated_worker_node_id": stack.node_id,
        "provider_id": stack.provider_id,
        "now": stack.clock.now,
    }
    parameters.update(overrides)
    return stack.gateway.transfer_manifest(**parameters)


# ----------------------------------------------------------------------


def test_authorized_worker_receives_the_job_input(tmp_path) -> None:
    stack, job_id, _hold = _dispatched(tmp_path)

    call = stack.executor.calls[0]
    assert call["files"]
    assert '"machine":"A"' in next(iter(call["files"].values()))
    assert call["plan"].job_id == job_id


def test_input_moves_on_the_f6_object_transfer_contract(tmp_path) -> None:
    def check(stack, job_id, snapshot):
        grant_id = analysis_grant_id(job_id, snapshot.ownership.attempt_id)
        reference = _reference(stack, job_id, snapshot)
        manifest = _manifest(stack, grant_id, reference)
        assert manifest.object_id == reference.artifact_id
        assert manifest.object_hash == reference.content_hash
        assert manifest.chunk_count >= 1
        chunk = stack.gateway.transfer_chunk(
            grant_id=grant_id,
            reference=reference,
            index=0,
            authenticated_session_id=stack.session_id,
            authenticated_worker_node_id=stack.node_id,
            provider_id=stack.provider_id,
            now=stack.clock.now,
        )
        assert chunk.transfer_id == manifest.transfer_id
        assert chunk.index == 0
        assert chunk.chunk_length == manifest.expected_chunk_length(0)

    _while_owned(tmp_path, check)


def test_worker_workspace_is_removed_after_execution(tmp_path) -> None:
    stack, _job_id, _hold = _dispatched(tmp_path)

    assert not stack.executor.calls[0]["workspace"].exists()


def test_unauthorized_worker_node_cannot_retrieve_the_input(tmp_path) -> None:
    def check(stack, job_id, snapshot):
        grant_id = analysis_grant_id(job_id, snapshot.ownership.attempt_id)
        with pytest.raises(FederationValidationError) as error:
            _manifest(
                stack,
                grant_id,
                _reference(stack, job_id, snapshot),
                authenticated_worker_node_id="node-attacker",
            )
        assert error.value.code == "artifact-worker-mismatch"

    _while_owned(tmp_path, check)


def test_unauthorized_provider_cannot_retrieve_the_input(tmp_path) -> None:
    def check(stack, job_id, snapshot):
        grant_id = analysis_grant_id(job_id, snapshot.ownership.attempt_id)
        with pytest.raises(FederationValidationError) as error:
            _manifest(
                stack,
                grant_id,
                _reference(stack, job_id, snapshot),
                provider_id="provider-somebody-else",
            )
        assert error.value.code == "artifact-provider-mismatch"

    _while_owned(tmp_path, check)


def test_another_federation_session_cannot_retrieve_the_input(tmp_path) -> None:
    def check(stack, job_id, snapshot):
        grant_id = analysis_grant_id(job_id, snapshot.ownership.attempt_id)
        with pytest.raises(FederationValidationError) as error:
            _manifest(
                stack,
                grant_id,
                _reference(stack, job_id, snapshot),
                authenticated_session_id="session-other-federation",
            )
        assert error.value.code == "cross-session-artifact-access"

    _while_owned(tmp_path, check)


def test_artifact_outside_the_grant_allowlist_is_refused(tmp_path) -> None:
    def check(stack, job_id, snapshot):
        reference = _reference(stack, job_id, snapshot)
        foreign = ArtifactInputReference(
            artifact_id="analysis-slice-somebody-elses-data",
            session_id=reference.session_id,
            job_id=job_id,
            content_hash=reference.content_hash,
            schema_id=reference.schema_id,
            endpoint_id=reference.endpoint_id,
        )
        with pytest.raises(FederationValidationError) as error:
            _manifest(
                stack,
                analysis_grant_id(job_id, snapshot.ownership.attempt_id),
                foreign,
            )
        assert error.value.code == "artifact-input-not-granted"

    _while_owned(tmp_path, check)


def test_reference_that_lies_about_content_identity_is_refused(tmp_path) -> None:
    def check(stack, job_id, snapshot):
        reference = _reference(stack, job_id, snapshot)
        forged = ArtifactInputReference(
            artifact_id=reference.artifact_id,
            session_id=reference.session_id,
            job_id=job_id,
            content_hash="sha256:" + "f" * 64,
            schema_id=reference.schema_id,
            endpoint_id=reference.endpoint_id,
        )
        with pytest.raises(FederationValidationError) as error:
            _manifest(
                stack,
                analysis_grant_id(job_id, snapshot.ownership.attempt_id),
                forged,
            )
        assert error.value.code == "input-reference-mismatch"

    _while_owned(tmp_path, check)


def test_expired_authorization_fails_closed(tmp_path) -> None:
    stack, job_id, hold = _dispatched(tmp_path)
    snapshot = hold["snapshot"]
    stack.clock.advance(hours=2)

    with pytest.raises(FederationValidationError) as error:
        _manifest(
            stack,
            analysis_grant_id(job_id, snapshot.ownership.attempt_id),
            _reference(stack, job_id, snapshot),
        )
    assert error.value.code == "artifact-grant-expired"


def test_grant_dies_with_the_attempt_that_owned_it(tmp_path) -> None:
    stack, job_id, hold = _dispatched(tmp_path)
    snapshot = hold["snapshot"]

    # The job completed, so ownership is released and the grant is stale even
    # though its own expiry has not been reached.
    with pytest.raises(FederationValidationError) as error:
        _manifest(
            stack,
            analysis_grant_id(job_id, snapshot.ownership.attempt_id),
            _reference(stack, job_id, snapshot),
        )
    assert error.value.code == "stale-artifact-grant"


def test_failed_attempt_immediately_loses_artifact_access(tmp_path) -> None:
    """Ending an attempt must invalidate the grant bound to it.

    Reassignment to a *different* provider additionally requires a second
    eligible provider, which a single-provider federation does not have; the
    fencing property under test here is the one that protects the data owner.
    """

    stack = build_stack(tmp_path, succeed=False)
    job_id = stack.submit().job_id
    first: dict[str, object] = {}
    original = stack.transport.request

    async def capture(*, target_node_id, request):
        snapshot = stack.store.snapshot(request.job.job_id)
        first.setdefault("attempt_id", snapshot.ownership.attempt_id)
        first.setdefault("snapshot", snapshot)
        return await original(target_node_id=target_node_id, request=request)

    stack.transport.request = capture
    asyncio.run(stack.scheduler.schedule(job_id))
    after = stack.store.snapshot(job_id)

    assert after.ownership is None
    assert after.job.attempts[-1].status.value == "failed"
    with pytest.raises(FederationValidationError) as error:
        _manifest(
            stack,
            analysis_grant_id(job_id, str(first["attempt_id"])),
            _reference(stack, job_id, first["snapshot"]),
        )
    assert error.value.code == "stale-artifact-grant"


def test_integrity_failure_prevents_execution(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id
    snapshot = stack.store.snapshot(job_id)
    slice_reference = next(
        item
        for item in snapshot.job.inputs
        if item.schema_name == ANALYSIS_DATA_SLICE_SCHEMA
    )
    descriptor = stack.authority.artifact(slice_reference.reference_id)
    stack.content_store.resolve(descriptor.object_key).write_bytes(b"corrupted")

    asyncio.run(stack.scheduler.schedule(job_id))

    assert stack.executor.calls == []
    assert stack.store.result_commit(job_id) is None


def test_denied_access_is_audited(tmp_path) -> None:
    stack, job_id, hold = _dispatched(tmp_path)
    snapshot = hold["snapshot"]

    with pytest.raises(FederationValidationError):
        _manifest(
            stack,
            analysis_grant_id(job_id, snapshot.ownership.attempt_id),
            _reference(stack, job_id, snapshot),
            authenticated_session_id="session-other-federation",
        )
    events = stack.authority.audit_trail(job_id)
    assert any(item.event_type == "access-denied" for item in events)
    assert any(item.event_type == "grant-issued" for item in events)


# ----------------------------------------------------------------------
# Archive parsing treats the slice as hostile input
# ----------------------------------------------------------------------


def _archive(members) -> bytes:
    buffer = io.BytesIO()
    with (
        gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w") as archive,
    ):
        for info, body in members:
            archive.addfile(info, None if body is None else io.BytesIO(body))
    return buffer.getvalue()


def test_slice_archive_member_cannot_escape_the_workspace(tmp_path) -> None:
    info = tarfile.TarInfo(name="../../escape.jsonl")
    info.size = 5
    path = tmp_path / "slice.tar.gz"
    path.write_bytes(_archive([(info, b"pwned")]))

    with pytest.raises(FederationValidationError) as error:
        extract_slice_archive(path, destination=tmp_path / "workspace")
    assert error.value.code in {
        "analysis-slice-path-escape",
        "analysis-slice-unsafe-name",
    }
    assert not (tmp_path.parent / "escape.jsonl").exists()


def test_slice_archive_cannot_deliver_a_symlink(tmp_path) -> None:
    info = tarfile.TarInfo(name="link")
    info.type = tarfile.SYMTYPE
    info.linkname = "/etc/passwd"
    path = tmp_path / "slice.tar.gz"
    path.write_bytes(_archive([(info, None)]))

    with pytest.raises(FederationValidationError) as error:
        extract_slice_archive(path, destination=tmp_path / "workspace")
    assert error.value.code == "analysis-slice-unsupported-member"


def test_slice_archive_is_bounded(tmp_path) -> None:
    info = tarfile.TarInfo(name="big.jsonl")
    body = b"x" * 4096
    info.size = len(body)
    path = tmp_path / "slice.tar.gz"
    path.write_bytes(_archive([(info, body)]))

    with pytest.raises(FederationValidationError) as error:
        extract_slice_archive(path, destination=tmp_path / "workspace", max_bytes=64)
    assert error.value.code == "analysis-slice-too-large"


@pytest.mark.parametrize(
    "key",
    ("../escape.json", "/absolute.json", "nested/../../escape.json", "a\\b.json"),
)
def test_content_store_rejects_traversal_keys(tmp_path, key) -> None:
    stack = build_stack(tmp_path)

    with pytest.raises(FederationValidationError):
        stack.content_store.resolve(key)
