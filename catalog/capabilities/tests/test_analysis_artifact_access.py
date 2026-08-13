"""Security boundary tests for analysis job input retrieval."""

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
from catalog.capabilities.tests.analysis_harness import (
    PROVIDER,
    build_stack,
)
from catalog.federation.errors import FederationValidationError


def _dispatched(tmp_path, **kwargs):
    """Submit and claim a job so a live grant exists for the selected worker."""

    stack = build_stack(tmp_path, **kwargs)
    job_id = stack.submit().job_id
    hold: dict[str, object] = {}
    original = stack.relay.request

    async def capture(*, target_node_id, request):
        snapshot = stack.store.snapshot(request.job.job_id)
        hold["snapshot"] = snapshot
        hold["request"] = request
        return await original(target_node_id=target_node_id, request=request)

    stack.relay.request = capture
    asyncio.run(stack.scheduler.schedule(job_id))
    return stack, job_id, hold


def _while_owned(tmp_path, check, **kwargs):
    """Run ``check(stack, job_id, snapshot)`` while ownership is still live."""

    stack = build_stack(tmp_path, **kwargs)
    job_id = stack.submit().job_id
    original = stack.relay.request
    observed: list[object] = []

    async def capture(*, target_node_id, request):
        snapshot = stack.store.snapshot(request.job.job_id)
        observed.append(check(stack, request.job.job_id, snapshot))
        return await original(target_node_id=target_node_id, request=request)

    stack.relay.request = capture
    asyncio.run(stack.scheduler.schedule(job_id))
    assert observed, "the dispatch hook never ran"
    return stack, job_id


def _reference(stack, job_id, snapshot, *, schema=ANALYSIS_DATA_SLICE_SCHEMA):
    reference = next(
        item for item in snapshot.job.inputs if item.schema_name == schema
    )
    return ArtifactInputReference(
        artifact_id=reference.reference_id,
        session_id=reference.session_id,
        job_id=job_id,
        content_hash=reference.content_hash,
        schema_id=reference.schema_name,
        endpoint_id=ANALYSIS_ENDPOINT_ID,
    )


def test_authorized_worker_receives_the_job_input(tmp_path) -> None:
    stack, job_id, _hold = _dispatched(tmp_path)

    call = stack.executor.calls[0]
    assert call["files"]
    assert '"machine":"A"' in next(iter(call["files"].values()))
    assert call["plan"].job_id == job_id


def test_worker_workspace_is_removed_after_execution(tmp_path) -> None:
    stack, _job_id, _hold = _dispatched(tmp_path)

    workspace = stack.executor.calls[0]["workspace"]
    assert not workspace.exists()


def test_unauthorized_worker_node_cannot_retrieve_the_input(tmp_path) -> None:
    stack, job_id, hold = _dispatched(tmp_path)
    snapshot = hold["snapshot"]
    grant_id = analysis_grant_id(job_id, snapshot.ownership.attempt_id)

    with pytest.raises(FederationValidationError) as error:
        list(
            stack.gateway.fetch(
                grant_id=grant_id,
                reference=_reference(stack, job_id, snapshot),
                authenticated_session_id=stack.session_id,
                authenticated_worker_node_id="node-attacker",
                provider_id=PROVIDER,
                now=stack.clock.now,
            )
        )
    assert error.value.code == "artifact-worker-mismatch"


def test_unauthorized_provider_cannot_retrieve_the_input(tmp_path) -> None:
    stack, job_id, hold = _dispatched(tmp_path)
    snapshot = hold["snapshot"]

    with pytest.raises(FederationValidationError) as error:
        list(
            stack.gateway.fetch(
                grant_id=analysis_grant_id(job_id, snapshot.ownership.attempt_id),
                reference=_reference(stack, job_id, snapshot),
                authenticated_session_id=stack.session_id,
                authenticated_worker_node_id=stack.worker_node_id,
                provider_id="provider-somebody-else",
                now=stack.clock.now,
            )
        )
    assert error.value.code == "artifact-provider-mismatch"


def test_another_federation_session_cannot_retrieve_the_input(tmp_path) -> None:
    stack, job_id, hold = _dispatched(tmp_path)
    snapshot = hold["snapshot"]

    with pytest.raises(FederationValidationError) as error:
        list(
            stack.gateway.fetch(
                grant_id=analysis_grant_id(job_id, snapshot.ownership.attempt_id),
                reference=_reference(stack, job_id, snapshot),
                authenticated_session_id="session-other-federation",
                authenticated_worker_node_id=stack.worker_node_id,
                provider_id=PROVIDER,
                now=stack.clock.now,
            )
        )
    assert error.value.code == "cross-session-artifact-access"


def test_expired_authorization_fails_closed(tmp_path) -> None:
    stack, job_id, hold = _dispatched(tmp_path)
    snapshot = hold["snapshot"]
    stack.clock.advance(hours=2)

    with pytest.raises(FederationValidationError) as error:
        list(
            stack.gateway.fetch(
                grant_id=analysis_grant_id(job_id, snapshot.ownership.attempt_id),
                reference=_reference(stack, job_id, snapshot),
                authenticated_session_id=stack.session_id,
                authenticated_worker_node_id=stack.worker_node_id,
                provider_id=PROVIDER,
                now=stack.clock.now,
            )
        )
    assert error.value.code == "artifact-grant-expired"


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
            list(
                stack.gateway.fetch(
                    grant_id=analysis_grant_id(job_id, snapshot.ownership.attempt_id),
                    reference=foreign,
                    authenticated_session_id=stack.session_id,
                    authenticated_worker_node_id=stack.worker_node_id,
                    provider_id=PROVIDER,
                    now=stack.clock.now,
                )
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
            list(
                stack.gateway.fetch(
                    grant_id=analysis_grant_id(job_id, snapshot.ownership.attempt_id),
                    reference=forged,
                    authenticated_session_id=stack.session_id,
                    authenticated_worker_node_id=stack.worker_node_id,
                    provider_id=PROVIDER,
                    now=stack.clock.now,
                )
            )
        assert error.value.code == "input-reference-mismatch"

    _while_owned(tmp_path, check)


def test_grant_dies_with_the_attempt_that_owned_it(tmp_path) -> None:
    stack, job_id, hold = _dispatched(tmp_path)
    snapshot = hold["snapshot"]

    # The job has completed, so ownership is released and the grant is stale even
    # though its own expiry has not yet been reached.
    with pytest.raises(FederationValidationError) as error:
        list(
            stack.gateway.fetch(
                grant_id=analysis_grant_id(job_id, snapshot.ownership.attempt_id),
                reference=_reference(stack, job_id, snapshot),
                authenticated_session_id=stack.session_id,
                authenticated_worker_node_id=stack.worker_node_id,
                provider_id=PROVIDER,
                now=stack.clock.now,
            )
        )
    assert error.value.code == "stale-artifact-grant"


def test_tampered_stored_content_fails_integrity_verification(tmp_path) -> None:
    stack = build_stack(tmp_path)
    job_id = stack.submit().job_id
    snapshot = stack.store.snapshot(job_id)
    slice_reference = next(
        item
        for item in snapshot.job.inputs
        if item.schema_name == ANALYSIS_DATA_SLICE_SCHEMA
    )
    descriptor = stack.authority.artifact(slice_reference.reference_id)
    stored = stack.content_store.resolve(descriptor.object_key)
    stored.write_bytes(b"x" * descriptor.size_bytes)

    with pytest.raises(FederationValidationError) as error:
        list(
            stack.content_store.stream(
                descriptor.object_key,
                content_hash=descriptor.content_hash,
                size_bytes=descriptor.size_bytes,
            )
        )
    assert error.value.code == "analysis-artifact-integrity-mismatch"


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
    assert stack.store.snapshot(job_id).job.status is not None
    assert stack.store.result_commit(job_id) is None


@pytest.mark.parametrize(
    "key",
    ("../escape.json", "/absolute.json", "nested/../../escape.json", "a\\b.json"),
)
def test_content_store_rejects_traversal_keys(tmp_path, key) -> None:
    stack = build_stack(tmp_path)

    with pytest.raises(FederationValidationError):
        stack.content_store.resolve(key)


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

    with pytest.raises(FederationValidationError) as error:
        extract_slice_archive(
            [_archive([(info, b"pwned")])],
            archive_path=tmp_path / "slice.tar.gz",
            destination=tmp_path / "workspace",
        )
    assert error.value.code in {
        "analysis-slice-path-escape",
        "analysis-slice-unsafe-name",
    }
    assert not (tmp_path.parent / "escape.jsonl").exists()


def test_slice_archive_cannot_deliver_a_symlink(tmp_path) -> None:
    info = tarfile.TarInfo(name="link")
    info.type = tarfile.SYMTYPE
    info.linkname = "/etc/passwd"

    with pytest.raises(FederationValidationError) as error:
        extract_slice_archive(
            [_archive([(info, None)])],
            archive_path=tmp_path / "slice.tar.gz",
            destination=tmp_path / "workspace",
        )
    assert error.value.code == "analysis-slice-unsupported-member"


def test_slice_archive_is_bounded(tmp_path) -> None:
    info = tarfile.TarInfo(name="big.jsonl")
    body = b"x" * 4096
    info.size = len(body)

    with pytest.raises(FederationValidationError) as error:
        extract_slice_archive(
            [_archive([(info, body)])],
            archive_path=tmp_path / "slice.tar.gz",
            destination=tmp_path / "workspace",
            max_bytes=64,
        )
    assert error.value.code == "analysis-slice-too-large"


def test_denied_access_is_audited(tmp_path) -> None:
    stack, job_id, hold = _dispatched(tmp_path)
    snapshot = hold["snapshot"]

    with pytest.raises(FederationValidationError):
        list(
            stack.gateway.fetch(
                grant_id=analysis_grant_id(job_id, snapshot.ownership.attempt_id),
                reference=_reference(stack, job_id, snapshot),
                authenticated_session_id="session-other-federation",
                authenticated_worker_node_id=stack.worker_node_id,
                provider_id=PROVIDER,
                now=stack.clock.now,
            )
        )
    events = stack.authority.audit_trail(job_id)
    assert any(item.event_type == "access-denied" for item in events)
    assert any(item.event_type == "grant-issued" for item in events)
