"""Recovery consequences for the analysis publication sequence.

Publishing one analysis work slice is not one transaction, and cannot be: the
plan and slice bodies are files, the job row and artifact descriptors are
SQLite. Submission writes the bodies, commits the job, commits each descriptor,
then commits the queue transition -- four durable commits after two filesystem
publications. A crash can land between any of them.

That is safe here because of two properties these tests pin, not because the
window is small:

* every identity is a deterministic function of the work slice, so a retry
  addresses exactly the same job, object keys and artifact IDs; and
* each step is idempotent-or-refused -- re-registering identical content
  returns the existing descriptor, while re-registering *different* content
  under a registered ID is rejected rather than silently accepted.

So the recovery contract is: restart, re-discover, re-submit. These tests build
each reachable partial state by removing the durable rows a crash would not
have committed, then re-submit and assert the state is repaired without a
duplicate or a conflicting identity.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from catalog.capabilities.analysis.contracts import (
    ORIGIN_AUTOMATIC_DISCOVERY,
    ORIGIN_MANUAL_UPLOAD,
    plan_artifact_id,
    slice_artifact_id,
)
from catalog.capabilities.tests.analysis_harness import build_stack, work_slice
from catalog.federation.errors import FederationValidationError


def _database(stack) -> Path:
    return stack.tmp_path / "capabilities" / "analysis_jobs.sqlite3"


# Every table job submission writes. They are committed in one transaction --
# job_store.submit() opens BEGIN IMMEDIATE and records the command-replay row
# beside the job row -- so a crash before that commit leaves none of them, and
# forgetting a job means forgetting all of them. Removing only capability_jobs
# would build a state no crash can produce: the replay row would then answer
# every retry with a cached result for a job that does not exist.
_JOB_SCOPED_TABLES = (
    "capability_job_results",
    "capability_job_cancellations",
    "capability_job_heartbeat_commands",
    "capability_job_heartbeats",
    "capability_job_retry_state",
    "capability_job_audit",
    "capability_job_commands",
    "capability_job_attempts",
    "capability_jobs",
)


def _execute(stack, statement: str, parameters: tuple = ()) -> None:
    """Remove durable rows a crash at that point would never have committed."""

    connection = sqlite3.connect(_database(stack))
    try:
        connection.execute(statement, parameters)
        connection.commit()
    finally:
        connection.close()


def _forget_job(stack, job_id: str) -> None:
    """Roll the durable job state back to before its submission commit."""

    connection = sqlite3.connect(_database(stack))
    try:
        for table in _JOB_SCOPED_TABLES:
            connection.execute(f"DELETE FROM {table} WHERE job_id=?", (job_id,))
        connection.commit()
    finally:
        connection.close()


def _registered(stack, artifact_id: str):
    try:
        return stack.gateway.authority.artifact(artifact_id)
    except FederationValidationError as exc:
        if exc.code == "artifact-not-found":
            return None
        raise


def _publication_state(stack, work) -> dict[str, object]:
    prefix = work.object_key_prefix
    plan = _registered(stack, plan_artifact_id(work))
    archive = _registered(stack, slice_artifact_id(work))
    return {
        "plan_bytes": stack.content_store.exists(f"{prefix}/plan.json"),
        "slice_bytes": stack.content_store.exists(f"{prefix}/slice.tar.gz"),
        "plan_hash": None if plan is None else plan.content_hash,
        "slice_hash": None if archive is None else archive.content_hash,
    }


@pytest.fixture()
def published(tmp_path: Path):
    """One completed submission, plus the identities it must keep on retry."""

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    outcome = stack.submit(work)
    assert outcome.created is True
    return stack, work, outcome, _publication_state(stack, work)


def test_a_completed_submission_repeats_without_creating_new_work(published) -> None:
    """The no-crash baseline: re-submission is a no-op, not a second job."""

    stack, work, outcome, state = published

    repeat = stack.submit(work)

    assert repeat.job_id == outcome.job_id
    assert repeat.created is False
    assert repeat.idempotency_key == outcome.idempotency_key
    assert _publication_state(stack, work) == state


def test_bodies_published_without_any_durable_row_are_recovered(published) -> None:
    """Crash after the filesystem publication, before the job commit.

    The plan and slice bodies are on disk with no job and no descriptors. The
    retry must adopt those exact bodies rather than treat them as foreign, and
    must produce the identities the first attempt would have.
    """

    stack, work, outcome, state = published
    _execute(stack, "DELETE FROM capability_artifacts")
    _forget_job(stack, outcome.job_id)
    assert state["plan_bytes"] and state["slice_bytes"]

    recovered = stack.submit(work)

    assert recovered.job_id == outcome.job_id
    assert recovered.created is True
    assert _publication_state(stack, work) == state


def test_a_committed_job_missing_its_descriptors_is_repaired(published) -> None:
    """Crash after the job commit, before either descriptor commit.

    The job is durable and discoverable but its inputs are unregistered, so a
    worker granted the job would find nothing to fetch. Re-submission must
    register them against the same job rather than fail on the existing row.
    """

    stack, work, outcome, _state = published
    _execute(stack, "DELETE FROM capability_artifacts")
    assert _registered(stack, plan_artifact_id(work)) is None
    assert _registered(stack, slice_artifact_id(work)) is None

    recovered = stack.submit(work)

    assert recovered.job_id == outcome.job_id
    assert recovered.created is False
    assert _registered(stack, plan_artifact_id(work)) is not None
    assert _registered(stack, slice_artifact_id(work)) is not None


def test_a_half_registered_publication_is_completed_on_retry(published) -> None:
    """Crash between the two descriptor commits.

    The plan is registered and the slice is not. The retry must leave the
    plan's descriptor untouched -- re-registering identical content returns the
    existing row -- and add the missing one.
    """

    stack, work, outcome, state = published
    _execute(
        stack,
        "DELETE FROM capability_artifacts WHERE artifact_id=?",
        (slice_artifact_id(work),),
    )
    plan_before = _registered(stack, plan_artifact_id(work))
    assert plan_before is not None
    assert _registered(stack, slice_artifact_id(work)) is None

    recovered = stack.submit(work)

    assert recovered.job_id == outcome.job_id
    plan_after = _registered(stack, plan_artifact_id(work))
    assert plan_after is not None
    assert plan_after.content_hash == plan_before.content_hash
    assert plan_after.created_at == plan_before.created_at
    assert _publication_state(stack, work) == state


def test_a_registered_slice_whose_body_changed_is_refused_not_republished(
    published,
) -> None:
    """The state that must never be silently accepted.

    A descriptor is durable evidence that specific bytes were published under
    that identity. If the body no longer matches, republishing would rebind a
    registered identity to different content and invalidate every grant and
    content hash already issued against it. Submission refuses instead.
    """

    stack, work, _outcome, _state = published
    body = stack.content_store.resolve(f"{work.object_key_prefix}/slice.tar.gz")
    body.write_bytes(b"not the deterministic archive")

    with pytest.raises(FederationValidationError) as raised:
        stack.submit(work)

    assert raised.value.code == "analysis-slice-registered-content-invalid"
    # The refusal is not a repair: the operator still owns the divergence.
    assert body.read_bytes() == b"not the deterministic archive"


def test_an_unregistered_stale_body_is_replaced_rather_than_refused(
    tmp_path: Path,
) -> None:
    """The mirror case: no descriptor means no identity has been promised yet.

    A body left by a crash before any registration carries no durable claim, so
    the deterministic archive may replace it.
    """

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    outcome = stack.submit(work)
    state = _publication_state(stack, work)
    _execute(stack, "DELETE FROM capability_artifacts")
    _forget_job(stack, outcome.job_id)
    body = stack.content_store.resolve(f"{work.object_key_prefix}/slice.tar.gz")
    body.write_bytes(b"truncated by a crash")

    recovered = stack.submit(work)

    assert recovered.job_id == outcome.job_id
    assert _publication_state(stack, work) == state


def test_a_corrupted_plan_body_is_repaired_rather_than_refused(published) -> None:
    """The plan and the slice are protected differently, and correctly so.

    ``plan_bytes()`` serializes ``plan_document()``, whose fields are a subset
    of the payload behind ``identity_digest`` plus ``job_id``, which is derived
    from that digest. So the plan body is a pure function of the artifact ID:
    the same ``analysis-plan-<digest>`` can only ever mean one byte string, and
    rewriting it unconditionally restores exactly what was registered.

    The slice archive is built from source files on disk and carries no such
    guarantee, which is why a mismatch there is refused instead. A retry must
    not paper over source data that no longer matches a promised identity.
    """

    stack, work, _outcome, state = published
    body = stack.content_store.resolve(f"{work.object_key_prefix}/plan.json")
    body.write_bytes(b'{"corrupt":true}')

    recovered = stack.submit(work)

    assert recovered.job_id == work.job_id
    assert _publication_state(stack, work) == state
    assert body.read_bytes() == work.plan_bytes()


def test_the_plan_body_is_a_pure_function_of_the_artifact_identity(
    tmp_path: Path,
) -> None:
    """Pins the property the unconditional plan rewrite depends on.

    If any field could change ``plan_bytes()`` without changing
    ``identity_digest``, the rewrite would rebind a registered artifact ID to
    different content instead of repairing it. ``origin`` is the field most
    likely to drift: automatic discovery and manual upload reach the same slice
    by different routes and must still publish one job.
    """

    stack = build_stack(tmp_path)
    discovered = work_slice(
        session_id=stack.session_id, origin=ORIGIN_AUTOMATIC_DISCOVERY
    )
    uploaded = work_slice(session_id=stack.session_id, origin=ORIGIN_MANUAL_UPLOAD)

    assert discovered.origin != uploaded.origin
    assert discovered.identity_digest == uploaded.identity_digest
    assert plan_artifact_id(discovered) == plan_artifact_id(uploaded)
    assert discovered.plan_bytes() == uploaded.plan_bytes()

    outcome = stack.submit(discovered)
    repeat = stack.submit(uploaded)

    assert repeat.job_id == outcome.job_id
    assert repeat.created is False
