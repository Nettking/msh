"""Crash-correct publication tests for deterministic analysis slice archives."""

from __future__ import annotations

import tarfile
from pathlib import Path

import pytest

from catalog.capabilities.analysis import packaging
from catalog.capabilities.analysis import scheduler as scheduler_module
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


# ---------------------------------------------------------------------------
# A registered slice is judged against its durable identity, never the source
# ---------------------------------------------------------------------------


def _registered(stack, work):
    """Submit once and return (object_key, destination, registered bytes)."""

    stack.submit(work)
    object_key = f"{work.object_key_prefix}/slice.tar.gz"
    destination = stack.content_store.resolve(object_key)
    return object_key, destination, destination.read_bytes()


def test_a_live_source_write_neither_overwrites_nor_invalidates_a_registered_slice(
    tmp_path: Path,
) -> None:
    """The regression this delivery closes.

    Recorder data is live: today's ``day.jsonl`` keeps being written while
    submissions run. A registered slice represents the source identified by
    ``work.source_signature``, which is part of the identity digest that
    produced this artifact ID -- materially changed data becomes a *different*
    job, so a write here cannot mean this artifact is wrong. Re-reading the
    live file to judge it turned an ordinary write into
    ``analysis-slice-registered-content-invalid`` and stranded the slice
    permanently, because every later retry re-read the same changed file.

    The immutability guarantee is unchanged and is asserted below: the
    registered bytes are never rewritten.
    """

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    object_key, destination, registered_bytes = _registered(stack, work)
    descriptor = stack.authority.artifact(slice_artifact_id(work))
    changed_files = source_files(
        stack.data_dir,
        body='{"timestamp":"2026-08-13T11:00:00Z","machine":"B"}\n',
    )

    outcome = stack.scheduler.submit(
        work,
        slice_files=changed_files,
        slice_root=stack.data_dir,
    )

    # The registered artifact is reused, not rebuilt and not rejected.
    assert outcome.job_id == work.job_id
    assert outcome.created is False
    assert destination.read_bytes() == registered_bytes
    reread = stack.authority.artifact(slice_artifact_id(work))
    assert reread.content_hash == descriptor.content_hash
    assert reread.size_bytes == descriptor.size_bytes
    assert stack.content_store.identity(object_key).content_hash == (
        descriptor.content_hash
    )


def test_real_corruption_of_a_registered_slice_still_fails_closed(
    tmp_path: Path,
) -> None:
    """Identity-based validation is strictly stronger than the source compare.

    The body no longer matches the hash the authority recorded, which no source
    write can explain. It is never rewritten to paper over the damage.
    """

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    _object_key, destination, registered_bytes = _registered(stack, work)
    corrupt = bytearray(registered_bytes)
    corrupt[-1] ^= 0xFF
    destination.write_bytes(bytes(corrupt))

    with pytest.raises(FederationValidationError) as error:
        stack.scheduler.submit(
            work,
            slice_files=source_files(stack.data_dir),
            slice_root=stack.data_dir,
        )

    assert error.value.code == "analysis-slice-registered-content-invalid"
    # Failing closed means leaving the damage visible, not replacing it.
    assert destination.read_bytes() == bytes(corrupt)


def test_a_truncated_registered_slice_still_fails_closed(tmp_path: Path) -> None:
    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    _object_key, destination, registered_bytes = _registered(stack, work)
    destination.write_bytes(registered_bytes[: len(registered_bytes) // 2])

    with pytest.raises(FederationValidationError) as error:
        stack.scheduler.submit(
            work,
            slice_files=source_files(stack.data_dir),
            slice_root=stack.data_dir,
        )

    assert error.value.code == "analysis-slice-registered-content-invalid"


def test_a_registered_slice_is_validated_without_reading_the_live_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The proof that a concurrent write cannot influence the verdict.

    If the registered path never opens the source, no interleaving of writes
    against it can produce either a false positive or a false negative.
    """

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    _object_key, _destination, _bytes = _registered(stack, work)

    def _forbidden(*_args, **_kwargs):
        raise AssertionError(
            "a registered slice must be validated against its durable "
            "identity, never against the live source"
        )

    monkeypatch.setattr(scheduler_module, "slice_archive_matches", _forbidden)
    monkeypatch.setattr(scheduler_module, "write_slice_archive", _forbidden)

    outcome = stack.scheduler.submit(
        work,
        slice_files=source_files(stack.data_dir),
        slice_root=stack.data_dir,
    )

    assert outcome.job_id == work.job_id


def test_a_registered_slice_still_enforces_input_confinement(
    tmp_path: Path,
) -> None:
    """Skipping the source read must not skip the path/name contract."""

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    _registered(stack, work)
    outside = tmp_path / "outside"
    outside.mkdir(parents=True, exist_ok=True)
    escaped = outside / "day.jsonl"
    escaped.write_text("{}\n", encoding="utf-8")

    with pytest.raises(FederationValidationError) as error:
        stack.scheduler.submit(
            work,
            slice_files=(escaped,),
            slice_root=stack.data_dir,
        )

    assert error.value.code == "analysis-slice-path-outside-root"


def test_a_lost_registered_body_is_rebuilt_only_to_its_registered_identity(
    tmp_path: Path,
) -> None:
    """The descriptor survived a crash that its body did not."""

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    object_key, destination, registered_bytes = _registered(stack, work)
    descriptor = stack.authority.artifact(slice_artifact_id(work))
    destination.unlink()

    outcome = stack.scheduler.submit(
        work,
        slice_files=source_files(stack.data_dir),
        slice_root=stack.data_dir,
    )

    assert outcome.job_id == work.job_id
    # Deterministic packing reproduces the registered identity exactly.
    assert destination.read_bytes() == registered_bytes
    assert stack.content_store.identity(object_key).content_hash == (
        descriptor.content_hash
    )


def test_a_lost_registered_body_that_cannot_be_reproduced_fails_closed(
    tmp_path: Path,
) -> None:
    """A repair that does not reproduce the registered identity is discarded."""

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    _object_key, destination, _registered_bytes = _registered(stack, work)
    destination.unlink()
    changed_files = source_files(
        stack.data_dir,
        body='{"timestamp":"2026-08-13T12:00:00Z","machine":"C"}\n',
    )

    with pytest.raises(FederationValidationError) as error:
        stack.scheduler.submit(
            work,
            slice_files=changed_files,
            slice_root=stack.data_dir,
        )

    assert error.value.code == "analysis-slice-registered-body-unrecoverable"
    # Nothing is left standing in place of the artifact it failed to rebuild.
    assert not destination.exists()


def test_a_winner_registering_mid_submission_is_reused_by_the_loser(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deterministic winner/loser interleave, no threads and no timing luck.

    The loser has already observed "not registered" when the winner completes
    its entire submission. The loser must converge on the winner's registration
    rather than conflicting with it.
    """

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    files = source_files(stack.data_dir)
    object_key = f"{work.object_key_prefix}/slice.tar.gz"
    destination = stack.content_store.resolve(object_key)
    original = scheduler_module.slice_archive_matches
    winner: dict[str, object] = {}

    def _race(*args, **kwargs):
        if not winner:
            monkeypatch.setattr(scheduler_module, "slice_archive_matches", original)
            winner["outcome"] = stack.scheduler.submit(
                work, slice_files=files, slice_root=stack.data_dir
            )
            winner["descriptor"] = stack.authority.artifact(slice_artifact_id(work))
            winner["bytes"] = destination.read_bytes()
        return original(*args, **kwargs)

    monkeypatch.setattr(scheduler_module, "slice_archive_matches", _race)
    loser = stack.scheduler.submit(work, slice_files=files, slice_root=stack.data_dir)

    assert winner, "the interleave never ran"
    assert loser.job_id == work.job_id
    descriptor = stack.authority.artifact(slice_artifact_id(work))
    assert descriptor.content_hash == winner["descriptor"].content_hash
    assert descriptor.size_bytes == winner["descriptor"].size_bytes
    assert destination.read_bytes() == winner["bytes"]


def test_repeated_submission_under_continuous_source_rewrites_strands_nothing(
    tmp_path: Path,
) -> None:
    """The release-gate shape, made deterministic.

    Every iteration rewrites the shared source and resubmits the same logical
    slice. One slice stays one job, the registered body never moves, and no
    iteration is rejected.
    """

    stack = build_stack(tmp_path)
    work = work_slice(session_id=stack.session_id)
    object_key, destination, registered_bytes = _registered(stack, work)
    descriptor = stack.authority.artifact(slice_artifact_id(work))

    for index in range(25):
        changed = source_files(
            stack.data_dir,
            body=f'{{"timestamp":"2026-08-13T10:00:{index:02d}Z","n":{index}}}\n',
        )
        outcome = stack.scheduler.submit(
            work, slice_files=changed, slice_root=stack.data_dir
        )
        assert outcome.job_id == work.job_id
        assert outcome.created is False

    assert destination.read_bytes() == registered_bytes
    assert stack.content_store.identity(object_key).content_hash == (
        descriptor.content_hash
    )


def test_a_source_rewritten_mid_comparison_reports_non_current_not_corrupt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The precise interleaving behind the observed release-gate failure.

    ``slice_archive_matches`` fstats each source file before and after reading
    it, so a concurrent write lands as ``False``. Its docstring is explicit
    that this means the archived snapshot is merely *non-current* -- the
    archive itself is untouched and still verifies. Treating that ``False`` as
    corruption of an already-registered slice is what turned an ordinary
    recorder write into ``analysis-slice-registered-content-invalid``.
    """

    root = tmp_path / "input"
    root.mkdir()
    source = root / "day.jsonl"
    source.write_text('{"timestamp":"2026-08-13T10:00:00Z"}\n', encoding="utf-8")
    destination = tmp_path / "artifacts" / "slice.tar.gz"
    destination.parent.mkdir(parents=True, exist_ok=True)
    packaging.write_slice_archive(destination, files=(source,), root=root)

    # Undisturbed, the archive is exactly the requested slice.
    assert packaging.slice_archive_matches(destination, files=(source,), root=root)

    calls = {"count": 0}
    real_signature = packaging._stat_signature

    def _rewritten_between_stats(value):
        calls["count"] += 1
        signature = real_signature(value)
        if calls["count"] % 2 == 0:  # the post-read stat sees a newer file
            return (*signature[:3], signature[3] + 1, signature[4])
        return signature

    monkeypatch.setattr(packaging, "_stat_signature", _rewritten_between_stats)

    assert not packaging.slice_archive_matches(
        destination, files=(source,), root=root
    )

    # The archive was never the problem: it still verifies once the source
    # stops moving, so "non-current" must never be reported as corruption.
    monkeypatch.setattr(packaging, "_stat_signature", real_signature)
    assert packaging.slice_archive_matches(destination, files=(source,), root=root)


def test_an_unstable_source_fails_closed_within_the_bounded_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Packing retries are finite: an endlessly changing source is not a loop.

    The instability is injected at the stability check itself rather than by
    rewriting the file and hoping the filesystem notices. A same-length rewrite
    is invisible to a platform whose inode and timestamp resolution cannot
    separate two writes in the same tick, which would silently turn this into a
    test of clock granularity instead of the retry bound.
    """

    root = tmp_path / "input"
    root.mkdir()
    source = root / "day.jsonl"
    source.write_text('{"n":0}\n', encoding="utf-8")
    destination = tmp_path / "artifacts" / "slice.tar.gz"
    destination.parent.mkdir(parents=True, exist_ok=True)
    calls = {"count": 0}
    real_signature = packaging._stat_signature

    def _always_moving(value):
        calls["count"] += 1
        signature = real_signature(value)
        if calls["count"] % 2 == 0:  # every post-read stat sees a newer file
            return (*signature[:3], signature[3] + 1, signature[4])
        return signature

    monkeypatch.setattr(packaging, "_stat_signature", _always_moving)

    with pytest.raises(FederationValidationError) as error:
        packaging.write_slice_archive(destination, files=(source,), root=root)

    assert error.value.code == "analysis-slice-source-changing"
    # Two stats per member per attempt, and never more attempts than the bound.
    assert calls["count"] == 2 * packaging._SOURCE_STABILITY_ATTEMPTS
    assert not destination.exists()
    assert list(destination.parent.glob(f".{destination.name}.*.partial")) == []
