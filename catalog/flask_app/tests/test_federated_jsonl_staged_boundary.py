from __future__ import annotations

from pathlib import Path

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.flask_app.tests.test_federated_jsonl_resource_admission import (
    _RecordingAdmission,
    _bridge,
    _published_batch,
    _reference,
)


def _rebind_parent_to_outside(staged: Path, outside: Path) -> Path:
    parent = staged.parent
    detached = parent.with_name(parent.name + "-detached")
    parent.rename(detached)
    outside.mkdir()
    try:
        parent.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        detached.rename(parent)
        pytest.skip(f"directory redirect creation unavailable: {exc}")
    return detached


def test_staged_rebind_cannot_redirect_retry_read(tmp_path: Path) -> None:
    batch, session_id = _published_batch(tmp_path)
    content = batch["content"]
    assert isinstance(content, dict)
    admission = _RecordingAdmission(fail_call=4)
    consumer = _bridge(tmp_path, "staged-read-boundary", admission=admission)

    with pytest.raises(FederationOperationError):
        consumer._ingest_remote(
            _reference(batch, session_id=session_id),
            content,
            local_node_id="node-consumer",
        )

    with consumer._connect() as connection:
        row = connection.execute(
            "SELECT chunk_path FROM seen_batches WHERE chunk_path IS NOT NULL LIMIT 1"
        ).fetchone()
    assert row is not None
    staged = Path(str(row["chunk_path"]))
    outside = tmp_path / "outside-staged-read"
    _rebind_parent_to_outside(staged, outside)
    (outside / staged.name).write_bytes(b"outside bytes must never be read")

    admission.fail_call = None
    with pytest.raises(FederationOperationError) as captured:
        consumer._retry_staged_materializations(session_id=session_id)

    assert captured.value.code == "federated-jsonl-filesystem-boundary"
    assert (outside / staged.name).read_bytes() == b"outside bytes must never be read"
    assert list(consumer.mirror_root.rglob("*.jsonl")) == []
    assert admission.active == 0


def test_staged_rebind_cannot_redirect_cleanup_unlink(tmp_path: Path) -> None:
    admission = _RecordingAdmission()
    consumer = _bridge(tmp_path, "staged-unlink-boundary", admission=admission)
    staged = consumer.cache_root / "remote" / "digest" / "00000000.chunk"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"managed")
    outside = tmp_path / "outside-staged-unlink"
    detached = _rebind_parent_to_outside(staged, outside)
    outside_target = outside / staged.name
    outside_target.write_bytes(b"outside must survive")

    with pytest.raises(FederationOperationError) as captured:
        consumer._unlink_staged(staged)

    assert captured.value.code == "federated-jsonl-filesystem-boundary"
    assert outside_target.read_bytes() == b"outside must survive"
    assert (detached / staged.name).read_bytes() == b"managed"
