"""Contracts for the preallocated storage allocation bound.

A device contributing the storage capability must never be able to consume its
host's disk. These tests pin the two properties that make that true: the budget
is *held* rather than merely counted, and a batch that does not fit is refused
before anything is written.

The floor is set to zero in most tests. The volume a test runs on is shared with
everything else on the machine, so a realistic floor would make the outcome
depend on the runner's free space rather than on the code. The floor has its own
tests, which set it relative to the observed free space instead.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.local_storage import (
    FilesystemBatchStorageProvider,
    LocalStorageService,
)
from catalog.federation.storage_allocation import (
    ALLOCATION_EXHAUSTED_CODE,
    MAXIMUM_FLOOR_BYTES,
    StorageAllocation,
    default_floor_bytes,
    describe,
    minimum_headroom_bytes,
)
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestState,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    WriteAuthority,
)

NOW = datetime(2026, 8, 24, tzinfo=timezone.utc)


def _request(content: object, *, batch_id: str = "batch-1", key: str = "idem-1"):
    body = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return BatchIngestRequest(
        authority=WriteAuthority(
            session_id="session-1",
            group_id="storage-main",
            actor_node_id="node-a",
            grant_id="grant-1",
            term=1,
            fencing_token=10,
            lease_expires_at=NOW + timedelta(minutes=5),
        ),
        dataset_id="telemetry",
        batch_id=batch_id,
        idempotency_key=key,
        content_hash="sha256:" + sha256(body.encode("utf-8")).hexdigest(),
        content=content,
        created_at=NOW,
    )


def _provider(root: Path, *, budget: int | None, floor: int = 0):
    return FilesystemBatchStorageProvider(
        root,
        allocation=StorageAllocation(root, budget_bytes=budget, floor_bytes=floor),
    )


def _stored_files(root: Path) -> list[Path]:
    return [item for item in (root / "batches").rglob("*") if item.is_file()]


def _catalogue_rows(root: Path) -> list[str]:
    connection = sqlite3.connect(root / "storage-index.sqlite3")
    try:
        return [
            str(row[0])
            for row in connection.execute("SELECT batch_id FROM committed_batches")
        ]
    finally:
        connection.close()


# -- the budget is held, not merely counted ------------------------------


def test_budget_is_preallocated_with_real_blocks(tmp_path: Path) -> None:
    """A sparse reservation would report the space without holding it."""

    root = tmp_path / "storage"
    StorageAllocation(root, budget_bytes=1024 * 1024, floor_bytes=0)

    status = (root / "allocation.reserve").stat()
    assert status.st_size == 1024 * 1024
    if hasattr(status, "st_blocks"):
        assert status.st_blocks * 512 >= 1024 * 1024


def test_reservation_shrinks_by_exactly_what_a_batch_consumes(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    allocation = StorageAllocation(root, budget_bytes=64 * 1024, floor_bytes=0)
    provider = FilesystemBatchStorageProvider(root, allocation=allocation)

    before = allocation.snapshot().remaining_bytes
    provider.ingest(_request({"rows": ["a"] * 4}))
    after = allocation.snapshot()

    stored = _stored_files(root)[0].stat().st_size
    assert before - after.remaining_bytes == stored
    assert after.used_bytes == stored


def test_used_survives_a_restart(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    provider = _provider(root, budget=64 * 1024)
    provider.ingest(_request({"rows": ["a"] * 4}))
    used = StorageAllocation(root, budget_bytes=64 * 1024, floor_bytes=0).snapshot().used_bytes

    restarted = StorageAllocation(root, budget_bytes=64 * 1024, floor_bytes=0)

    assert restarted.snapshot().used_bytes == used


def test_raising_the_budget_keeps_bytes_already_consumed(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    provider = _provider(root, budget=64 * 1024)
    provider.ingest(_request({"rows": ["a"] * 4}))
    used = StorageAllocation(root, budget_bytes=64 * 1024, floor_bytes=0).snapshot().used_bytes

    grown = StorageAllocation(root, budget_bytes=128 * 1024, floor_bytes=0).snapshot()

    assert grown.budget_bytes == 128 * 1024
    assert grown.used_bytes == used
    assert grown.remaining_bytes == 128 * 1024 - used


def test_lowering_the_budget_below_what_is_used_offers_nothing_more(
    tmp_path: Path,
) -> None:
    root = tmp_path / "storage"
    provider = _provider(root, budget=64 * 1024)
    provider.ingest(_request({"rows": ["a"] * 200}))
    used = StorageAllocation(root, budget_bytes=64 * 1024, floor_bytes=0).snapshot().used_bytes
    assert used > 0

    shrunk = StorageAllocation(root, budget_bytes=1, floor_bytes=0).snapshot()

    assert shrunk.remaining_bytes == 0


# -- refusal happens before anything is written --------------------------


def test_batch_over_the_budget_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    provider = _provider(root, budget=2048)

    with pytest.raises(FederationValidationError) as caught:
        provider.ingest(_request({"rows": ["y" * 200] * 60}))

    assert caught.value.code == ALLOCATION_EXHAUSTED_CODE


def test_a_refused_batch_leaves_no_file_and_no_catalogue_row(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    provider = _provider(root, budget=2048)
    provider.ingest(_request({"rows": ["a"]}, batch_id="kept", key="kept"))
    kept = _stored_files(root)

    with pytest.raises(FederationValidationError):
        provider.ingest(_request({"rows": ["y" * 200] * 60}, batch_id="huge", key="huge"))

    assert _stored_files(root) == kept
    assert _catalogue_rows(root) == ["kept"]


def test_a_refusal_consumes_no_budget(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    allocation = StorageAllocation(root, budget_bytes=2048, floor_bytes=0)
    provider = FilesystemBatchStorageProvider(root, allocation=allocation)
    before = allocation.snapshot().remaining_bytes

    with pytest.raises(FederationValidationError):
        provider.ingest(_request({"rows": ["y" * 200] * 60}))

    assert allocation.snapshot().remaining_bytes == before


def test_refusal_travels_as_a_protocol_error(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    provider = _provider(root, budget=2048)
    request = _request({"rows": ["y" * 200] * 60})
    envelope = StorageRequestEnvelope(
        request_id="request-1",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        operation=StorageOperation.BATCH_INGEST,
        session_id="session-1",
        actor_node_id="node-a",
        authorization_context={"scopes": ["storage:write"]},
        payload=request.to_dict(),
    )

    response = LocalStorageService(provider).dispatch(envelope)

    assert not response.ok
    assert response.error.code is StorageErrorCode.ALLOCATION_EXHAUSTED


def test_redelivering_a_stored_batch_is_not_charged_twice(tmp_path: Path) -> None:
    """Idempotent re-delivery returns before the payload is ever claimed."""

    root = tmp_path / "storage"
    allocation = StorageAllocation(root, budget_bytes=64 * 1024, floor_bytes=0)
    provider = FilesystemBatchStorageProvider(root, allocation=allocation)
    provider.ingest(_request({"rows": ["a"] * 4}))
    after_first = allocation.snapshot().remaining_bytes

    result = provider.ingest(_request({"rows": ["a"] * 4}))

    assert result.state is BatchIngestState.ALREADY_STORED
    assert allocation.snapshot().remaining_bytes == after_first


def test_a_failed_write_returns_its_claim(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "storage"
    allocation = StorageAllocation(root, budget_bytes=64 * 1024, floor_bytes=0)
    provider = FilesystemBatchStorageProvider(root, allocation=allocation)
    before = allocation.snapshot().remaining_bytes

    def _fail(*_args, **_kwargs):
        raise OSError("simulated write failure")

    monkeypatch.setattr(os, "replace", _fail)
    with pytest.raises(OSError):
        provider.ingest(_request({"rows": ["a"] * 4}))

    assert allocation.snapshot().remaining_bytes == before


# -- the floor protects the host regardless of the budget ----------------


def test_a_budget_that_would_breach_the_floor_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    free = describe(tmp_path, floor_bytes=0).volume_free_bytes

    with pytest.raises(FederationValidationError) as caught:
        StorageAllocation(root, budget_bytes=free, floor_bytes=free)

    assert caught.value.code == ALLOCATION_EXHAUSTED_CODE


def test_an_unbudgeted_device_still_refuses_below_the_floor(tmp_path: Path) -> None:
    """Floor-only mode is what protects a device nobody gave a budget."""

    root = tmp_path / "storage"
    allocation = StorageAllocation(root, budget_bytes=None, floor_bytes=0)
    free = allocation.snapshot().volume_free_bytes
    allocation.floor_bytes = free  # nothing more may be consumed

    with pytest.raises(FederationValidationError) as caught:
        allocation.claim(1)

    assert caught.value.code == ALLOCATION_EXHAUSTED_CODE


def test_an_unbudgeted_device_holds_no_reservation(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    StorageAllocation(root, budget_bytes=4096, floor_bytes=0)
    assert (root / "allocation.reserve").exists()

    StorageAllocation(root, budget_bytes=None, floor_bytes=0)

    assert not (root / "allocation.reserve").exists()


# -- what a device advertises --------------------------------------------


def test_describe_reads_state_without_reserving_anything(tmp_path: Path) -> None:
    root = tmp_path / "never-assigned"

    snapshot = describe(root, floor_bytes=1024)

    assert snapshot.budget_bytes is None
    assert not root.exists()
    assert snapshot.as_capacity_envelope()["allocation_mode"] == "floor-only"


def test_describe_reports_the_live_budget_and_use(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    provider = _provider(root, budget=64 * 1024)
    provider.ingest(_request({"rows": ["a"] * 4}))

    envelope = describe(root, floor_bytes=0).as_capacity_envelope()

    assert envelope["allocation_mode"] == "preallocated"
    assert envelope["budget_bytes"] == 64 * 1024
    assert envelope["used_bytes"] > 0
    assert envelope["remaining_bytes"] == 64 * 1024 - envelope["used_bytes"]


def test_capacity_envelope_exposes_no_paths(tmp_path: Path) -> None:
    """A capacity advertisement must not disclose host layout."""

    root = tmp_path / "storage"
    _provider(root, budget=64 * 1024)

    envelope = describe(root, floor_bytes=0).as_capacity_envelope()

    rendered = json.dumps(envelope)
    assert str(tmp_path) not in rendered
    assert all(isinstance(value, (int, str, type(None))) for value in envelope.values())


# -- an unbounded provider keeps working ---------------------------------


def test_a_provider_without_an_allocation_is_unchanged(tmp_path: Path) -> None:
    root = tmp_path / "storage"
    provider = FilesystemBatchStorageProvider(root)

    result = provider.ingest(_request({"rows": ["y" * 200] * 60}))

    assert result.state is BatchIngestState.STORED


# -- the headroom policy is derived, not picked --------------------------


def test_the_minimum_covers_a_whole_update_cycle() -> None:
    """2 GiB could not cover one update, which is why it is not the floor.

    An update rebuilds three images and none of them shares the dependency
    layer, because the build commit is written into ENV above the install.
    The measured dependency tree alone is about 726 MiB installed.
    """

    measured_dependency_tree = 726 * 1024**2
    one_update = 3 * measured_dependency_tree

    assert minimum_headroom_bytes(windows=False) > one_update


def test_windows_reserves_more_than_posix() -> None:
    """Docker Desktop's disk image grows on demand and does not shrink."""

    assert minimum_headroom_bytes(windows=True) > minimum_headroom_bytes(
        windows=False
    )


def test_the_derived_floor_is_never_below_the_platform_minimum(
    tmp_path: Path,
) -> None:
    assert default_floor_bytes(tmp_path) >= minimum_headroom_bytes()


class _Usage:
    """Stand-in for ``shutil.disk_usage`` on a volume no runner actually has."""

    def __init__(self, total: int) -> None:
        self.total = total
        self.used = 0
        self.free = total


def test_the_derived_floor_is_capped_for_a_large_volume(monkeypatch) -> None:
    """A very large disk must not reserve an absurd amount it never needed."""

    petabyte = 1024**5
    monkeypatch.setattr(
        "catalog.federation.storage_allocation.shutil.disk_usage",
        lambda _path: _Usage(petabyte),
    )

    assert default_floor_bytes("/") == MAXIMUM_FLOOR_BYTES


def test_an_unset_floor_is_derived_rather_than_zero(tmp_path: Path) -> None:
    """Omitting the floor must never mean "no floor"."""

    allocation = StorageAllocation(tmp_path / "storage", budget_bytes=None)

    assert allocation.floor_bytes >= minimum_headroom_bytes()


def test_an_explicit_zero_floor_is_honoured(tmp_path: Path) -> None:
    """An operator who measured their own host outranks the policy."""

    allocation = StorageAllocation(
        tmp_path / "storage", budget_bytes=None, floor_bytes=0
    )

    assert allocation.floor_bytes == 0

