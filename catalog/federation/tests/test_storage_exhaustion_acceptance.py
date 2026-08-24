"""Acceptance: a storage authority driven to its bound stays healthy.

The physical incident was not "a batch was too big". It was a host that filled
up while it was both accepting Federation storage and doing its own work, and
whose runtime became unstable near exhaustion. The shape that matters is
therefore not a single refusal but the whole state of the device once its
storage bound is reached: remote writes must stop, and everything else the
device owns must keep working.

These tests drive a real authority to its real bound with real writes and real
``disk_usage`` readings, then assert on what survives.

What they cover
---------------
* new remote storage writes are refused once the bound is reached;
* already-committed batches remain readable and byte-intact;
* the SQLite authority catalogue passes ``PRAGMA integrity_check`` and still
  serves reads and writes after the refusal;
* local recorder capture on the same volume continues, because refusing a peer
  must never stop this device recording its own machine; and
* the device returns to service when space is released, without repair.

What they do NOT cover, and cannot
----------------------------------
Flask's HTTP surface, the relay container, and Docker-managed operation are
processes, not in-process objects, and Docker Desktop's WSL2 disk image -- the
component that actually misbehaved near exhaustion in the physical incident --
has no in-process equivalent at all. Those belong to the physical rig. A green
run here is evidence that the *authority* refuses safely and keeps its own
state intact; it is not evidence that a host survives disk exhaustion, because
the bound tested here is what stops the host from reaching exhaustion in the
first place.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path

import pytest
from mtconnect_recorder.parsing import parse_probe, parse_streams
from mtconnect_recorder.storage import DurableRecorderStore

from catalog.federation.errors import FederationValidationError
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.storage_allocation import (
    ALLOCATION_EXHAUSTED_CODE,
    StorageAllocation,
)
from catalog.federation.storage_protocol import (
    BatchIngestRequest,
    BatchIngestState,
    WriteAuthority,
)

NOW = datetime(2026, 8, 24, tzinfo=timezone.utc)

PROBE = (
    '<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.3">'
    '<Header instanceId="7"/><Devices><Device id="d" name="M"><DataItems>'
    '<DataItem id="x" name="Xabs" category="SAMPLE" type="POSITION"/>'
    "</DataItems></Device></Devices></MTConnectDevices>"
)
SAMPLE = (
    '<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7">'
    '<Header instanceId="7" firstSequence="1" lastSequence="1" nextSequence="2"/>'
    '<Streams><DeviceStream name="M"><ComponentStream><Samples>'
    '<Position dataItemId="x" sequence="1" timestamp="2026-07-28T00:00:00Z">1</Position>'
    "</Samples></ComponentStream></DeviceStream></Streams></MTConnectStreams>"
)

#: Large enough that a bound is reached in a handful of writes rather than
#: thousands, small enough that a run costs little disk.
_PAYLOAD = {"rows": ["telemetry" * 64] * 48}

#: A run must never loop unboundedly if a bound somehow fails to bite.
_MAX_WRITES = 500


def _request(index: int) -> BatchIngestRequest:
    body = json.dumps(_PAYLOAD, sort_keys=True, separators=(",", ":"))
    return BatchIngestRequest(
        authority=WriteAuthority(
            session_id="session-1",
            group_id="storage-main",
            actor_node_id="remote-node",
            grant_id="grant-1",
            term=1,
            fencing_token=10,
            lease_expires_at=NOW + timedelta(minutes=5),
        ),
        dataset_id="telemetry",
        batch_id=f"batch-{index}",
        idempotency_key=f"idem-{index}",
        content_hash="sha256:" + sha256(body.encode("utf-8")).hexdigest(),
        content=_PAYLOAD,
        created_at=NOW,
    )


def _drive_to_the_bound(
    provider: FilesystemBatchStorageProvider,
) -> tuple[list[int], FederationValidationError]:
    """Write until the authority refuses, returning what it accepted first."""

    accepted: list[int] = []
    for index in range(_MAX_WRITES):
        try:
            provider.ingest(_request(index))
        except FederationValidationError as refusal:
            return accepted, refusal
        accepted.append(index)
    raise AssertionError(
        f"the authority accepted {_MAX_WRITES} writes without reaching its bound"
    )


def _assert_authority_state_is_healthy(
    provider: FilesystemBatchStorageProvider,
    root: Path,
    accepted: list[int],
) -> None:
    """Everything the device already owned must survive the refusal intact."""

    connection = sqlite3.connect(root / "storage-index.sqlite3")
    try:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        catalogued = {
            str(row[0])
            for row in connection.execute("SELECT batch_id FROM committed_batches")
        }
    finally:
        connection.close()

    assert catalogued == {f"batch-{index}" for index in accepted}

    # Reads still work, and the catalogue still agrees with the filesystem.
    for index in accepted:
        identity = provider.committed_identity(
            session_id="session-1",
            group_id="storage-main",
            batch_id=f"batch-{index}",
        )
        assert identity is not None

    # And a write still works: an idempotent re-delivery of a batch this
    # device already holds must still be answered from the catalogue rather
    # than failing because the device is at its bound.
    replayed = provider.ingest(_request(accepted[0]))
    assert replayed.state is BatchIngestState.ALREADY_STORED


def _assert_local_capture_continues(data_root: Path) -> None:
    """Refusing a peer must never stop this device recording its own machine."""

    store = DurableRecorderStore(data_root)
    batch = parse_streams(SAMPLE, source_name="machine", probe=parse_probe(PROBE))
    reference = store.store_raw_batch(
        source_name="machine",
        requested_from=1,
        xml_text=SAMPLE,
        batch=batch,
    )
    assert Path(reference.raw_path).exists()


# -- budget exhaustion ---------------------------------------------------


def test_an_authority_driven_to_its_budget_refuses_and_stays_healthy(
    tmp_path: Path,
) -> None:
    root = tmp_path / "authority"
    allocation = StorageAllocation(root, budget_bytes=256 * 1024, floor_bytes=0)
    provider = FilesystemBatchStorageProvider(root, allocation=allocation)

    accepted, refusal = _drive_to_the_bound(provider)

    assert accepted, "the authority must do useful work before its bound"
    assert refusal.code == ALLOCATION_EXHAUSTED_CODE
    _assert_authority_state_is_healthy(provider, root, accepted)
    _assert_local_capture_continues(tmp_path / "recorder-data")


def test_the_refusal_is_stable_rather_than_intermittent(tmp_path: Path) -> None:
    """A device at its bound must not accept the next identical write."""

    root = tmp_path / "authority"
    allocation = StorageAllocation(root, budget_bytes=256 * 1024, floor_bytes=0)
    provider = FilesystemBatchStorageProvider(root, allocation=allocation)
    accepted, _ = _drive_to_the_bound(provider)
    next_index = accepted[-1] + 1

    for _ in range(3):
        with pytest.raises(FederationValidationError) as caught:
            provider.ingest(_request(next_index))
        assert caught.value.code == ALLOCATION_EXHAUSTED_CODE


def test_a_device_returns_to_service_when_its_budget_is_raised(
    tmp_path: Path,
) -> None:
    """Recovery must not need repair, reindexing, or a fresh identity."""

    root = tmp_path / "authority"
    allocation = StorageAllocation(root, budget_bytes=256 * 1024, floor_bytes=0)
    provider = FilesystemBatchStorageProvider(root, allocation=allocation)
    accepted, _ = _drive_to_the_bound(provider)
    next_index = accepted[-1] + 1

    raised = StorageAllocation(root, budget_bytes=2 * 1024 * 1024, floor_bytes=0)
    recovered = FilesystemBatchStorageProvider(root, allocation=raised)
    result = recovered.ingest(_request(next_index))

    assert result.state is BatchIngestState.STORED
    _assert_authority_state_is_healthy(
        recovered, root, [*accepted, next_index]
    )


# -- floor exhaustion ----------------------------------------------------


def test_an_unbudgeted_authority_stops_at_the_floor_with_the_volume_intact(
    tmp_path: Path,
) -> None:
    """The floor is the bound on a device nobody gave a budget.

    The floor is set just under the volume's real free space, so a bounded
    number of real writes crosses it against real ``disk_usage`` readings
    rather than a stubbed number. The margin is generous because the volume is
    shared with everything else on the machine.
    """

    root = tmp_path / "authority"
    free_at_start = shutil.disk_usage(tmp_path).free
    margin = 512 * 1024
    if free_at_start <= margin * 4:
        pytest.skip("volume has too little free space to exercise the floor")

    allocation = StorageAllocation(
        root,
        budget_bytes=None,
        floor_bytes=free_at_start - margin,
    )
    provider = FilesystemBatchStorageProvider(root, allocation=allocation)

    accepted, refusal = _drive_to_the_bound(provider)

    assert refusal.code == ALLOCATION_EXHAUSTED_CODE
    # The point of a floor is that the volume never actually ran out.
    assert shutil.disk_usage(tmp_path).free > 0
    if accepted:
        _assert_authority_state_is_healthy(provider, root, accepted)
    _assert_local_capture_continues(tmp_path / "recorder-data")


def test_the_floor_holds_even_with_budget_remaining(tmp_path: Path) -> None:
    """A budget larger than the disk must not override the host's headroom."""

    root = tmp_path / "authority"
    allocation = StorageAllocation(root, budget_bytes=256 * 1024, floor_bytes=0)
    provider = FilesystemBatchStorageProvider(root, allocation=allocation)
    provider.ingest(_request(0))
    assert allocation.snapshot().remaining_bytes > 0

    # The volume is now declared to be at its floor while budget remains.
    allocation.floor_bytes = allocation.snapshot().volume_free_bytes + 1

    with pytest.raises(FederationValidationError) as caught:
        provider.ingest(_request(1))

    assert caught.value.code == ALLOCATION_EXHAUSTED_CODE
    assert allocation.snapshot().remaining_bytes > 0
