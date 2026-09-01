from __future__ import annotations

from catalog.mtconnect_recorder import runtime as recorder_runtime
from catalog.mtconnect_recorder.publication_frontier import RecorderPublicationFrontier
from catalog.mtconnect_recorder.publication_frontier_runtime import (
    install_publication_frontier_runtime,
)
from catalog.mtconnect_recorder.storage import (
    DurableRecorderStore as DirectRecorderStore,
)

_SAMPLE = '''<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7"><Header instanceId="7" firstSequence="1" lastSequence="1" nextSequence="2"/><Streams><DeviceStream name="M"><ComponentStream><Samples><Position dataItemId="x" sequence="1" timestamp="2026-08-31T00:00:00Z">1</Position></Samples></ComponentStream></DeviceStream></Streams></MTConnectStreams>'''


def _batch():
    return recorder_runtime.parse_streams(
        _SAMPLE,
        source_name="machine",
        probe=None,
        max_observations=10,
        max_sequence_span=10,
    )


def test_publication_frontier_writer_is_scoped_to_runtime_store(tmp_path):
    """Runtime composition must not change unrelated direct-store output."""

    # Importing a submodule directly can pre-populate package.runtime and bypass
    # the package __getattr__ hook. Production startup calls the same explicit
    # installer after B01 composition; invoke it here so this regression tests
    # the boundary itself rather than Python package import order.
    install_publication_frontier_runtime(recorder_runtime)
    assert recorder_runtime.DurableRecorderStore is not DirectRecorderStore

    direct = DirectRecorderStore(tmp_path / "direct")
    runtime_store = recorder_runtime.DurableRecorderStore(tmp_path / "runtime")
    batch = _batch()

    direct.store_batch(
        source_name="machine",
        requested_from=1,
        xml_text=_SAMPLE,
        batch=batch,
    )
    runtime_store.store_batch(
        source_name="machine",
        requested_from=1,
        xml_text=_SAMPLE,
        batch=batch,
    )

    assert not (direct.root / "publication_pending").exists()
    pending = RecorderPublicationFrontier(runtime_store).pending(
        source_name="machine",
        instance_id=7,
    )
    assert len(pending) == 1
    assert pending[0].ref.first_sequence == 1
    assert pending[0].ref.next_sequence == 2
