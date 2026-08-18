"""The recorder may start before its MTConnect source exists.

Physical acceptance starts the recorder first and powers up the machine agent
afterwards. The recorder must therefore keep retrying a source that is not
answering yet, must not busy-poll it, and must start recording on its own as
soon as the agent appears — with no manual restart.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from catalog.mtconnect_recorder import runtime as rt
from catalog.mtconnect_recorder.storage import DurableRecorderStore

from .conftest import Observation, stamp, streams_document

SOURCE = "mazak-cell"
BASE_URL = "http://machine-agent.invalid:5000"
INSTANCE_ID = 1_755_000_001

PROBE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.3">
  <Header creationTime="2026-08-07T09:00:33Z" sender="agent-alpha"
          instanceId="1755000001" version="1.3.0" assetBufferSize="1024"
          assetCount="0" bufferSize="131072"/>
  <Devices>
    <Device id="d1" name="MachineAlpha" uuid="MACHINE-ALPHA-0001">
      <Description manufacturer="Example"/>
      <DataItems>
        <DataItem category="EVENT" id="execution" type="EXECUTION"/>
      </DataItems>
    </Device>
  </Devices>
</MTConnectDevices>
"""


def _streams(*, first: int, last: int) -> str:
    return streams_document(
        instance_id=INSTANCE_ID,
        observations=[
            Observation(
                sequence=sequence,
                component="Controller",
                element="Execution",
                data_item_id="execution",
                value="ACTIVE",
                category="Events",
                timestamp=stamp(sequence),
            )
            for sequence in range(first, last + 1)
        ],
        first_sequence=first,
        last_sequence=last,
    )


class _Agent:
    """An MTConnect agent that is unreachable until it is powered on."""

    def __init__(self) -> None:
        self.online = False
        self.current_fetches = 0

    def client(self, base_url: str, *, timeout: float):
        del timeout
        agent = self

        class _Client:
            def __init__(self) -> None:
                self.base_url = base_url

            def fetch_current(self) -> str:
                agent.current_fetches += 1
                if not agent.online:
                    raise ConnectionError("agent is not answering yet")
                return _streams(first=1, last=3)

            def fetch_probe(self) -> str:
                if not agent.online:
                    raise ConnectionError("agent is not answering yet")
                return PROBE_XML

            def fetch_sample(self, *, from_sequence: int, count: int) -> str:
                del count
                if not agent.online:
                    raise ConnectionError("agent is not answering yet")
                return _streams(first=from_sequence, last=3)

        return _Client()


@pytest.fixture
def recorder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    agent = _Agent()
    monkeypatch.setattr(rt, "MtconnectClient", agent.client)

    clock = {"now": 1_000.0}
    monkeypatch.setattr(rt.time, "monotonic", lambda: clock["now"])

    service = rt.RecorderRuntime()
    service.store = DurableRecorderStore(tmp_path / "data")
    service.enabled = True
    service.configuration_ready = True
    service.sources = {SOURCE: BASE_URL}
    return service, agent, clock


def test_recorder_started_before_its_source_retries_with_bounded_backoff(
    recorder,
) -> None:
    service, agent, clock = recorder

    service.run_fetch_cycle()

    assert agent.current_fetches == 1
    assert service.state == "error"
    status = service.source_status[SOURCE]
    assert "ConnectionError" in status["last_error"]
    assert status["next_retry_seconds"] >= rt.BACKOFF_INITIAL
    assert service.observations_written == 0

    # The source is not due again yet, so the recorder must not busy-poll it.
    service.run_fetch_cycle()
    assert agent.current_fetches == 1

    # Backoff grows while the agent stays offline, and stays bounded.
    for _ in range(12):
        clock["now"] += rt.BACKOFF_MAX
        service.run_fetch_cycle()
    assert service.source_status[SOURCE]["next_retry_seconds"] <= rt.BACKOFF_MAX


def test_recorder_starts_recording_when_the_source_appears(recorder) -> None:
    service, agent, clock = recorder

    service.run_fetch_cycle()
    assert service.state == "error"
    offline_attempts = agent.current_fetches

    # The machine is powered on. No operator restarts the recorder.
    agent.online = True
    clock["now"] += rt.BACKOFF_MAX
    service.run_fetch_cycle()

    assert agent.current_fetches > offline_attempts
    assert service.state == "recording"
    assert service.last_error == ""
    assert service.observations_written > 0
    assert service.raw_batches_written > 0
    assert service.last_commit_at

    status = service.source_status[SOURCE]
    assert status["last_error"] == ""
    assert status["last_success_at"]
    assert status["agent_instance_id"] == INSTANCE_ID
    # A recovered source is polled at the normal cadence again.
    assert service.backoff[SOURCE] == rt.BACKOFF_INITIAL
    assert service.next_attempt_at[SOURCE] == 0.0


def test_first_real_data_is_durably_written_with_its_raw_manifest(recorder) -> None:
    service, agent, clock = recorder

    agent.online = True
    clock["now"] += rt.BACKOFF_MAX
    service.run_fetch_cycle()

    archived = service.store.iter_raw_batches(
        source_name=SOURCE,
        instance_id=INSTANCE_ID,
    )
    assert archived
    assert service.checkpoints[SOURCE].agent_instance_id == INSTANCE_ID
    assert service.checkpoints[SOURCE].next_sequence > 1
