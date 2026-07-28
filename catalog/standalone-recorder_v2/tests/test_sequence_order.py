from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import pytest


RECORDER_PATH = Path(__file__).resolve().parents[1] / "standalone-recorder_v2.py"


OUT_OF_ORDER_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7">
  <Header creationTime="2026-07-28T10:00:00Z" sender="agent" instanceId="77" bufferSize="4096" firstSequence="10" lastSequence="20" nextSequence="21"/>
  <Streams>
    <DeviceStream name="Mazak" uuid="MAZAK-001">
      <ComponentStream component="Rotary" componentId="spindle" name="C">
        <Samples>
          <SpindleSpeed dataItemId="rpm" sequence="20" timestamp="2026-07-28T09:00:20Z">200</SpindleSpeed>
        </Samples>
      </ComponentStream>
      <ComponentStream component="Linear" componentId="x" name="X">
        <Samples>
          <Position dataItemId="xpos" sequence="10" timestamp="2026-07-28T09:00:10Z">10</Position>
        </Samples>
      </ComponentStream>
    </DeviceStream>
  </Streams>
</MTConnectStreams>
"""


PROBE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.3">
  <Header creationTime="2026-07-28T08:00:00Z" sender="agent" instanceId="77" bufferSize="4096"/>
  <Devices>
    <Device id="d1" name="Mazak" uuid="MAZAK-001">
      <Components>
        <Axes id="axes" name="base">
          <Components>
            <Rotary id="spindle" name="C">
              <DataItems>
                <DataItem category="SAMPLE" id="rpm" name="Srpm" type="SPINDLE_SPEED" units="REVOLUTION/MINUTE"/>
              </DataItems>
            </Rotary>
            <Linear id="x" name="X">
              <DataItems>
                <DataItem category="SAMPLE" id="xpos" name="Xabs" type="POSITION" units="MILLIMETER"/>
              </DataItems>
            </Linear>
          </Components>
        </Axes>
      </Components>
    </Device>
  </Devices>
</MTConnectDevices>
"""


def load_recorder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MSH_RECORDER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv(
        "MSH_RECORDER_LOG_FILE",
        str(tmp_path / "data" / "source_state" / "recorder.log"),
    )
    module_name = f"ordered_recorder_{tmp_path.name}"
    spec = importlib.util.spec_from_file_location(module_name, RECORDER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_observations_and_wide_snapshots_follow_global_sequence(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    probe = recorder.parse_probe(PROBE_XML)
    batch = recorder.parse_streams(
        OUT_OF_ORDER_XML,
        source_name="Mazak",
        probe=probe,
        received_at="2026-07-28T10:00:01Z",
    )

    assert [row["sequence"] for row in batch.observations] == [10, 20]

    store = recorder.DurableRecorderStore(tmp_path / "data")
    path, _ = store.store_normalized_batch(source_name="Mazak", batch=batch)
    snapshots = [json.loads(line) for line in path.read_text().splitlines()]

    assert [row["sequence"] for row in snapshots] == [10, 20]
    assert snapshots[0]["Xabs"] == 10
    assert "Srpm" not in snapshots[0]
    assert snapshots[1]["Xabs"] == 10
    assert snapshots[1]["Srpm"] == 200
