from __future__ import annotations

import gzip
import importlib.util
import json
import sys
from pathlib import Path

import pytest

RECORDER_PATH = Path(__file__).resolve().parents[1] / "standalone-recorder_v2.py"


PROBE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.3">
  <Header creationTime="2026-07-28T07:34:05Z" sender="agent" instanceId="77" bufferSize="4096"/>
  <Devices>
    <Device id="d1" name="Mazak" uuid="MAZAK-001">
      <Description serialNumber="SERIAL-1">Mazak test machine</Description>
      <Components>
        <Axes id="axes" name="base">
          <Components>
            <Linear id="x" name="X">
              <DataItems>
                <DataItem category="SAMPLE" coordinateSystem="MACHINE" id="xp" name="Xabs" nativeUnits="MILLIMETER" subType="ACTUAL" type="POSITION" units="MILLIMETER"/>
                <DataItem category="CONDITION" id="xt" name="Xtravel" type="POSITION"/>
              </DataItems>
            </Linear>
          </Components>
        </Axes>
      </Components>
    </Device>
  </Devices>
</MTConnectDevices>
"""


SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7">
  <Header creationTime="2026-07-28T07:35:00Z" sender="agent" instanceId="77" bufferSize="4096" firstSequence="10" lastSequence="12" nextSequence="13"/>
  <Streams>
    <DeviceStream name="Mazak" uuid="MAZAK-001">
      <ComponentStream component="Linear" componentId="x" name="X">
        <Samples>
          <Position dataItemId="xp" sequence="10" timestamp="2026-07-28T07:34:58.000Z">1.25</Position>
          <Position dataItemId="xp" sequence="11" timestamp="2026-07-28T07:34:59.000Z">2.50</Position>
        </Samples>
        <Condition>
          <Fault dataItemId="xt" sequence="12" timestamp="2026-07-28T07:35:00.000Z" nativeCode="X42" qualifier="HIGH">Travel exceeded</Fault>
        </Condition>
      </ComponentStream>
    </DeviceStream>
  </Streams>
</MTConnectStreams>
"""


def load_recorder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MSH_RECORDER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv(
        "MSH_RECORDER_LOG_FILE",
        str(tmp_path / "data" / "source_state" / "recorder.log"),
    )
    module_name = f"lossless_recorder_{tmp_path.name}"
    spec = importlib.util.spec_from_file_location(module_name, RECORDER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_normalize_agent_url_accepts_endpoint_urls(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    assert recorder.normalize_agent_base_url("http://10.0.0.5:5000/current") == "http://10.0.0.5:5000"
    assert recorder.normalize_agent_base_url("http://host/mtconnect/sample?from=10") == "http://host/mtconnect"
    assert recorder.normalize_agent_base_url("https://host/probe") == "https://host"


def test_probe_metadata_is_retained(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    probe = recorder.parse_probe(PROBE_XML)
    item = probe.data_items["xp"]
    assert item["name"] == "Xabs"
    assert item["type"] == "POSITION"
    assert item["sub_type"] == "ACTUAL"
    assert item["units"] == "MILLIMETER"
    assert item["coordinate_system"] == "MACHINE"
    assert item["serial_number"] == "SERIAL-1"
    assert [part["type"] for part in item["component_path"]] == ["Axes", "Linear"]


def test_sample_parsing_preserves_every_observation_and_condition_attributes(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    probe = recorder.parse_probe(PROBE_XML)
    batch = recorder.parse_streams(
        SAMPLE_XML,
        source_name="Mazak",
        probe=probe,
        received_at="2026-07-28T07:35:01.000Z",
    )

    assert batch.header.instance_id == 77
    assert batch.first_observation_sequence == 10
    assert batch.last_observation_sequence == 12
    assert [row["sequence"] for row in batch.observations] == [10, 11, 12]
    assert batch.observations[0]["value"] == 1.25
    assert batch.observations[0]["machine_id"] == "MAZAK-001"
    assert batch.observations[0]["machine"] == "Mazak SERIAL-1"
    assert batch.observations[0]["machine_name"] == "Mazak SERIAL-1"
    assert batch.observations[0]["data_item_id"] == "xp"
    assert batch.observations[0]["unit"] == "MILLIMETER"

    condition = batch.observations[2]
    assert condition["category"] == "CONDITION"
    assert condition["condition_level"] == "Fault"
    assert condition["native_value"] == "Travel exceeded"
    assert condition["attributes"]["nativeCode"] == "X42"
    assert condition["attributes"]["qualifier"] == "HIGH"


def test_machine_id_falls_back_to_probe_serial_then_device_id(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    probe_without_uuid = PROBE_XML.replace(' uuid="MAZAK-001"', "")
    stream_without_uuid = SAMPLE_XML.replace(' uuid="MAZAK-001"', "")

    serial_probe = recorder.parse_probe(probe_without_uuid)
    serial_batch = recorder.parse_streams(
        stream_without_uuid,
        source_name="generic-mazak-alias",
        probe=serial_probe,
    )
    assert serial_batch.observations[0]["machine_id"] == "SERIAL-1"
    assert serial_batch.observations[0]["machine"] == "Mazak SERIAL-1"

    device_id_probe = recorder.parse_probe(
        probe_without_uuid.replace(' serialNumber="SERIAL-1"', "")
    )
    device_id_batch = recorder.parse_streams(
        stream_without_uuid,
        source_name="generic-mazak-alias",
        probe=device_id_probe,
    )
    assert device_id_batch.observations[0]["machine_id"] == "d1"
    assert device_id_batch.observations[0]["machine"] == "Mazak [d1]"


def test_source_names_cannot_share_a_storage_directory(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)

    with pytest.raises(ValueError, match="same storage directory"):
        recorder.runtime._parse_sources_text(
            "Mazak One=http://10.0.0.1:5000;"
            "MAZAK-ONE=http://10.0.0.2:5000"
        )


def test_json_source_names_cannot_share_a_storage_directory(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    monkeypatch.setenv(
        "MSH_RECORDER_SOURCES_JSON",
        json.dumps(
            {
                "Mazak One": "http://10.0.0.1:5000",
                "mazak-one": "http://10.0.0.2:5000",
            }
        ),
    )

    with pytest.raises(ValueError, match="same storage directory"):
        recorder.runtime._sources_from_environment()


def test_batch_files_are_raw_first_and_normalized_idempotently(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    probe = recorder.parse_probe(PROBE_XML)
    batch = recorder.parse_streams(SAMPLE_XML, source_name="Mazak", probe=probe)
    store = recorder.DurableRecorderStore(tmp_path / "data")

    first = store.store_batch(
        source_name="Mazak",
        requested_from=10,
        xml_text=SAMPLE_XML,
        batch=batch,
    )
    second = store.store_batch(
        source_name="Mazak",
        requested_from=10,
        xml_text=SAMPLE_XML,
        batch=batch,
    )

    assert first.normalized_path == second.normalized_path
    assert first.observation_path == second.observation_path
    assert first.raw_path == second.raw_path
    assert gzip.decompress(first.raw_path.read_bytes()).decode("utf-8") == SAMPLE_XML

    observations = [json.loads(line) for line in first.observation_path.read_text().splitlines()]
    assert len(observations) == 3
    assert [row["sequence"] for row in observations] == [10, 11, 12]

    snapshots = [json.loads(line) for line in first.normalized_path.read_text().splitlines()]
    assert len(snapshots) == 3
    assert [row["sequence"] for row in snapshots] == [10, 11, 12]
    assert snapshots[-1]["Xabs"] == 2.5
    assert snapshots[-1]["Xtravel"] == "Fault"


def test_sequence_plan_records_agent_buffer_overflow(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    checkpoint = recorder.SourceCheckpoint(
        source_name="Mazak",
        base_url="http://agent:5000",
        machine_id="MAZAK-001",
        agent_instance_id=77,
        next_sequence=5,
        probe_sha256="abc",
    )
    header = recorder.StreamHeader(
        instance_id=77,
        first_sequence=10,
        last_sequence=100,
        next_sequence=101,
    )
    plan = recorder.plan_sequence(checkpoint, header)
    assert plan.requested_from == 10
    assert plan.gap_from == 5
    assert plan.gap_to == 9
    assert plan.reason == "agent_buffer_overflow"


def test_checkpoint_only_contains_committed_next_sequence(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(recorder.runtime, "STATE_FILE", state_file)
    runtime = recorder.RecorderRuntime()
    runtime.checkpoints["Mazak"] = recorder.SourceCheckpoint(
        source_name="Mazak",
        base_url="http://agent:5000",
        machine_id="MAZAK-001",
        agent_instance_id=77,
        next_sequence=13,
        probe_sha256="abc",
        last_raw_file="raw.xml.gz",
        last_observation_file="observations.ndjson",
        last_normalized_file="batch.jsonl",
        latest_values={"MAZAK-001": {"Xabs": 2.5}},
    )
    runtime.save_state()
    runtime.executor.shutdown(wait=True, cancel_futures=True)

    payload = json.loads(state_file.read_text())
    assert payload["schema"] == "msh.mtconnect_recorder.checkpoints.v3"
    assert payload["sources"]["Mazak"]["next_sequence"] == 13
    assert payload["sources"]["Mazak"]["last_raw_file"] == "raw.xml.gz"
    assert payload["sources"]["Mazak"]["latest_values"]["MAZAK-001"]["Xabs"] == 2.5


def test_checkpoint_alias_moves_to_discovered_machine_identity(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    runtime = recorder.RecorderRuntime()
    runtime.checkpoints["Mazak"] = recorder.SourceCheckpoint(
        source_name="Mazak",
        base_url="http://192.168.200.249:5000/current",
        machine_id="MAZAK-001",
        agent_instance_id=77,
        next_sequence=13,
        probe_sha256="abc",
    )

    changed = runtime.reconcile_checkpoint_aliases(
        {"MAZAK-001": "http://192.168.200.249:5000"}
    )
    runtime.executor.shutdown(wait=True, cancel_futures=True)

    assert changed is True
    assert "Mazak" not in runtime.checkpoints
    assert runtime.checkpoints["MAZAK-001"].next_sequence == 13
    assert runtime.checkpoints["MAZAK-001"].source_name == "MAZAK-001"
    assert runtime.checkpoints["MAZAK-001"].storage_aliases == ["Mazak"]


def test_alias_migration_recovers_old_namespace_raw_batch_after_restart(
    tmp_path,
    monkeypatch,
):
    recorder = load_recorder(tmp_path, monkeypatch)
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(recorder.runtime, "STATE_FILE", state_file)
    probe = recorder.parse_probe(PROBE_XML)
    batch = recorder.parse_streams(SAMPLE_XML, source_name="Mazak", probe=probe)

    first_runtime = recorder.RecorderRuntime()
    first_runtime.store.store_probe(
        source_name="Mazak",
        instance_id=77,
        xml_text=PROBE_XML,
        probe=probe,
    )
    old_raw = first_runtime.store.store_raw_batch(
        source_name="Mazak",
        requested_from=10,
        xml_text=SAMPLE_XML,
        batch=batch,
    )
    first_runtime.checkpoints["Mazak"] = recorder.SourceCheckpoint(
        source_name="Mazak",
        base_url="http://192.168.200.249:5000",
        machine_id="MAZAK-001",
        agent_instance_id=77,
        next_sequence=10,
        probe_sha256=probe.sha256,
    )
    assert first_runtime.reconcile_checkpoint_aliases(
        {"MAZAK-001": "http://192.168.200.249:5000"}
    )
    first_runtime.save_state()
    first_runtime.executor.shutdown(wait=True, cancel_futures=True)

    class CurrentOnlyClient:
        def __init__(self, base_url, *, timeout):
            self.base_url = base_url
            self.timeout = timeout

        def fetch_current(self):
            return SAMPLE_XML

        def fetch_probe(self):
            pytest.fail("The archived probe under the old alias should be reused.")

        def fetch_sample(self, *, from_sequence, count):
            pytest.fail("The archived raw batch should satisfy recovery.")

    monkeypatch.setattr(recorder.runtime, "MtconnectClient", CurrentOnlyClient)
    restarted_runtime = recorder.RecorderRuntime()
    restarted_runtime.load_state()

    _, success, error = restarted_runtime.capture_source(
        "MAZAK-001",
        "http://192.168.200.249:5000",
    )
    restarted_runtime.executor.shutdown(wait=True, cancel_futures=True)

    assert success is True, error
    checkpoint = restarted_runtime.checkpoints["MAZAK-001"]
    assert checkpoint.next_sequence == 13
    assert checkpoint.storage_aliases == ["Mazak"]
    assert Path(checkpoint.last_raw_file) == old_raw.raw_path
    assert "MAZAK-001" in Path(checkpoint.last_observation_file).parts


def test_orphaned_raw_batch_is_recovered_before_checkpoint_advances(tmp_path, monkeypatch):
    recorder = load_recorder(tmp_path, monkeypatch)
    monkeypatch.setattr(recorder.runtime, "STATE_FILE", tmp_path / "state.json")
    probe = recorder.parse_probe(PROBE_XML)
    batch = recorder.parse_streams(SAMPLE_XML, source_name="Mazak", probe=probe)
    runtime = recorder.RecorderRuntime()
    raw = runtime.store.store_raw_batch(
        source_name="Mazak",
        requested_from=10,
        xml_text=SAMPLE_XML,
        batch=batch,
    )

    expected = runtime._recover_archived_batches(
        source_name="Mazak",
        base_url="http://agent:5000",
        instance_id=77,
        expected=10,
        probe=probe,
    )
    runtime.executor.shutdown(wait=True, cancel_futures=True)

    assert expected == 13
    assert runtime.checkpoints["Mazak"].next_sequence == 13
    assert raw.raw_path.exists()
    observation_path = Path(runtime.checkpoints["Mazak"].last_observation_file)
    normalized = Path(runtime.checkpoints["Mazak"].last_normalized_file)
    assert observation_path.exists()
    assert normalized.exists()
    assert len(observation_path.read_text().splitlines()) == 3
    assert len(normalized.read_text().splitlines()) == 3
