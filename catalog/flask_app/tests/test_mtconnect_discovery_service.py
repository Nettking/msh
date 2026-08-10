from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from catalog.flask_app.services.capability_config_service import (
    CapabilityConfig,
    parse_recorder_sources,
    save_capability_config,
)
from catalog.flask_app.services.mtconnect_discovery_service import (
    MAX_CONNECT_TIMEOUT_SECONDS,
    MAX_READ_TIMEOUT_SECONDS,
    MtconnectDiscoveryError,
    MtconnectDiscoveryService,
    validate_scan_cidr,
)


def _probe_xml(
    *,
    name: str = "Mazak",
    uuid: str = "",
    serial: str = "",
    device_id: str = "d1",
) -> str:
    attributes = []
    if device_id:
        attributes.append(f'id="{device_id}"')
    if name:
        attributes.append(f'name="{name}"')
    if uuid:
        attributes.append(f'uuid="{uuid}"')
    description = (
        f'<Description serialNumber="{serial}">Machine</Description>'
        if serial
        else "<Description>Machine</Description>"
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.7">
  <Header creationTime="2026-07-28T10:00:00Z" sender="agent" instanceId="1" bufferSize="4096"/>
  <Devices>
    <Device {' '.join(attributes)}>
      {description}
      <Components/>
    </Device>
  </Devices>
</MTConnectDevices>
"""


def _config(*, sources: str = "") -> CapabilityConfig:
    return CapabilityConfig(
        ai_provider_mode="local",
        ai_provider_name="This computer",
        ai_profile="laptop-standard",
        ai_model="llama3.2:3b",
        ollama_base_url="http://ollama:11434",
        recorder_sources=sources,
        recorder_poll_interval="0.2",
        recorder_include_condition=False,
        updated_at="before",
    )


def _service(
    tmp_path: Path,
    probe_fetcher,
    **kwargs,
) -> MtconnectDiscoveryService:
    return MtconnectDiscoveryService(
        scan_path=tmp_path / "scan.json",
        config_path=tmp_path / "capability-config.json",
        checkpoint_path=tmp_path / "checkpoint.json",
        probe_fetcher=probe_fetcher,
        **kwargs,
    )


@pytest.mark.parametrize(
    "cidr",
    [
        "192.168.200.0/24",
        "10.20.30.0/25",
        "172.16.4.8/32",
    ],
)
def test_validate_scan_cidr_accepts_explicit_small_private_ipv4(cidr) -> None:
    assert str(validate_scan_cidr(cidr))


@pytest.mark.parametrize(
    "cidr",
    [
        "192.168.200.9",
        "192.168.200.0/23",
        "8.8.8.0/24",
        "127.0.0.0/24",
        "2001:db8::/120",
        "not-a-network",
    ],
)
def test_validate_scan_cidr_rejects_unsafe_or_oversized_networks(cidr) -> None:
    with pytest.raises(MtconnectDiscoveryError):
        validate_scan_cidr(cidr)


def test_scan_uses_concurrency_bounded_timeouts_and_unique_data_names(
    tmp_path,
) -> None:
    barrier = threading.Barrier(2)
    calls: list[tuple[str, float, float]] = []
    calls_lock = threading.Lock()

    def fetch(probe_url, connect_timeout, read_timeout):
        with calls_lock:
            calls.append((probe_url, connect_timeout, read_timeout))
        barrier.wait(timeout=2)
        if "192.168.44.1" in probe_url:
            return _probe_xml(uuid="MAZAK-001", serial="SERIAL-1")
        return _probe_xml(uuid="MAZAK-002", serial="SERIAL-2")

    service = _service(
        tmp_path,
        fetch,
        max_workers=8,
        connect_timeout_seconds=99,
        read_timeout_seconds=99,
    )
    scan = service.scan("192.168.44.0/30")

    assert len(calls) == 2
    assert all(call[1] <= MAX_CONNECT_TIMEOUT_SECONDS for call in calls)
    assert all(call[2] <= MAX_READ_TIMEOUT_SECONDS for call in calls)
    assert scan["state"] == "complete"
    assert scan["agents_found"] == 2
    assert scan["machines_found"] == 2
    assert scan["addresses_scanned"] == 2

    source_names = [result["source_name"] for result in scan["results"]]
    display_names = [
        result["machines"][0]["display_name"] for result in scan["results"]
    ]
    machine_ids = [
        result["machines"][0]["machine_id"] for result in scan["results"]
    ]
    assert len(set(source_names)) == 2
    assert source_names == ["MAZAK-001", "MAZAK-002"]
    assert len(set(display_names)) == 2
    assert machine_ids == ["MAZAK-001", "MAZAK-002"]
    assert all(name.startswith("Mazak") for name in display_names)

    saved = json.loads((tmp_path / "scan.json").read_text(encoding="utf-8"))
    assert saved == scan


def test_identity_falls_back_from_uuid_to_serial_device_id_and_host(
    tmp_path,
) -> None:
    def fetch(probe_url, *_timeouts):
        if "10.9.8.1" in probe_url:
            return _probe_xml(uuid="", serial="SERIAL-A", device_id="d1")
        if "10.9.8.2" in probe_url:
            return _probe_xml(uuid="", serial="", device_id="controller")
        if "10.9.8.3" in probe_url:
            return _probe_xml(uuid="", serial="", device_id="")
        raise OSError("closed")

    service = _service(tmp_path, fetch)
    scan = service.scan("10.9.8.0/29")
    machines = {
        result["host"]: result["machines"][0] for result in scan["results"]
    }

    assert machines["10.9.8.1"]["identity_kind"] == "serial"
    assert machines["10.9.8.1"]["machine_id"] == "serial:SERIAL-A"
    assert machines["10.9.8.2"]["identity_kind"] == "device_id"
    assert "10.9.8.2:5000" in machines["10.9.8.2"]["machine_id"]
    assert machines["10.9.8.3"]["identity_kind"] == "host"
    assert machines["10.9.8.3"]["machine_id"].endswith("10.9.8.3:5000")


def test_one_agent_with_multiple_devices_is_one_recorder_source(tmp_path) -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.7">
  <Header creationTime="2026-07-28T10:00:00Z" sender="agent" instanceId="1" bufferSize="4096"/>
  <Devices>
    <Device id="d1" name="Mazak" uuid="MACHINE-A"><Components/></Device>
    <Device id="d2" name="Mazak" uuid="MACHINE-B"><Components/></Device>
  </Devices>
</MTConnectDevices>
"""

    service = _service(tmp_path, lambda *_args: xml)
    scan = service.scan("192.168.55.9/32")

    assert scan["agents_found"] == 1
    assert scan["machines_found"] == 2
    assert scan["results"][0]["machine_count"] == 2
    assert {
        machine["machine_id"] for machine in scan["results"][0]["machines"]
    } == {"MACHINE-A", "MACHINE-B"}

    merged = service.merge_selected_results(
        _config(),
        [scan["results"][0]["source_id"]],
        scan=scan,
    )
    assert len(parse_recorder_sources(merged.recorder_sources)) == 1


def test_recommended_cidr_prefers_scan_then_capability_config_then_checkpoint(
    tmp_path,
) -> None:
    service = _service(tmp_path, lambda *_args: "")
    (tmp_path / "scan.json").write_text(
        json.dumps({"cidr": "192.168.70.0/25", "results": []}),
        encoding="utf-8",
    )
    config = _config(
        sources="Configured=http://10.22.33.44:5000/current"
    )
    save_capability_config(config, tmp_path / "capability-config.json")
    assert service.recommended_cidr(config) == "192.168.70.0/25"

    (tmp_path / "scan.json").unlink()
    assert service.recommended_cidr() == "10.22.33.0/24"

    (tmp_path / "checkpoint.json").write_text(
        json.dumps(
            {
                "sources": {
                    "old": {"base_url": "http://172.20.7.80:5000"}
                }
            }
        ),
        encoding="utf-8",
    )
    assert service.recommended_cidr(_config()) == "172.20.7.0/24"


def test_merge_selected_results_preserves_existing_sources_without_role_authority(
    tmp_path,
) -> None:
    service = _service(tmp_path, lambda *_args: "")
    result = {
        "source_id": "mtconnect-source-new",
        "source_name": "Mazak-MAZAK-002",
        "base_url": "http://192.168.200.252:5000",
    }
    scan = {"results": [result]}
    config = _config(
        sources="Existing=http://192.168.200.251:5000/current"
    )

    merged = service.merge_selected_results(
        config,
        ["mtconnect-source-new"],
        scan=scan,
    )
    parsed = parse_recorder_sources(merged.recorder_sources)
    assert parsed == {
        "Existing": "http://192.168.200.251:5000",
        "Mazak-MAZAK-002": "http://192.168.200.252:5000",
    }
    assert merged.updated_at != "before"
    assert merged.ai_model == config.ai_model
    assert not hasattr(merged, "deployment_mode")


def test_merge_replaces_generic_alias_for_same_agent_with_uuid(tmp_path) -> None:
    service = _service(tmp_path, lambda *_args: "")
    result = {
        "source_id": "mtconnect-source-existing",
        "source_name": "MAZAK-M7ZDA13010Z",
        "base_url": "http://192.168.200.249:5000",
    }

    merged = service.merge_selected_results(
        _config(sources="Mazak=http://192.168.200.249:5000/current"),
        [result],
        scan={"results": [result]},
    )

    assert parse_recorder_sources(merged.recorder_sources) == {
        "MAZAK-M7ZDA13010Z": "http://192.168.200.249:5000"
    }


def test_scan_continues_when_hosts_are_closed_or_not_mtconnect(tmp_path) -> None:
    def fetch(probe_url, *_timeouts):
        if "192.168.88.1" in probe_url:
            return _probe_xml(uuid="ONLY-MACHINE")
        if "192.168.88.2" in probe_url:
            return "<html>not MTConnect</html>"
        raise TimeoutError("closed")

    service = _service(tmp_path, fetch)
    scan = service.scan("192.168.88.0/29")

    assert scan["state"] == "complete"
    assert scan["agents_found"] == 1
    assert scan["machines_found"] == 1
    assert scan["failure_count"] == 5
