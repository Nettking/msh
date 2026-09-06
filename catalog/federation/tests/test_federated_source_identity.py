"""A private agent address never becomes a Federation-visible identity.

MTConnect discovery has to know where an agent lives, and the recorder has to
keep that address to reach it. Neither fact may reach Federation. Physical
acceptance B03 found the first half of this: a scan report carrying a private
CIDR was refused ``nonpublic-payload``. The same defect has a second face --
once such a source is configured, the recorder's capability re-announcement
carries its name in ``properties.source_names`` and is refused
``nonpublic-property``, taking the recorder off the air on reconnect.

The split these tests pin:

* ``display_name`` is only ever an operator label, so discovery derives a safe
  one from a digest of the agent identity instead of its address.
* ``source_name`` is the recorder's durable on-disk batch directory and
  checkpoint key, so it is left alone locally and projected only where it
  crosses into Federation. Renaming it would orphan recorded data.
* ``source_id`` was already an opaque digest and is what selection uses, so it
  crosses unchanged.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.recorder_control_events import (
    SCAN_REPORT_EVENT,
    federated_source_label,
    federated_source_labels,
    local_source_names_for,
    scan_report_payload,
    sources_report_payload,
)
from catalog.federation.redaction import is_nonpublic_location_text
from catalog.flask_app.services.capability_config_service import (
    CapabilityConfig,
    parse_recorder_sources,
)
from catalog.flask_app.services.mtconnect_discovery_service import (
    MtconnectDiscoveryService,
    _short_digest,
)
from catalog.mtconnect_recorder.parsing import machine_display_name
from catalog.relay.service import _ensure_bounded_json

HOST = "192.168.1.50"
PORT = 5000
IDENTITY = f"{HOST}:{PORT}"
PUBLIC_TOKEN = f"agent-{_short_digest(IDENTITY, length=12)}"
NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)

# The address must not appear in any Federation-visible form.
FORBIDDEN = (HOST, IDENTITY, f"{HOST}-{PORT}")


def _assert_no_address(blob: object, label: str) -> None:
    text = repr(blob)
    for needle in FORBIDDEN:
        assert needle not in text, f"{label} leaked {needle!r}: {text}"


def _label(**kwargs: object) -> str:
    """Render a display name the way discovery now renders one."""

    return machine_display_name(
        machine_id=PUBLIC_TOKEN,
        fallback=f"MTConnect {PUBLIC_TOKEN}",
        **kwargs,
    )


# --- discovery labels, across every identity strength an agent may have -----


@pytest.mark.parametrize(
    ("case", "device_name", "serial_number"),
    [
        ("no serial and no name", None, None),
        ("name without a serial", "Mazak", None),
        ("device id without a serial", "M1", None),
        ("uuid-shaped name without a serial", "b7c1e2f4-1111-2222-3333-444455556666", None),
        ("serial number present", "Mazak", "SN-7781"),
    ],
)
def test_a_discovered_label_never_carries_the_agent_address(
    case: str, device_name: str | None, serial_number: str | None
):
    label = _label(device_name=device_name, serial_number=serial_number)
    assert not is_nonpublic_location_text(label), case
    _assert_no_address(label, case)
    assert label, case


def test_an_agent_that_identifies_itself_keeps_its_readable_name():
    """The digest is a fallback, not a blanket replacement."""

    assert _label(device_name="Mazak", serial_number="SN-7781") == "Mazak SN-7781"


def test_a_weak_agent_still_reads_as_itself_plus_a_stable_token():
    assert _label(device_name="Mazak", serial_number=None) == f"Mazak [{PUBLIC_TOKEN}]"


def test_two_weak_agents_receive_distinct_deterministic_identities():
    """Identical metadata, different agents, must not collapse together."""

    other_identity = "192.168.1.51:5000"
    other_token = f"agent-{_short_digest(other_identity, length=12)}"
    assert other_token != PUBLIC_TOKEN
    first = machine_display_name(
        device_name="Mazak", serial_number=None,
        machine_id=PUBLIC_TOKEN, fallback=f"MTConnect {PUBLIC_TOKEN}",
    )
    second = machine_display_name(
        device_name="Mazak", serial_number=None,
        machine_id=other_token, fallback=f"MTConnect {other_token}",
    )
    assert first != second
    _assert_no_address(second, "second weak agent")


def test_an_identity_is_stable_across_rescan_and_restart():
    """The token is a pure function of the agent identity, so it cannot drift."""

    assert PUBLIC_TOKEN == f"agent-{_short_digest(IDENTITY, length=12)}"
    assert federated_source_label("192.168.1.50-5000") == federated_source_label(
        "192.168.1.50-5000"
    )


# --- the publication projection ---------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "192.168.1.50-5000",
        "Mazak [192.168.1.50:5000]",
        "MTConnect 192.168.1.50:5000",
        "2 MTConnect devices at 192.168.1.50:5000",
        "10.0.0.7-5000",
    ],
)
def test_an_address_bearing_name_is_projected(name: str):
    projected = federated_source_label(name)
    assert projected.startswith("mtconnect-agent-")
    assert not is_nonpublic_location_text(projected)
    _assert_no_address(projected, name)


@pytest.mark.parametrize("name", ["mazak-sn-7781", "Mazak SN-7781", "cell-4-lathe", ""])
def test_a_public_safe_name_is_left_readable(name: str):
    assert federated_source_label(name) == name


def test_projection_preserves_order_and_collapses_duplicates():
    assert federated_source_labels(["a", "b", "a"]) == ("a", "b")


# --- migration of an already-configured address-derived source --------------


def test_an_existing_ip_derived_source_can_still_be_removed():
    """The recorder maps a published label back to its local configured name.

    An operator who configured a source before this fix has an address-derived
    name on disk. It keeps that name -- the batch directory and checkpoints
    depend on it -- and Federation only ever sees the projection, so removal
    must accept the projection and resolve it back.
    """

    configured = ("192.168.1.50-5000", "mazak-sn-7781")
    published = federated_source_label("192.168.1.50-5000")
    assert published != "192.168.1.50-5000"
    assert local_source_names_for([published], configured) == ("192.168.1.50-5000",)


def test_removal_still_accepts_a_local_name_directly():
    """A coordinator holding a pre-projection name keeps working."""

    configured = ("192.168.1.50-5000", "mazak-sn-7781")
    assert local_source_names_for(["192.168.1.50-5000"], configured) == (
        "192.168.1.50-5000",
    )
    assert local_source_names_for(["mazak-sn-7781"], configured) == ("mazak-sn-7781",)


def test_removal_of_an_unknown_source_resolves_to_nothing():
    assert local_source_names_for(["nope"], ("mazak-sn-7781",)) == ()


def test_migration_neither_duplicates_nor_loses_a_source():
    """One configured source projects to exactly one published label."""

    configured = ("192.168.1.50-5000", "mazak-sn-7781")
    published = federated_source_labels(list(configured))
    assert len(published) == len(configured)
    assert len(set(published)) == len(configured)
    for label in published:
        assert local_source_names_for([label], configured) != ()


# --- the two payloads that failed physically --------------------------------


def _report_for_a_real_agent(**overrides: object) -> dict[str, object]:
    payload = dict(
        request_id="recorder-scan-62fe4f8acecf418bb55d0691308d9346",
        target_node_id="node-0law0FuBEa8num0alBy1iPfmDzNfM7JRkpHeCAo-J98",
        scan_id="scan-1",
        state="complete",
        cidr="192.168.1.0/24",
        port=PORT,
        results=[
            {
                "source_id": "mtconnect-source-" + _short_digest("u|i", length=16),
                "source_name": "192.168.1.50-5000",
                "display_name": "Mazak [192.168.1.50:5000]",
                "machine_count": 1,
            }
        ],
        configured_source_names=["192.168.1.50-5000"],
        message="1 machine(s) from 1 MTConnect Agent(s)",
        now=NOW,
    )
    payload.update(overrides)
    return scan_report_payload(**payload)


def test_a_scan_report_naming_a_real_agent_now_publishes():
    """The case B03 would have hit at the first discovered machine."""

    report = _report_for_a_real_agent()
    _ensure_bounded_json(report, field="payload", event_type=SCAN_REPORT_EVENT)
    _assert_no_address(report, "scan report")


def test_the_scan_report_keeps_the_opaque_source_id_for_selection():
    """Selection uses ``source_id``; projecting the labels must not disturb it."""

    report = _report_for_a_real_agent()
    assert report["results"][0]["source_id"].startswith("mtconnect-source-")


def test_a_sources_report_naming_a_real_agent_publishes():
    report = sources_report_payload(
        request_id="r",
        target_node_id="n",
        scan_id="s",
        state="complete",
        configured_source_names=["192.168.1.50-5000"],
        message="configured",
        now=NOW,
    )
    _ensure_bounded_json(report, field="payload", event_type="recorder.control.sources.reported")
    _assert_no_address(report, "sources report")


def test_capability_reannouncement_with_a_real_agent_source_succeeds():
    """The second face of the defect: reconnect must not strand the recorder."""

    announcement = CapabilityAnnouncement(
        capability_id="recorder-node-x",
        node_id="node-x",
        session_id="session-1",
        type="recorder",
        protocol="mtconnect",
        protocol_version="1",
        status=CapabilityStatus.READY,
        properties={
            "kind": "standalone-recorder",
            "source_count": 1,
            "source_names": list(federated_source_labels(["192.168.1.50-5000"])),
            "dataset_schema": "fcp.mtconnect.observations.v1",
            "logical_storage": True,
        },
        announced_at=NOW,
    )
    _assert_no_address(announcement.properties, "capability properties")


def test_capability_reannouncement_still_refuses_an_unprojected_address():
    """The capability filter itself is not weakened by any of this."""

    with pytest.raises(FederationValidationError) as caught:
        CapabilityAnnouncement(
            capability_id="recorder-node-x",
            node_id="node-x",
            session_id="session-1",
            type="recorder",
            protocol="mtconnect",
            protocol_version="1",
            status=CapabilityStatus.READY,
            properties={"kind": "standalone-recorder", "source_names": ["192.168.1.50-5000"]},
            announced_at=NOW,
        )
    assert caught.value.code == "nonpublic-property"


def test_a_real_agent_scan_report_publishes_over_a_real_relay(tmp_path):
    """End to end, through a live relay server and node client."""

    from catalog.federation.coordinator import SessionCoordinator
    from catalog.node.client import RelayNodeClient, RelayRemoteError
    from catalog.relay.service import RelayServer

    async def scenario() -> str:
        relay = RelayServer(
            SessionCoordinator(tmp_path / "relay.sqlite3", clock=lambda: NOW),
            host="127.0.0.1",
            port=0,
            auth_timeout_seconds=3.0,
            send_timeout_seconds=3.0,
            heartbeat_timeout_seconds=300,
            sweep_interval_seconds=300,
        )
        await relay.start()
        recorder = RelayNodeClient(
            state_directory=tmp_path / "recorder",
            relay_url=relay.url,
            display_name="Recorder",
            allow_insecure_local=True,
            heartbeat_interval=300,
            request_timeout=3.0,
            clock=lambda: NOW,
        )
        try:
            token = relay.coordinator.create_enrollment_token(
                ttl_seconds=60, max_uses=1
            )["token"]
            await recorder.connect(enrollment_token=token)
            session = await recorder.create_session("real agent scan")
            try:
                await recorder.append_event(
                    session_id=str(session["session_id"]),
                    event_type=SCAN_REPORT_EVENT,
                    payload=_report_for_a_real_agent(target_node_id=recorder.node_id),
                )
            except RelayRemoteError as error:
                return error.code
            return "accepted"
        finally:
            await recorder.disconnect()
            await relay.stop()

    assert asyncio.run(scenario()) == "accepted"


# --- the whole lifecycle, driven through the real discovery service ----------


WEAK_URL = f"http://{HOST}:{PORT}"
LEGACY_NAME = f"{HOST}-{PORT}"


def _probe_xml(*, name: str = "", uuid: str = "", serial: str = "", device_id: str = "") -> str:
    attributes = [f'{key}="{value}"' for key, value in
                  (("id", device_id), ("name", name), ("uuid", uuid)) if value]
    description = (
        f'<Description serialNumber="{serial}">Machine</Description>'
        if serial
        else "<Description>Machine</Description>"
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.7">
  <Header creationTime="2026-07-28T10:00:00Z" sender="agent" instanceId="1" bufferSize="4096"/>
  <Devices><Device {' '.join(attributes)}>{description}<Components/></Device></Devices>
</MTConnectDevices>
"""


def _config(sources: str = "") -> CapabilityConfig:
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


def _service(tmp_path, xml: str) -> MtconnectDiscoveryService:
    return MtconnectDiscoveryService(
        scan_path=tmp_path / "scan.json",
        config_path=tmp_path / "capability-config.json",
        checkpoint_path=tmp_path / "checkpoint.json",
        probe_fetcher=lambda url, connect, read: xml,
    )


def _scan_one(tmp_path, xml: str) -> dict[str, object]:
    """Scan a single-address network and return the one discovered source."""

    scan = _service(tmp_path, xml).scan(f"{HOST}/32", port=PORT)
    assert scan["results"], "the probe fixture should yield one source"
    return scan["results"][0]


def _assert_result_is_publishable(result: dict[str, object], case: str) -> None:
    """No endpoint material may survive into the published projection."""

    published = scan_report_payload(
        request_id="r",
        target_node_id="n",
        scan_id="s",
        state="complete",
        cidr=f"{HOST.rsplit('.', 1)[0]}.0/24",
        port=PORT,
        results=[result],
        configured_source_names=[str(result["source_name"])],
        message="1 machine(s) from 1 MTConnect Agent(s)",
        now=NOW,
    )
    _ensure_bounded_json(published, field="payload", event_type=SCAN_REPORT_EVENT)
    text = repr(published)
    for needle in (*FORBIDDEN, WEAK_URL, f"{WEAK_URL}/probe", "probe_url", "base_url"):
        assert needle not in text, f"{case} leaked {needle!r}"


def test_a_uuid_machine_keeps_its_readable_identity(tmp_path):
    result = _scan_one(tmp_path, _probe_xml(name="Mazak", uuid="b7c1-uuid-2222"))
    assert "b7c1-uuid-2222" in str(result["display_name"])
    _assert_result_is_publishable(result, "uuid")


def test_a_serial_machine_keeps_its_readable_identity(tmp_path):
    result = _scan_one(tmp_path, _probe_xml(name="Mazak", serial="SN-7781"))
    assert "SN-7781" in str(result["display_name"])
    _assert_result_is_publishable(result, "serial")


@pytest.mark.parametrize(
    ("case", "kwargs"),
    [
        ("device name only", {"name": "Mazak"}),
        ("device id only", {"device_id": "d1"}),
        ("no useful metadata", {}),
    ],
)
def test_a_weak_machine_is_discoverable_and_publishable(tmp_path, case, kwargs):
    """The case that made B03's next step fail."""

    result = _scan_one(tmp_path, _probe_xml(**kwargs))
    assert not is_nonpublic_location_text(str(result["source_name"])), case
    assert not is_nonpublic_location_text(str(result["display_name"])), case
    _assert_result_is_publishable(result, case)


def test_two_weak_sources_at_different_addresses_stay_distinct(tmp_path):
    first = _scan_one(tmp_path / "a", _probe_xml(name="Mazak"))

    service = MtconnectDiscoveryService(
        scan_path=tmp_path / "b" / "scan.json",
        config_path=tmp_path / "b" / "capability-config.json",
        checkpoint_path=tmp_path / "b" / "checkpoint.json",
        probe_fetcher=lambda url, connect, read: _probe_xml(name="Mazak"),
    )
    second = service.scan("192.168.1.51/32", port=PORT)["results"][0]

    assert first["source_name"] != second["source_name"]
    assert first["source_id"] != second["source_id"]


def test_a_repeated_scan_yields_the_same_identity(tmp_path):
    """Rescan stability: the identity is a function of the agent, not the run."""

    xml = _probe_xml(name="Mazak")
    first = _scan_one(tmp_path / "one", xml)
    second = _scan_one(tmp_path / "two", xml)
    assert first["source_name"] == second["source_name"]
    assert first["display_name"] == second["display_name"]
    assert first["source_id"] == second["source_id"]


def test_a_weak_source_can_be_selected_and_configured(tmp_path):
    service = _service(tmp_path, _probe_xml(name="Mazak"))
    scan = service.scan(f"{HOST}/32", port=PORT)
    result = scan["results"][0]
    configured = service.merge_selected_results(_config(), [str(result["source_id"])])
    sources = parse_recorder_sources(configured.recorder_sources)

    # The public name is the durable key; the endpoint is its local value.
    assert str(result["source_name"]) in sources
    assert sources[str(result["source_name"])] == WEAK_URL
    assert not is_nonpublic_location_text(str(result["source_name"]))


def test_a_configured_weak_source_survives_restart(tmp_path):
    """Restart stability: the durable key round-trips unchanged."""

    service = _service(tmp_path, _probe_xml(name="Mazak"))
    result = service.scan(f"{HOST}/32", port=PORT)["results"][0]
    configured = service.merge_selected_results(_config(), [str(result["source_id"])])
    reloaded = parse_recorder_sources(
        _config(configured.recorder_sources).recorder_sources
    )
    assert reloaded == parse_recorder_sources(configured.recorder_sources)
    assert federated_source_labels(list(reloaded)) == tuple(reloaded)


# --- migration of a deployment configured before this fix -------------------


def test_a_legacy_ip_derived_source_publishes_safely(tmp_path):
    """The endpoint stays local; Federation sees only the projection."""

    sources = parse_recorder_sources(f"{LEGACY_NAME}={WEAK_URL}")
    assert sources == {LEGACY_NAME: WEAK_URL}
    published = federated_source_labels(list(sources))
    _assert_no_address(published, "legacy projection")
    assert WEAK_URL not in repr(published)
    assert len(published) == 1


def test_legacy_migration_is_idempotent():
    """Projecting an already-projected label changes nothing further."""

    once = federated_source_label(LEGACY_NAME)
    assert federated_source_label(once) == once
    assert federated_source_label(federated_source_label(once)) == once


def test_legacy_migration_neither_duplicates_nor_loses_the_endpoint(tmp_path):
    sources = parse_recorder_sources(f"{LEGACY_NAME}={WEAK_URL};mazak-sn-7781=http://10.0.0.9:5000")
    published = federated_source_labels(list(sources))
    assert len(published) == len(sources) == 2
    assert len(set(published)) == 2
    # The endpoint is still there locally, and still reachable by its own key.
    assert sources[LEGACY_NAME] == WEAK_URL


def test_a_legacy_source_is_still_removable_after_migration():
    configured = (LEGACY_NAME, "mazak-sn-7781")
    published = federated_source_label(LEGACY_NAME)
    assert local_source_names_for([published], configured) == (LEGACY_NAME,)
    assert local_source_names_for([LEGACY_NAME], configured) == (LEGACY_NAME,)


def test_reselecting_a_legacy_source_replaces_rather_than_duplicates(tmp_path):
    """merge_selected_results drops any entry sharing the endpoint first."""

    service = _service(tmp_path, _probe_xml(name="Mazak"))
    result = service.scan(f"{HOST}/32", port=PORT)["results"][0]
    legacy = _config(f"{LEGACY_NAME}={WEAK_URL}")
    merged = service.merge_selected_results(legacy, [str(result["source_id"])])
    sources = parse_recorder_sources(merged.recorder_sources)

    assert len(sources) == 1, sources
    assert LEGACY_NAME not in sources
    assert sources[str(result["source_name"])] == WEAK_URL


# --- a weak-identity machine, end to end over a real relay -------------------


def test_a_discovered_weak_machine_reports_over_a_real_relay(tmp_path):
    from catalog.federation.coordinator import SessionCoordinator
    from catalog.node.client import RelayNodeClient, RelayRemoteError
    from catalog.relay.service import RelayServer

    result = _scan_one(tmp_path / "scan", _probe_xml(name="Mazak"))
    report = scan_report_payload(
        request_id="recorder-scan-weak",
        target_node_id="node-x",
        scan_id="scan-1",
        state="complete",
        cidr="192.168.1.0/24",
        port=PORT,
        results=[result],
        configured_source_names=[str(result["source_name"])],
        message="1 machine(s) from 1 MTConnect Agent(s)",
        now=NOW,
    )
    _assert_no_address(report, "weak-identity relay report")
    assert WEAK_URL not in repr(report)

    async def scenario() -> str:
        relay = RelayServer(
            SessionCoordinator(tmp_path / "relay.sqlite3", clock=lambda: NOW),
            host="127.0.0.1", port=0,
            auth_timeout_seconds=3.0, send_timeout_seconds=3.0,
            heartbeat_timeout_seconds=300, sweep_interval_seconds=300,
        )
        await relay.start()
        recorder = RelayNodeClient(
            state_directory=tmp_path / "recorder", relay_url=relay.url,
            display_name="Recorder", allow_insecure_local=True,
            heartbeat_interval=300, request_timeout=3.0, clock=lambda: NOW,
        )
        try:
            token = relay.coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
            await recorder.connect(enrollment_token=token)
            session = await recorder.create_session("weak identity scan")
            try:
                await recorder.append_event(
                    session_id=str(session["session_id"]),
                    event_type=SCAN_REPORT_EVENT,
                    payload={**report, "target_node_id": recorder.node_id},
                )
            except RelayRemoteError as error:
                return error.code
            return "accepted"
        finally:
            await recorder.disconnect()
            await relay.stop()

    assert asyncio.run(scenario()) == "accepted"
