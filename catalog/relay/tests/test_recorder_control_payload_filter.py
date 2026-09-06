"""The relay admits one bounded recorder-control scan network and nothing else.

``fcp.recorder-control.v1`` intentionally carries a private scan CIDR, and the
generic transport filter always alters two fields of a scan payload: ``cidr``
by value, because it reads as an address, and ``port`` by key, because ``port``
is a location key. Physical Federation v1 acceptance B03 hit exactly that: a
recorder executed a targeted scan, durably queued its report, and could never
publish it -- every attempt failed ``nonpublic-payload`` while the node stayed
connected with fresh heartbeats.

So the relay compares against an expectation in which only those two fields of
that one schema are masked, and only when the CIDR satisfies the recorder's own
RFC1918 ``/24``-or-smaller discovery contract. These tests pin both halves of
the bargain: the scan events route, and every neighbouring shape a caller might
reach for still fails.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.recorder_control_events import (
    SCAN_REPORT_EVENT,
    SCAN_REQUEST_EVENT,
    SCHEMA,
    SOURCES_REQUEST_EVENT,
    is_publishable_scan_cidr,
    scan_command_payload,
    scan_report_payload,
    sources_command_payload,
)
from catalog.relay.service import _ensure_bounded_json

NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)

# The exact identifiers physical acceptance B03 recorded.
B03_TARGET_NODE = "node-0law0FuBEa8num0alBy1iPfmDzNfM7JRkpHeCAo-J98"
B03_REQUEST_ID = "recorder-scan-62fe4f8acecf418bb55d0691308d9346"
B03_CIDR = "172.19.0.0/24"


def _request(**overrides: Any) -> dict[str, Any]:
    payload = scan_command_payload(
        request_id=B03_REQUEST_ID,
        target_node_id=B03_TARGET_NODE,
        cidr="192.168.1.0/24",
        port=5000,
        now=NOW,
    )
    payload.update(overrides)
    return payload


def _report(**overrides: Any) -> dict[str, Any]:
    payload = scan_report_payload(
        request_id=B03_REQUEST_ID,
        target_node_id=B03_TARGET_NODE,
        scan_id="scan-1",
        state="complete",
        cidr=B03_CIDR,
        port=5000,
        results=[],
        configured_source_names=[],
        message="0 machine(s) from 0 MTConnect Agent(s)",
        now=NOW,
    )
    payload.update(overrides)
    return payload


def _accepts(payload: dict[str, Any], event_type: str) -> None:
    _ensure_bounded_json(payload, field="payload", event_type=event_type)


def _rejects(payload: dict[str, Any], event_type: str | None = SCAN_REPORT_EVENT) -> None:
    """Reject under the most permissive event type unless told otherwise.

    Defaulting to a real scan event keeps every boundary case honest: it proves
    the payload itself is refused, not merely that the event type was wrong.
    """

    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload", event_type=event_type)
    assert caught.value.code == "nonpublic-payload"


# --- the physical failure, and the traffic that must now route ---------------


def test_the_b03_scan_report_publishes():
    """The exact report the recorder queued and could not publish."""

    _accepts(_report(), SCAN_REPORT_EVENT)


def test_a_scan_request_with_a_private_network_is_routable():
    _accepts(_request(cidr="192.168.1.0/24"), SCAN_REQUEST_EVENT)


def test_a_scan_request_without_an_explicit_network_is_routable():
    """B03's request named no CIDR; the recorder inferred it.

    ``port`` alone is redacted by the generic pass, so this payload was
    rejected too. Allowing ``cidr`` by itself would not have made a single
    scan event routable.
    """

    _accepts(
        scan_command_payload(
            request_id=B03_REQUEST_ID,
            target_node_id=B03_TARGET_NODE,
            cidr="",
            port=5000,
            now=NOW,
        ),
        SCAN_REQUEST_EVENT,
    )


@pytest.mark.parametrize("cidr", ["10.1.2.0/25", "172.16.5.0/26", "192.168.9.7/32"])
def test_networks_smaller_than_a_slash_24_are_routable(cidr: str):
    _accepts(_request(cidr=cidr), SCAN_REQUEST_EVENT)
    _accepts(_report(cidr=cidr), SCAN_REPORT_EVENT)


# --- the security boundary the allowance must not cross ----------------------


@pytest.mark.parametrize("cidr", ["8.8.8.0/24", "1.1.1.1/32", "203.0.113.0/24"])
def test_public_ipv4_is_still_rejected(cidr: str):
    _rejects(_request(cidr=cidr), SCAN_REQUEST_EVENT)
    _rejects(_report(cidr=cidr), SCAN_REPORT_EVENT)


@pytest.mark.parametrize("cidr", ["192.168.0.0/16", "10.0.0.0/8", "172.16.0.0/12"])
def test_a_network_wider_than_a_slash_24_is_still_rejected(cidr: str):
    _rejects(_request(cidr=cidr), SCAN_REQUEST_EVENT)
    _rejects(_report(cidr=cidr), SCAN_REPORT_EVENT)


@pytest.mark.parametrize("cidr", ["fd00::/64", "::1/128", "fe80::/10"])
def test_ipv6_is_still_rejected(cidr: str):
    _rejects(_request(cidr=cidr), SCAN_REQUEST_EVENT)
    _rejects(_report(cidr=cidr), SCAN_REPORT_EVENT)


@pytest.mark.parametrize(
    "cidr",
    ["not-a-cidr", "172.19.0.0", "192.168.1.0/33", "192.168.1.0/-1", " 192.168.1.0/24"],
)
def test_a_malformed_cidr_is_still_rejected(cidr: str):
    _rejects(_request(cidr=cidr), SCAN_REQUEST_EVENT)


def test_a_private_address_in_another_field_is_still_rejected():
    _rejects(_report(message="agent reachable at 192.168.5.5"))
    _rejects(_request(target_node_id="node-10.0.0.7"))


def test_backend_path_and_credential_material_is_still_rejected():
    _rejects(_report(database_path="/var/lib/fcp/state.sqlite3"))
    _rejects(_report(token="s3cret-abcdefghijklmnopqrstuvwxyz012345"))
    _rejects(_report(base_url="https://backend.internal/api"))


def test_another_schema_carrying_a_cidr_gains_nothing():
    _rejects(
        {
            "schema": "fcp.some-other.v1",
            "command": "scan",
            "cidr": "192.168.1.0/24",
            "port": 5000,
        }
    )


def test_a_non_scan_recorder_control_payload_gains_nothing():
    """The allowance is keyed on ``command == "scan"``, not the schema alone."""

    _rejects(
        {
            "schema": SCHEMA,
            "command": "sources",
            "cidr": "192.168.1.0/24",
            "port": 5000,
        }
    )


def test_an_unbounded_port_is_still_rejected():
    _rejects(_request(port=0))
    _rejects(_request(port=70000))
    _rejects(_request(port="5000"))


def test_a_source_change_command_is_unaffected():
    _ensure_bounded_json(
        sources_command_payload(
            request_id=B03_REQUEST_ID,
            target_node_id=B03_TARGET_NODE,
            scan_id="scan-1",
            add_source_ids=["source-a"],
            remove_source_names=[],
            now=NOW,
        ),
        field="payload",
        event_type=SOURCES_REQUEST_EVENT,
    )


# --- the filter still refuses raw labels, so the fix rests on the producer ---


def test_a_raw_address_bearing_label_reaching_the_filter_is_refused():
    """An address-bearing label is refused if it ever reaches the relay.

    Discovery no longer produces such labels, and ``scan_report_payload``
    projects any that survive from an older configuration, so this shape should
    not occur in practice. The results here are injected *after* that
    projection, which is the point: it proves the relay allowance still covers
    only ``cidr`` and ``port``, and that publication is safe because the
    producer and the projection make it so -- not because the filter was
    relaxed to tolerate addresses in result strings.

    If this ever starts passing, the allowance has been widened and the
    Federation-visible identity work has been undone.
    """

    _rejects(
        _report(
            results=[
                {
                    "source_id": "abc123",
                    "source_name": "192.168.1.50-5000",
                    "display_name": "Mazak [192.168.1.50:5000]",
                    "machine_count": 1,
                }
            ],
        ),
        SCAN_REPORT_EVENT,
    )


def test_a_report_naming_an_agent_with_a_serial_number_publishes():
    """A label that carries no address needs no projection and routes as-is.

    Together with the test above this bounds the allowance from both sides:
    public-safe labels cross untouched, address-bearing ones never do.
    """

    _accepts(
        _report(
            results=[
                {
                    "source_id": "abc123",
                    "source_name": "mazak-sn-7781",
                    "display_name": "Mazak SN-7781",
                    "machine_count": 1,
                }
            ],
        ),
        SCAN_REPORT_EVENT,
    )


# --- the allowance is scoped to the two scan event types ---------------------


def test_both_scan_event_types_admit_a_valid_scan_payload():
    _accepts(_request(cidr="192.168.1.0/24"), SCAN_REQUEST_EVENT)
    _accepts(_report(cidr=B03_CIDR), SCAN_REPORT_EVENT)


@pytest.mark.parametrize(
    "event_type",
    [
        "some.other.event",
        "recorder.control.sources.requested",
        "recorder.control.sources.reported",
        "storage.batch.published",
        "",
    ],
)
def test_an_unrelated_event_type_gains_nothing_from_a_scan_shaped_payload(event_type):
    """The payload cannot vouch for the event that carried it.

    An arbitrary session event may put a byte-identical
    ``fcp.recorder-control.v1`` scan payload on the wire. Only the two scan
    event types earn the exemption, so every other event still fails.
    """

    _rejects(_request(cidr="192.168.1.0/24"), event_type)
    _rejects(_report(cidr=B03_CIDR), event_type)


def test_a_payload_with_no_event_type_gains_nothing():
    """Message routing validates payloads without an event type at all.

    ``_route_message`` calls the filter with no event type, so node-to-node
    messages can never carry a scan network however they are shaped.
    """

    _rejects(_request(cidr="192.168.1.0/24"), None)
    _rejects(_report(cidr=B03_CIDR), None)


# --- the allowance does not recurse into look-alike nested objects -----------


def test_an_unrelated_outer_schema_cannot_smuggle_a_nested_scan_object():
    """The exemption belongs to the event payload, not to anything shaped like one."""

    _rejects(
        {"schema": "fcp.some-other.v1", "data": _request(cidr="192.168.1.0/24")},
        SCAN_REPORT_EVENT,
    )


def test_a_list_cannot_smuggle_a_nested_scan_object():
    _rejects(
        {"schema": "fcp.some-other.v1", "items": [_request(cidr="192.168.1.0/24")]},
        SCAN_REPORT_EVENT,
    )


def test_a_deeply_nested_scan_object_is_still_refused():
    _rejects(
        {"anything": {"deep": {"deeper": _report(cidr=B03_CIDR)}}},
        SCAN_REPORT_EVENT,
    )


def test_a_valid_top_level_payload_cannot_carry_a_nested_twin():
    """A real scan report does not license a second scan object inside it."""

    _rejects(
        {
            **_report(cidr=B03_CIDR),
            "nested": {
                "schema": SCHEMA,
                "command": "scan",
                "cidr": "10.0.0.0/24",
                "port": 5000,
            },
        },
        SCAN_REPORT_EVENT,
    )


def test_the_scan_payload_at_the_protocol_boundary_is_still_admitted():
    """The positive control for the two negatives above."""

    _accepts(_report(cidr=B03_CIDR), SCAN_REPORT_EVENT)


# --- the predicate itself, pinned to the recorder's enforcing validator ------


def test_the_relay_predicate_agrees_with_the_recorder_discovery_validator():
    """The relay cannot import the Flask-layer validator, so pin them by test.

    ``is_publishable_scan_cidr`` restates the contract that
    ``validate_scan_cidr`` enforces on the recorder side. If either moves, this
    fails rather than letting the two drift apart silently.
    """

    from catalog.flask_app.services.mtconnect_discovery_service import (
        MtconnectDiscoveryError,
        validate_scan_cidr,
    )

    candidates = [
        "192.168.1.0/24",
        "10.1.2.0/25",
        "172.16.5.0/26",
        "192.168.9.7/32",
        "172.19.0.0/24",
        "8.8.8.0/24",
        "203.0.113.0/24",
        "192.168.0.0/16",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "fd00::/64",
        "::1/128",
        "not-a-cidr",
        "172.19.0.0",
        "192.168.1.0/33",
        "",
    ]
    for candidate in candidates:
        try:
            validate_scan_cidr(candidate)
        except MtconnectDiscoveryError:
            enforced = False
        else:
            enforced = True
        assert is_publishable_scan_cidr(candidate) is enforced, candidate


# --- the same bargain, through a real relay server and node client -----------


NOW_CLOCK = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
REQUEST_TIMEOUT_SECONDS = 3.0


async def _drive_scan_report(
    tmp_path,
    cidr: str,
    *,
    event_type: str = SCAN_REPORT_EVENT,
    wrap: bool = False,
) -> str:
    """Publish one recorder-control scan report over a real relay connection.

    Returns ``"accepted"`` or the relay's rejection code. This is the hop that
    failed in physical acceptance: the recorder is a node, so its report
    traverses the relay's transport filter, whereas the coordinator-side
    request does not.
    """

    from catalog.federation.coordinator import SessionCoordinator
    from catalog.node.client import RelayNodeClient, RelayRemoteError
    from catalog.relay.service import RelayServer

    relay = RelayServer(
        SessionCoordinator(tmp_path / "relay.sqlite3", clock=lambda: NOW_CLOCK),
        host="127.0.0.1",
        port=0,
        auth_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
        send_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
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
        request_timeout=REQUEST_TIMEOUT_SECONDS,
        clock=lambda: NOW_CLOCK,
    )
    try:
        token = relay.coordinator.create_enrollment_token(
            ttl_seconds=60, max_uses=1
        )["token"]
        await recorder.connect(enrollment_token=token)
        session = await recorder.create_session("B03 recorder control")
        session_id = str(session["session_id"])
        payload = _report(cidr=cidr, target_node_id=recorder.node_id)
        if wrap:
            payload = {"schema": "fcp.some-other.v1", "data": payload}
        try:
            await recorder.append_event(
                session_id=session_id,
                event_type=event_type,
                payload=payload,
            )
        except RelayRemoteError as error:
            return error.code
        return "accepted"
    finally:
        await recorder.disconnect()
        await relay.stop()


def test_a_bounded_scan_report_publishes_over_a_real_relay(tmp_path):
    assert asyncio.run(_drive_scan_report(tmp_path, B03_CIDR)) == "accepted"


def test_a_public_scan_report_is_still_refused_over_a_real_relay(tmp_path):
    assert (
        asyncio.run(_drive_scan_report(tmp_path, "8.8.8.0/24")) == "nonpublic-payload"
    )


def test_a_scan_request_event_publishes_over_a_real_relay(tmp_path):
    assert (
        asyncio.run(
            _drive_scan_report(tmp_path, B03_CIDR, event_type=SCAN_REQUEST_EVENT)
        )
        == "accepted"
    )


def test_a_wrong_event_type_is_refused_over_a_real_relay(tmp_path):
    """The scope hole, driven end to end through the real relay.

    A byte-identical scan payload under an unrelated event type must not earn
    the CIDR exemption at the server.
    """

    assert (
        asyncio.run(
            _drive_scan_report(tmp_path, B03_CIDR, event_type="some.other.event")
        )
        == "nonpublic-payload"
    )


def test_a_nested_scan_object_is_refused_over_a_real_relay(tmp_path):
    """The smuggling hole, driven end to end through the real relay."""

    assert (
        asyncio.run(_drive_scan_report(tmp_path, B03_CIDR, wrap=True))
        == "nonpublic-payload"
    )
