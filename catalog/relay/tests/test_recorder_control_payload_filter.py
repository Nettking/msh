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
    SCHEMA,
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


def _rejects(payload: dict[str, Any]) -> None:
    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload")
    assert caught.value.code == "nonpublic-payload"


# --- the physical failure, and the traffic that must now route ---------------


def test_the_b03_scan_report_publishes():
    """The exact report the recorder queued and could not publish."""

    _ensure_bounded_json(_report(), field="payload")


def test_a_scan_request_with_a_private_network_is_routable():
    _ensure_bounded_json(_request(cidr="192.168.1.0/24"), field="payload")


def test_a_scan_request_without_an_explicit_network_is_routable():
    """B03's request named no CIDR; the recorder inferred it.

    ``port`` alone is redacted by the generic pass, so this payload was
    rejected too. Allowing ``cidr`` by itself would not have made a single
    scan event routable.
    """

    _ensure_bounded_json(
        scan_command_payload(
            request_id=B03_REQUEST_ID,
            target_node_id=B03_TARGET_NODE,
            cidr="",
            port=5000,
            now=NOW,
        ),
        field="payload",
    )


@pytest.mark.parametrize("cidr", ["10.1.2.0/25", "172.16.5.0/26", "192.168.9.7/32"])
def test_networks_smaller_than_a_slash_24_are_routable(cidr: str):
    _ensure_bounded_json(_request(cidr=cidr), field="payload")
    _ensure_bounded_json(_report(cidr=cidr), field="payload")


# --- the security boundary the allowance must not cross ----------------------


@pytest.mark.parametrize("cidr", ["8.8.8.0/24", "1.1.1.1/32", "203.0.113.0/24"])
def test_public_ipv4_is_still_rejected(cidr: str):
    _rejects(_request(cidr=cidr))
    _rejects(_report(cidr=cidr))


@pytest.mark.parametrize("cidr", ["192.168.0.0/16", "10.0.0.0/8", "172.16.0.0/12"])
def test_a_network_wider_than_a_slash_24_is_still_rejected(cidr: str):
    _rejects(_request(cidr=cidr))
    _rejects(_report(cidr=cidr))


@pytest.mark.parametrize("cidr", ["fd00::/64", "::1/128", "fe80::/10"])
def test_ipv6_is_still_rejected(cidr: str):
    _rejects(_request(cidr=cidr))
    _rejects(_report(cidr=cidr))


@pytest.mark.parametrize(
    "cidr",
    ["not-a-cidr", "172.19.0.0", "192.168.1.0/33", "192.168.1.0/-1", " 192.168.1.0/24"],
)
def test_a_malformed_cidr_is_still_rejected(cidr: str):
    _rejects(_request(cidr=cidr))


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
    )


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


async def _drive_scan_report(tmp_path, cidr: str) -> str:
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
        try:
            await recorder.append_event(
                session_id=session_id,
                event_type="recorder.control.scan.reported",
                payload=_report(cidr=cidr, target_node_id=recorder.node_id),
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
