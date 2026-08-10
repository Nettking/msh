from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.recorder_control_events import (
    MAX_SOURCE_ITEMS,
    SCAN_REQUEST_EVENT,
    SOURCES_REQUEST_EVENT,
    parse_command,
    scan_command_payload,
    scan_report_payload,
    sources_command_payload,
)

NOW = datetime(2026, 8, 10, 21, 30, tzinfo=timezone.utc)
TARGET = "node-recorder"


def test_scan_command_is_bounded_and_round_trips() -> None:
    payload = scan_command_payload(
        request_id="scan-one",
        target_node_id=TARGET,
        cidr="192.168.10.0/24",
        port=5000,
        now=NOW,
    )

    parsed = parse_command(
        SCAN_REQUEST_EVENT,
        payload,
        now=NOW + timedelta(seconds=10),
    )

    assert parsed == {
        "command": "scan",
        "request_id": "scan-one",
        "target_node_id": TARGET,
        "cidr": "192.168.10.0/24",
        "port": 5000,
    }


def test_expired_recorder_control_command_is_rejected() -> None:
    payload = scan_command_payload(
        request_id="scan-expired",
        target_node_id=TARGET,
        cidr="",
        port=5000,
        now=NOW,
    )

    with pytest.raises(FederationValidationError) as caught:
        parse_command(
            SCAN_REQUEST_EVENT,
            payload,
            now=NOW + timedelta(minutes=3),
        )

    assert caught.value.code == "expired-recorder-control-command"


def test_source_changes_do_not_accept_unbounded_selection() -> None:
    with pytest.raises(FederationValidationError):
        sources_command_payload(
            request_id="sources-many",
            target_node_id=TARGET,
            scan_id="scan-one",
            add_source_ids=[f"source-{index}" for index in range(MAX_SOURCE_ITEMS + 1)],
            remove_source_names=[],
            now=NOW,
        )


def test_source_change_round_trip_contains_ids_not_urls() -> None:
    payload = sources_command_payload(
        request_id="sources-one",
        target_node_id=TARGET,
        scan_id="scan-one",
        add_source_ids=["source-abc"],
        remove_source_names=["old-machine"],
        now=NOW,
    )

    parsed = parse_command(SOURCES_REQUEST_EVENT, payload, now=NOW)

    assert parsed["add_source_ids"] == ("source-abc",)
    assert parsed["remove_source_names"] == ("old-machine",)
    assert "http://" not in str(payload)


def test_scan_report_redacts_discovery_network_endpoints() -> None:
    report = scan_report_payload(
        request_id="scan-one",
        target_node_id=TARGET,
        scan_id="scan-result",
        state="complete",
        cidr="192.168.10.0/24",
        port=5000,
        results=[
            {
                "source_id": "source-abc",
                "source_name": "machine-one",
                "display_name": "Machine one",
                "machine_count": 1,
                "base_url": "http://192.168.10.22:5000",
                "host": "192.168.10.22",
            }
        ],
        configured_source_names=[],
        message="Scan complete",
        now=NOW,
    )

    assert report["results"] == [
        {
            "source_id": "source-abc",
            "source_name": "machine-one",
            "display_name": "Machine one",
            "machine_count": 1,
        }
    ]
    assert "192.168.10.22" not in str(report)
