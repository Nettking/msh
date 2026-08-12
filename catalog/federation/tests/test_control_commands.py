from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from catalog.federation.control_commands import (
    ControlCommandEnvelope,
    correlated_event_request_id,
    ensure_bounded_json,
    parse_utc_stamp,
    stamp_utc,
)


def test_issue_deduplicates_targets_without_changing_wire_fields() -> None:
    created = datetime(2026, 8, 12, 20, 0, tzinfo=timezone.utc)
    envelope = ControlCommandEnvelope.issue(
        request_id="request-one",
        target_node_ids=("node-a", "node-a", "node-b"),
        created_at=created,
        expires_at=created + timedelta(minutes=10),
        max_lifetime=timedelta(minutes=10),
    )

    assert envelope.payload_fields() == {
        "request_id": "request-one",
        "target_node_ids": ["node-a", "node-b"],
        "created_at": "2026-08-12T20:00:00Z",
        "expires_at": "2026-08-12T20:10:00Z",
    }


def test_parse_payload_can_preserve_or_reject_duplicate_targets() -> None:
    now = datetime(2026, 8, 12, 20, 0, tzinfo=timezone.utc)
    payload = {
        "request_id": "request-one",
        "target_node_ids": ["node-a", "node-a"],
        "created_at": stamp_utc(now),
        "expires_at": stamp_utc(now + timedelta(minutes=5)),
    }

    parsed = ControlCommandEnvelope.parse_payload(
        payload,
        max_lifetime=timedelta(minutes=10),
        require_unique_targets=False,
        now=now,
    )
    assert parsed.target_node_ids == ("node-a", "node-a")

    with pytest.raises(ValueError, match="malformed_targets"):
        ControlCommandEnvelope.parse_payload(
            payload,
            max_lifetime=timedelta(minutes=10),
            require_unique_targets=True,
            now=now,
        )


def test_parse_payload_rejects_reversed_lifetime() -> None:
    now = datetime(2026, 8, 12, 20, 0, tzinfo=timezone.utc)
    payload = {
        "request_id": "request-one",
        "target_node_ids": ["node-a"],
        "created_at": stamp_utc(now + timedelta(seconds=30)),
        "expires_at": stamp_utc(now + timedelta(seconds=10)),
    }

    with pytest.raises(ValueError, match="expired_or_invalid_request"):
        ControlCommandEnvelope.parse_payload(
            payload,
            max_lifetime=timedelta(minutes=10),
            require_unique_targets=False,
            now=now,
        )


def test_timestamp_helpers_require_aware_values() -> None:
    with pytest.raises(ValueError, match="malformed_timestamp"):
        stamp_utc(datetime(2026, 8, 12, 20, 0))
    with pytest.raises(ValueError, match="malformed_timestamp"):
        parse_utc_stamp("2026-08-12T20:00:00")


def test_bounded_json_and_correlation_id_are_deterministic() -> None:
    ensure_bounded_json({"value": "ok"}, max_bytes=64, error_code="too_large")
    with pytest.raises(ValueError, match="too_large"):
        ensure_bounded_json({"value": "x" * 100}, max_bytes=32, error_code="too_large")

    first = correlated_event_request_id("report", "request-one", "node-a")
    second = correlated_event_request_id("report", "request-one", "node-a")
    assert first == second
    assert len(first) <= 64
