from __future__ import annotations

import threading

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.mtconnect_recorder.federation_node import (
    RecorderFederationNode,
    RecorderFederationSnapshot,
    select_storage_authority,
)
from start_recorder import build_parser


def _status(*capabilities: dict[str, object]) -> dict[str, object]:
    return {
        "sessions": [
            {
                "session_id": "session-1",
                "created_by_node_id": "node-owner",
            }
        ],
        "capabilities": list(capabilities),
    }


def _authority(
    *,
    node_id: str = "node-owner",
    group_ids: list[str] | None = None,
    status: str = "ready",
) -> dict[str, object]:
    return {
        "capability_id": "logical-storage-authority",
        "node_id": node_id,
        "session_id": "session-1",
        "type": "storage-control",
        "protocol": "fcp.storage-control",
        "protocol_version": "1",
        "status": status,
        "properties": {
            "kind": "recorder-logical-storage-authority",
            "group_ids": group_ids if group_ids is not None else ["telemetry"],
        },
    }


def test_select_storage_authority_auto_selects_single_owner_group() -> None:
    selected = select_storage_authority(
        _status(_authority()),
        session_id="session-1",
        requested_group=None,
    )

    assert selected.state == "ready"
    assert selected.authority_node_id == "node-owner"
    assert selected.group_id == "telemetry"


def test_select_storage_authority_rejects_non_owner_self_advertisement() -> None:
    selected = select_storage_authority(
        _status(_authority(node_id="node-untrusted")),
        session_id="session-1",
        requested_group=None,
    )

    assert selected.state == "authority-unavailable"
    assert selected.authority_node_id is None
    assert selected.group_id is None


def test_select_storage_authority_requires_explicit_choice_for_multiple_groups() -> None:
    selected = select_storage_authority(
        _status(_authority(group_ids=["telemetry", "archive"])),
        session_id="session-1",
        requested_group=None,
    )

    assert selected.state == "storage-group-required"
    assert selected.authority_node_id == "node-owner"
    assert selected.group_id is None


def test_select_storage_authority_accepts_requested_advertised_group() -> None:
    selected = select_storage_authority(
        _status(_authority(group_ids=["telemetry", "archive"])),
        session_id="session-1",
        requested_group="archive",
    )

    assert selected.state == "ready"
    assert selected.authority_node_id == "node-owner"
    assert selected.group_id == "archive"


def test_select_storage_authority_refuses_requested_unadvertised_group() -> None:
    selected = select_storage_authority(
        _status(_authority()),
        session_id="session-1",
        requested_group="unknown",
    )

    assert selected.state == "storage-group-unavailable"
    assert selected.authority_node_id == "node-owner"
    assert selected.group_id is None


def test_start_recorder_parser_accepts_first_join_and_storage_arguments() -> None:
    args = build_parser().parse_args(
        [
            "Mazak=http://127.0.0.1:5000",
            "--federation-key",
            "FCP1-test-key",
            "--device-name",
            "Machine recorder",
            "--storage-group",
            "telemetry",
            "--require-federation",
            "--require-data-sharing",
            "--sharing-timeout",
            "30",
        ]
    )

    assert args.inputs == ["Mazak=http://127.0.0.1:5000"]
    assert args.federation_key == "FCP1-test-key"
    assert args.device_name == "Machine recorder"
    assert args.storage_group == "telemetry"
    assert args.require_federation is True
    assert args.require_data_sharing is True
    assert args.sharing_timeout == 30.0


def test_start_recorder_parser_accepts_pairing_key_as_only_input() -> None:
    args = build_parser().parse_args(["FCP1-test-key"])

    assert args.inputs == ["FCP1-test-key"]
    assert args.federation_key is None
    assert args.storage_group is None


def test_start_recorder_parser_remains_backward_compatible_local_only() -> None:
    args = build_parser().parse_args(["Mazak=http://127.0.0.1:5000"])

    assert args.federation_key is None
    assert args.storage_group is None
    assert args.require_federation is False
    assert args.require_data_sharing is False


def _sharing_node(storage_state: str) -> RecorderFederationNode:
    node = RecorderFederationNode.__new__(RecorderFederationNode)
    node._lock = threading.RLock()
    node._stop = threading.Event()
    node._snapshot = RecorderFederationSnapshot(
        status="connected",
        storage_state=storage_state,
        storage_group="telemetry" if storage_state == "up-to-date" else None,
    )
    return node


def test_sharing_readiness_accepts_only_an_active_storage_path() -> None:
    node = _sharing_node("up-to-date")

    snapshot = node.wait_until_sharing_ready(timeout_seconds=0.1)

    assert snapshot.storage_group == "telemetry"


def test_sharing_readiness_fails_closed_with_actionable_group_state() -> None:
    node = _sharing_node("storage-group-required")

    with pytest.raises(FederationOperationError) as excinfo:
        node.wait_until_sharing_ready(timeout_seconds=0.01)

    assert excinfo.value.code == "recorder-sharing-not-ready"
    assert "--storage-group" in excinfo.value.message
