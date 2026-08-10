from __future__ import annotations

from catalog.mtconnect_recorder.federation_node import select_storage_authority
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
        ]
    )

    assert args.sources == ["Mazak=http://127.0.0.1:5000"]
    assert args.federation_key == "FCP1-test-key"
    assert args.device_name == "Machine recorder"
    assert args.storage_group == "telemetry"
    assert args.require_federation is True


def test_start_recorder_parser_remains_backward_compatible_local_only() -> None:
    args = build_parser().parse_args(["Mazak=http://127.0.0.1:5000"])

    assert args.federation_key is None
    assert args.storage_group is None
    assert args.require_federation is False
