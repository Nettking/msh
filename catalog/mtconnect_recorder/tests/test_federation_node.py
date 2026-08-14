from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.federation.recorder_delivery import (
    RECORDER_STORAGE_SCHEMA,
    RecorderDeliveryRunResult,
)
from catalog.mtconnect_recorder.federation_node import (
    RecorderFederationNode,
    RecorderFederationSnapshot,
    _publication_cycle_status,
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


def test_start_recorder_parser_rejects_abbreviated_security_options() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--federation-k=FCP1-secret"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--data-d", "other-data"])


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), 121.0])
def test_federation_node_rejects_unbounded_request_timeout(
    timeout: float,
    tmp_path,
) -> None:
    with pytest.raises(ValueError, match="finite"):
        RecorderFederationNode(
            data_directory=tmp_path,
            display_name="recorder",
            source_names=(),
            request_timeout=timeout,
        )


def _sharing_node(
    storage_state: str,
    *,
    last_committed_count: int = 0,
    jsonl_state: str = "ready",
) -> RecorderFederationNode:
    node = RecorderFederationNode.__new__(RecorderFederationNode)
    node._lock = threading.RLock()
    node._stop = threading.Event()
    node._publication_future = Future()
    node._snapshot = RecorderFederationSnapshot(
        status="connected",
        storage_state=storage_state,
        storage_group="telemetry" if storage_state == "up-to-date" else None,
        last_committed_count=last_committed_count,
        jsonl_state=jsonl_state,
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


def test_pending_failed_outbox_is_not_mistaken_for_active_sharing() -> None:
    node = _sharing_node("publishing", last_committed_count=0)

    with pytest.raises(FederationOperationError) as excinfo:
        node.wait_until_sharing_ready(timeout_seconds=0.01)

    assert excinfo.value.code == "recorder-sharing-not-ready"


def test_sharing_readiness_requires_headless_jsonl_publication() -> None:
    node = _sharing_node("up-to-date", jsonl_state="backlogged")

    with pytest.raises(FederationOperationError) as excinfo:
        node.wait_until_sharing_ready(timeout_seconds=0.01)

    assert excinfo.value.code == "recorder-sharing-not-ready"
    assert "JSONL" in excinfo.value.message


def test_committed_backlog_cycle_proves_active_sharing() -> None:
    node = _sharing_node("publishing", last_committed_count=1)

    snapshot = node.wait_until_sharing_ready(timeout_seconds=0.1)

    assert snapshot.last_committed_count == 1


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), 601.0])
def test_sharing_readiness_timeout_is_finite_and_bounded(timeout: float) -> None:
    node = _sharing_node("discovering")

    with pytest.raises(ValueError, match="finite"):
        node.wait_until_sharing_ready(timeout_seconds=timeout)


def test_sharing_readiness_rejects_stopped_or_finished_worker() -> None:
    stopped = _sharing_node("up-to-date")
    stopped._stop.set()
    with pytest.raises(FederationOperationError) as stopped_error:
        stopped.wait_until_sharing_ready(timeout_seconds=0.1)
    assert stopped_error.value.code == "recorder-publication-stopped"

    finished = _sharing_node("up-to-date")
    finished._publication_future.set_result(None)
    with pytest.raises(FederationOperationError) as finished_error:
        finished.wait_until_sharing_ready(timeout_seconds=0.1)
    assert finished_error.value.code == "recorder-publication-stopped"


def test_sharing_readiness_rechecks_worker_after_ready_snapshot() -> None:
    node = _sharing_node("up-to-date")
    snapshot = node._snapshot

    def snapshot_during_worker_exit() -> RecorderFederationSnapshot:
        node._publication_future.set_result(None)
        return snapshot

    node.snapshot = snapshot_during_worker_exit

    with pytest.raises(FederationOperationError) as excinfo:
        node.wait_until_sharing_ready(timeout_seconds=0.1)

    assert excinfo.value.code == "recorder-publication-stopped"


def test_headless_jsonl_publisher_uses_the_selected_storage_route() -> None:
    context = object()
    state = object()
    calls: list[tuple[object, object, str, str]] = []

    class Service:
        @staticmethod
        def authorized_context() -> object:
            return context

    class Publisher:
        @staticmethod
        def publish_local_once(
            runtime_state: object,
            trusted_context: object,
            *,
            authority_node_id: str,
            group_id: str,
        ) -> object:
            calls.append(
                (
                    runtime_state,
                    trusted_context,
                    authority_node_id,
                    group_id,
                )
            )
            return SimpleNamespace(published_chunks=2)

    node = RecorderFederationNode.__new__(RecorderFederationNode)
    node.service = Service()
    node.jsonl_publisher = Publisher()

    result = asyncio.run(
        node._publish_jsonl_once(
            state,
            authority_node_id="node-owner",
            group_id="fcp-local-storage",
        )
    )

    assert result.published_chunks == 2
    assert calls == [
        (state, context, "node-owner", "fcp-local-storage"),
    ]


def test_publication_cycle_status_is_session_scoped_and_failure_aware() -> None:
    old_session = SimpleNamespace(
        session_id="session-old",
        schema_id=RECORDER_STORAGE_SCHEMA,
        destination_id="group-current",
        last_error="old failure",
    )
    current_failure = SimpleNamespace(
        session_id="session-current",
        schema_id=RECORDER_STORAGE_SCHEMA,
        destination_id="group-current",
        last_error="storage rejected",
    )
    old_group_failure = SimpleNamespace(
        session_id="session-current",
        schema_id=RECORDER_STORAGE_SCHEMA,
        destination_id="group-old",
        last_error="old group rejected",
    )

    assert _publication_cycle_status(
        pending_entries=(old_session,),
        session_id="session-current",
        group_id="group-current",
        delivery=RecorderDeliveryRunResult(attempted=0, committed=0, pending=0),
    ) == ("up-to-date", 0, None)
    assert _publication_cycle_status(
        pending_entries=(old_session, current_failure),
        session_id="session-current",
        group_id="group-current",
        delivery=RecorderDeliveryRunResult(attempted=1, committed=0, pending=1),
    ) == ("backlogged", 1, "recorder-delivery-pending")

    current_backlog = SimpleNamespace(
        session_id="session-current",
        schema_id=RECORDER_STORAGE_SCHEMA,
        destination_id="group-current",
        last_error=None,
    )
    assert _publication_cycle_status(
        pending_entries=(old_group_failure, current_backlog),
        session_id="session-current",
        group_id="group-current",
        delivery=RecorderDeliveryRunResult(attempted=1, committed=1, pending=0),
    ) == ("publishing", 1, None)
