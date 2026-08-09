from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.federation.software_update import UpdateInspection
from catalog.flask_app.services import federation_update_service as module
from catalog.flask_app.services.federation_update_events import (
    APPLY_REQUEST_EVENT,
    CHECK_REQUEST_EVENT,
)
from catalog.flask_app.services.federation_update_service import (
    FederationUpdateService,
)

ACTOR = "node-owner"
REMOTE = "node-remote"
OFFLINE = "node-offline"
CURRENT = "1" * 40
TARGET = "2" * 40


class _Local:
    def __init__(self, state_file: Path | None = None) -> None:
        self.state_file = state_file
        self.apply_calls: list[tuple[str, str | None]] = []
        self.latest: UpdateInspection | None = None

    def inspect(
        self,
        *,
        target: str | None = None,
        fetch: bool = True,
    ) -> UpdateInspection:
        assert fetch is True
        resolved = target or TARGET
        return UpdateInspection(
            "update_available",
            CURRENT,
            resolved,
            running_commit=CURRENT,
        )

    def apply(
        self,
        target: str,
        *,
        request_id: str | None = None,
    ) -> UpdateInspection:
        if self.state_file is not None:
            persisted = json.loads(self.state_file.read_text(encoding="utf-8"))
            assert persisted["operation"] == "apply"
            assert ACTOR in persisted["expected_update_node_ids"]
        self.apply_calls.append((target, request_id))
        return UpdateInspection(
            "activation_queued",
            CURRENT,
            target,
            "host_activation_queued",
            "queued",
            CURRENT,
            request_id,
        )

    def latest_result(self) -> UpdateInspection | None:
        return self.latest


class _Coordinator:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.session = SimpleNamespace(created_by_node_id=ACTOR)
        self.store = self

    def get_session(self, session_id: str) -> object:
        assert session_id == "session-one"
        return self.session

    def append_event(self, **kwargs: Any) -> object:
        self.events.append(dict(kwargs))
        return SimpleNamespace(revision=len(self.events))

    def replay_page(self, **_kwargs: Any) -> tuple[tuple[object, ...], int]:
        return (), 0


class _Authority:
    devices: tuple[object, ...] = ()

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def snapshot(self) -> object:
        return SimpleNamespace(available=True, devices=self.devices)


def _device(node_id: str, state: str, label: str) -> object:
    return SimpleNamespace(node_id=node_id, state=state, label=label)


def _install_context(
    monkeypatch: pytest.MonkeyPatch,
    coordinator: _Coordinator,
    devices: tuple[object, ...],
) -> None:
    context = SimpleNamespace(
        coordinator=coordinator,
        binding=SimpleNamespace(internal_session_id="session-one"),
        credentials=SimpleNamespace(
            identity=SimpleNamespace(node_id=ACTOR)
        ),
    )
    onboarding = SimpleNamespace(authorized_context=lambda: context)
    monkeypatch.setattr(
        module,
        "get_capability_onboarding_service",
        lambda: onboarding,
    )
    _Authority.devices = devices
    monkeypatch.setattr(module, "FederationAuthorityAdapter", _Authority)


def test_check_targets_only_devices_reported_connected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    coordinator = _Coordinator()
    _install_context(
        monkeypatch,
        coordinator,
        (
            _device(ACTOR, "connected", "Owner"),
            _device(REMOTE, "connected", "Remote"),
            _device(OFFLINE, "disconnected", "Offline"),
        ),
    )
    service = FederationUpdateService(_Local(), tmp_path / "updates.json")

    snapshot = service.check()

    check_events = [
        item for item in coordinator.events
        if item["event_type"] == CHECK_REQUEST_EVENT
    ]
    assert len(check_events) == 1
    assert check_events[0]["actor_node_id"] == ACTOR
    assert check_events[0]["payload"]["target_node_ids"] == [REMOTE]
    by_id = {item["node_id"]: item for item in snapshot["devices"]}
    assert by_id[REMOTE]["state"] == "checking"
    assert by_id[OFFLINE]["state"] == "offline"
    assert by_id[OFFLINE]["reachable"] is False


def test_update_all_rechecks_reachability_and_queues_coordinator_last(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state_file = tmp_path / "updates.json"
    local = _Local(state_file)
    coordinator = _Coordinator()
    _install_context(
        monkeypatch,
        coordinator,
        (
            _device(ACTOR, "connected", "Owner"),
            _device(REMOTE, "disconnected", "Remote"),
        ),
    )
    service = FederationUpdateService(local, state_file)
    now = datetime.now(timezone.utc)
    service._save(
        {
            "operation": "check",
            "status": "update_available",
            "request_id": "check-one",
            "checked_at": service._stamp(now),
            "check_expires_at": service._stamp(now + timedelta(minutes=5)),
            "report_deadline": service._stamp(now - timedelta(seconds=1)),
            "target_commit": TARGET,
            "expected_report_node_ids": [],
            "eligible_count": 2,
            "devices": [
                service._device(
                    ACTOR,
                    "Owner",
                    UpdateInspection(
                        "update_available",
                        CURRENT,
                        TARGET,
                        running_commit=CURRENT,
                    ),
                ),
                service._device(
                    REMOTE,
                    "Remote",
                    UpdateInspection(
                        "update_available",
                        CURRENT,
                        TARGET,
                        running_commit=CURRENT,
                    ),
                ),
            ],
        }
    )

    rollout = service.update_all(confirmed_target=TARGET)

    assert not any(
        item["event_type"] == APPLY_REQUEST_EVENT
        for item in coordinator.events
    )
    assert len(local.apply_calls) == 1
    assert local.apply_calls[0][0] == TARGET
    assert rollout["expected_update_node_ids"] == [ACTOR]
    by_id = {item["node_id"]: item for item in rollout["devices"]}
    assert by_id[REMOTE]["state"] == "offline"
    assert by_id[REMOTE]["code"] == "node_offline_not_queued"


def test_runtime_success_requires_exact_running_commit() -> None:
    stale = FederationUpdateService._normalize_runtime(
        UpdateInspection(
            "up_to_date",
            TARGET,
            TARGET,
            running_commit=CURRENT,
        )
    )
    false_success = FederationUpdateService._normalize_runtime(
        UpdateInspection(
            "runtime_verified",
            TARGET,
            TARGET,
            running_commit=CURRENT,
        )
    )
    verified = FederationUpdateService._normalize_runtime(
        UpdateInspection(
            "runtime_verified",
            TARGET,
            TARGET,
            running_commit=TARGET,
        )
    )

    assert stale.state == "activation_required"
    assert stale.code == "runtime_outdated"
    assert false_success.state == "error"
    assert false_success.code == "runtime_verification_mismatch"
    assert verified.state == "runtime_verified"


def test_apply_aggregation_is_updated_only_when_every_expected_node_verified(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    coordinator = _Coordinator()
    _install_context(monkeypatch, coordinator, ())
    service = FederationUpdateService(_Local(), tmp_path / "updates.json")
    context = SimpleNamespace(coordinator=coordinator)
    now = datetime.now(timezone.utc)
    base = {
        "operation": "apply",
        "request_id": "apply-one",
        "target_commit": TARGET,
        "report_deadline": service._stamp(now + timedelta(minutes=5)),
        "expected_update_node_ids": [REMOTE, OFFLINE],
        "devices": [
            service._device(
                REMOTE,
                "Remote",
                UpdateInspection(
                    "runtime_verified",
                    TARGET,
                    TARGET,
                    running_commit=TARGET,
                ),
            ),
            service._device(
                OFFLINE,
                "Other",
                UpdateInspection(
                    "runtime_verified",
                    TARGET,
                    TARGET,
                    running_commit=TARGET,
                ),
            ),
        ],
    }

    complete = service._refresh_apply(base, context, ACTOR)
    assert complete["status"] == "updated"

    failed = dict(base)
    failed["devices"] = [
        base["devices"][0],
        service._device(
            OFFLINE,
            "Other",
            UpdateInspection(
                "failed",
                CURRENT,
                TARGET,
                "activation_failed",
                "failed",
                CURRENT,
            ),
        ),
    ]
    partial = service._refresh_apply(failed, context, ACTOR)
    assert partial["status"] == "update_completed_with_failures"
