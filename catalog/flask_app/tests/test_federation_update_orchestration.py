from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.update_drain import SQLiteNodeUpdateDrainStore
from catalog.federation.software_update import UpdateInspection
from catalog.flask_app.services import federation_update_service as module
from catalog.flask_app.services.federation_update_events import (
    APPLY_REQUEST_EVENT,
    CHECK_REQUEST_EVENT,
    report_payload,
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
        self.inspect_request_ids: list[str | None] = []
        self.latest: UpdateInspection | None = None

    def inspect(
        self,
        *,
        target: str | None = None,
        fetch: bool = True,
        request_id: str | None = None,
    ) -> UpdateInspection:
        assert fetch is True
        self.inspect_request_ids.append(request_id)
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
    capabilities: tuple[object, ...] = ()

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def snapshot(self) -> object:
        return SimpleNamespace(
            available=True,
            devices=self.devices,
            capabilities=self.capabilities,
        )


class _EventCoordinator(_Coordinator):
    def append_event(self, **kwargs: Any) -> object:
        event = SimpleNamespace(
            revision=len(self.events) + 1,
            session_id=kwargs["session_id"],
            event_type=kwargs["event_type"],
            actor_node_id=kwargs["actor_node_id"],
            payload=kwargs["payload"],
        )
        self.events.append(event)
        return event

    def replay_page(self, **kwargs: Any) -> tuple[tuple[object, ...], int]:
        start = kwargs["last_applied_revision"]
        return (
            tuple(event for event in self.events if event.revision > start),
            len(self.events),
        )

    def append_report(self, *, node_id: str, payload: dict[str, object]) -> None:
        self.append_event(
            session_id="session-one",
            event_type="software.update.apply.reported",
            actor_node_id=node_id,
            payload=payload,
        )


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
    _Authority.capabilities = ()
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
    local = _Local()
    service = FederationUpdateService(local, tmp_path / "updates.json")

    snapshot = service.check()

    # The other half of the check contract: an operator-initiated local check
    # owns no durable command identity, so it passes none and the handoff mints
    # a fresh one per call. Only replayed Federation commands carry one.
    assert local.inspect_request_ids == [None]
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


def test_update_all_rejects_while_remote_check_is_pending(
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
            _device(REMOTE, "connected", "Remote"),
        ),
    )
    service = FederationUpdateService(local, state_file)
    now = datetime.now(timezone.utc)
    service._save(
        {
            "operation": "check",
            "status": "checking",
            "request_id": "check-pending",
            "checked_at": service._stamp(now),
            "check_expires_at": service._stamp(now + timedelta(minutes=5)),
            "report_deadline": service._stamp(now + timedelta(seconds=30)),
            "target_commit": TARGET,
            "expected_report_node_ids": [REMOTE],
            "eligible_count": 1,
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
                        "checking",
                        target_commit=TARGET,
                    ),
                ),
            ],
        }
    )

    with pytest.raises(ValueError, match="check_in_progress"):
        service.update_all(confirmed_target=TARGET)

    assert local.apply_calls == []
    assert not any(
        item["event_type"] == APPLY_REQUEST_EVENT
        for item in coordinator.events
    )


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
    context = SimpleNamespace(
        coordinator=coordinator,
        binding=SimpleNamespace(internal_session_id="session-one"),
    )
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


def test_update_all_serializes_remote_then_local_activation_behind_drain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    coordinator = _EventCoordinator()
    _install_context(
        monkeypatch,
        coordinator,
        (
            _device(ACTOR, "connected", "Owner"),
            _device(REMOTE, "connected", "Remote"),
        ),
    )
    _Authority.capabilities = (
        SimpleNamespace(node_id=ACTOR, capability_id="provider-owner"),
        SimpleNamespace(node_id=REMOTE, capability_id="provider-remote"),
    )
    local = _Local()
    drains = SQLiteNodeUpdateDrainStore(
        SQLiteJobLifecycleStore(tmp_path / "analysis-jobs.sqlite3")
    )
    service = FederationUpdateService(
        local,
        tmp_path / "updates.json",
        drain_store=drains,
    )
    now = datetime.now(timezone.utc)
    service._save(
        {
            "operation": "check",
            "status": "update_available",
            "request_id": "check-rolling",
            "checked_at": service._stamp(now),
            "check_expires_at": service._stamp(now + timedelta(minutes=5)),
            "report_deadline": service._stamp(now + timedelta(minutes=5)),
            "target_commit": TARGET,
            "expected_report_node_ids": [],
            "eligible_count": 2,
            "devices": [
                service._device(
                    ACTOR,
                    "Owner",
                    UpdateInspection("update_available", CURRENT, TARGET, running_commit=CURRENT),
                ),
                service._device(
                    REMOTE,
                    "Remote",
                    UpdateInspection("update_available", CURRENT, TARGET, running_commit=CURRENT),
                ),
            ],
        }
    )

    first = service.update_all(confirmed_target=TARGET)
    apply_events = [
        event for event in coordinator.events if event.event_type == APPLY_REQUEST_EVENT
    ]
    assert len(apply_events) == 1
    assert apply_events[0].payload["target_node_ids"] == [REMOTE]
    assert apply_events[0].payload["request_id"] == first["request_id"]
    assert apply_events[0].payload["drain_node_id"] == REMOTE
    assert apply_events[0].payload["drain_provider_ids"] == ["provider-remote"]
    assert local.apply_calls == []

    coordinator.append_report(
        node_id=REMOTE,
        payload=report_payload(
            request_id=str(first["request_id"]),
            node_id=REMOTE,
            result=UpdateInspection(
                "runtime_verified",
                TARGET,
                TARGET,
                running_commit=TARGET,
            ),
        ),
    )
    second = service.snapshot()
    assert len(local.apply_calls) == 1
    assert second["rollout"]["current_index"] == 1
    by_id = {item["node_id"]: item for item in second["devices"]}
    assert by_id[ACTOR]["state"] == "activation_queued"
    assert drains.get(session_id="session-one", node_id=REMOTE) is None
    assert drains.get(session_id="session-one", node_id=ACTOR) is not None

    local.latest = UpdateInspection(
        "runtime_verified",
        TARGET,
        TARGET,
        running_commit=TARGET,
        request_id=second["local_host_request_id"],
    )
    complete = service.snapshot()

    assert complete["status"] == "updated"
    assert complete["rollout"]["state"] == "completed"
    owner_drain = drains.get(session_id="session-one", node_id=ACTOR)
    assert owner_drain is not None
    assert owner_drain.state.value == "ready"


# Native MTConnect capture has its own correlated graceful-stop protocol. It
# does not register an F7 job executor or an authoritative F7 ownership store.
def _capture_capability(**changes: Any) -> dict[str, Any]:
    value = {
        "session_id": "session-one",
        "node_id": REMOTE,
        "capability_id": f"recorder-{REMOTE}",
        "type": "recorder",
        "protocol": "mtconnect",
        "protocol_version": "1",
        "status": "ready",
        "properties": {"kind": "standalone-recorder"},
    }
    value.update(changes)
    return value


def _projected_capabilities(*rows: dict[str, Any]) -> tuple[object, ...]:
    from catalog.federation.projections.authority_adapter import (
        FederationAuthorityAdapter,
    )

    adapter = FederationAuthorityAdapter(
        None, actor_node_id=ACTOR, internal_session_id="session-one"
    )
    return adapter._capabilities({"capabilities": rows}, {ACTOR, REMOTE})


@pytest.mark.parametrize("capability_id", [f"recorder-{REMOTE}", "recorder-local"])
def test_capture_update_reaches_existing_native_handoff_without_f7_authority(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capability_id: str
) -> None:
    from catalog.mtconnect_recorder.federation_update import (
        RecorderFederationUpdateWorker,
    )
    from catalog.mtconnect_recorder.tests.test_recorder_federation_update import (
        _Handoff,
        _Node,
    )

    coordinator = _EventCoordinator()
    _install_context(
        monkeypatch, coordinator,
        (_device(ACTOR, "connected", "Owner"), _device(REMOTE, "connected", "Recorder")),
    )
    _Authority.capabilities = _projected_capabilities(
        _capture_capability(capability_id=capability_id)
    )
    local = _Local()
    service = FederationUpdateService(local, tmp_path / "updates.json")
    now = datetime.now(timezone.utc)
    service._save({
        "operation": "check", "status": "update_available", "request_id": "check-capture",
        "checked_at": service._stamp(now),
        "check_expires_at": service._stamp(now + timedelta(minutes=5)),
        "report_deadline": service._stamp(now + timedelta(minutes=5)),
        "target_commit": TARGET, "expected_report_node_ids": [], "eligible_count": 1,
        "devices": [
            service._device(ACTOR, "Owner", UpdateInspection("up_to_date", TARGET, TARGET, running_commit=TARGET)),
            service._device(REMOTE, "Recorder", UpdateInspection("update_available", CURRENT, TARGET, running_commit=CURRENT)),
        ],
    })
    rollout = service.update_all(confirmed_target=TARGET)
    command = next(e for e in coordinator.events if e.event_type == APPLY_REQUEST_EVENT)
    assert "drain_node_id" not in command.payload
    assert "drain_provider_ids" not in command.payload
    assert rollout["rollout"]["provider_ids_by_node"] == {REMOTE: []}

    # The real native processor has no F7 store. Exercise the authenticated
    # replay/handoff path, not merely a classification helper returning False.
    command.revision = 2
    created = SimpleNamespace(
        revision=1, session_id="session-one", event_type="session.created",
        actor_node_id=ACTOR, payload={"session_id": "session-one"},
    )
    node = _Node(tmp_path, (created, command))
    node.context.credentials.identity.node_id = REMOTE
    node.context.binding.internal_session_id = "session-one"
    handoff = _Handoff()
    worker = RecorderFederationUpdateWorker(node, data_directory=tmp_path, handoff=handoff)
    assert worker.processor.drain_store is None
    assert worker.process_once() is True
    assert len(handoff.applies) == 1 and handoff.applies[0][0] == TARGET
    assert handoff.applies[0][1].startswith("fed-")
    assert node.appended[-1]["payload"]["state"] == "activation_queued"
    assert local.apply_calls == []


@pytest.mark.parametrize("change", [
    {"type": "analysis"},
    {"protocol": "registered-compute"},
    {"protocol_version": "2"},
    {"capability_id": "compute-provider-recorder-like"},
    {"capability_id": "recorder-another-node"},
    {"properties": {}},
    {"properties": {"kind": "registered-compute-handler"}},
    {"properties": {"kind": " standalone-recorder "}},
])
def test_unknown_or_changed_capture_metadata_remains_in_f7_drain(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: dict[str, Any]
) -> None:
    coordinator = _EventCoordinator()
    _install_context(monkeypatch, coordinator, (_device(REMOTE, "connected", "Remote"),))
    _Authority.capabilities = _projected_capabilities(_capture_capability(**change))
    service = FederationUpdateService(_Local(), tmp_path / "updates.json")
    assert service._provider_ids_by_node(module.get_capability_onboarding_service().authorized_context(), ACTOR) == {
        REMOTE: (_Authority.capabilities[0].capability_id,)
    }


def test_capture_classification_never_excludes_other_providers_on_same_node(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    coordinator = _EventCoordinator()
    _install_context(monkeypatch, coordinator, (_device(REMOTE, "connected", "Mixed"),))
    _Authority.capabilities = _projected_capabilities(
        _capture_capability(),
        _capture_capability(capability_id="analysis-provider", type="analysis", protocol="analysis"),
        _capture_capability(capability_id="storage-provider", type="storage", protocol="storage"),
    )
    service = FederationUpdateService(_Local(), tmp_path / "updates.json")
    providers = service._provider_ids_by_node(module.get_capability_onboarding_service().authorized_context(), ACTOR)
    assert providers == {REMOTE: ("analysis-provider", "storage-provider")}

    from catalog.flask_app.services.federation_update_events import (
        FederationUpdateEventProcessor,
    )

    processor = FederationUpdateEventProcessor(object(), object(), tmp_path / "processor.json")
    with pytest.raises(ValueError, match="update-drain-unavailable"):
        processor._request_drain(
            {"drain_node_id": REMOTE, "drain_provider_ids": list(providers[REMOTE])},
            session_id="session-one", local_node=REMOTE,
        )

    # Classifying one capture capability must not remove the actual F7 fence
    # for another provider on that node, or declare its owned job quiescent.
    from catalog.capabilities.tests import test_update_drain as workload
    from catalog.capabilities.update_drain import NodeUpdateDrainTarget
    from catalog.federation.errors import FederationValidationError

    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    drains = SQLiteNodeUpdateDrainStore(jobs)
    workload._claim(jobs, provider_id="analysis-provider")
    drains.request_drain(
        session_id=workload.SESSION,
        target=NodeUpdateDrainTarget(REMOTE, providers[REMOTE]),
        command_id="mixed-node-drain",
        now=workload.NOW + timedelta(seconds=3),
    )
    assert not drains.is_quiescent(session_id=workload.SESSION, node_id=REMOTE)
    with pytest.raises(FederationValidationError):
        workload._claim(jobs, provider_id="analysis-provider", job_id="new-job")


def test_capture_kind_projection_is_bounded_and_session_membership_scoped() -> None:
    rows = _projected_capabilities(
        _capture_capability(),
        _capture_capability(session_id="other-session"),
        _capture_capability(node_id="other-node"),
        _capture_capability(capability_id="unknown", properties={"kind": "x" * 100000}),
    )
    assert len(rows) == 2
    assert {r.capability_id: r.kind for r in rows} == {
        f"recorder-{REMOTE}": "standalone-recorder", "unknown": None,
    }


@pytest.mark.parametrize("change", [
    {"type": " recorder "},
    {"protocol": " mtconnect "},
    {"protocol_version": " 1 "},
    {"capability_id": f" recorder-{REMOTE} "},
    {"node_id": f" {REMOTE} ", "capability_id": "recorder-local"},
])
def test_raw_noncanonical_capture_metadata_never_gains_exemption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: dict[str, Any]
) -> None:
    from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
    from catalog.federation.projections.authority_adapter import (
        FederationAuthorityAdapter,
    )

    values = _capture_capability(**change)
    values["status"] = CapabilityStatus.READY
    values["announced_at"] = datetime.now(timezone.utc)
    announcement = CapabilityAnnouncement(**values)
    # The actual model retains these strings. Presentation normalization must
    # not turn them into a proof of capture-only update semantics.
    assert all(getattr(announcement, key) == value for key, value in change.items())
    adapter = FederationAuthorityAdapter(
        None, actor_node_id=ACTOR, internal_session_id="session-one"
    )
    records = adapter._capabilities(
        {"capabilities": [announcement]}, {ACTOR, announcement.node_id}
    )
    assert len(records) == 1
    coordinator = _EventCoordinator()
    _install_context(monkeypatch, coordinator, (_device(REMOTE, "connected", "Remote"),))
    _Authority.capabilities = records
    service = FederationUpdateService(_Local(), tmp_path / "updates.json")
    assert records[0].kind is None
    assert service._provider_ids_by_node(
        module.get_capability_onboarding_service().authorized_context(), ACTOR
    ) == {records[0].node_id: (records[0].capability_id,)}
