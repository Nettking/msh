"""Leader-issued, policy-bounded Federation capability requests.

The Federation creator may ask reachable *other* members to refresh their local
capability inspection, run the already-registered bounded benchmark plan, and
contribute every candidate that the member's existing local contribution policy
allows.  The request contains no executable, path, endpoint, credential,
benchmark identifier, candidate identifier, or provider grant.

Members independently authenticate the immutable session creator, execute only
local product services, retain local policy authority, and report bounded counts
back through the authoritative Federation event log.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from flask import current_app

from catalog.federation.control_commands import (
    ControlCommandEnvelope,
    ensure_bounded_json,
)
from catalog.federation.control_commands import (
    correlated_event_request_id as _event_request_id,
)
from catalog.federation.control_commands import (
    parse_utc_stamp as _parse_stamp,
)
from catalog.federation.control_commands import (
    stamp_utc as _stamp,
)
from catalog.federation.onboarding_models import (
    BenchmarkState,
    ContributionActivationState,
    ContributionDesiredState,
    ContributionPolicyState,
)
from catalog.federation.projections.authority_adapter import FederationAuthorityAdapter

from .capability_benchmark_service import get_capability_benchmark_service
from .capability_contribution_service import get_capability_contribution_service
from .capability_onboarding_service import get_capability_onboarding_service

EVENT_SCHEMA = "fcp.federation-capability-request-event.v1"
STATE_SCHEMA = "fcp.federation-capability-request-state.v1"
PROCESSOR_SCHEMA = "fcp.federation-capability-request-processor.v1"
REQUEST_EVENT = "capability.onboarding.requested"
REPORT_EVENT = "capability.onboarding.reported"
SESSION_CREATED_EVENT = "session.created"
REQUEST_ACTIONS = ("benchmark", "contribute")
MAX_TARGETS = 256
MAX_EVENT_BYTES = 8192
COMMAND_TTL = timedelta(minutes=10)
REPORT_WINDOW = timedelta(minutes=10)
CONNECTED_STATES = frozenset({"connected", "online", "ready", "active"})
TERMINAL_STATES = frozenset({"completed", "partial", "failed", "offline"})
_MAX_PROCESSOR_REPORTS = 32


def _bounded(value: object) -> None:
    ensure_bounded_json(
        value,
        max_bytes=MAX_EVENT_BYTES,
        error_code="capability_event_too_large",
    )


def request_payload(
    *,
    request_id: str,
    target_node_ids: tuple[str, ...],
    created_at: datetime,
    expires_at: datetime,
) -> dict[str, object]:
    envelope = ControlCommandEnvelope.issue(
        request_id=request_id,
        target_node_ids=target_node_ids,
        created_at=created_at,
        expires_at=expires_at,
        max_lifetime=COMMAND_TTL,
        max_targets=MAX_TARGETS,
    )
    value: dict[str, object] = {
        "schema": EVENT_SCHEMA,
        **envelope.payload_fields(),
        "actions": list(REQUEST_ACTIONS),
    }
    _bounded(value)
    return value


def validate_request_payload(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or value.get("schema") != EVENT_SCHEMA:
        raise ValueError("malformed_message")
    ControlCommandEnvelope.parse_payload(
        value,
        max_lifetime=COMMAND_TTL,
        max_targets=MAX_TARGETS,
        require_unique_targets=True,
    )
    actions = value.get("actions")
    if actions != list(REQUEST_ACTIONS):
        raise ValueError("unsupported_actions")
    _bounded(value)
    return value


def report_payload(
    *,
    request_id: str,
    node_id: str,
    state: str,
    benchmarks_attempted: int,
    benchmarks_passed: int,
    benchmark_errors: int,
    contribution_candidates: int,
    contributions_enabled: int,
    contributions_blocked: int,
    contribution_errors: int,
    message: str,
) -> dict[str, object]:
    if state not in {"completed", "partial", "failed"}:
        raise ValueError("invalid_report_state")
    if not request_id or len(request_id) > 128:
        raise ValueError("malformed_request_id")
    if not node_id or len(node_id) > 512:
        raise ValueError("malformed_node_id")
    counts = (
        benchmarks_attempted,
        benchmarks_passed,
        benchmark_errors,
        contribution_candidates,
        contributions_enabled,
        contributions_blocked,
        contribution_errors,
    )
    if any(
        isinstance(item, bool) or not isinstance(item, int) or item < 0
        for item in counts
    ):
        raise ValueError("malformed_report_counts")
    value: dict[str, object] = {
        "schema": EVENT_SCHEMA,
        "request_id": request_id,
        "node_id": node_id,
        "state": state,
        "benchmarks_attempted": benchmarks_attempted,
        "benchmarks_passed": benchmarks_passed,
        "benchmark_errors": benchmark_errors,
        "contribution_candidates": contribution_candidates,
        "contributions_enabled": contributions_enabled,
        "contributions_blocked": contributions_blocked,
        "contribution_errors": contribution_errors,
        "message": str(message)[:512],
        "reported_at": _stamp(datetime.now(timezone.utc)),
    }
    _bounded(value)
    return value


def parse_report(value: object) -> tuple[str, str, dict[str, object]] | None:
    if not isinstance(value, dict) or value.get("schema") != EVENT_SCHEMA:
        return None
    request_id = value.get("request_id")
    node_id = value.get("node_id")
    state = value.get("state")
    if not all(isinstance(item, str) and item for item in (request_id, node_id, state)):
        return None
    if state not in {"completed", "partial", "failed"}:
        return None
    count_fields = (
        "benchmarks_attempted",
        "benchmarks_passed",
        "benchmark_errors",
        "contribution_candidates",
        "contributions_enabled",
        "contributions_blocked",
        "contribution_errors",
    )
    normalized: dict[str, object] = {"state": state}
    for field in count_fields:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            return None
        normalized[field] = item
    message = value.get("message")
    if not isinstance(message, str):
        return None
    normalized["message"] = message[:512]
    reported_at = value.get("reported_at")
    try:
        _parse_stamp(reported_at)
    except (TypeError, ValueError):
        return None
    normalized["reported_at"] = reported_at
    return request_id, node_id, normalized


def _read_json(
    path: Path, *, schema: str, default: dict[str, object]
) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        if len(raw) > 256 * 1024:
            raise ValueError
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return dict(default)
    if not isinstance(value, dict) or value.get("schema") != schema:
        return dict(default)
    return value


def _write_json(path: Path, value: dict[str, object]) -> None:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if len(payload) > 256 * 1024:
        raise ValueError("capability_state_too_large")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        os.chmod(temporary, 0o600)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if fd >= 0:
            os.close(fd)
        temporary.unlink(missing_ok=True)


def _append_remote_event(
    service: Any,
    context: Any,
    *,
    event_type: str,
    payload: dict[str, object],
    request_id: str,
) -> None:
    remote = service.remote_store.load()
    if remote is None:
        context.coordinator.append_event(
            session_id=context.binding.internal_session_id,
            actor_node_id=context.credentials.identity.node_id,
            request_id=request_id,
            event_type=event_type,
            payload=payload,
        )
        return
    service.relay_runtime.append_session_event(
        remote,
        session_id=context.binding.internal_session_id,
        event_type=event_type,
        payload=payload,
        request_id=request_id,
    )


class FederationCapabilityRequestService:
    """Issue and aggregate leader requests for remote capability contribution."""

    def __init__(self, state_file: Path | str) -> None:
        self.state_file = Path(state_file)
        self._lock = threading.RLock()

    def _context(self) -> tuple[Any, str]:
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            raise PermissionError("federation_authority_required")
        actor = context.credentials.identity.node_id
        session = context.coordinator.store.get_session(
            context.binding.internal_session_id
        )
        if session is None or session.created_by_node_id != actor:
            raise PermissionError("capability_request_authority_required")
        return context, actor

    @staticmethod
    def _is_connected(state: str) -> bool:
        return state.casefold() in CONNECTED_STATES

    def _authority_devices(self, context: Any, actor: str) -> tuple[Any, ...]:
        snapshot = FederationAuthorityAdapter(
            context.coordinator,
            actor_node_id=actor,
            internal_session_id=context.binding.internal_session_id,
        ).snapshot()
        return snapshot.devices if snapshot.available else ()

    def _load(self) -> dict[str, object]:
        return _read_json(
            self.state_file,
            schema=STATE_SCHEMA,
            default={"schema": STATE_SCHEMA, "status": "not_requested", "devices": []},
        )

    def _save(self, value: dict[str, object]) -> None:
        _write_json(self.state_file, {**value, "schema": STATE_SCHEMA})

    @staticmethod
    def _devices_by_id(value: dict[str, object]) -> dict[str, dict[str, object]]:
        devices = value.get("devices")
        if not isinstance(devices, list):
            return {}
        return {
            str(item.get("node_id")): dict(item)
            for item in devices
            if isinstance(item, dict) and isinstance(item.get("node_id"), str)
        }

    def _reports(
        self,
        context: Any,
        actor: str,
        *,
        request_id: str,
    ) -> dict[str, dict[str, object]]:
        reports: dict[str, dict[str, object]] = {}
        last_revision = 0
        for _ in range(128):
            events, current_revision = context.coordinator.replay_page(
                session_id=context.binding.internal_session_id,
                actor_node_id=actor,
                last_applied_revision=last_revision,
                limit=1000,
            )
            for event in events:
                last_revision = int(event.revision)
                if event.event_type != REPORT_EVENT:
                    continue
                parsed = parse_report(event.payload)
                if parsed is None:
                    continue
                reported_request, node_id, report = parsed
                if reported_request != request_id or node_id != event.actor_node_id:
                    continue
                reports[node_id] = report
            if not events or last_revision >= current_revision:
                break
        return reports

    def _refresh(
        self,
        value: dict[str, object],
        context: Any,
        actor: str,
    ) -> dict[str, object]:
        request_id = value.get("request_id")
        if not isinstance(request_id, str):
            return value
        expected = tuple(
            item
            for item in value.get("expected_report_node_ids", [])
            if isinstance(item, str)
        )
        devices = self._devices_by_id(value)
        for node_id, report in self._reports(
            context,
            actor,
            request_id=request_id,
        ).items():
            if node_id not in expected or node_id not in devices:
                continue
            devices[node_id] = {**devices[node_id], **report, "reachable": True}

        deadline_raw = value.get("report_deadline")
        try:
            deadline = _parse_stamp(deadline_raw)
        except (TypeError, ValueError):
            deadline = None
        if deadline is not None and datetime.now(timezone.utc) >= deadline:
            for node_id in expected:
                current = devices.get(node_id)
                if current is not None and current.get("state") == "requested":
                    devices[node_id] = {
                        **current,
                        "state": "failed",
                        "reachable": False,
                        "message": "The member did not report completion before the bounded request deadline.",
                    }

        ordered = list(devices.values())
        expected_states = [
            devices[node_id].get("state") for node_id in expected if node_id in devices
        ]
        if any(state == "requested" for state in expected_states):
            status = "requested"
        elif expected_states and all(state == "completed" for state in expected_states):
            status = "completed"
        elif expected_states:
            status = "completed_with_failures"
        else:
            status = str(value.get("status") or "not_requested")
        return {**value, "status": status, "devices": ordered}

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            value = self._load()
            try:
                context, actor = self._context()
                refreshed = self._refresh(value, context, actor)
            except Exception:  # noqa: BLE001 - passive status read fails closed
                return value
            if refreshed != value:
                self._save(refreshed)
            return refreshed

    def request_all(self) -> dict[str, object]:
        context, actor = self._context()
        with self._lock:
            now = datetime.now(timezone.utc)
            request_id = f"capability-{uuid.uuid4().hex}"
            targets: list[str] = []
            devices: list[dict[str, object]] = []
            for device in self._authority_devices(context, actor):
                if device.node_id == actor:
                    continue
                reachable = self._is_connected(device.state)
                if reachable:
                    targets.append(device.node_id)
                devices.append(
                    {
                        "node_id": device.node_id[:512],
                        "label": str(device.label)[:128],
                        "reachable": reachable,
                        "state": "requested" if reachable else "offline",
                        "message": (
                            "Waiting for this member to benchmark its local capabilities and contribute policy-allowed services."
                            if reachable
                            else "This member was offline and no capability request was queued."
                        ),
                    }
                )
            if targets:
                context.coordinator.append_event(
                    session_id=context.binding.internal_session_id,
                    actor_node_id=actor,
                    request_id=f"capability-request-{request_id}",
                    event_type=REQUEST_EVENT,
                    payload=request_payload(
                        request_id=request_id,
                        target_node_ids=tuple(targets),
                        created_at=now,
                        expires_at=now + COMMAND_TTL,
                    ),
                )
            value: dict[str, object] = {
                "schema": STATE_SCHEMA,
                "status": "requested" if targets else "no_reachable_members",
                "request_id": request_id,
                "requested_at": _stamp(now),
                "report_deadline": _stamp(now + REPORT_WINDOW),
                "expected_report_node_ids": targets,
                "devices": devices,
            }
            self._save(value)
            return value


class FederationCapabilityRequestProcessor:
    """Replay authenticated leader requests and execute local bounded services."""

    def __init__(self, service: Any, state_file: Path | str) -> None:
        self.service = service
        self.state_file = Path(state_file)

    @staticmethod
    def _empty_state() -> dict[str, object]:
        return {
            "schema": PROCESSOR_SCHEMA,
            "last_revision": 0,
            "authority_node_id": None,
            "pending_reports": {},
        }

    def _load(self) -> dict[str, object]:
        return _read_json(
            self.state_file,
            schema=PROCESSOR_SCHEMA,
            default=self._empty_state(),
        )

    def _save(self, state: dict[str, object]) -> None:
        pending = state.get("pending_reports")
        if isinstance(pending, dict) and len(pending) > _MAX_PROCESSOR_REPORTS:
            ordered = list(pending.items())[-_MAX_PROCESSOR_REPORTS:]
            state["pending_reports"] = dict(ordered)
        _write_json(self.state_file, {**state, "schema": PROCESSOR_SCHEMA})

    @staticmethod
    def _pin_authority(state: dict[str, object], event: Any) -> str | None:
        current = state.get("authority_node_id")
        if current is not None and not isinstance(current, str):
            raise ValueError("malformed_pinned_capability_authority")
        if event.event_type != SESSION_CREATED_EVENT:
            return current
        candidate = getattr(event, "actor_node_id", None)
        if not isinstance(candidate, str) or not candidate:
            raise ValueError("missing_session_creator_identity")
        if isinstance(current, str) and current != candidate:
            raise ValueError("session_creator_identity_changed")
        state["authority_node_id"] = candidate
        return candidate

    def _flush_reports(self, context: Any, state: dict[str, object]) -> None:
        pending = state.get("pending_reports")
        if not isinstance(pending, dict) or not pending:
            return
        node_id = context.credentials.identity.node_id
        changed = False
        for federation_request_id, payload in list(pending.items()):
            if not isinstance(federation_request_id, str) or not isinstance(
                payload, dict
            ):
                pending.pop(federation_request_id, None)
                changed = True
                continue
            _append_remote_event(
                self.service,
                context,
                event_type=REPORT_EVENT,
                payload=payload,
                request_id=_event_request_id(
                    "capability-report", federation_request_id, node_id
                ),
            )
            pending.pop(federation_request_id, None)
            changed = True
        if changed:
            state["pending_reports"] = pending
            self._save(state)

    @staticmethod
    def _execution_report(request_id: str, node_id: str) -> dict[str, object]:
        benchmark_service = get_capability_benchmark_service()
        contribution_service = get_capability_contribution_service()

        benchmarks_attempted = 0
        benchmarks_passed = 0
        benchmark_errors = 0
        contribution_candidates = 0
        contributions_enabled = 0
        contributions_blocked = 0
        contribution_errors = 0

        try:
            snapshot = benchmark_service.inspection_service.run()
            for item in benchmark_service.plan(snapshot):
                if not item.runnable:
                    continue
                benchmarks_attempted += 1
                try:
                    result = benchmark_service.run(
                        benchmark_id=item.benchmark_id,
                        target_service_id=item.target_service_id,
                    )
                except Exception:  # noqa: BLE001 - bounded runner failure is reported
                    benchmark_errors += 1
                    continue
                if result.state is BenchmarkState.PASSED:
                    benchmarks_passed += 1

            candidates = contribution_service.recommend(require_benchmark_review=True)
            contribution_candidates = len(candidates)
            allowed = tuple(
                candidate
                for candidate in candidates
                if candidate.policy_state is ContributionPolicyState.ALLOWED
                and not candidate.missing_prerequisites
            )
            contributions_blocked = contribution_candidates - len(allowed)
            for candidate in allowed:
                try:
                    outcomes = contribution_service.apply_choices(
                        {candidate.candidate_id: ContributionDesiredState.ENABLED.value}
                    )
                except Exception:  # noqa: BLE001 - local policy remains authoritative
                    contribution_errors += 1
                    continue
                if not outcomes:
                    contribution_errors += 1
                    continue
                outcome = outcomes[0]
                if (
                    outcome.desired_state is ContributionDesiredState.ENABLED
                    and outcome.activation_state
                    in {
                        ContributionActivationState.ACTIVE,
                        ContributionActivationState.PENDING,
                    }
                ):
                    contributions_enabled += 1
                elif outcome.activation_state is ContributionActivationState.BLOCKED:
                    contributions_blocked += 1
                else:
                    contribution_errors += 1
        except Exception as exc:  # noqa: BLE001 - failure becomes a bounded report
            return report_payload(
                request_id=request_id,
                node_id=node_id,
                state="failed",
                benchmarks_attempted=benchmarks_attempted,
                benchmarks_passed=benchmarks_passed,
                benchmark_errors=max(1, benchmark_errors),
                contribution_candidates=contribution_candidates,
                contributions_enabled=contributions_enabled,
                contributions_blocked=contributions_blocked,
                contribution_errors=contribution_errors,
                message=(
                    "The bounded capability request stopped safely before completion "
                    f"({type(exc).__name__})."
                ),
            )

        operation_errors = benchmark_errors + contribution_errors
        state = "completed" if operation_errors == 0 else "partial"
        message = (
            f"Benchmarked {benchmarks_attempted} target(s); "
            f"{benchmarks_passed} passed; enabled {contributions_enabled} "
            f"policy-allowed contribution(s); {contributions_blocked} candidate(s) "
            "remained blocked or ineligible."
        )
        if operation_errors:
            message += f" {operation_errors} bounded operation(s) reported an error."
        return report_payload(
            request_id=request_id,
            node_id=node_id,
            state=state,
            benchmarks_attempted=benchmarks_attempted,
            benchmarks_passed=benchmarks_passed,
            benchmark_errors=benchmark_errors,
            contribution_candidates=contribution_candidates,
            contributions_enabled=contributions_enabled,
            contributions_blocked=contributions_blocked,
            contribution_errors=contribution_errors,
            message=message,
        )

    def process(self, context: Any) -> None:
        remote = self.service.remote_store.load()
        if remote is None:
            return
        state = self._load()
        self._flush_reports(context, state)
        last_revision = state.get("last_revision", 0)
        if (
            isinstance(last_revision, bool)
            or not isinstance(last_revision, int)
            or last_revision < 0
        ):
            last_revision = 0
        authority = state.get("authority_node_id")
        if not isinstance(authority, str) or not authority:
            authority = None
            last_revision = 0
            state["last_revision"] = 0
        local_node = context.credentials.identity.node_id

        for _ in range(64):
            events, current_revision = context.coordinator.replay_page(
                session_id=context.binding.internal_session_id,
                actor_node_id=local_node,
                last_applied_revision=last_revision,
                limit=32,
            )
            if not events:
                break
            for event in events:
                try:
                    authority = self._pin_authority(state, event)
                    if event.event_type != REQUEST_EVENT:
                        continue
                    if authority is None or event.actor_node_id != authority:
                        continue
                    payload = validate_request_payload(event.payload)
                    targets = payload["target_node_ids"]
                    if local_node not in targets:
                        continue
                    federation_request_id = str(payload["request_id"])
                    report = self._execution_report(
                        federation_request_id,
                        local_node,
                    )
                    pending = state.get("pending_reports")
                    if not isinstance(pending, dict):
                        pending = {}
                    pending[federation_request_id] = report
                    state["pending_reports"] = pending
                    self._save(state)
                    self._flush_reports(context, state)
                finally:
                    last_revision = int(event.revision)
                    state["last_revision"] = last_revision
                    self._save(state)
            if last_revision >= current_revision:
                break


def get_federation_capability_request_service() -> FederationCapabilityRequestService:
    configured = current_app.config.get("FEDERATION_CAPABILITY_REQUEST_SERVICE")
    if isinstance(configured, FederationCapabilityRequestService):
        return configured
    onboarding_database = Path(
        current_app.config.get(
            "CAPABILITY_ONBOARDING_STATE_DATABASE",
            "data/federation/onboarding/onboarding.sqlite3",
        )
    )
    federation_root = onboarding_database.parent.parent
    state_file = Path(
        current_app.config.get(
            "FEDERATION_CAPABILITY_REQUEST_STATE",
            federation_root / "capability-requests" / "leader-status.json",
        )
    )
    service = FederationCapabilityRequestService(state_file)
    current_app.config["FEDERATION_CAPABILITY_REQUEST_SERVICE"] = service
    return service


__all__ = [
    "EVENT_SCHEMA",
    "PROCESSOR_SCHEMA",
    "REPORT_EVENT",
    "REQUEST_EVENT",
    "FederationCapabilityRequestProcessor",
    "FederationCapabilityRequestService",
    "get_federation_capability_request_service",
    "parse_report",
    "report_payload",
    "request_payload",
    "validate_request_payload",
]
