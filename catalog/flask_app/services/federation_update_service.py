"""Manual-only orchestration for verified Federation FCP software updates."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from flask import current_app

from catalog.federation.projections.authority_adapter import FederationAuthorityAdapter
from catalog.federation.software_update import (
    APPROVED_BRANCH,
    APPROVED_REPOSITORY,
    OID_RE,
    UpdateInspection,
)

from .capability_onboarding_service import get_capability_onboarding_service
from .federation_update_events import (
    APPLY_REPORT_EVENT,
    APPLY_REQUEST_EVENT,
    CHECK_REPORT_EVENT,
    CHECK_REQUEST_EVENT,
    command_payload,
    inspection_from_report,
)
from .federation_update_handoff import HostUpdateHandoff

SCHEMA = "fcp.federation-update.v2"
LEGACY_SCHEMA = "fcp.federation-update.v1"
CHECK_FRESHNESS = timedelta(minutes=10)
CHECK_REPORT_WINDOW = timedelta(seconds=45)
APPLY_REPORT_WINDOW = timedelta(minutes=20)
COMMAND_TTL = timedelta(minutes=10)
ELIGIBLE_STATES = frozenset({"update_available", "activation_required"})
PENDING_APPLY_STATES = frozenset({"activation_queued", "updating", "requested"})
CONNECTED_STATES = frozenset({"connected", "online", "ready", "active"})


class LocalUpdateAdapter(Protocol):
    def inspect(
        self,
        *,
        target: str | None = None,
        fetch: bool = True,
    ) -> UpdateInspection: ...

    def apply(
        self,
        target: str,
        *,
        request_id: str | None = None,
    ) -> UpdateInspection: ...

    def latest_result(self) -> UpdateInspection | None: ...


@dataclass(frozen=True)
class UpdateIntent:
    """Retained bounded direct-handler contract for compatibility and tests."""

    request_id: str
    session_id: str
    sender_node_id: str
    repository: str
    branch: str
    target_commit: str
    created_at: str
    expires_at: str
    schema: str = LEGACY_SCHEMA

    def validate(self) -> None:
        if self.schema != LEGACY_SCHEMA or len(json.dumps(self.__dict__)) > 4096:
            raise ValueError("malformed_message")
        if not OID_RE.fullmatch(self.target_commit):
            raise ValueError("malformed_target")
        if self.repository != APPROVED_REPOSITORY or self.branch != APPROVED_BRANCH:
            raise ValueError("unapproved_source")
        if len(self.request_id) > 128 or not self.request_id:
            raise ValueError("malformed_request_id")


class FederationUpdateService:
    """Coordinate exact-commit updates without granting Flask host execution.

    The authoritative session creator may publish declarative update intents to
    the existing Federation event log.  Each target device independently asks
    its local host-owned agent to validate Git, fast-forward, rebuild, restart,
    and prove the running build commit.  A checkout update alone is never
    counted as success.
    """

    def __init__(self, local: LocalUpdateAdapter, state_file: Path | str) -> None:
        self.local = local
        self.state_file = Path(state_file)
        self._lock = threading.RLock()

    def _context(self):
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            raise PermissionError("federation_authority_required")
        session = context.coordinator.store.get_session(
            context.binding.internal_session_id
        )
        actor = context.credentials.identity.node_id
        if session is None or session.created_by_node_id != actor:
            raise PermissionError("update_authority_required")
        return context, actor

    def _authorize_intent(self, intent: UpdateIntent) -> None:
        intent.validate()
        context = get_capability_onboarding_service().authorized_context()
        if context is None or intent.session_id != context.binding.internal_session_id:
            raise PermissionError("wrong_federation")
        session = context.coordinator.store.get_session(intent.session_id)
        if session is None or session.created_by_node_id != intent.sender_node_id:
            raise PermissionError("unauthorized_sender")
        now = datetime.now(timezone.utc)
        try:
            created = datetime.fromisoformat(intent.created_at.replace("Z", "+00:00"))
            expires = datetime.fromisoformat(intent.expires_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("malformed_timestamp") from exc
        if (
            created.tzinfo is None
            or expires.tzinfo is None
            or created > now + timedelta(minutes=1)
            or expires <= now
            or expires - created > timedelta(minutes=15)
        ):
            raise ValueError("expired_or_invalid_request")

    def receive_check(self, intent: UpdateIntent) -> UpdateInspection:
        self._authorize_intent(intent)
        return self._normalize_runtime(
            self.local.inspect(target=intent.target_commit, fetch=True)
        )

    def receive_apply(self, intent: UpdateIntent) -> UpdateInspection:
        self._authorize_intent(intent)
        return self.local.apply(intent.target_commit, request_id=intent.request_id)

    @staticmethod
    def _normalize_runtime(result: UpdateInspection) -> UpdateInspection:
        if (
            result.state == "up_to_date"
            and result.target_commit
            and result.running_commit != result.target_commit
        ):
            return UpdateInspection(
                "activation_required",
                result.current_commit,
                result.target_commit,
                "runtime_outdated",
                "The source is current, but the running FCP build is not verified at the target commit.",
                result.running_commit,
                result.request_id,
            )
        if (
            result.state == "runtime_verified"
            and (
                not result.target_commit
                or result.running_commit != result.target_commit
            )
        ):
            return UpdateInspection(
                "error",
                result.current_commit,
                result.target_commit,
                "runtime_verification_mismatch",
                "The host reported completion without proving the requested running commit.",
                result.running_commit,
                result.request_id,
            )
        return result

    @staticmethod
    def _device(
        node_id: str,
        label: str,
        result: UpdateInspection,
        *,
        reachable: bool = True,
    ) -> dict[str, object]:
        return {
            "node_id": node_id[:512],
            "label": label[:128],
            "reachable": reachable,
            **result.to_dict(),
        }

    def _load(self) -> dict[str, object]:
        try:
            raw = self.state_file.read_bytes()
            if len(raw) > 256 * 1024:
                raise ValueError
            value = json.loads(raw.decode("utf-8"))
            if not isinstance(value, dict) or value.get("schema") != SCHEMA:
                raise ValueError
            return value
        except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
            return {"schema": SCHEMA, "status": "not_checked", "devices": []}

    def _save(self, value: dict[str, object]) -> None:
        value = {**value, "schema": SCHEMA}
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(payload) > 256 * 1024:
            raise ValueError("update_state_too_large")
        self.state_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, name = tempfile.mkstemp(
            prefix=f".{self.state_file.name}.",
            dir=self.state_file.parent,
        )
        temporary = Path(name)
        try:
            os.chmod(temporary, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                descriptor = -1
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.state_file)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _parse_deadline(value: object) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _stamp(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _authority_devices(self, context: Any, actor: str) -> tuple[Any, ...]:
        snapshot = FederationAuthorityAdapter(
            context.coordinator,
            actor_node_id=actor,
            internal_session_id=context.binding.internal_session_id,
        ).snapshot()
        return snapshot.devices if snapshot.available else ()

    @staticmethod
    def _is_connected(state: str) -> bool:
        return state.casefold() in CONNECTED_STATES

    def _append_request_event(
        self,
        context: Any,
        actor: str,
        *,
        event_type: str,
        request_id: str,
        target: str,
        target_node_ids: tuple[str, ...],
        now: datetime,
    ) -> None:
        if not target_node_ids:
            return
        context.coordinator.append_event(
            session_id=context.binding.internal_session_id,
            actor_node_id=actor,
            request_id=f"{event_type}-{request_id}",
            event_type=event_type,
            payload=command_payload(
                request_id=request_id,
                target_commit=target,
                target_node_ids=target_node_ids,
                created_at=now,
                expires_at=now + COMMAND_TTL,
            ),
        )

    def _reports(
        self,
        context: Any,
        actor: str,
        *,
        event_type: str,
        request_id: str,
        target: str,
    ) -> dict[str, UpdateInspection]:
        reports: dict[str, UpdateInspection] = {}
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
                if event.event_type != event_type:
                    continue
                parsed = inspection_from_report(event.payload)
                if parsed is None:
                    continue
                reported_request, node_id, result = parsed
                if (
                    reported_request != request_id
                    or node_id != event.actor_node_id
                    or result.target_commit != target
                ):
                    continue
                reports[node_id] = self._normalize_runtime(result)
            if not events or last_revision >= current_revision:
                break
        return reports

    @staticmethod
    def _devices_by_id(value: dict[str, object]) -> dict[str, dict[str, object]]:
        raw = value.get("devices")
        if not isinstance(raw, list):
            return {}
        return {
            str(item.get("node_id")): dict(item)
            for item in raw
            if isinstance(item, dict) and isinstance(item.get("node_id"), str)
        }

    def _refresh_check(
        self,
        value: dict[str, object],
        context: Any,
        actor: str,
    ) -> dict[str, object]:
        request_id = value.get("request_id")
        target = value.get("target_commit")
        if not isinstance(request_id, str) or not isinstance(target, str):
            return value
        devices = self._devices_by_id(value)
        expected = tuple(
            item
            for item in value.get("expected_report_node_ids", [])
            if isinstance(item, str)
        )
        for node_id, result in self._reports(
            context,
            actor,
            event_type=CHECK_REPORT_EVENT,
            request_id=request_id,
            target=target,
        ).items():
            if node_id not in expected or node_id not in devices:
                continue
            prior = devices[node_id]
            devices[node_id] = self._device(
                node_id,
                str(prior.get("label") or node_id),
                result,
                reachable=True,
            )
        deadline = self._parse_deadline(value.get("report_deadline"))
        expired = deadline is not None and datetime.now(timezone.utc) >= deadline
        if expired:
            for node_id in expected:
                current = devices.get(node_id)
                if current is not None and current.get("state") == "checking":
                    devices[node_id] = self._device(
                        node_id,
                        str(current.get("label") or node_id),
                        UpdateInspection(
                            "unavailable",
                            target_commit=target,
                            code="check_timeout",
                            message="The device did not report its bounded update check in time.",
                        ),
                        reachable=False,
                    )
        ordered = list(devices.values())
        pending = any(item.get("state") == "checking" for item in ordered)
        eligible = sum(item.get("state") in ELIGIBLE_STATES for item in ordered)
        if pending:
            status = "checking"
        elif eligible:
            status = "update_available"
        elif ordered and all(item.get("state") == "up_to_date" for item in ordered):
            status = "up_to_date"
        else:
            status = "checked"
        return {
            **value,
            "status": status,
            "devices": ordered,
            "eligible_count": eligible,
        }

    def _refresh_apply(
        self,
        value: dict[str, object],
        context: Any,
        actor: str,
    ) -> dict[str, object]:
        request_id = value.get("request_id")
        target = value.get("target_commit")
        if not isinstance(request_id, str) or not isinstance(target, str):
            return value
        devices = self._devices_by_id(value)
        expected = tuple(
            item
            for item in value.get("expected_update_node_ids", [])
            if isinstance(item, str)
        )
        remote_expected = set(expected) - {actor}
        for node_id, result in self._reports(
            context,
            actor,
            event_type=APPLY_REPORT_EVENT,
            request_id=request_id,
            target=target,
        ).items():
            if node_id not in remote_expected or node_id not in devices:
                continue
            prior = devices[node_id]
            devices[node_id] = self._device(
                node_id,
                str(prior.get("label") or node_id),
                result,
                reachable=result.state != "error",
            )

        local_host_request = value.get("local_host_request_id")
        if actor in expected and isinstance(local_host_request, str):
            result_for = getattr(self.local, "result_for", None)
            if callable(result_for):
                latest = result_for(local_host_request)
            else:
                latest = self.local.latest_result()
            if (
                latest is not None
                and latest.request_id == local_host_request
                and latest.target_commit == target
            ):
                latest = self._normalize_runtime(latest)
                prior = devices.get(actor, {"label": "This device"})
                devices[actor] = self._device(
                    actor,
                    str(prior.get("label") or "This device"),
                    latest,
                    reachable=latest.state != "error",
                )

        deadline = self._parse_deadline(value.get("report_deadline"))
        expired = deadline is not None and datetime.now(timezone.utc) >= deadline
        if expired:
            for node_id in expected:
                current = devices.get(node_id)
                if current is None or current.get("state") not in PENDING_APPLY_STATES:
                    continue
                devices[node_id] = self._device(
                    node_id,
                    str(current.get("label") or node_id),
                    UpdateInspection(
                        "failed",
                        target_commit=target,
                        code="activation_timeout",
                        message="The device did not prove the requested running commit before the rollout deadline.",
                    ),
                    reachable=False,
                )

        ordered = list(devices.values())
        expected_states = [
            devices[node_id].get("state")
            for node_id in expected
            if node_id in devices
        ]
        pending = any(state in PENDING_APPLY_STATES for state in expected_states)
        all_verified = bool(expected_states) and all(
            state == "runtime_verified" for state in expected_states
        )
        failures = any(
            state not in PENDING_APPLY_STATES and state != "runtime_verified"
            for state in expected_states
        )
        if pending:
            status = "updating"
        elif all_verified:
            status = "updated"
        elif failures:
            status = "update_completed_with_failures"
        else:
            status = "no_update_required"
        return {
            **value,
            "status": status,
            "devices": ordered,
            "eligible_count": 0,
        }

    def _refresh(
        self,
        value: dict[str, object],
        context: Any,
        actor: str,
    ) -> dict[str, object]:
        operation = value.get("operation")
        if operation == "check":
            return self._refresh_check(value, context, actor)
        if operation == "apply":
            return self._refresh_apply(value, context, actor)
        return value

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            value = self._load()
            try:
                context, actor = self._context()
                refreshed = self._refresh(value, context, actor)
            except Exception:  # passive status must remain available
                return value
            if refreshed != value:
                self._save(refreshed)
            return refreshed

    def check(self) -> dict[str, object]:
        context, actor = self._context()
        with self._lock:
            now = datetime.now(timezone.utc)
            local = self._normalize_runtime(self.local.inspect(fetch=True))
            target = local.target_commit
            devices = [self._device(actor, "This device", local)]
            request_id = f"check-{uuid.uuid4().hex}"
            remote_targets: list[str] = []
            if isinstance(target, str) and OID_RE.fullmatch(target):
                for device in self._authority_devices(context, actor):
                    if device.node_id == actor:
                        continue
                    reachable = self._is_connected(device.state)
                    if reachable:
                        remote_targets.append(device.node_id)
                        result = UpdateInspection(
                            "checking",
                            target_commit=target,
                            message="Waiting for this device to complete its local bounded update check.",
                        )
                    else:
                        result = UpdateInspection(
                            "offline",
                            target_commit=target,
                            code="node_offline",
                            message="Device was not reachable during this update check.",
                        )
                    devices.append(
                        self._device(
                            device.node_id,
                            device.label,
                            result,
                            reachable=reachable,
                        )
                    )
                self._append_request_event(
                    context,
                    actor,
                    event_type=CHECK_REQUEST_EVENT,
                    request_id=request_id,
                    target=target,
                    target_node_ids=tuple(remote_targets),
                    now=now,
                )
            eligible = sum(item.get("state") in ELIGIBLE_STATES for item in devices)
            value: dict[str, object] = {
                "schema": SCHEMA,
                "operation": "check",
                "status": "checking" if remote_targets else (
                    "update_available" if eligible else local.state
                ),
                "request_id": request_id,
                "checked_at": self._stamp(now),
                "check_expires_at": self._stamp(now + CHECK_FRESHNESS),
                "report_deadline": self._stamp(now + CHECK_REPORT_WINDOW),
                "repository": APPROVED_REPOSITORY,
                "branch": APPROVED_BRANCH,
                "target_commit": target,
                "devices": devices,
                "expected_report_node_ids": remote_targets,
                "eligible_count": eligible,
            }
            self._save(value)
            return value

    def update_all(self, *, confirmed_target: str) -> dict[str, object]:
        context, actor = self._context()
        with self._lock:
            checked = self._refresh(self._load(), context, actor)
            target = checked.get("target_commit")
            expires = self._parse_deadline(checked.get("check_expires_at"))
            if (
                checked.get("operation") != "check"
                or target != confirmed_target
                or not isinstance(target, str)
                or not OID_RE.fullmatch(target)
            ):
                raise ValueError("stale_or_mismatched_confirmation")
            if expires is None or expires <= datetime.now(timezone.utc):
                raise ValueError("expired_check")
            checked_devices = self._devices_by_id(checked)
            if checked.get("status") == "checking" or any(
                item.get("state") == "checking"
                for item in checked_devices.values()
            ):
                raise ValueError("check_in_progress")

            now = datetime.now(timezone.utc)
            devices = checked_devices
            authority = {
                device.node_id: device
                for device in self._authority_devices(context, actor)
            }
            eligible_ids = {
                node_id
                for node_id, item in devices.items()
                if item.get("state") in ELIGIBLE_STATES
            }
            remote_targets: list[str] = []
            for node_id in sorted(eligible_ids - {actor}):
                authority_device = authority.get(node_id)
                if authority_device is not None and self._is_connected(authority_device.state):
                    remote_targets.append(node_id)
                    current = devices[node_id]
                    devices[node_id] = {
                        **current,
                        "state": "requested",
                        "message": "The verified runtime update was requested on this reachable device.",
                        "code": None,
                    }
                else:
                    current = devices[node_id]
                    devices[node_id] = self._device(
                        node_id,
                        str(current.get("label") or node_id),
                        UpdateInspection(
                            "offline",
                            current_commit=current.get("current_commit") if isinstance(current.get("current_commit"), str) else None,
                            target_commit=target,
                            code="node_offline_not_queued",
                            message="The device became unreachable and was not queued for update.",
                            running_commit=current.get("running_commit") if isinstance(current.get("running_commit"), str) else None,
                        ),
                        reachable=False,
                    )

            request_id = f"apply-{uuid.uuid4().hex}"
            self._append_request_event(
                context,
                actor,
                event_type=APPLY_REQUEST_EVENT,
                request_id=request_id,
                target=target,
                target_node_ids=tuple(remote_targets),
                now=now,
            )

            expected = list(remote_targets)
            local_host_request_id: str | None = None
            if actor in eligible_ids:
                expected.append(actor)
                local_host_request_id = f"local-{uuid.uuid4().hex}"
                current = devices.get(actor, {"label": "This device"})
                devices[actor] = {
                    **current,
                    "state": "activation_queued",
                    "code": "host_activation_queued",
                    "message": "The local host agent will rebuild, restart, and verify this FCP device last.",
                }

            value: dict[str, object] = {
                **checked,
                "schema": SCHEMA,
                "operation": "apply",
                "status": "updating" if expected else "no_update_required",
                "request_id": request_id,
                "updated_at": self._stamp(now),
                "report_deadline": self._stamp(now + APPLY_REPORT_WINDOW),
                "expected_update_node_ids": expected,
                "devices": list(devices.values()),
                "eligible_count": 0,
                "local_host_request_id": local_host_request_id,
            }
            self._save(value)

            # Coordinator activation is deliberately queued last.  The host
            # handoff adds a short activation grace so this HTTP request can
            # finish before the old Flask container is replaced.
            if local_host_request_id is not None:
                local_result = self.local.apply(
                    target,
                    request_id=local_host_request_id,
                )
                current = devices.get(actor, {"label": "This device"})
                devices[actor] = self._device(
                    actor,
                    str(current.get("label") or "This device"),
                    self._normalize_runtime(local_result),
                    reachable=True,
                )
                value["devices"] = list(devices.values())
                self._save(value)
            return value


def get_federation_update_service() -> FederationUpdateService:
    configured = current_app.config.get("FEDERATION_UPDATE_SERVICE")
    if isinstance(configured, FederationUpdateService):
        return configured

    configured_adapter = current_app.config.get("FEDERATION_UPDATE_LOCAL_ADAPTER")
    if configured_adapter is not None:
        local = configured_adapter
    else:
        onboarding_database = Path(
            current_app.config.get(
                "CAPABILITY_ONBOARDING_STATE_DATABASE",
                "data/federation/onboarding/onboarding.sqlite3",
            )
        )
        federation_root = onboarding_database.parent.parent
        handoff = Path(
            current_app.config.get(
                "FEDERATION_UPDATE_HANDOFF_DIR",
                federation_root / "update-agent",
            )
        )
        local = HostUpdateHandoff(handoff)

    onboarding_database = Path(
        current_app.config.get(
            "CAPABILITY_ONBOARDING_STATE_DATABASE",
            "data/federation/onboarding/onboarding.sqlite3",
        )
    )
    federation_root = onboarding_database.parent.parent
    state = Path(
        current_app.config.get(
            "FEDERATION_UPDATE_STATE",
            federation_root / "update-status.json",
        )
    )
    return FederationUpdateService(local, state)
