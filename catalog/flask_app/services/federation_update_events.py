"""Durable Federation control-plane events for host-owned software updates.

Update intents travel through the existing authoritative session event log. They
contain no command, path, URL, credential, or executable. Each target device
independently revalidates the exact commit through its local host agent.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from catalog.federation.software_update import (
    APPROVED_BRANCH,
    APPROVED_REPOSITORY,
    OID_RE,
    UpdateInspection,
)

from .federation_update_handoff import HostUpdateHandoff

EVENT_SCHEMA = "msh.federation-update-event.v1"
PROCESSOR_SCHEMA = "msh.federation-update-processor.v1"
CHECK_REQUEST_EVENT = "software.update.check.requested"
CHECK_REPORT_EVENT = "software.update.check.reported"
APPLY_REQUEST_EVENT = "software.update.apply.requested"
APPLY_REPORT_EVENT = "software.update.apply.reported"
SESSION_CREATED_EVENT = "session.created"
MAX_TARGETS = 256
MAX_EVENT_BYTES = 8192


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_stamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise TypeError("malformed_timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("malformed_timestamp")
    return parsed.astimezone(timezone.utc)


def _bounded(value: object) -> None:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > MAX_EVENT_BYTES:
        raise ValueError("update_event_too_large")


def command_payload(
    *,
    request_id: str,
    target_commit: str,
    target_node_ids: tuple[str, ...],
    created_at: datetime,
    expires_at: datetime,
) -> dict[str, object]:
    if not request_id or len(request_id) > 128:
        raise ValueError("malformed_request_id")
    if not OID_RE.fullmatch(target_commit):
        raise ValueError("malformed_target")
    targets = tuple(dict.fromkeys(target_node_ids))
    if not targets or len(targets) > MAX_TARGETS:
        raise ValueError("malformed_targets")
    if any(
        not isinstance(item, str) or not item or len(item) > 512
        for item in targets
    ):
        raise ValueError("malformed_targets")
    if expires_at <= created_at or (expires_at - created_at).total_seconds() > 900:
        raise ValueError("invalid_lifetime")
    value: dict[str, object] = {
        "schema": EVENT_SCHEMA,
        "request_id": request_id,
        "repository": APPROVED_REPOSITORY,
        "branch": APPROVED_BRANCH,
        "target_commit": target_commit,
        "target_node_ids": list(targets),
        "created_at": _stamp(created_at),
        "expires_at": _stamp(expires_at),
    }
    _bounded(value)
    return value


def validate_command_payload(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or value.get("schema") != EVENT_SCHEMA:
        raise ValueError("malformed_message")
    request_id = value.get("request_id")
    target = value.get("target_commit")
    targets = value.get("target_node_ids")
    if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
        raise ValueError("malformed_request_id")
    if (
        value.get("repository") != APPROVED_REPOSITORY
        or value.get("branch") != APPROVED_BRANCH
    ):
        raise ValueError("unapproved_source")
    if not isinstance(target, str) or not OID_RE.fullmatch(target):
        raise ValueError("malformed_target")
    if (
        not isinstance(targets, list)
        or not 1 <= len(targets) <= MAX_TARGETS
        or any(
            not isinstance(item, str) or not item or len(item) > 512
            for item in targets
        )
    ):
        raise ValueError("malformed_targets")
    created = _parse_stamp(value.get("created_at"))
    expires = _parse_stamp(value.get("expires_at"))
    now = datetime.now(timezone.utc)
    if (
        created > now + timedelta(minutes=1)
        or expires <= now
        or (expires - created).total_seconds() > 900
    ):
        raise ValueError("expired_or_invalid_request")
    _bounded(value)
    return value


def report_payload(
    *,
    request_id: str,
    node_id: str,
    result: UpdateInspection,
) -> dict[str, object]:
    message = result.message or ""
    if len(message) > 512:
        message = message[:512]
    value: dict[str, object] = {
        "schema": EVENT_SCHEMA,
        "request_id": request_id,
        "node_id": node_id,
        "state": result.state,
        "current_commit": result.current_commit,
        "target_commit": result.target_commit,
        "running_commit": result.running_commit,
        "code": result.code,
        "message": message,
        "reported_at": _stamp(datetime.now(timezone.utc)),
    }
    _bounded(value)
    return value


def inspection_from_report(
    value: object,
) -> tuple[str, str, UpdateInspection] | None:
    if not isinstance(value, dict) or value.get("schema") != EVENT_SCHEMA:
        return None
    request_id = value.get("request_id")
    node_id = value.get("node_id")
    state = value.get("state")
    if not all(
        isinstance(item, str) and item
        for item in (request_id, node_id, state)
    ):
        return None
    for field in ("current_commit", "target_commit", "running_commit"):
        commit = value.get(field)
        if commit is not None and (
            not isinstance(commit, str) or not OID_RE.fullmatch(commit)
        ):
            return None
    return (
        request_id,
        node_id,
        UpdateInspection(
            state=state,
            current_commit=(
                value.get("current_commit")
                if isinstance(value.get("current_commit"), str)
                else None
            ),
            target_commit=(
                value.get("target_commit")
                if isinstance(value.get("target_commit"), str)
                else None
            ),
            code=value.get("code") if isinstance(value.get("code"), str) else None,
            message=(
                value.get("message")
                if isinstance(value.get("message"), str)
                else None
            ),
            running_commit=(
                value.get("running_commit")
                if isinstance(value.get("running_commit"), str)
                else None
            ),
        ),
    )


def _event_request_id(prefix: str, request_id: str, node_id: str) -> str:
    digest = hashlib.sha256(
        f"{request_id}\0{node_id}".encode()
    ).hexdigest()[:32]
    return f"{prefix}-{digest}"


def _host_request_id(request_id: str, node_id: str) -> str:
    digest = hashlib.sha256(
        f"host\0{request_id}\0{node_id}".encode()
    ).hexdigest()[:40]
    return f"fed-{digest}"


def _empty_state() -> dict[str, object]:
    return {
        "schema": PROCESSOR_SCHEMA,
        "last_revision": 0,
        "authority_node_id": None,
        "pending": {},
    }


def _read_state(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return _empty_state()
    if not isinstance(value, dict) or value.get("schema") != PROCESSOR_SCHEMA:
        return _empty_state()
    return value


def _write_state(path: Path, value: dict[str, object]) -> None:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
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


class FederationUpdateEventProcessor:
    """Replay authoritative update intents and bridge them to the host agent."""

    def __init__(
        self,
        service: Any,
        handoff: HostUpdateHandoff,
        state_file: Path | str,
    ) -> None:
        self.service = service
        self.handoff = handoff
        self.state_file = Path(state_file)

    def _report(
        self,
        context: Any,
        *,
        event_type: str,
        federation_request_id: str,
        result: UpdateInspection,
    ) -> None:
        node_id = context.credentials.identity.node_id
        _append_remote_event(
            self.service,
            context,
            event_type,
            report_payload(
                request_id=federation_request_id,
                node_id=node_id,
                result=result,
            ),
            _event_request_id(
                "update-report",
                ":".join(
                    (
                        federation_request_id,
                        event_type,
                        result.state,
                        result.running_commit or "",
                    )
                ),
                node_id,
            ),
        )

    def _host_result(self, request_id: str) -> UpdateInspection | None:
        result_for = getattr(self.handoff, "result_for", None)
        if callable(result_for):
            result = result_for(request_id)
            return result if isinstance(result, UpdateInspection) else result
        latest = self.handoff.latest_result()
        if latest is None or latest.request_id != request_id:
            return None
        return latest

    def _finish_pending(
        self,
        context: Any,
        state: dict[str, object],
    ) -> None:
        pending = state.get("pending")
        if not isinstance(pending, dict) or not pending:
            return
        changed = False
        for federation_request_id, record in list(pending.items()):
            if not isinstance(record, dict):
                pending.pop(federation_request_id, None)
                changed = True
                continue
            host_request_id = record.get("host_request_id")
            target_commit = record.get("target_commit")
            if not isinstance(host_request_id, str):
                pending.pop(federation_request_id, None)
                changed = True
                continue
            result = self._host_result(host_request_id)
            if result is None or result.target_commit != target_commit:
                continue
            self._report(
                context,
                event_type=APPLY_REPORT_EVENT,
                federation_request_id=federation_request_id,
                result=result,
            )
            pending.pop(federation_request_id, None)
            changed = True
        if changed:
            state["pending"] = pending
            _write_state(self.state_file, state)

    @staticmethod
    def _pin_authority(
        state: dict[str, object],
        event: Any,
    ) -> str | None:
        current = state.get("authority_node_id")
        if current is not None and not isinstance(current, str):
            raise ValueError("malformed_pinned_update_authority")
        if event.event_type != SESSION_CREATED_EVENT:
            return current
        candidate = getattr(event, "actor_node_id", None)
        if not isinstance(candidate, str) or not candidate:
            raise ValueError("missing_session_creator_identity")
        if isinstance(current, str) and current != candidate:
            raise ValueError("session_creator_identity_changed")
        state["authority_node_id"] = candidate
        return candidate

    def process(self, context: Any) -> None:
        remote = self.service.remote_store.load()
        if remote is None:
            return
        state = _read_state(self.state_file)
        self._finish_pending(context, state)
        last_revision = state.get("last_revision", 0)
        if (
            isinstance(last_revision, bool)
            or not isinstance(last_revision, int)
            or last_revision < 0
        ):
            last_revision = 0
        authority = state.get("authority_node_id")
        if not isinstance(authority, str) or not authority:
            # Existing paired nodes do not persist the creator identity. Replay
            # from the immutable session-created event once and pin its
            # authenticated actor before accepting any update command.
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
                    if event.event_type not in {
                        CHECK_REQUEST_EVENT,
                        APPLY_REQUEST_EVENT,
                    }:
                        continue
                    if authority is None or event.actor_node_id != authority:
                        continue
                    payload = validate_command_payload(event.payload)
                    targets = payload["target_node_ids"]
                    if local_node not in targets:
                        continue
                    federation_request_id = str(payload["request_id"])
                    target = str(payload["target_commit"])
                    if event.event_type == CHECK_REQUEST_EVENT:
                        result = self.handoff.inspect(target=target, fetch=True)
                        self._report(
                            context,
                            event_type=CHECK_REPORT_EVENT,
                            federation_request_id=federation_request_id,
                            result=result,
                        )
                    else:
                        pending = state.get("pending")
                        if not isinstance(pending, dict):
                            pending = {}
                        host_request_id = _host_request_id(
                            federation_request_id,
                            local_node,
                        )
                        existing = self._host_result(host_request_id)
                        if (
                            existing is not None
                            and existing.target_commit == target
                        ):
                            self._report(
                                context,
                                event_type=APPLY_REPORT_EVENT,
                                federation_request_id=federation_request_id,
                                result=existing,
                            )
                            pending.pop(federation_request_id, None)
                        else:
                            queued = self.handoff.apply(
                                target,
                                request_id=host_request_id,
                            )
                            if queued.state == "activation_queued":
                                pending[federation_request_id] = {
                                    "host_request_id": host_request_id,
                                    "target_commit": target,
                                }
                            else:
                                # A bounded handoff that did not queue the
                                # request is a terminal report for this command;
                                # never persist a phantom pending operation.
                                pending.pop(federation_request_id, None)
                            state["pending"] = pending
                            _write_state(self.state_file, state)
                            self._report(
                                context,
                                event_type=APPLY_REPORT_EVENT,
                                federation_request_id=federation_request_id,
                                result=queued,
                            )
                finally:
                    last_revision = int(event.revision)
                    state["last_revision"] = last_revision
                    _write_state(self.state_file, state)
            if last_revision >= current_revision:
                break


__all__ = [
    "APPLY_REPORT_EVENT",
    "APPLY_REQUEST_EVENT",
    "CHECK_REPORT_EVENT",
    "CHECK_REQUEST_EVENT",
    "EVENT_SCHEMA",
    "SESSION_CREATED_EVENT",
    "FederationUpdateEventProcessor",
    "command_payload",
    "inspection_from_report",
    "report_payload",
    "validate_command_payload",
]
