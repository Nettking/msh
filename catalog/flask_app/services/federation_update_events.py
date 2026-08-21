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

from catalog.federation.control_commands import (
    ControlCommandEnvelope,
    ensure_bounded_json,
)
from catalog.federation.control_commands import (
    correlated_event_request_id as _event_request_id,
)
from catalog.federation.control_commands import (
    stamp_utc as _stamp,
)
from catalog.federation.software_trial import (
    TrialRefused,
    TrialSelection,
    trial_selection,
    validate_trial_summary,
)
from catalog.federation.software_update import (
    APPROVED_BRANCH,
    APPROVED_REPOSITORY,
    BRANCH_RE,
    OID_RE,
    UpdateInspection,
)

from .federation_update_handoff import HostUpdateHandoff

EVENT_SCHEMA = "fcp.federation-update-event.v1"
PROCESSOR_SCHEMA = "fcp.federation-update-processor.v1"
CHECK_REQUEST_EVENT = "software.update.check.requested"
CHECK_REPORT_EVENT = "software.update.check.reported"
APPLY_REQUEST_EVENT = "software.update.apply.requested"
APPLY_REPORT_EVENT = "software.update.apply.reported"
#: Branch trials travel through the same authoritative log, with the same
#: leader fencing and the same bounded envelope. Only the payload differs.
TRIAL_REQUEST_EVENT = "software.trial.requested"
TRIAL_REPORT_EVENT = "software.trial.reported"
SESSION_CREATED_EVENT = "session.created"
MAX_TARGETS = 256
MAX_EVENT_BYTES = 8192


def _bounded(value: object) -> None:
    ensure_bounded_json(
        value,
        max_bytes=MAX_EVENT_BYTES,
        error_code="update_event_too_large",
    )


def command_payload(
    *,
    request_id: str,
    target_commit: str,
    target_node_ids: tuple[str, ...],
    created_at: datetime,
    expires_at: datetime,
) -> dict[str, object]:
    envelope = ControlCommandEnvelope.issue(
        request_id=request_id,
        target_node_ids=target_node_ids,
        created_at=created_at,
        expires_at=expires_at,
        max_lifetime=timedelta(minutes=15),
        max_targets=MAX_TARGETS,
    )
    if not OID_RE.fullmatch(target_commit):
        raise ValueError("malformed_target")
    value: dict[str, object] = {
        "schema": EVENT_SCHEMA,
        **envelope.payload_fields(),
        "repository": APPROVED_REPOSITORY,
        "branch": APPROVED_BRANCH,
        "target_commit": target_commit,
    }
    _bounded(value)
    return value


def validate_command_payload(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or value.get("schema") != EVENT_SCHEMA:
        raise ValueError("malformed_message")
    ControlCommandEnvelope.parse_payload(
        value,
        max_lifetime=timedelta(minutes=15),
        max_targets=MAX_TARGETS,
        require_unique_targets=False,
    )
    target = value.get("target_commit")
    if (
        value.get("repository") != APPROVED_REPOSITORY
        or value.get("branch") != APPROVED_BRANCH
    ):
        raise ValueError("unapproved_source")
    if not isinstance(target, str) or not OID_RE.fullmatch(target):
        raise ValueError("malformed_target")
    _bounded(value)
    return value


def trial_command_payload(
    *,
    request_id: str,
    selection: TrialSelection,
    target_node_ids: tuple[str, ...],
    created_at: datetime,
    expires_at: datetime,
) -> dict[str, object]:
    """Publish one branch trial command through the existing bounded envelope.

    The payload can express exactly three things about *what* to run: an
    approved repository, a branch name on it, and one exact commit. There is no
    field a path, URL, command, argument or environment value could travel in,
    which is what keeps a trial from being a remote-execution channel.
    """

    envelope = ControlCommandEnvelope.issue(
        request_id=request_id,
        target_node_ids=target_node_ids,
        created_at=created_at,
        expires_at=expires_at,
        max_lifetime=timedelta(minutes=15),
        max_targets=MAX_TARGETS,
    )
    selection.validate()
    value: dict[str, object] = {
        "schema": EVENT_SCHEMA,
        **envelope.payload_fields(),
        **selection.to_dict(),
    }
    _bounded(value)
    return value


def validate_trial_command_payload(value: object) -> tuple[dict[str, object], TrialSelection]:
    """Re-read a trial command with no trust in the leader that published it."""

    if not isinstance(value, dict) or value.get("schema") != EVENT_SCHEMA:
        raise ValueError("malformed_message")
    ControlCommandEnvelope.parse_payload(
        value,
        max_lifetime=timedelta(minutes=15),
        max_targets=MAX_TARGETS,
        require_unique_targets=False,
    )
    try:
        selection = trial_selection(value)
    except TrialRefused as refused:
        raise ValueError(refused.code) from refused
    _bounded(value)
    return value, selection


def trial_report_payload(
    *,
    request_id: str,
    node_id: str,
    document: dict[str, object],
) -> dict[str, object]:
    """Report one device's bounded trial outcome back to the Federation."""

    def text(field: str, limit: int = 512) -> str | None:
        candidate = document.get(field)
        return candidate[:limit] if isinstance(candidate, str) else None

    def commit(field: str) -> str | None:
        candidate = document.get(field)
        return (
            candidate.lower()
            if isinstance(candidate, str) and OID_RE.fullmatch(candidate.lower())
            else None
        )

    def branch(field: str) -> str | None:
        candidate = document.get(field)
        return (
            candidate
            if isinstance(candidate, str) and BRANCH_RE.fullmatch(candidate)
            else None
        )

    def flags(field: str) -> dict[str, object] | None:
        candidate = document.get(field)
        if not isinstance(candidate, dict):
            return None
        return {
            key: bool(item)
            for key, item in sorted(candidate.items())
            if isinstance(key, str) and isinstance(item, bool)
        }

    state = document.get("state")
    value: dict[str, object] = {
        "schema": EVENT_SCHEMA,
        "request_id": request_id,
        "node_id": node_id,
        "state": state if isinstance(state, str) and state else "error",
        "code": text("code", 128),
        "message": text("message"),
        "branch": branch("branch"),
        "target_commit": commit("target_commit"),
        "running_commit": commit("running_commit"),
        "safe_branch": branch("safe_branch"),
        "safe_commit": commit("safe_commit"),
        "acceptance": flags("acceptance"),
        "recovery": flags("recovery"),
        "reported_at": _stamp(datetime.now(timezone.utc)),
    }
    _bounded(value)
    return value


def trial_from_report(value: object) -> tuple[str, str, dict[str, object]] | None:
    """Read one device's trial report, keeping only the bounded shape."""

    if not isinstance(value, dict) or value.get("schema") != EVENT_SCHEMA:
        return None
    request_id = value.get("request_id")
    node_id = value.get("node_id")
    state = value.get("state")
    if not all(
        isinstance(item, str) and item for item in (request_id, node_id, state)
    ):
        return None
    return request_id, node_id, trial_report_payload(
        request_id=request_id,
        node_id=node_id,
        document=value,
    )


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
    trial = validate_trial_summary(result.trial)
    if trial is not None:
        # Additive: a device that has never left approved main reports nothing
        # here, exactly as every device did before branch trials existed.
        value["trial"] = trial
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
    if not all(isinstance(item, str) and item for item in (request_id, node_id, state)):
        return None
    commits: dict[str, str | None] = {}
    for field in ("current_commit", "target_commit", "running_commit"):
        commit = value.get(field)
        # Older host agents serialize an unknown running build as an empty
        # string. Treat only that optional field as absent so coordinators can
        # still classify the source-current runtime as activation_required.
        if field == "running_commit" and commit == "":
            commit = None
        if commit is not None and (
            not isinstance(commit, str) or not OID_RE.fullmatch(commit)
        ):
            return None
        commits[field] = commit
    return (
        request_id,
        node_id,
        UpdateInspection(
            state=state,
            current_commit=commits["current_commit"],
            target_commit=commits["target_commit"],
            code=value.get("code") if isinstance(value.get("code"), str) else None,
            message=(
                value.get("message") if isinstance(value.get("message"), str) else None
            ),
            running_commit=commits["running_commit"],
            trial=validate_trial_summary(value.get("trial")),
        ),
    )


#: Trial states after which nothing more will be reported for that request.
SETTLED_TRIAL_STATES = frozenset(
    {
        "trial_running",
        "safe_restored",
        "rollback_failed",
        "trial_operator_stopped",
        "refused",
        "error",
        "dirty",
        "unsupported_checkout",
        "up_to_date",
    }
)


def _trial_is_settled(document: dict[str, object]) -> bool:
    return str(document.get("state") or "") in SETTLED_TRIAL_STATES


def _host_request_id(request_id: str, node_id: str) -> str:
    digest = hashlib.sha256(f"host\0{request_id}\0{node_id}".encode()).hexdigest()[:40]
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

    def _report_trial(
        self,
        context: Any,
        *,
        federation_request_id: str,
        document: dict[str, object],
    ) -> None:
        node_id = context.credentials.identity.node_id
        payload = trial_report_payload(
            request_id=federation_request_id,
            node_id=node_id,
            document=document,
        )
        _append_remote_event(
            self.service,
            context,
            TRIAL_REPORT_EVENT,
            payload,
            _event_request_id(
                "trial-report",
                ":".join(
                    (
                        federation_request_id,
                        str(payload.get("state")),
                        str(payload.get("running_commit") or ""),
                    )
                ),
                node_id,
            ),
        )

    def _host_trial_result(self, request_id: str) -> dict[str, object] | None:
        reader = getattr(self.handoff, "trial_result_for", None)
        return reader(request_id) if callable(reader) else None

    def _host_result(self, request_id: str) -> UpdateInspection | None:
        result_for = getattr(self.handoff, "result_for", None)
        if callable(result_for):
            return result_for(request_id)
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
            if record.get("kind") == "trial":
                document = self._host_trial_result(host_request_id)
                if document is None or document.get("target_commit") != target_commit:
                    continue
                self._report_trial(
                    context,
                    federation_request_id=federation_request_id,
                    document=document,
                )
                if not _trial_is_settled(document):
                    # A trial that is still starting or verifying keeps its
                    # pending record so the later outcome is reported too.
                    continue
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

    def _process_trial_request(
        self,
        context: Any,
        event: Any,
        state: dict[str, object],
        *,
        local_node: str,
    ) -> dict[str, object]:
        """Bridge one leader-authorized branch trial to the local host agent.

        The host agent is the party that decides whether the trial is safe. All
        this does is refuse anything outside the bounded shape and hand the
        selection across; it never resolves a path, an interpreter or a command.
        """

        payload, selection = validate_trial_command_payload(event.payload)
        if local_node not in payload["target_node_ids"]:
            return state
        federation_request_id = str(payload["request_id"])
        host_request_id = _host_request_id(federation_request_id, local_node)
        pending = state.get("pending")
        if not isinstance(pending, dict):
            pending = {}
        existing = self._host_trial_result(host_request_id)
        if existing is not None:
            self._report_trial(
                context,
                federation_request_id=federation_request_id,
                document=existing,
            )
            if _trial_is_settled(existing):
                pending.pop(federation_request_id, None)
                state["pending"] = pending
                _write_state(self.state_file, state)
                return state
        queued = self.handoff.trial(selection, request_id=host_request_id)
        if _trial_is_settled(queued):
            pending.pop(federation_request_id, None)
        else:
            # The recorder activation watcher only accepts a stop request for
            # work this device is genuinely waiting on, so the pending record is
            # written before the report, exactly as an update does.
            pending[federation_request_id] = {
                "kind": "trial",
                "host_request_id": host_request_id,
                "target_commit": selection.target_commit,
            }
        state["pending"] = pending
        _write_state(self.state_file, state)
        self._report_trial(
            context,
            federation_request_id=federation_request_id,
            document=queued,
        )
        return state

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
                        TRIAL_REQUEST_EVENT,
                    }:
                        continue
                    if authority is None or event.actor_node_id != authority:
                        continue
                    if event.event_type == TRIAL_REQUEST_EVENT:
                        state = self._process_trial_request(
                            context,
                            event,
                            state,
                            local_node=local_node,
                        )
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
                        if existing is not None and existing.target_commit == target:
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
    "TRIAL_REPORT_EVENT",
    "TRIAL_REQUEST_EVENT",
    "FederationUpdateEventProcessor",
    "command_payload",
    "inspection_from_report",
    "report_payload",
    "trial_command_payload",
    "trial_from_report",
    "trial_report_payload",
    "validate_command_payload",
    "validate_trial_command_payload",
]
