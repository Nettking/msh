"""Durable Federation control-plane events for host-owned software updates.

Update intents travel through the existing authoritative session event log. They
contain no command, path, URL, credential, or executable. Each target device
independently revalidates the exact commit through its local host agent.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from catalog.capabilities.update_drain import (
    NodeUpdateDrainTarget,
    SQLiteNodeUpdateDrainStore,
)
from catalog.federation.authoritative_replay import replay_authoritative_history
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
from catalog.federation.errors import FederationValidationError
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
TRIAL_RETIREMENT_STATE_KEY = "trial_result_retirements"
# The member processor retains its fixed resource ceiling. Reaching it is an
# explicit bounded failure, never an implicit end-of-history.
_PROCESSOR_REPLAY_PAGE_EVENTS = 32
_MAX_PROCESSOR_REPLAY_PAGES = 64


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
    drain_node_id: str | None = None,
    drain_provider_ids: tuple[str, ...] = (),
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
    if drain_provider_ids or drain_node_id is not None:
        if not isinstance(drain_node_id, str) or not drain_node_id.strip():
            raise ValueError("malformed_drain_node")
        try:
            drain_target = NodeUpdateDrainTarget(
                drain_node_id,
                tuple(drain_provider_ids),
            )
        except (FederationValidationError, TypeError, ValueError) as exc:
            raise ValueError("malformed_drain_provider_ids") from exc
        if drain_target.node_id not in target_node_ids:
            raise ValueError("drain_node_not_targeted")
    value: dict[str, object] = {
        "schema": EVENT_SCHEMA,
        **envelope.payload_fields(),
        "repository": APPROVED_REPOSITORY,
        "branch": APPROVED_BRANCH,
        "target_commit": target_commit,
    }
    if drain_provider_ids:
        value["drain_node_id"] = drain_node_id
        value["drain_provider_ids"] = list(drain_target.provider_ids)
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
    drain_node_id = value.get("drain_node_id")
    drain_provider_ids = value.get("drain_provider_ids")
    if drain_node_id is not None or drain_provider_ids is not None:
        if not isinstance(drain_node_id, str):
            raise ValueError("malformed_drain_node")
        if not isinstance(drain_provider_ids, list):
            raise ValueError("malformed_drain_provider_ids")
        try:
            target = NodeUpdateDrainTarget(
                drain_node_id,
                tuple(drain_provider_ids),
            )
        except (FederationValidationError, TypeError, ValueError) as exc:
            raise ValueError("malformed_drain_provider_ids") from exc
        if target.node_id not in value.get("target_node_ids", []):
            raise ValueError("drain_node_not_targeted")
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
    trial_branch = branch("trial_branch")
    trial_commit = commit("trial_commit")
    if trial_branch is not None and trial_commit is not None:
        value["trial_branch"] = trial_branch
        value["trial_commit"] = trial_commit
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


def _inspect_host_update(
    handoff: HostUpdateHandoff,
    *,
    target: str,
    fetch: bool,
    request_id: str,
) -> UpdateInspection:
    """Run a check with a stable id when the adapter supports it.

    The production host handoff accepts ``request_id`` so a restart can recover
    a check without issuing a second host request.  A few older embedded
    adapters and test doubles still implement the pre-idempotency shape; keep
    those readers compatible while the real handoff takes the durable path.
    """

    method = handoff.inspect
    try:
        parameters = inspect.signature(method).parameters
    except (TypeError, ValueError):
        parameters = None
    accepts_request_id = parameters is None or (
        "request_id" in parameters
        or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
    )
    if accepts_request_id:
        return method(target=target, fetch=fetch, request_id=request_id)
    return method(target=target, fetch=fetch)


def _empty_state() -> dict[str, object]:
    return {
        "schema": PROCESSOR_SCHEMA,
        "last_revision": 0,
        "authority_node_id": None,
        "pending": {},
        "in_flight": None,
        TRIAL_RETIREMENT_STATE_KEY: [],
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
        drain_store: SQLiteNodeUpdateDrainStore | None = None,
        health: Any | None = None,
    ) -> None:
        self.service = service
        self.handoff = handoff
        self.state_file = Path(state_file)
        self.drain_store = drain_store
        self.health = health

    def _drain_target(
        self,
        payload: dict[str, object],
        *,
        local_node: str,
    ) -> NodeUpdateDrainTarget | None:
        provider_ids = payload.get("drain_provider_ids")
        drain_node = payload.get("drain_node_id")
        if provider_ids is None and drain_node is None:
            return None
        if not isinstance(drain_node, str) or drain_node != local_node:
            raise ValueError("drain-node-identity-mismatch")
        if not isinstance(provider_ids, list):
            raise TypeError("malformed-drain-provider-set")
        try:
            return NodeUpdateDrainTarget(drain_node, tuple(provider_ids))
        except (FederationValidationError, TypeError, ValueError) as exc:
            raise ValueError("malformed-drain-provider-set") from exc

    def _request_drain(
        self,
        payload: dict[str, object],
        *,
        session_id: str,
        local_node: str,
    ):
        target = self._drain_target(payload, local_node=local_node)
        if target is None:
            return None
        if self.drain_store is None:
            raise ValueError("update-drain-unavailable")
        if self.health is not None:
            for provider_id in target.provider_ids:
                record = self.health.store.get(
                    session_id=session_id,
                    capability_id=provider_id,
                )
                if record is None or record.node_id != target.node_id:
                    raise ValueError("drain-provider-identity-mismatch")
        return self.drain_store.request_drain(
            session_id=session_id,
            target=target,
            command_id=f"update-drain-{payload['request_id']}",
            now=datetime.now(timezone.utc),
        ).record

    @staticmethod
    def _trial_retirement_ids(state: dict[str, object]) -> list[str]:
        raw = state.get(TRIAL_RETIREMENT_STATE_KEY)
        if not isinstance(raw, list):
            return []
        values: list[str] = []
        seen: set[str] = set()
        for item in raw:
            if (
                isinstance(item, str)
                and 0 < len(item) <= 256
                and item not in seen
            ):
                seen.add(item)
                values.append(item)
        return values

    def _retire_trial_result(self, request_id: str) -> bool:
        """Delete exactly one per-request trial publication, best effort."""

        directory = getattr(self.handoff, "directory", None)
        if not isinstance(directory, Path):
            try:
                directory = Path(directory)
            except (TypeError, ValueError):
                return False
        digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
        path = directory / f"trial-result-{digest}.json"
        try:
            path.unlink(missing_ok=True)
        except OSError:
            return False
        return True

    def _drain_trial_retirements(self, state: dict[str, object]) -> None:
        retirements = self._trial_retirement_ids(state)
        if not retirements:
            if state.get(TRIAL_RETIREMENT_STATE_KEY) not in (None, []):
                state[TRIAL_RETIREMENT_STATE_KEY] = []
                _write_state(self.state_file, state)
            return
        remaining = [
            request_id
            for request_id in retirements
            if not self._retire_trial_result(request_id)
        ]
        if (
            remaining != retirements
            or state.get(TRIAL_RETIREMENT_STATE_KEY) != retirements
        ):
            state[TRIAL_RETIREMENT_STATE_KEY] = remaining
            _write_state(self.state_file, state)

    def _persist_trial_retirement(
        self,
        state: dict[str, object],
        request_id: str,
    ) -> None:
        retirements = self._trial_retirement_ids(state)
        if request_id not in retirements:
            retirements.append(request_id)
        state[TRIAL_RETIREMENT_STATE_KEY] = retirements
        # This write is the crash-safety boundary. The pending reader has
        # already been removed in memory, and the durable retirement intent is
        # committed in the same atomic state replacement before any unlink.
        _write_state(self.state_file, state)
        self._drain_trial_retirements(state)

    def _report(
        self,
        context: Any,
        *,
        event_type: str,
        federation_request_id: str,
        result: UpdateInspection,
    ) -> None:
        node_id = context.credentials.identity.node_id
        self._send_stored_report(
            self.service,
            context,
            event_type=event_type,
            payload=report_payload(
                request_id=federation_request_id,
                node_id=node_id,
                result=result,
            ),
            request_id=self._report_request_id(
                federation_request_id=federation_request_id,
                event_type=event_type,
                result=result,
                node_id=node_id,
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

    @staticmethod
    def _report_request_id(
        *,
        federation_request_id: str,
        event_type: str,
        result: UpdateInspection,
        node_id: str,
    ) -> str:
        return _event_request_id(
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
        )

    @staticmethod
    def _send_stored_report(
        service: Any,
        context: Any,
        *,
        event_type: str,
        payload: dict[str, object],
        request_id: str,
    ) -> None:
        _append_remote_event(service, context, event_type, payload, request_id)

    def _store_result_report(
        self,
        context: Any,
        state: dict[str, object],
        marker: dict[str, object],
        *,
        event_type: str,
        federation_request_id: str,
        result: UpdateInspection,
    ) -> None:
        node_id = context.credentials.identity.node_id
        payload = report_payload(
            request_id=federation_request_id,
            node_id=node_id,
            result=result,
        )
        marker["report_event_type"] = event_type
        marker["report_payload"] = payload
        marker["report_request_id"] = self._report_request_id(
            federation_request_id=federation_request_id,
            event_type=event_type,
            result=result,
            node_id=node_id,
        )
        state["in_flight"] = marker
        # Persist the exact host outcome before report publication. A report
        # outage after this point is retried from the marker.
        _write_state(self.state_file, state)

    def _complete_in_flight(
        self,
        state: dict[str, object],
        *,
        revision: int,
    ) -> None:
        state["in_flight"] = None
        previous = state.get("last_revision", 0)
        if isinstance(previous, bool) or not isinstance(previous, int):
            previous = 0
        state["last_revision"] = max(previous, revision)
        _write_state(self.state_file, state)

    def _recover_in_flight(
        self,
        context: Any,
        state: dict[str, object],
    ) -> None:
        """Finish an interrupted update without blindly reissuing it."""

        marker = state.get("in_flight")
        if marker is None:
            return
        if not isinstance(marker, dict):
            raise TypeError("malformed_update_in_flight_marker")
        request_id = marker.get("request_id")
        node_id = marker.get("node_id")
        revision = marker.get("revision")
        host_request_id = marker.get("host_request_id")
        target_commit = marker.get("target_commit")
        kind = marker.get("kind")
        if (
            not isinstance(request_id, str)
            or not request_id
            or not isinstance(node_id, str)
            or not node_id
            or isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision < 0
            or not isinstance(host_request_id, str)
            or not host_request_id
            or not isinstance(target_commit, str)
            or kind not in {"check", "apply", "trial"}
        ):
            raise ValueError("malformed_update_in_flight_marker")

        payload = marker.get("report_payload")
        event_type = marker.get("report_event_type")
        report_request_id = marker.get("report_request_id")
        if not (
            isinstance(payload, dict)
            and isinstance(event_type, str)
            and isinstance(report_request_id, str)
        ):
            if kind == "trial":
                document = self._host_trial_result(host_request_id)
                if document is None:
                    document = {
                        "state": "failed",
                        "code": "processor-crash-window",
                        "message": (
                            "The software trial crossed a process crash window; "
                            "its local outcome is unknown and was not retried."
                        ),
                        "target_commit": target_commit,
                    }
                node_id = context.credentials.identity.node_id
                payload = trial_report_payload(
                    request_id=request_id,
                    node_id=node_id,
                    document=document,
                )
                marker["report_event_type"] = TRIAL_REPORT_EVENT
                marker["report_payload"] = payload
                marker["report_request_id"] = _event_request_id(
                    "trial-report",
                    ":".join(
                        (
                            request_id,
                            str(payload.get("state")),
                            str(payload.get("running_commit") or ""),
                        )
                    ),
                    node_id,
                )
                marker["settled"] = _trial_is_settled(document)
                state["in_flight"] = marker
                _write_state(self.state_file, state)
            else:
                result = self._host_result(host_request_id)
                host_outcome_unknown = result is None
                if result is None or result.target_commit != target_commit:
                    result = UpdateInspection(
                        "error",
                        target_commit=target_commit,
                        code="processor-crash-window",
                        message=(
                            "The software update crossed a process crash window; "
                            "its local outcome is unknown and was not retried."
                        ),
                        request_id=host_request_id,
                    )
                if kind == "apply":
                    pending = state.get("pending")
                    if host_outcome_unknown and isinstance(pending, dict):
                        pending.pop(str(marker.get("pending_key")), None)
                        state["pending"] = pending
                    if (
                        result.state == "runtime_verified"
                        and marker.get("drain_revision") is not None
                        and not self._clear_drain(context, marker)
                    ):
                        result = UpdateInspection(
                            "failed",
                            target_commit=target_commit,
                            code="drain-clear-failed",
                            message="The exact candidate was reported healthy but the durable drain could not be retired.",
                            request_id=host_request_id,
                        )
                self._store_result_report(
                    context,
                    state,
                    marker,
                    event_type=(
                        CHECK_REPORT_EVENT if kind == "check" else APPLY_REPORT_EVENT
                    ),
                    federation_request_id=request_id,
                    result=result,
                )
            payload = marker.get("report_payload")
            event_type = marker.get("report_event_type")
            report_request_id = marker.get("report_request_id")
        if not (
            isinstance(payload, dict)
            and isinstance(event_type, str)
            and isinstance(report_request_id, str)
        ):
            raise TypeError("malformed_update_in_flight_report")
        self._send_stored_report(
            self.service,
            context,
            event_type=event_type,
            payload=payload,
            request_id=report_request_id,
        )
        if kind == "trial" and marker.get("settled"):
            pending = state.get("pending")
            if isinstance(pending, dict):
                pending.pop(request_id, None)
                state["pending"] = pending
            self._persist_trial_retirement(state, host_request_id)
        self._complete_in_flight(state, revision=revision)

    def _clear_drain(
        self,
        context: Any,
        record: dict[str, object],
    ) -> bool:
        revision = record.get("drain_revision")
        if revision is None:
            return True
        if (
            self.drain_store is None
            or isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision <= 0
        ):
            return False
        try:
            self.drain_store.clear_drain(
                session_id=context.binding.internal_session_id,
                node_id=context.credentials.identity.node_id,
                expected_revision=revision,
                now=datetime.now(timezone.utc),
            )
        except Exception:  # noqa: BLE001 - a clear conflict must fail closed
            return False
        return True

    def _drain_is_quiescent(self, context: Any, record: dict[str, object]) -> bool:
        revision = record.get("drain_revision")
        if revision is None:
            return True
        if self.drain_store is None:
            return False
        current = self.drain_store.get(
            session_id=context.binding.internal_session_id,
            node_id=context.credentials.identity.node_id,
        )
        if current is None or current.revision != revision:
            raise ValueError("update-drain-revision-changed")
        return self.drain_store.is_quiescent(
            session_id=context.binding.internal_session_id,
            node_id=context.credentials.identity.node_id,
        )

    @staticmethod
    def _draining_result(target_commit: str) -> UpdateInspection:
        return UpdateInspection(
            "draining",
            target_commit=target_commit,
            code="workload-draining",
            message=(
                "The provider is durably draining; existing authoritative work "
                "must finish before host activation can start."
            ),
        )

    def _finish_pending_apply(
        self,
        context: Any,
        state: dict[str, object],
        pending: dict[str, object],
        federation_request_id: str,
        record: dict[str, object],
    ) -> bool:
        """Advance one durable remote apply, or leave it waiting on quiescence."""

        target_commit = record.get("target_commit")
        host_request_id = record.get("host_request_id")
        revision = record.get("revision")
        if (
            not isinstance(target_commit, str)
            or not isinstance(host_request_id, str)
            or isinstance(revision, bool)
            or not isinstance(revision, int)
        ):
            pending.pop(federation_request_id, None)
            return True

        try:
            if not self._drain_is_quiescent(context, record):
                self._report(
                    context,
                    event_type=APPLY_REPORT_EVENT,
                    federation_request_id=federation_request_id,
                    result=self._draining_result(target_commit),
                )
                return False
        except Exception:  # noqa: BLE001 - a drain authority race fails closed
            result = UpdateInspection(
                "failed",
                target_commit=target_commit,
                code="drain-authority-lost",
                message="The durable drain identity changed before activation; update aborted.",
                request_id=host_request_id,
            )
            self._report(
                context,
                event_type=APPLY_REPORT_EVENT,
                federation_request_id=federation_request_id,
                result=result,
            )
            pending.pop(federation_request_id, None)
            return True

        marker = {
            "kind": "apply",
            "phase": "host",
            "request_id": federation_request_id,
            "node_id": context.credentials.identity.node_id,
            "revision": revision,
            "host_request_id": host_request_id,
            "target_commit": target_commit,
            "pending_key": federation_request_id,
            **{
                key: record[key]
                for key in ("drain_revision", "drain_node_id", "drain_provider_ids")
                if key in record
            },
        }
        result = self._host_result(host_request_id)
        if result is None:
            state["in_flight"] = marker
            _write_state(self.state_file, state)
            result = self.handoff.apply(target_commit, request_id=host_request_id)

        if result.target_commit != target_commit:
            result = UpdateInspection(
                "failed",
                target_commit=target_commit,
                code="candidate-mismatch",
                message="The host returned a result for a different update candidate.",
                request_id=host_request_id,
            )
        if result.state == "runtime_verified" and not self._clear_drain(context, record):
            result = UpdateInspection(
                "failed",
                target_commit=target_commit,
                code="drain-clear-failed",
                message="The exact candidate was reported healthy but the durable drain could not be retired.",
                request_id=host_request_id,
            )
        if result.state == "activation_queued":
            pending[federation_request_id] = record
        else:
            pending.pop(federation_request_id, None)
        state["pending"] = pending
        self._store_result_report(
            context,
            state,
            marker,
            event_type=APPLY_REPORT_EVENT,
            federation_request_id=federation_request_id,
            result=result,
        )
        self._send_stored_report(
            self.service,
            context,
            event_type=APPLY_REPORT_EVENT,
            payload=marker["report_payload"],  # type: ignore[arg-type]
            request_id=str(marker["report_request_id"]),
        )
        self._complete_in_flight(state, revision=revision)
        return True

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
                state["pending"] = pending
                self._persist_trial_retirement(state, host_request_id)
                # Every change accumulated so far was included in the durable
                # state write above. Later records may set this again.
                changed = False
                continue
            if "drain_revision" in record:
                changed = self._finish_pending_apply(
                    context,
                    state,
                    pending,
                    federation_request_id,
                    record,
                ) or changed
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
                self._persist_trial_retirement(state, host_request_id)
                return state
        queued = self.handoff.trial(selection, request_id=host_request_id)
        settled = _trial_is_settled(queued)
        if settled:
            pending.pop(federation_request_id, None)
            state["pending"] = pending
            self._report_trial(
                context,
                federation_request_id=federation_request_id,
                document=queued,
            )
            self._persist_trial_retirement(state, host_request_id)
            return state
        # The recorder activation watcher only accepts a stop request for work
        # this device is genuinely waiting on, so the pending record is written
        # before the report, exactly as an update does.
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
        self._recover_in_flight(context, state)
        self._drain_trial_retirements(state)
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

        def advance(event: Any) -> None:
            nonlocal last_revision
            last_revision = int(event.revision)
            state["last_revision"] = last_revision
            _write_state(self.state_file, state)

        def apply_page(events: tuple[Any, ...]) -> None:
            nonlocal authority, last_revision, state
            for event in events:
                authority = self._pin_authority(state, event)
                if event.event_type not in {
                    CHECK_REQUEST_EVENT,
                    APPLY_REQUEST_EVENT,
                    TRIAL_REQUEST_EVENT,
                }:
                    advance(event)
                    continue
                if authority is None or event.actor_node_id != authority:
                    advance(event)
                    continue
                if event.event_type == TRIAL_REQUEST_EVENT:
                    payload, selection = validate_trial_command_payload(event.payload)
                    if local_node not in payload["target_node_ids"]:
                        advance(event)
                        continue
                    federation_request_id = str(payload["request_id"])
                    host_request_id = _host_request_id(
                        federation_request_id,
                        local_node,
                    )
                    state["in_flight"] = {
                        "kind": "trial",
                        "request_id": federation_request_id,
                        "node_id": local_node,
                        "revision": int(event.revision),
                        "host_request_id": host_request_id,
                        "target_commit": selection.target_commit,
                    }
                    _write_state(self.state_file, state)
                    state = self._process_trial_request(
                        context,
                        event,
                        state,
                        local_node=local_node,
                    )
                    self._complete_in_flight(
                        state,
                        revision=int(event.revision),
                    )
                    last_revision = int(event.revision)
                    continue

                payload = validate_command_payload(event.payload)
                targets = payload["target_node_ids"]
                if local_node not in targets:
                    advance(event)
                    continue
                federation_request_id = str(payload["request_id"])
                target = str(payload["target_commit"])
                host_request_id = _host_request_id(
                    federation_request_id,
                    local_node,
                )
                kind = (
                    "check"
                    if event.event_type == CHECK_REQUEST_EVENT
                    else "apply"
                )
                marker = {
                    "kind": kind,
                    "phase": "drain" if kind == "apply" else "host",
                    "request_id": federation_request_id,
                    "node_id": local_node,
                    "revision": int(event.revision),
                    "host_request_id": host_request_id,
                    "target_commit": target,
                }
                state["in_flight"] = marker
                # This marker is the pre-side-effect crash boundary. Restart
                # can report/recover it without reissuing an accepted command.
                _write_state(self.state_file, state)

                if kind == "check":
                    result = _inspect_host_update(
                        self.handoff,
                        target=target,
                        fetch=True,
                        request_id=host_request_id,
                    )
                    self._store_result_report(
                        context,
                        state,
                        marker,
                        event_type=CHECK_REPORT_EVENT,
                        federation_request_id=federation_request_id,
                        result=result,
                    )
                else:
                    pending = state.get("pending")
                    if not isinstance(pending, dict):
                        pending = {}
                    result: UpdateInspection | None = None
                    drain_record = None
                    try:
                        drain_record = self._request_drain(
                            payload,
                            session_id=context.binding.internal_session_id,
                            local_node=local_node,
                        )
                    except Exception as exc:  # noqa: BLE001 - fail closed
                        result = UpdateInspection(
                            "failed",
                            target_commit=target,
                            code=(
                                "drain-authority-lost"
                                if isinstance(exc, ValueError)
                                else "drain-request-failed"
                            ),
                            message="The durable workload drain could not be established; update aborted.",
                            request_id=host_request_id,
                        )
                        pending.pop(federation_request_id, None)
                    else:
                        if drain_record is not None:
                            record = {
                                "kind": "apply",
                                "revision": int(event.revision),
                                "host_request_id": host_request_id,
                                "target_commit": target,
                                "drain_revision": drain_record.revision,
                                "drain_node_id": drain_record.node_id,
                                "drain_provider_ids": list(drain_record.provider_ids),
                            }
                            marker.update(
                                {
                                    key: record[key]
                                    for key in (
                                        "drain_revision",
                                        "drain_node_id",
                                        "drain_provider_ids",
                                    )
                                }
                            )
                            _write_state(self.state_file, state)
                            try:
                                quiescent = self._drain_is_quiescent(
                                    context,
                                    record,
                                )
                            except Exception:  # noqa: BLE001 - a drain race fails closed
                                quiescent = False
                                result = UpdateInspection(
                                    "failed",
                                    target_commit=target,
                                    code="drain-authority-lost",
                                    message="The durable workload drain identity changed; update aborted.",
                                    request_id=host_request_id,
                                )
                            if not quiescent and result is None:
                                pending[federation_request_id] = record
                                result = self._draining_result(target)
                            elif result is None:
                                existing = self._host_result(host_request_id)
                                if existing is not None and existing.target_commit == target:
                                    result = existing
                                    pending.pop(federation_request_id, None)
                                else:
                                    result = self.handoff.apply(
                                        target,
                                        request_id=host_request_id,
                                    )
                                    if result.state == "activation_queued":
                                        pending[federation_request_id] = record
                                    else:
                                        pending.pop(federation_request_id, None)
                        else:
                            existing = self._host_result(host_request_id)
                            if existing is not None and existing.target_commit == target:
                                result = existing
                                pending.pop(federation_request_id, None)
                            else:
                                result = self.handoff.apply(
                                    target,
                                    request_id=host_request_id,
                                )
                                if result.state == "activation_queued":
                                    pending[federation_request_id] = {
                                        "host_request_id": host_request_id,
                                        "target_commit": target,
                                    }
                                else:
                                    pending.pop(federation_request_id, None)
                        if (
                            drain_record is not None
                            and result.state == "runtime_verified"
                            and not self._clear_drain(context, record)
                        ):
                            result = UpdateInspection(
                                "failed",
                                target_commit=target,
                                code="drain-clear-failed",
                                message="The exact candidate was reported healthy but the durable drain could not be retired.",
                                request_id=host_request_id,
                            )
                    state["pending"] = pending
                    self._store_result_report(
                        context,
                        state,
                        marker,
                        event_type=APPLY_REPORT_EVENT,
                        federation_request_id=federation_request_id,
                        result=result,
                    )

                self._send_stored_report(
                    self.service,
                    context,
                    event_type=str(marker["report_event_type"]),
                    payload=marker["report_payload"],  # type: ignore[arg-type]
                    request_id=str(marker["report_request_id"]),
                )
                self._complete_in_flight(
                    state,
                    revision=int(event.revision),
                )
                last_revision = int(event.revision)

        replay_authoritative_history(
            lambda revision: context.coordinator.replay_page(
                session_id=context.binding.internal_session_id,
                actor_node_id=local_node,
                last_applied_revision=revision,
                limit=_PROCESSOR_REPLAY_PAGE_EVENTS,
            ),
            apply_page=apply_page,
            max_pages=_MAX_PROCESSOR_REPLAY_PAGES,
            start_revision=last_revision,
        )


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
