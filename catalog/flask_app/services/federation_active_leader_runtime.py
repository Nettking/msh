"""Bind leader-owned Flask controls to transferable Federation leadership.

The core Federation keeps immutable session creation provenance while
``session.leader.changed`` records a coordinator-authored monotonic leadership
chain. This module adapts existing leader-owned product services without
weakening their local safety boundaries:

* software-update and capability-request issuers require the current leader;
* remotely paired successor leaders append the same bounded session events
  through their authenticated relay client;
* update/capability processors follow the coordinator-authored leader chain and
  fence commands from former leaders after a term change; and
* existing processor state is replayed once from session genesis when upgraded
  from creator-pinned authority.

The adapter preserves the original service classes for configured test/product
instances and is installed once by ``services.__init__`` before submodules are
returned to callers.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from flask import current_app

from catalog.federation.session_leadership import (
    INITIAL_TERM,
    LEADER_CHANGED_EVENT,
    LEADERSHIP_SCHEMA,
)

from . import federation_capability_requests as capability_requests
from . import federation_update_events as update_events
from . import federation_update_service as update_service
from .capability_onboarding_service import get_capability_onboarding_service
from .federation_leader_authority import (
    require_federation_leader,
    resolve_federation_leader,
)

_LEGACY_CAPABILITY_SERVICE = capability_requests.FederationCapabilityRequestService
_LEGACY_CAPABILITY_PROCESSOR = capability_requests.FederationCapabilityRequestProcessor
_LEGACY_UPDATE_SERVICE = update_service.FederationUpdateService
_LEGACY_UPDATE_PROCESSOR = update_events.FederationUpdateEventProcessor
_INSTALLED = False


def _append_authenticated_event(
    context: Any,
    *,
    event_type: str,
    payload: dict[str, object],
    request_id: str,
) -> None:
    """Append through local coordinator authority or the paired relay runtime."""

    onboarding = get_capability_onboarding_service()
    remote_store = getattr(onboarding, "remote_store", None)
    load_remote = getattr(remote_store, "load", None)
    remote = load_remote() if callable(load_remote) else None
    if remote is None:
        context.coordinator.append_event(
            session_id=context.binding.internal_session_id,
            actor_node_id=context.credentials.identity.node_id,
            request_id=request_id,
            event_type=event_type,
            payload=payload,
        )
        return
    relay_runtime = getattr(onboarding, "relay_runtime", None)
    append = getattr(relay_runtime, "append_session_event", None)
    if not callable(append):
        raise PermissionError("federation_leader_transport_unavailable")
    append(
        remote,
        session_id=context.binding.internal_session_id,
        event_type=event_type,
        payload=payload,
        request_id=request_id,
    )


def _prepare_processor_state(
    state: dict[str, object],
    context: Any,
) -> bool:
    """Upgrade creator-pinned processor state for transferable leadership.

    Older unit fakes and retained compatibility facades may not expose the
    coordinator ID unless a leadership transition is present. In that case term
    1 creator behavior remains valid; a transition itself still fails closed
    unless its coordinator identity can be authenticated.
    """

    changed = False
    coordinator_id = str(getattr(context.coordinator, "coordinator_id", "") or "")
    persisted_coordinator = state.get("coordinator_id")
    if coordinator_id:
        if persisted_coordinator is not None and persisted_coordinator != coordinator_id:
            raise ValueError("federation_coordinator_identity_changed")
        if persisted_coordinator != coordinator_id:
            state["coordinator_id"] = coordinator_id
            changed = True

    # Older processors pinned session.created forever and could already have a
    # cursor beyond a new leadership event. Replay once from genesis so the
    # coordinator-authored chain is reconstructed before another command runs.
    authority_term = state.get("authority_term")
    if (
        isinstance(authority_term, bool)
        or not isinstance(authority_term, int)
        or authority_term < INITIAL_TERM
    ):
        state["last_revision"] = 0
        state["authority_node_id"] = None
        state["authority_term"] = 0
        changed = True
    return changed


def _pin_leadership_event(state: dict[str, object], event: Any) -> str | None:
    current = state.get("authority_node_id")
    if current is not None and not isinstance(current, str):
        raise ValueError("malformed_pinned_federation_authority")
    raw_term = state.get("authority_term", 0)
    if isinstance(raw_term, bool) or not isinstance(raw_term, int) or raw_term < 0:
        raise ValueError("malformed_pinned_federation_term")
    term = raw_term

    if event.event_type == "session.created":
        candidate = getattr(event, "actor_node_id", None)
        if not isinstance(candidate, str) or not candidate:
            raise ValueError("missing_session_creator_identity")
        if current is None:
            state["authority_node_id"] = candidate
            state["authority_term"] = INITIAL_TERM
            return candidate
        if term == INITIAL_TERM and current != candidate:
            raise ValueError("session_creator_identity_changed")
        return current

    if event.event_type != LEADER_CHANGED_EVENT:
        return current
    coordinator_id = state.get("coordinator_id")
    if not isinstance(coordinator_id, str) or event.actor_node_id != coordinator_id:
        # Generic members may append public session events, but only the durable
        # coordinator authority can advance the leader term.
        return current
    payload = event.payload
    next_leader = payload.get("leader_node_id") if isinstance(payload, dict) else None
    previous = (
        payload.get("previous_leader_node_id") if isinstance(payload, dict) else None
    )
    next_term = payload.get("term") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("schema") != LEADERSHIP_SCHEMA
        or payload.get("session_id") != event.session_id
        or current is None
        or previous != current
        or not isinstance(next_leader, str)
        or not next_leader
        or isinstance(next_term, bool)
        or not isinstance(next_term, int)
        or next_term != term + 1
    ):
        raise ValueError("malformed_federation_leadership_transition")
    state["authority_node_id"] = next_leader
    state["authority_term"] = next_term
    return next_leader


class ActiveLeaderFederationUpdateService(_LEGACY_UPDATE_SERVICE):
    """Existing bounded updater with transferable leader issuance authority."""

    def _context(self):
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            raise PermissionError("federation_authority_required")
        authority = require_federation_leader(context)
        actor = context.credentials.identity.node_id
        if authority.leader_node_id != actor:
            raise PermissionError("update_authority_required")
        return context, actor

    def _authorize_intent(self, intent: update_service.UpdateIntent) -> None:
        intent.validate()
        context = get_capability_onboarding_service().authorized_context()
        if context is None or intent.session_id != context.binding.internal_session_id:
            raise PermissionError("wrong_federation")
        authority = resolve_federation_leader(context)
        if intent.sender_node_id != authority.leader_node_id:
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
        del actor
        if not target_node_ids:
            return
        _append_authenticated_event(
            context,
            event_type=event_type,
            request_id=f"{event_type}-{request_id}",
            payload=update_service.command_payload(
                request_id=request_id,
                target_commit=target,
                target_node_ids=target_node_ids,
                created_at=now,
                expires_at=now + update_service.COMMAND_TTL,
            ),
        )


class ActiveLeaderFederationCapabilityRequestService(_LEGACY_CAPABILITY_SERVICE):
    """Existing bounded capability request flow with transferable leadership."""

    def _context(self) -> tuple[Any, str]:
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            raise PermissionError("federation_authority_required")
        authority = require_federation_leader(context)
        actor = context.credentials.identity.node_id
        if authority.leader_node_id != actor:
            raise PermissionError("capability_request_authority_required")
        return context, actor

    def request_all(self) -> dict[str, object]:
        context, actor = self._context()
        with self._lock:
            now = datetime.now(timezone.utc)
            request_id = f"capability-{capability_requests.uuid.uuid4().hex}"
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
                _append_authenticated_event(
                    context,
                    event_type=capability_requests.REQUEST_EVENT,
                    request_id=f"capability-request-{request_id}",
                    payload=capability_requests.request_payload(
                        request_id=request_id,
                        target_node_ids=tuple(targets),
                        created_at=now,
                        expires_at=now + capability_requests.COMMAND_TTL,
                    ),
                )
            value: dict[str, object] = {
                "schema": capability_requests.STATE_SCHEMA,
                "status": "requested" if targets else "no_reachable_members",
                "request_id": request_id,
                "requested_at": capability_requests._stamp(now),
                "report_deadline": capability_requests._stamp(
                    now + capability_requests.REPORT_WINDOW
                ),
                "expected_report_node_ids": targets,
                "devices": devices,
            }
            self._save(value)
            return value


class ActiveLeaderFederationUpdateEventProcessor(_LEGACY_UPDATE_PROCESSOR):
    """Accept update commands from the leader valid at their ordered revision."""

    @staticmethod
    def _pin_authority(state: dict[str, object], event: Any) -> str | None:
        return _pin_leadership_event(state, event)

    def process(self, context: Any) -> None:
        state = update_events._read_state(self.state_file)
        if _prepare_processor_state(state, context):
            update_events._write_state(self.state_file, state)
        super().process(context)


class ActiveLeaderFederationCapabilityRequestProcessor(_LEGACY_CAPABILITY_PROCESSOR):
    """Accept capability requests from the leader valid at their revision."""

    @staticmethod
    def _pin_authority(state: dict[str, object], event: Any) -> str | None:
        return _pin_leadership_event(state, event)

    def process(self, context: Any) -> None:
        state = self._load()
        if _prepare_processor_state(state, context):
            self._save(state)
        super().process(context)


def _get_active_update_service() -> ActiveLeaderFederationUpdateService:
    configured = current_app.config.get("FEDERATION_UPDATE_SERVICE")
    if isinstance(configured, ActiveLeaderFederationUpdateService):
        return configured
    if isinstance(configured, _LEGACY_UPDATE_SERVICE):
        service = ActiveLeaderFederationUpdateService(
            configured.local,
            configured.state_file,
        )
        current_app.config["FEDERATION_UPDATE_SERVICE"] = service
        return service

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
        local = update_service.HostUpdateHandoff(handoff)
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
    service = ActiveLeaderFederationUpdateService(local, state)
    current_app.config["FEDERATION_UPDATE_SERVICE"] = service
    return service


def _get_active_capability_request_service() -> ActiveLeaderFederationCapabilityRequestService:
    configured = current_app.config.get("FEDERATION_CAPABILITY_REQUEST_SERVICE")
    if isinstance(configured, ActiveLeaderFederationCapabilityRequestService):
        return configured
    if isinstance(configured, _LEGACY_CAPABILITY_SERVICE):
        service = ActiveLeaderFederationCapabilityRequestService(configured.state_file)
        current_app.config["FEDERATION_CAPABILITY_REQUEST_SERVICE"] = service
        return service
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
    service = ActiveLeaderFederationCapabilityRequestService(state_file)
    current_app.config["FEDERATION_CAPABILITY_REQUEST_SERVICE"] = service
    return service


def install_active_leader_runtime() -> None:
    """Replace creator-pinned Flask service bindings once per interpreter."""

    global _INSTALLED
    if _INSTALLED:
        return
    capability_requests.FederationCapabilityRequestService = (
        ActiveLeaderFederationCapabilityRequestService
    )
    capability_requests.FederationCapabilityRequestProcessor = (
        ActiveLeaderFederationCapabilityRequestProcessor
    )
    capability_requests.get_federation_capability_request_service = (
        _get_active_capability_request_service
    )
    update_service.FederationUpdateService = ActiveLeaderFederationUpdateService
    update_service.get_federation_update_service = _get_active_update_service
    update_events.FederationUpdateEventProcessor = ActiveLeaderFederationUpdateEventProcessor
    _INSTALLED = True


__all__ = [
    "ActiveLeaderFederationCapabilityRequestProcessor",
    "ActiveLeaderFederationCapabilityRequestService",
    "ActiveLeaderFederationUpdateEventProcessor",
    "ActiveLeaderFederationUpdateService",
    "install_active_leader_runtime",
]
