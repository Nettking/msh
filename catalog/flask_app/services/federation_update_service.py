"""Manual-only orchestration for exact-commit Federation MSH updates."""

from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

from flask import current_app

from catalog.federation.software_update import (
    APPROVED_BRANCH,
    APPROVED_REPOSITORY,
    OID_RE,
    GitUpdateAdapter,
    UpdateInspection,
)

from .capability_onboarding_service import get_capability_onboarding_service

SCHEMA = "msh.federation-update.v1"
MAX_HISTORY = 20


@dataclass(frozen=True)
class UpdateIntent:
    request_id: str
    session_id: str
    sender_node_id: str
    repository: str
    branch: str
    target_commit: str
    created_at: str
    expires_at: str
    schema: str = SCHEMA

    def validate(self) -> None:
        if self.schema != SCHEMA or len(json.dumps(self.__dict__)) > 4096:
            raise ValueError("malformed_message")
        if not OID_RE.fullmatch(self.target_commit):
            raise ValueError("malformed_target")
        if self.repository != APPROVED_REPOSITORY or self.branch != APPROVED_BRANCH:
            raise ValueError("unapproved_source")
        if len(self.request_id) > 64 or not self.request_id:
            raise ValueError("malformed_request_id")


class UpdatePeer(Protocol):
    node_id: str
    label: str
    reachable: bool

    def inspect_update(self, intent: UpdateIntent) -> UpdateInspection: ...
    def apply_update(self, intent: UpdateIntent) -> UpdateInspection: ...


class FederationUpdateService:
    """Explicit checks and rollouts bound to the session creator authority.

    Membership alone is deliberately insufficient.  Until MSH gains a user
    login/administrator role, the existing session creator identity is the
    smallest authoritative coordinator identity available.  Browser CSRF is
    request-integrity protection, not operator authentication.  Peer adapters
    must use the existing authenticated, session-bound control path.
    """

    def __init__(self, git: GitUpdateAdapter, state_file: Path, peers: tuple[UpdatePeer, ...] = ()) -> None:
        self.git = git
        self.state_file = state_file
        self.peers = peers
        self._lock = threading.Lock()

    def _context(self):
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            raise PermissionError("federation_authority_required")
        session = context.coordinator.store.get_session(context.binding.internal_session_id)
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
            created = datetime.fromisoformat(intent.created_at)
            expires = datetime.fromisoformat(intent.expires_at)
        except ValueError as exc:
            raise ValueError("malformed_timestamp") from exc
        if created.tzinfo is None or expires.tzinfo is None or created > now + timedelta(minutes=1) or expires <= now or expires - created > timedelta(minutes=15):
            raise ValueError("expired_or_invalid_request")

    def receive_check(self, intent: UpdateIntent) -> UpdateInspection:
        """Authenticated control transports call this bounded, read-only handler."""
        self._authorize_intent(intent)
        return self.git.inspect(target=intent.target_commit, fetch=True)

    def receive_apply(self, intent: UpdateIntent) -> UpdateInspection:
        """Idempotently receive one declarative update intent (never argv)."""
        self._authorize_intent(intent)
        if not self._lock.acquire(blocking=False):
            return UpdateInspection("blocked", code="update_in_progress", message="Another update is active on this device.")
        try:
            state = self.snapshot()
            history = state.get("received", [])
            if not isinstance(history, list):
                history = []
            previous = next((item for item in history if isinstance(item, dict) and item.get("request_id") == intent.request_id), None)
            if previous is not None:
                if previous.get("target_commit") != intent.target_commit:
                    return UpdateInspection("blocked", code="request_id_conflict", message="The request identifier was already used for another target.")
                return UpdateInspection(str(previous.get("state", "unknown")), target_commit=intent.target_commit, code=previous.get("code"), message=previous.get("message"))
            result = self.git.apply(intent.target_commit)
            record = {"request_id": intent.request_id, "target_commit": intent.target_commit, "state": result.state, "code": result.code, "message": result.message, "recorded_at": datetime.now(timezone.utc).isoformat()}
            state["received"] = ([record] + history)[:MAX_HISTORY]
            self._save(state)
            return result
        finally:
            self._lock.release()

    def snapshot(self) -> dict[str, object]:
        try:
            value = json.loads(self.state_file.read_text(encoding="utf-8"))
            if not isinstance(value, dict) or value.get("schema") != SCHEMA:
                raise ValueError
            return value
        except (OSError, ValueError, json.JSONDecodeError):
            return {"schema": SCHEMA, "status": "not_checked", "devices": []}

    def _save(self, value: dict[str, object]) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_file.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, separators=(",", ":")), encoding="utf-8")
        temporary.replace(self.state_file)

    @staticmethod
    def _device(node_id: str, label: str, result: UpdateInspection, reachable: bool = True) -> dict[str, object]:
        return {"node_id": node_id[:128], "label": label[:128], "reachable": reachable, **result.to_dict()}

    def check(self) -> dict[str, object]:
        context, actor = self._context()
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("update_in_progress")
        try:
            local = self.git.inspect(fetch=True)
            target = local.target_commit
            now = datetime.now(timezone.utc)
            devices = [self._device(actor, "This device", local)]
            if target:
                intent = UpdateIntent(str(uuid.uuid4()), context.binding.internal_session_id, actor, APPROVED_REPOSITORY, APPROVED_BRANCH, target, now.isoformat(), (now + timedelta(minutes=10)).isoformat())
                for peer in self.peers:
                    if not peer.reachable:
                        devices.append(self._device(peer.node_id, peer.label, UpdateInspection("offline", code="node_offline", message="Device was not reachable during this check."), False))
                        continue
                    try:
                        result = peer.inspect_update(intent)
                    except Exception:  # transport diagnostics are never exposed
                        result = UpdateInspection("error", code="remote_unavailable", message="The device did not complete the bounded check.")
                    devices.append(self._device(peer.node_id, peer.label, result))
            eligible = sum(item.get("state") == "update_available" for item in devices)
            value = {"schema": SCHEMA, "status": "update_available" if eligible else local.state, "checked_at": now.isoformat(), "expires_at": (now + timedelta(minutes=10)).isoformat(), "repository": APPROVED_REPOSITORY, "branch": APPROVED_BRANCH, "target_commit": target, "devices": devices, "eligible_count": eligible}
            self._save(value)
            return value
        finally:
            self._lock.release()

    def update_all(self, *, confirmed_target: str) -> dict[str, object]:
        context, actor = self._context()
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("update_in_progress")
        try:
            checked = self.snapshot()
            target = checked.get("target_commit")
            expires = checked.get("expires_at")
            if target != confirmed_target or not isinstance(target, str) or not OID_RE.fullmatch(target):
                raise ValueError("stale_or_mismatched_confirmation")
            if not isinstance(expires, str) or datetime.fromisoformat(expires) <= datetime.now(timezone.utc):
                raise ValueError("expired_check")
            now = datetime.now(timezone.utc)
            intent = UpdateIntent(str(uuid.uuid4()), context.binding.internal_session_id, actor, APPROVED_REPOSITORY, APPROVED_BRANCH, target, now.isoformat(), (now + timedelta(minutes=10)).isoformat())
            intent.validate()
            checked_by_id = {item.get("node_id"): item for item in checked.get("devices", []) if isinstance(item, dict)}
            results: list[dict[str, object]] = []
            active = {**checked, "status": "updating", "request_id": intent.request_id, "devices": [
                {**item, "state": "updating"} if item.get("state") == "update_available" else item
                for item in checked.get("devices", []) if isinstance(item, dict)
            ]}
            self._save(active)
            # Snapshot reachable peers now; no intent is retained for offline peers.
            for peer in tuple(self.peers):
                prior = checked_by_id.get(peer.node_id, {})
                if not peer.reachable:
                    results.append(self._device(peer.node_id, peer.label, UpdateInspection("offline", code="node_offline", message="Device was offline when Update source on all devices was pressed."), False))
                elif prior.get("state") != "update_available":
                    results.append(self._device(peer.node_id, peer.label, UpdateInspection("blocked", code="not_eligible", message="Device was not eligible in the confirmed check.")))
                else:
                    try:
                        results.append(self._device(peer.node_id, peer.label, peer.apply_update(intent)))
                    except Exception:
                        results.append(self._device(peer.node_id, peer.label, UpdateInspection("failed", code="remote_unavailable", message="The device did not complete the update.")))
                active["devices"] = results + [item for item in active["devices"] if item.get("node_id") not in {result["node_id"] for result in results}]
                self._save(active)
            # The coordinating checkout is always last.
            local_prior = checked_by_id.get(actor, {})
            local = self.git.apply(target) if local_prior.get("state") == "update_available" else UpdateInspection("blocked", code="not_eligible", message="This device was not eligible in the confirmed check.")
            results.append(self._device(actor, "This device", local))
            failed = any(item["state"] in {"failed", "error"} for item in results)
            needs_activation = any(item["state"] == "source_updated_restart_required" for item in results)
            status = "source_update_completed_with_failures" if failed else "source_updated_restart_required" if needs_activation else "source_update_completed"
            value = {**checked, "status": status, "request_id": intent.request_id, "updated_at": datetime.now(timezone.utc).isoformat(), "devices": results, "eligible_count": 0}
            self._save(value)
            return value
        finally:
            self._lock.release()


def get_federation_update_service() -> FederationUpdateService:
    configured = current_app.config.get("FEDERATION_UPDATE_SERVICE")
    if isinstance(configured, FederationUpdateService):
        return configured
    root = Path(current_app.config.get("MSH_REPOSITORY_ROOT") or Path(__file__).resolve().parents[3])
    state = Path(current_app.config.get("FEDERATION_UPDATE_STATE") or root / "data" / "federation-update-status.json")
    peers = tuple(current_app.config.get("FEDERATION_UPDATE_PEERS") or ())
    return FederationUpdateService(GitUpdateAdapter(root), state, peers)
