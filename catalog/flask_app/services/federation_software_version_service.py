"""Leader-owned Federation software-version selection.

"Update all devices" answers one question: is every device on approved main?
This service answers a different one -- *which* approved version is a given
device running, and can I temporarily put it on a test branch and get it back?

It grants no new authority anywhere:

* the branch list comes from the local host agent, which reads it from the
  approved repository it is already pinned to. Flask still runs no Git;
* a request names an approved repository, a branch and one exact commit, and
  travels through the same authoritative event log with the same leader
  fencing as an update; and
* the receiving device re-resolves the branch on its own approved remote,
  pins the exact known-good commit it is running, and is the only party that
  decides whether the trial is safe to start.

Trials target *other* devices. Putting the coordinator that is serving this
page onto a test branch would replace the process rendering it, which is a
different feature with different guarantees.
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

from catalog.federation.projections.authority_adapter import FederationAuthorityAdapter
from catalog.federation.software_trial import (
    TRIAL_FAILED,
    TRIAL_REQUEST_TTL_SECONDS,
    TRIAL_RUNNING,
    TrialRefused,
    TrialSelection,
)
from catalog.federation.software_update import (
    APPROVED_BRANCH,
    APPROVED_REPOSITORY,
    BRANCH_RE,
    OID_RE,
)

from .capability_onboarding_service import get_capability_onboarding_service
from .federation_active_leader_runtime import (
    _append_authenticated_event,
    get_active_update_service,
)
from .federation_leader_authority import require_federation_leader
from .federation_update_events import (
    TRIAL_REPORT_EVENT,
    TRIAL_REQUEST_EVENT,
    trial_command_payload,
    trial_from_report,
)

SCHEMA = "fcp.federation-software-version.v1"
COMMAND_TTL = timedelta(seconds=TRIAL_REQUEST_TTL_SECONDS)
#: How long the coordinator keeps waiting for a device's trial outcome. It
#: covers a prevalidation, a graceful stop, a relaunch, a bounded startup
#: window and, if that fails, a full verified rollback.
REPORT_WINDOW = timedelta(minutes=10)
CONNECTED_STATES = frozenset({"connected", "online", "ready", "active"})
MAX_STATE_BYTES = 256 * 1024

#: Device software states the UI renders, mapped from what a device reported.
PENDING_TRIAL_STATES = frozenset(
    {
        "requested",
        "trial_requested",
        "preparing",
        "trial_starting",
        "trial_verifying",
        "rollback_starting",
        "rollback_verifying",
    }
)


class FederationSoftwareVersionService:
    """Offer approved branches, and run one bounded trial per device."""

    def __init__(self, local: Any, state_file: Path | str) -> None:
        self.local = local
        self.state_file = Path(state_file)
        self._lock = threading.RLock()

    # ---- authority -------------------------------------------------------

    def _context(self) -> tuple[Any, str]:
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            raise PermissionError("federation_authority_required")
        authority = require_federation_leader(context)
        actor = context.credentials.identity.node_id
        if authority.leader_node_id != actor:
            raise PermissionError("software_version_authority_required")
        return context, actor

    # ---- durable state ---------------------------------------------------

    def _empty(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA,
            "status": "idle",
            "repository": APPROVED_REPOSITORY,
            "devices": [],
            "expected_report_node_ids": [],
        }

    def _load(self) -> dict[str, Any]:
        try:
            raw = self.state_file.read_bytes()
            if len(raw) > MAX_STATE_BYTES:
                raise ValueError
            value = json.loads(raw.decode("utf-8"))
            if not isinstance(value, dict) or value.get("schema") != SCHEMA:
                raise ValueError
            return value
        except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
            return self._empty()

    def _save(self, value: dict[str, Any]) -> None:
        payload = json.dumps(
            {**value, "schema": SCHEMA},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(payload) > MAX_STATE_BYTES:
            raise ValueError("software_version_state_too_large")
        self.state_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, name = tempfile.mkstemp(
            prefix=f".{self.state_file.name}.", dir=self.state_file.parent
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

    # ---- branches --------------------------------------------------------

    def branches(self) -> tuple[dict[str, str], ...]:
        """Approved-repository branches, approved main always first.

        Never raises: an unreachable or unsupported host agent means the
        dropdown offers approved main only, which is the safe default rather
        than an error page.
        """

        listed: tuple[dict[str, str], ...] = ()
        reader = getattr(self.local, "approved_branches", None)
        if callable(reader):
            try:
                listed = tuple(
                    item.to_dict() if hasattr(item, "to_dict") else item
                    for item in reader()
                )
            except Exception:  # noqa: BLE001 - a dropdown never fails a page
                listed = ()
        result: list[dict[str, str]] = []
        for item in listed:
            name = item.get("name")
            commit = item.get("commit")
            if (
                isinstance(name, str)
                and BRANCH_RE.fullmatch(name)
                and isinstance(commit, str)
                and OID_RE.fullmatch(commit)
            ):
                result.append({"name": name, "commit": commit.lower()})
        result.sort(key=lambda item: (item["name"] != APPROVED_BRANCH, item["name"]))
        return tuple(result)

    # ---- device view -----------------------------------------------------

    @staticmethod
    def _is_connected(state: str) -> bool:
        return str(state).casefold() in CONNECTED_STATES

    def _authority_devices(self, context: Any, actor: str) -> tuple[Any, ...]:
        snapshot = FederationAuthorityAdapter(
            context.coordinator,
            actor_node_id=actor,
            internal_session_id=context.binding.internal_session_id,
        ).snapshot()
        return snapshot.devices if snapshot.available else ()

    @staticmethod
    def _update_rows() -> dict[str, dict[str, Any]]:
        """What the last update check learned about each device's version."""

        try:
            snapshot = get_active_update_service().snapshot()
        except Exception:  # noqa: BLE001 - a passive view never fails a page
            return {}
        rows = snapshot.get("devices")
        if not isinstance(rows, list):
            return {}
        return {
            str(row.get("node_id")): row
            for row in rows
            if isinstance(row, dict) and isinstance(row.get("node_id"), str)
        }

    @staticmethod
    def _row(
        node_id: str,
        label: str,
        *,
        connected: bool,
        update_row: dict[str, Any] | None,
        report: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """One device's software version, from durable evidence only."""

        update_row = update_row or {}
        trial = update_row.get("trial")
        trial = trial if isinstance(trial, dict) else {}
        # The device decides whether it is still on a test branch, because only
        # it can see its own running commit. A retained record of a finished
        # trial is history, not current state.
        on_branch = bool(trial.get("active"))
        branch = trial.get("branch") if on_branch else APPROVED_BRANCH
        commit = update_row.get("running_commit") or update_row.get("current_commit")
        row: dict[str, Any] = {
            "node_id": node_id[:512],
            "label": label[:128],
            "connected": connected,
            "branch": branch or APPROVED_BRANCH,
            "commit": commit,
            "on_test_branch": on_branch,
            "software_state": update_row.get("state") or "unknown",
            "safe_branch": trial.get("safe_branch"),
            "safe_commit": trial.get("safe_commit"),
            "failure_reason": trial.get("failure_reason"),
            "message": update_row.get("message"),
            "acceptance": None,
            "recovery": None,
            "trial_state": None,
        }
        if report:
            reported_branch = report.get("branch")
            reported_commit = report.get("running_commit") or report.get(
                "target_commit"
            )
            state = str(report.get("state") or "")
            row.update(
                {
                    "trial_state": state,
                    "software_state": state or row["software_state"],
                    "message": report.get("message") or row["message"],
                    "safe_branch": report.get("safe_branch") or row["safe_branch"],
                    "safe_commit": report.get("safe_commit") or row["safe_commit"],
                    "acceptance": report.get("acceptance"),
                    "recovery": report.get("recovery"),
                }
            )
            if state == TRIAL_RUNNING and reported_branch:
                row["branch"] = reported_branch
                row["on_test_branch"] = True
                row["commit"] = reported_commit or row["commit"]
            elif state in PENDING_TRIAL_STATES and reported_branch:
                row["branch"] = reported_branch
                row["on_test_branch"] = True
            elif state in {"safe_restored", TRIAL_FAILED}:
                row["branch"] = report.get("safe_branch") or APPROVED_BRANCH
                row["on_test_branch"] = False
                row["commit"] = report.get("safe_commit") or row["commit"]
        return row

    def _reports(
        self,
        context: Any,
        actor: str,
        *,
        request_id: str,
    ) -> dict[str, dict[str, Any]]:
        reports: dict[str, dict[str, Any]] = {}
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
                if event.event_type != TRIAL_REPORT_EVENT:
                    continue
                parsed = trial_from_report(event.payload)
                if parsed is None:
                    continue
                reported_request, node_id, document = parsed
                if reported_request != request_id or node_id != event.actor_node_id:
                    continue
                reports[node_id] = document
            if not events or last_revision >= current_revision:
                break
        return reports

    def _refresh(
        self,
        value: dict[str, Any],
        context: Any,
        actor: str,
    ) -> dict[str, Any]:
        request_id = value.get("request_id")
        expected = [
            item
            for item in value.get("expected_report_node_ids", [])
            if isinstance(item, str)
        ]
        reports = (
            self._reports(context, actor, request_id=request_id)
            if isinstance(request_id, str) and expected
            else {}
        )
        update_rows = self._update_rows()
        rows: list[dict[str, Any]] = []
        for device in self._authority_devices(context, actor):
            if device.node_id == actor:
                continue
            rows.append(
                self._row(
                    device.node_id,
                    str(device.label),
                    connected=self._is_connected(device.state),
                    update_row=update_rows.get(device.node_id),
                    report=reports.get(device.node_id),
                )
            )
        deadline = _parse_stamp(value.get("report_deadline"))
        expired = deadline is not None and datetime.now(timezone.utc) >= deadline
        pending = [
            row
            for row in rows
            if row["node_id"] in expected
            and (row.get("trial_state") or "requested") in PENDING_TRIAL_STATES
        ]
        if expired:
            for row in pending:
                row["trial_state"] = "trial_report_timeout"
                row["software_state"] = "trial_report_timeout"
                row["message"] = (
                    "The device did not report a trial outcome before the "
                    "coordinator's deadline. Check the device itself: its own "
                    "automatic fallback is what restores recording, not this "
                    "page."
                )
            pending = []
        if not expected:
            status = "idle"
        elif pending:
            status = "switching"
        else:
            status = "reported"
        return {**value, "status": status, "devices": rows}

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            value = self._load()
            try:
                context, actor = self._context()
            except Exception:  # noqa: BLE001 - a passive view stays available
                return {**value, "branches": [], "can_manage": False}
            try:
                refreshed = self._refresh(value, context, actor)
            except Exception:  # noqa: BLE001 - a passive view stays available
                return {**value, "branches": [], "can_manage": True}
            if {k: v for k, v in refreshed.items() if k != "branches"} != value:
                self._save(refreshed)
            return {
                **refreshed,
                "branches": [dict(item) for item in self.branches()],
                "can_manage": True,
            }

    # ---- the one action --------------------------------------------------

    def switch_version(
        self,
        *,
        node_ids: list[str],
        branch: str,
        target_commit: str,
    ) -> dict[str, Any]:
        """Ask named devices to run one approved branch at one exact commit."""

        context, actor = self._context()
        try:
            selection = TrialSelection(
                APPROVED_REPOSITORY, branch, target_commit.lower()
            ).validate()
        except TrialRefused as refused:
            raise ValueError(refused.code) from refused
        with self._lock:
            authority = {
                device.node_id: device
                for device in self._authority_devices(context, actor)
            }
            targets: list[str] = []
            for node_id in node_ids:
                # The coordinator serving this page is never a target: the
                # trial would replace the process rendering it, and its
                # fallback could not be verified from inside itself.
                if node_id == actor or node_id not in authority:
                    continue
                if not self._is_connected(authority[node_id].state):
                    continue
                if node_id not in targets:
                    targets.append(node_id)
            if not targets:
                raise ValueError("no_reachable_selected_devices")
            now = datetime.now(timezone.utc)
            request_id = f"trial-{uuid.uuid4().hex}"
            _append_authenticated_event(
                context,
                event_type=TRIAL_REQUEST_EVENT,
                request_id=f"{TRIAL_REQUEST_EVENT}-{request_id}",
                payload=trial_command_payload(
                    request_id=request_id,
                    selection=selection,
                    target_node_ids=tuple(targets),
                    created_at=now,
                    expires_at=now + COMMAND_TTL,
                ),
            )
            value: dict[str, Any] = {
                "schema": SCHEMA,
                "status": "switching",
                "repository": APPROVED_REPOSITORY,
                "request_id": request_id,
                "branch": selection.branch,
                "target_commit": selection.target_commit,
                "requested_at": _stamp(now),
                "report_deadline": _stamp(now + REPORT_WINDOW),
                "expected_report_node_ids": targets,
                "devices": [],
            }
            refreshed = self._refresh(value, context, actor)
            self._save(refreshed)
            return refreshed


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_stamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return None if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def get_federation_software_version_service() -> FederationSoftwareVersionService:
    configured = current_app.config.get("FEDERATION_SOFTWARE_VERSION_SERVICE")
    if isinstance(configured, FederationSoftwareVersionService):
        return configured
    service = get_active_update_service()
    onboarding_database = Path(
        current_app.config.get(
            "CAPABILITY_ONBOARDING_STATE_DATABASE",
            "data/federation/onboarding/onboarding.sqlite3",
        )
    )
    federation_root = onboarding_database.parent.parent
    state = Path(
        current_app.config.get(
            "FEDERATION_SOFTWARE_VERSION_STATE",
            federation_root / "software-version.json",
        )
    )
    resolved = FederationSoftwareVersionService(service.local, state)
    current_app.config["FEDERATION_SOFTWARE_VERSION_SERVICE"] = resolved
    return resolved


__all__ = [
    "REPORT_WINDOW",
    "SCHEMA",
    "FederationSoftwareVersionService",
    "get_federation_software_version_service",
]
