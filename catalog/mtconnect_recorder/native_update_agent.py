"""Host-side update agent for the native standalone MTConnect recorder.

This agent speaks the same bounded file handoff the Compose host agents use, so
the Federation side of software updates is unchanged: a request arrives as one
declarative JSON document naming an approved commit, and a bounded result comes
back. Only the *activation* differs, because the runtime being replaced is a
native process rather than a container.

Responsibilities, in the order they must happen:

1. answer ``check`` from the real checkout and the running recorder's own commit;
2. for ``apply``, prevalidate everything that can fail while capture is still
   running -- including whether the current interpreter can still run the target;
3. ask the supervised recorder to stop, by writing an activation the recorder
   will only accept if it names that exact process and a pending update it is
   actually waiting for;
4. after the supervisor confirms the process is gone, fast-forward the checkout;
5. after the supervisor relaunches, prove the replacement is a *different*
   process running the *exact* target commit with a healthy Federation, and only
   then report success.

The agent never kills a process, never rewrites history, never runs
``reset``/``clean``/``stash``, never builds or starts a Compose service, and
never takes a path, command or argument from a peer.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from catalog.federation.software_update import (
    APPROVED_BRANCH,
    APPROVED_REPOSITORY,
    OID_RE,
    GitUpdateAdapter,
)

from .native_identity import NONCE_RE
from .native_update import (
    RECORDER_EXIT_TIMEOUT_SECONDS,
    REPLACEMENT_TIMEOUT_SECONDS,
    REQUEST_ID_RE,
    REQUEST_SCHEMA,
    RESULT_SCHEMA,
    STAGE_CLAIMED,
    STAGE_COMPLETED,
    STAGE_FAILED,
    STAGE_PREVALIDATED,
    STAGE_RECORDER_EXITED,
    STAGE_REPLACEMENT_STARTED,
    STAGE_REPLACEMENT_VERIFIED,
    STAGE_SOURCE_UPDATED,
    STAGE_STOP_REQUESTED,
    ActivationDecision,
    NativeRecorderUpdatePaths,
    NativeUpdateJournal,
    RecorderRuntimeStatus,
    activation_intent,
    dependency_changes,
    parse_stamp,
    pending_federation_updates,
    process_is_running,
    read_recorder_status,
    stamp,
    utc_now,
    write_json_atomic,
)

MAX_REQUEST_BYTES = 8192

#: The recorder queues its host request and then persists the matching pending
#: Federation record. The two writes are not atomic with each other, so the
#: agent waits briefly for the record rather than refusing a real update that
#: simply had not been written yet.
PENDING_VISIBILITY_ATTEMPTS = 10
PENDING_VISIBILITY_INTERVAL_SECONDS = 0.5


class UpdateRefused(Exception):
    """A bounded, reportable refusal that left capture untouched."""

    def __init__(self, code: str, message: str, *, state: str = "error") -> None:
        super().__init__(code)
        self.code = code
        self.message = message
        self.state = state


def validate_request(value: object) -> tuple[str, str, str | None, datetime | None]:
    if not isinstance(value, dict) or value.get("schema") != REQUEST_SCHEMA:
        raise UpdateRefused("malformed_message", "Unsupported update request.")
    request_id = value.get("request_id")
    action = value.get("action")
    target = value.get("target_commit")
    if not isinstance(request_id, str) or not REQUEST_ID_RE.fullmatch(request_id):
        raise UpdateRefused("malformed_request_id", "Unsupported update request.")
    if action not in {"check", "apply"}:
        raise UpdateRefused("malformed_action", "Unsupported update request.")
    if (
        value.get("repository") != APPROVED_REPOSITORY
        or value.get("branch") != APPROVED_BRANCH
    ):
        raise UpdateRefused("unapproved_source", "Unsupported update source.")
    if target is not None and (
        not isinstance(target, str) or not OID_RE.fullmatch(target)
    ):
        raise UpdateRefused("malformed_target", "Unsupported update target.")
    if action == "apply" and target is None:
        raise UpdateRefused("malformed_target", "Unsupported update target.")
    created = parse_stamp(value.get("created_at"))
    expires = parse_stamp(value.get("expires_at"))
    if created is None or expires is None:
        raise UpdateRefused("malformed_timestamp", "Unsupported update request.")
    now = utc_now()
    if (
        created > now + timedelta(minutes=1)
        or expires <= now
        or (expires - created) > timedelta(minutes=15)
    ):
        raise UpdateRefused(
            "expired_or_invalid_request",
            "The update request expired before it could be applied.",
        )
    activate_after: datetime | None = None
    if action == "apply":
        activate_after = parse_stamp(value.get("activate_after"))
        if activate_after is None:
            raise UpdateRefused(
                "malformed_activation_grace", "Unsupported update request."
            )
        if (
            activate_after < created
            or activate_after > expires
            or (activate_after - created) > timedelta(seconds=30)
        ):
            raise UpdateRefused(
                "invalid_activation_grace", "Unsupported update request."
            )
    return request_id, str(action), target, activate_after


class NativeRecorderUpdateAgent:
    """Drive one native recorder's update lifecycle from the host side."""

    def __init__(
        self,
        *,
        repository_root: Path | str,
        data_directory: Path | str,
        supervisor_session: str,
        adapter: GitUpdateAdapter | None = None,
        now: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not NONCE_RE.fullmatch(str(supervisor_session)):
            raise ValueError("malformed_supervisor_session")
        self.root = Path(repository_root).resolve()
        self.paths = NativeRecorderUpdatePaths(data_directory)
        self.supervisor_session = str(supervisor_session)
        self.adapter = adapter or GitUpdateAdapter(self.root)
        self.journal = NativeUpdateJournal(self.paths.journal_file)
        self._now = now
        self._sleep = sleep
        self.paths.directory.mkdir(parents=True, exist_ok=True)

    # ---- bounded result channel -----------------------------------------

    def _result_path(self, request_id: str) -> Path:
        digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
        return self.paths.directory / f"result-{digest}.json"

    def _write_result(
        self,
        *,
        request_id: str,
        action: str,
        state: str,
        current: str | None,
        target: str | None,
        running: str | None,
        code: str | None,
        message: str,
    ) -> dict[str, object]:
        value: dict[str, object] = {
            "schema": RESULT_SCHEMA,
            "request_id": request_id,
            "action": action,
            "state": state,
            "current_commit": current,
            "target_commit": target,
            "running_commit": running,
            "code": code,
            "message": message[:512],
            "completed_at": stamp(self._now()),
        }
        write_json_atomic(self._result_path(request_id), value)
        write_json_atomic(self.paths.result_file, value)
        return value

    # ---- local observations ---------------------------------------------

    def recorder_status(self) -> RecorderRuntimeStatus:
        return read_recorder_status(self.paths.status_file)

    def _running_commit(self) -> str | None:
        status = self.recorder_status()
        if not status.is_fresh(now=self._now()) or not status.is_running():
            return None
        return status.build_commit

    def _current_commit(self) -> str | None:
        inspection = self.adapter.inspect(fetch=False)
        return inspection.current_commit

    # ---- prevalidation ---------------------------------------------------

    def prevalidate(self, target: str) -> Any:
        """Refuse everything refusable while the recorder is still recording."""

        inspection = self.adapter.inspect(target=target, fetch=True)
        if inspection.state not in {"update_available", "up_to_date"}:
            raise UpdateRefused(
                inspection.code or "target_unavailable",
                inspection.message
                or "The recorder checkout is not eligible for activation.",
                state=inspection.state,
            )
        current = inspection.current_commit
        if current is None or not OID_RE.fullmatch(current):
            raise UpdateRefused(
                "unsupported_checkout", "This is not a supported FCP Git checkout."
            )
        changed = dependency_changes(self.root, current, target)
        if changed:
            # The interpreter that is running right now was prepared for the
            # current commit. Stopping capture first and only then discovering
            # the replacement cannot start is the one failure this update path
            # must never produce, so refuse while the recorder is still healthy.
            raise UpdateRefused(
                "dependency_change_requires_manual_update",
                (
                    "This update changes Python dependency inputs ("
                    + ", ".join(changed[:4])
                    + "). Prepare the environment and update this recorder "
                    "manually so capture is never stopped for a runtime that "
                    "cannot start."
                ),
            )
        return inspection

    def _supervised_recorder(self) -> RecorderRuntimeStatus:
        status = self.recorder_status()
        if not status.present:
            raise UpdateRefused(
                "recorder_runtime_unavailable",
                "No native recorder runtime identity was published.",
            )
        if status.supervisor_session != self.supervisor_session:
            raise UpdateRefused(
                "recorder_not_supervised",
                "The running recorder belongs to a different supervisor.",
            )
        if not status.is_fresh(now=self._now()):
            raise UpdateRefused(
                "recorder_heartbeat_stale",
                "The recorder heartbeat is too old to activate an update.",
            )
        if not status.is_running():
            raise UpdateRefused(
                "recorder_runtime_unavailable",
                "The supervised recorder process is not running.",
            )
        return status

    # ---- request handling ------------------------------------------------

    def _claim_request(self) -> dict[str, Any] | None:
        request_file = self.paths.request_file
        processing = request_file.with_name(
            f"processing-{utc_now().timestamp():.6f}.json"
        )
        try:
            request_file.replace(processing)
        except (FileNotFoundError, OSError):
            return None
        try:
            if processing.stat().st_size > MAX_REQUEST_BYTES:
                return None
            return json.loads(processing.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        finally:
            processing.unlink(missing_ok=True)

    def handle_check(self, request_id: str, target: str | None) -> dict[str, object]:
        inspection = self.adapter.inspect(target=target, fetch=True)
        return self._write_result(
            request_id=request_id,
            action="check",
            state=inspection.state,
            current=inspection.current_commit,
            target=inspection.target_commit,
            running=self._running_commit(),
            code=inspection.code,
            message=inspection.message or "",
        )

    def handle_apply(self, request_id: str, target: str) -> dict[str, object] | None:
        """Prevalidate and request a graceful stop; never mutate the checkout.

        Returns a bounded result when the request is already finished or is
        refused, and ``None`` when a stop was requested and the supervisor now
        owns the remaining stages.
        """

        finished = self.journal.history_entry(request_id)
        if finished is not None:
            stored = finished.get("result")
            if isinstance(stored, dict):
                # A duplicate request is answered from the retained result. The
                # checkout is never fast-forwarded or relaunched a second time.
                write_json_atomic(self._result_path(request_id), stored)
                write_json_atomic(self.paths.result_file, stored)
                return stored

        active = self.journal.active()
        if active is not None and active.get("request_id") != request_id:
            return self._write_result(
                request_id=request_id,
                action="apply",
                state="error",
                current=self._current_commit(),
                target=target,
                running=self._running_commit(),
                code="host_update_busy",
                message="Another recorder update activation is already running.",
            )

        try:
            self.journal.claim(request_id=request_id, target_commit=target)
            return self._drive_activation(request_id, target)
        except UpdateRefused as refused:
            return self._fail(request_id, target, refused)

    def _drive_activation(
        self,
        request_id: str,
        target: str,
    ) -> dict[str, object] | None:
        inspection = self.prevalidate(target)
        current = str(inspection.current_commit)
        self.journal.advance(
            STAGE_PREVALIDATED,
            current_commit=current,
            target_commit=target,
        )

        if inspection.state == "up_to_date" and self._running_commit() == target:
            # Source and runtime already match. Report the verified runtime
            # without stopping capture at all.
            result = self._write_result(
                request_id=request_id,
                action="apply",
                state="runtime_verified",
                current=current,
                target=target,
                running=target,
                code="updated",
                message=(
                    "The native recorder is already running the requested commit."
                ),
            )
            self.journal.finish(STAGE_COMPLETED, result=result)
            return result

        status = self._supervised_recorder()
        self._await_pending_federation_update(request_id, target)
        intent = activation_intent(
            request_id=request_id,
            target_commit=target,
            supervisor_session=self.supervisor_session,
            recorder_pid=int(status.pid or 0),
            process_nonce=str(status.process_nonce),
            now=self._now(),
        )
        write_json_atomic(self.paths.activation_file, intent)
        self.journal.advance(
            STAGE_STOP_REQUESTED,
            recorder_pid=status.pid,
            process_nonce=status.process_nonce,
            stop_requested_at=stamp(self._now()),
            activation_expires_at=intent["expires_at"],
        )
        return None

    def _await_pending_federation_update(self, request_id: str, target: str) -> None:
        """Require real, already authenticated Federation work for this commit."""

        for attempt in range(PENDING_VISIBILITY_ATTEMPTS):
            pending = pending_federation_updates(self.paths.federation_state_file)
            if pending.get(request_id) == target:
                return
            if attempt + 1 < PENDING_VISIBILITY_ATTEMPTS:
                self._sleep(PENDING_VISIBILITY_INTERVAL_SECONDS)
        raise UpdateRefused(
            "no_pending_federation_update",
            "The recorder has no pending Federation update request for this commit.",
        )

    def _fail(
        self,
        request_id: str,
        target: str | None,
        refused: UpdateRefused,
    ) -> dict[str, object]:
        self.paths.activation_file.unlink(missing_ok=True)
        result = self._write_result(
            request_id=request_id,
            action="apply",
            state=refused.state,
            current=self._current_commit(),
            target=target,
            running=self._running_commit(),
            code=refused.code,
            message=refused.message,
        )
        active = self.journal.active()
        if active is not None and active.get("request_id") == request_id:
            self.journal.finish(STAGE_FAILED, result=result)
        return result

    # ---- resumable stages ------------------------------------------------

    def resume(self) -> bool:
        """Advance an interrupted activation. Safe to call repeatedly."""

        active = self.journal.active()
        if active is None:
            return False
        request_id = str(active.get("request_id") or "")
        target = str(active.get("target_commit") or "")
        if not REQUEST_ID_RE.fullmatch(request_id) or not OID_RE.fullmatch(target):
            self.journal.discard_active()
            return True
        stage = active.get("stage")
        try:
            if stage in {STAGE_CLAIMED, STAGE_PREVALIDATED}:
                # Nothing durable happened yet: no stop was requested and the
                # checkout is untouched, so re-driving is safe.
                self._drive_activation(request_id, target)
                return True
            if stage == STAGE_STOP_REQUESTED:
                return self._resume_stop_requested(active, request_id, target)
            if stage in {
                STAGE_RECORDER_EXITED,
                STAGE_SOURCE_UPDATED,
                STAGE_REPLACEMENT_STARTED,
            }:
                return self._resume_replacement(active, request_id, target)
            if stage == STAGE_REPLACEMENT_VERIFIED:
                self._complete(active, request_id, target)
                return True
        except UpdateRefused as refused:
            self._fail(request_id, target, refused)
            return True
        except (OSError, RuntimeError, ValueError) as exc:
            # A resumable stage must never be able to end the agent. Report the
            # activation as failed and leave the checkout exactly as it is.
            self._fail(
                request_id,
                target,
                UpdateRefused(
                    "host_update_failed",
                    "The recorder host update agent stopped safely while "
                    f"resuming an activation ({type(exc).__name__}).",
                ),
            )
            return True
        return False

    def _deadline_passed(self, active: dict[str, Any], seconds: float) -> bool:
        updated = parse_stamp(active.get("updated_at"))
        if updated is None:
            return True
        return (self._now() - updated).total_seconds() > seconds

    def _resume_stop_requested(
        self,
        active: dict[str, Any],
        request_id: str,
        target: str,
    ) -> bool:
        expected_nonce = active.get("process_nonce")
        expected_pid = active.get("recorder_pid")
        status = self.recorder_status()
        if status.present and status.process_nonce != expected_nonce:
            # A different recorder instance is running. Our activation can only
            # ever apply to the instance it named, so drop it rather than let a
            # stale marker stop an unrelated recorder.
            raise UpdateRefused(
                "activation_superseded",
                "The recorder was replaced before the update could activate.",
            )
        if process_is_running(expected_pid) and status.process_nonce == expected_nonce:
            expires = parse_stamp(active.get("activation_expires_at"))
            if expires is not None and self._now() >= expires:
                raise UpdateRefused(
                    "activation_expired",
                    "The recorder did not stop before the activation expired.",
                )
            return False
        self.journal.advance(STAGE_RECORDER_EXITED)
        return True

    def _resume_replacement(
        self,
        active: dict[str, Any],
        request_id: str,
        target: str,
    ) -> bool:
        stage = active.get("stage")
        if stage == STAGE_RECORDER_EXITED:
            if self._current_commit() == target:
                active = self.journal.advance(STAGE_SOURCE_UPDATED)
            elif self._deadline_passed(active, RECORDER_EXIT_TIMEOUT_SECONDS):
                raise UpdateRefused(
                    "source_update_incomplete",
                    "The checkout was not fast-forwarded after the recorder exited.",
                )
            else:
                return False
        if self._deadline_passed(active, REPLACEMENT_TIMEOUT_SECONDS):
            raise UpdateRefused(
                "replacement_verification_timeout",
                "The replacement recorder did not prove the updated runtime.",
            )
        if not self._replacement_proven(active, target):
            return False
        self.journal.advance(STAGE_REPLACEMENT_VERIFIED)
        self._complete(self.journal.active() or active, request_id, target)
        return True

    def _replacement_proven(self, active: dict[str, Any], target: str) -> bool:
        status = self.recorder_status()
        expected_nonce = active.get("replacement_nonce")
        activated_at = parse_stamp(active.get("stop_requested_at"))
        return (
            status.present
            and status.is_running()
            and status.is_fresh(now=self._now())
            # A *different* process. Both fields must move: a recycled PID or a
            # survivor reusing its nonce is not a replacement.
            and status.pid != active.get("recorder_pid")
            and status.process_nonce != active.get("process_nonce")
            and (expected_nonce is None or status.process_nonce == expected_nonce)
            and status.supervisor_session == self.supervisor_session
            and status.build_commit == target
            and activated_at is not None
            and status.heartbeat_at is not None
            and status.heartbeat_at > activated_at
            and status.federation_is_healthy()
        )

    def _complete(
        self,
        active: dict[str, Any],
        request_id: str,
        target: str,
    ) -> dict[str, object]:
        self.paths.activation_file.unlink(missing_ok=True)
        status = self.recorder_status()
        result = self._write_result(
            request_id=request_id,
            action="apply",
            state="runtime_verified",
            current=target,
            target=target,
            running=status.build_commit,
            code="updated",
            message=(
                "The native recorder source, process, and Federation membership "
                "were updated and verified."
            ),
        )
        self.journal.finish(STAGE_COMPLETED, result=result)
        return result

    # ---- supervisor entry points ----------------------------------------

    def finalize_after_exit(self) -> dict[str, object]:
        """Fast-forward the checkout, only after the recorder has exited.

        Called by the supervisor once the supervised child has returned the
        approved-update exit code. This is the *only* place the checkout is
        mutated, and it refuses to run while the named process is still alive.
        """

        active = self.journal.active()
        if active is None:
            return {"relaunch": False, "code": "no_pending_activation"}
        request_id = str(active.get("request_id") or "")
        target = str(active.get("target_commit") or "")
        if not REQUEST_ID_RE.fullmatch(request_id) or not OID_RE.fullmatch(target):
            self.journal.discard_active()
            return {"relaunch": False, "code": "malformed_activation"}
        if active.get("stage") not in {
            STAGE_STOP_REQUESTED,
            STAGE_RECORDER_EXITED,
            STAGE_SOURCE_UPDATED,
        }:
            return {"relaunch": False, "code": "unexpected_stage"}
        try:
            if active.get("stage") == STAGE_STOP_REQUESTED:
                status = self.recorder_status()
                if (
                    process_is_running(active.get("recorder_pid"))
                    and status.process_nonce == active.get("process_nonce")
                ):
                    raise UpdateRefused(
                        "recorder_still_running",
                        "The recorder process had not exited; the checkout was "
                        "left untouched.",
                    )
                active = self.journal.advance(STAGE_RECORDER_EXITED)
            self.paths.activation_file.unlink(missing_ok=True)
            if active.get("stage") != STAGE_SOURCE_UPDATED:
                active = self._fast_forward(active, target)
        except UpdateRefused as refused:
            self._fail(request_id, target, refused)
            return {"relaunch": False, "code": refused.code}
        return {"relaunch": True, "code": "source_updated", "target_commit": target}

    def _fast_forward(self, active: dict[str, Any], target: str) -> dict[str, Any]:
        inspection = self.adapter.inspect(target=target, fetch=True)
        if inspection.state == "up_to_date":
            return self.journal.advance(STAGE_SOURCE_UPDATED, current_commit=target)
        if inspection.state != "update_available":
            raise UpdateRefused(
                inspection.code or "target_unavailable",
                inspection.message
                or "The checkout stopped being eligible before activation.",
                state=inspection.state,
            )
        applied = self.adapter.apply(target)
        if applied.state != "source_updated_restart_required":
            raise UpdateRefused(
                applied.code or "update_failed",
                applied.message or "The safe fast-forward update did not complete.",
            )
        if self._current_commit() != target:
            raise UpdateRefused(
                "update_failed", "The checkout did not reach the requested commit."
            )
        return self.journal.advance(STAGE_SOURCE_UPDATED, current_commit=target)

    def mark_relaunched(self, process_nonce: str) -> bool:
        """Record the exact replacement the supervisor started."""

        if not NONCE_RE.fullmatch(str(process_nonce)):
            raise ValueError("malformed_process_nonce")
        active = self.journal.active()
        if active is None or active.get("stage") not in {
            STAGE_SOURCE_UPDATED,
            STAGE_RECORDER_EXITED,
        }:
            return False
        self.journal.advance(
            STAGE_REPLACEMENT_STARTED,
            replacement_nonce=str(process_nonce),
        )
        return True

    # ---- polling ---------------------------------------------------------

    def poll_once(self) -> bool:
        worked = self.resume()
        request = self._claim_request()
        if request is None:
            return worked
        request_id, action, target, activate_after = "invalid-request", "unknown", None, None
        try:
            request_id, action, target, activate_after = validate_request(request)
        except UpdateRefused as refused:
            self._write_result(
                request_id=(
                    request_id
                    if REQUEST_ID_RE.fullmatch(request_id)
                    else "invalid-request"
                ),
                action=action if action in {"check", "apply"} else "unknown",
                state=refused.state,
                current=None,
                target=target,
                running=None,
                code=refused.code,
                message=refused.message,
            )
            return True
        if activate_after is not None:
            delay = (activate_after - self._now()).total_seconds()
            if delay > 0:
                self._sleep(min(delay, 30.0))
        try:
            if action == "check":
                self.handle_check(request_id, target)
            else:
                self.handle_apply(request_id, str(target))
        except UpdateRefused as refused:
            self._fail(request_id, target, refused)
        except (OSError, RuntimeError, ValueError) as exc:
            self._fail(
                request_id,
                target,
                UpdateRefused(
                    "host_update_failed",
                    "The recorder host update agent stopped safely before it "
                    f"could change anything ({type(exc).__name__}).",
                ),
            )
        return True


__all__ = [
    "ActivationDecision",
    "NativeRecorderUpdateAgent",
    "UpdateRefused",
    "validate_request",
]
