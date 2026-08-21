"""Host-side lifecycle for one native recorder branch trial.

The ordinary updater answers "move this recorder to approved main". This agent
answers a different question -- "run this recorder on a test branch, and if that
does not come back, put the version that was working back" -- and it answers it
with the same rules, in the same order:

1. everything refusable is refused **while capture is still running**: approved
   repository, real branch on that repository, exact commit contained in it,
   clean supported checkout, unchanged dependency inputs, and an interpreter
   that can actually load the trial tree;
2. the exact commit the running recorder *proved* it was on is pinned as the
   fallback before anything stops;
3. the recorder is asked to stop the same correlated way an update asks;
4. the supervisor launches the trial from a **separate worktree**, so the
   production checkout never moves and rollback needs no Git at all;
5. the replacement must prove six independent conditions inside a bounded
   window, and if it cannot, the pinned safe version is relaunched and has to
   prove the same six conditions itself.

The verdict on a trial is deliberately made from *outside* the branch under
test. A trial child runs from the trial worktree, so any check living in its own
tree is a check that branch could omit, break or simply predate -- and a branch
that never fails itself would keep an unproven recorder running forever. So the
watchdog runs from the permanent checkout (:meth:`evaluate_trial`), and the code
inside the trial tree is only an actuator: it reads a stop marker addressed to
its exact process instance and ends its own capture, the way Ctrl+C would
(:meth:`verify_once`). Rollback verification is in-process again, because a
rollback always runs from the permanent checkout by construction.

A peer names an approved repository branch and commit. Every path, interpreter,
argument, data directory and timeout here is local.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from catalog.federation.software_trial import (
    ROLLBACK_FAILED,
    ROLLBACK_STARTING,
    ROLLBACK_STARTUP_TIMEOUT_SECONDS,
    ROLLBACK_VERIFYING,
    SAFE_RESTORED,
    TRIAL_FAILED,
    TRIAL_OPERATOR_STOPPED,
    TRIAL_PREPARING,
    TRIAL_RESULT_SCHEMA,
    TRIAL_RUNNING,
    TRIAL_STARTING,
    TRIAL_STARTUP_TIMEOUT_SECONDS,
    TRIAL_VERIFYING,
    TrialRefused,
    TrialSelection,
    trial_stop_applies,
    trial_stop_request,
)
from catalog.federation.software_update import APPROVED_BRANCH, OID_RE

from .native_identity import NONCE_RE
from .native_trial import (
    TrialJournal,
    TrialPlan,
    TrialWorktrees,
    deadline_passed,
    evaluate_startup,
    expectation_from,
    failure_reason,
    new_trial_id,
    running_trial,
)
from .native_update import (
    ACTIVATION_TTL_SECONDS,
    NativeRecorderUpdatePaths,
    RecorderRuntimeStatus,
    dependency_changes,
    parse_stamp,
    process_is_running,
    read_json,
    read_recorder_status,
    stamp,
    utc_now,
    write_json_atomic,
)

#: Bounded import probe. Identical in spirit to the one the Windows launcher
#: already runs before it starts anything: proof that this interpreter can load
#: *this* tree. Running it before capture stops is what keeps "the branch cannot
#: even be imported" from becoming a stopped recorder.
PROBE_CODE = (
    "import catalog.mtconnect_recorder as recorder; "
    "import catalog.mtconnect_recorder.runtime; "
    "import scripts.start_tailscale_recorder as launcher; "
    "import requests; "
    "raise SystemExit(0 if callable(launcher.main) and callable(recorder.run) else 1)"
)
PROBE_TIMEOUT_SECONDS = 60.0


class TrialRefusedLocally(Exception):
    """A bounded refusal that left capture, the checkout and data untouched."""

    def __init__(self, code: str, message: str, *, state: str = "refused") -> None:
        super().__init__(code)
        self.code = code
        self.message = message
        self.state = state


class NativeRecorderTrialAgent:
    """Drive one recorder's branch trial, and its verified fallback."""

    def __init__(
        self,
        *,
        repository_root: Path | str,
        paths: NativeRecorderUpdatePaths,
        supervisor_session: str,
        adapter: Any,
        now: Callable[[], datetime] = utc_now,
        interpreter: str | None = None,
        worktrees: TrialWorktrees | None = None,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        request_capture_stop: Callable[[], bool] | None = None,
    ) -> None:
        self.root = Path(repository_root).resolve()
        self.paths = paths
        self.supervisor_session = str(supervisor_session)
        self.adapter = adapter
        self.journal = TrialJournal(paths.trial_journal_file)
        self.worktrees = worktrees or TrialWorktrees(self.root)
        self._now = now
        self._interpreter = interpreter or sys.executable
        self._runner = runner
        self._request_capture_stop = request_capture_stop

    # ---- bounded result channel -----------------------------------------

    def _result_path(self, request_id: str) -> Path:
        digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
        return self.paths.directory / f"trial-result-{digest}.json"

    def _write_result(
        self,
        *,
        request_id: str,
        state: str,
        code: str | None,
        message: str,
        record: dict[str, Any] | None = None,
        acceptance: dict[str, object] | None = None,
        recovery: dict[str, object] | None = None,
    ) -> dict[str, object]:
        source = record or {}
        value: dict[str, object] = {
            "schema": TRIAL_RESULT_SCHEMA,
            "request_id": request_id,
            "action": "trial",
            "state": state,
            "trial_id": source.get("trial_id"),
            "branch": source.get("trial_branch"),
            "target_commit": source.get("trial_commit"),
            "safe_branch": source.get("safe_branch", APPROVED_BRANCH),
            "safe_commit": source.get("safe_commit"),
            "running_commit": self.running_commit(),
            "code": code,
            "message": message[:512],
            "acceptance": acceptance,
            "recovery": recovery,
            "completed_at": stamp(self._now()),
        }
        write_json_atomic(self._result_path(request_id), value)
        write_json_atomic(self.paths.trial_result_file, value)
        return value

    def result_for(self, request_id: str) -> dict[str, object] | None:
        value = read_json(self._result_path(request_id))
        if value is None or value.get("schema") != TRIAL_RESULT_SCHEMA:
            return None
        return value if value.get("request_id") == request_id else None

    # ---- local observations ---------------------------------------------

    def recorder_status(self) -> RecorderRuntimeStatus:
        return read_recorder_status(self.paths.status_file)

    def running_commit(self) -> str | None:
        status = self.recorder_status()
        if not status.is_fresh(now=self._now()) or not status.is_running():
            return None
        return status.build_commit

    def supervised_recorder(self) -> RecorderRuntimeStatus:
        status = self.recorder_status()
        if not status.present:
            raise TrialRefusedLocally(
                "recorder_runtime_unavailable",
                "No native recorder runtime identity was published.",
            )
        if status.supervisor_session != self.supervisor_session:
            raise TrialRefusedLocally(
                "recorder_not_supervised",
                "The running recorder belongs to a different supervisor.",
            )
        if not status.is_fresh(now=self._now()) or not status.is_running():
            raise TrialRefusedLocally(
                "recorder_heartbeat_stale",
                "The recorder is not proving a live, fresh heartbeat.",
            )
        return status

    # ---- prevalidation ---------------------------------------------------

    def prevalidate(self, selection: TrialSelection) -> dict[str, Any]:
        """Refuse everything refusable while the recorder is still recording.

        Nothing in here stops capture, mutates the production checkout, or
        touches recorder data. The trial worktree is created here on purpose:
        checking out the branch and proving the interpreter can load it is the
        most valuable thing that can still be done safely.
        """

        selection.validate()
        failure, current = self.adapter.checkout_baseline()
        if failure is not None:
            raise TrialRefusedLocally(
                failure.code or "unsupported_checkout",
                failure.message or "The recorder checkout cannot host a trial.",
                state=failure.state,
            )
        safe_commit = str(current)
        status = self.supervised_recorder()
        if status.build_commit != safe_commit:
            # The running process and the checkout disagree. Pinning either one
            # would be a guess, and a guessed fallback is worse than no trial.
            raise TrialRefusedLocally(
                "running_commit_mismatch",
                "The running recorder is not the commit this checkout has, so "
                "no exact known-good version could be pinned.",
            )
        tip = self.adapter.fetch_branch(selection.branch)
        if tip is None:
            raise TrialRefusedLocally(
                "branch_unavailable",
                f"Branch {selection.branch} could not be resolved from the "
                "approved FCP source.",
            )
        if not self.adapter.contains_commit(tip, selection.target_commit):
            raise TrialRefusedLocally(
                "target_not_on_branch",
                "The requested commit is not contained in that approved branch.",
            )
        if selection.target_commit == safe_commit:
            raise TrialRefusedLocally(
                "already_running_target",
                "The recorder is already running that exact commit.",
                state="up_to_date",
            )
        changed = dependency_changes(self.root, safe_commit, selection.target_commit)
        if changed:
            raise TrialRefusedLocally(
                "dependency_change_requires_manual_trial",
                (
                    "This branch changes Python dependency inputs ("
                    + ", ".join(changed[:4])
                    + "). Prepare the environment manually so capture is never "
                    "stopped for a runtime that cannot start."
                ),
            )
        trial_id = new_trial_id()
        try:
            trial_root = self.worktrees.create(trial_id, selection.target_commit)
        except (RuntimeError, ValueError, OSError) as exc:
            raise TrialRefusedLocally(
                "trial_worktree_unavailable",
                "The trial checkout could not be prepared; capture was not "
                f"stopped ({type(exc).__name__}).",
            ) from exc
        self._probe(trial_root)
        return {
            "trial_id": trial_id,
            "trial_root": str(trial_root),
            "trial_branch": selection.branch,
            "trial_commit": selection.target_commit,
            "repository": selection.repository,
            "safe_branch": APPROVED_BRANCH,
            "safe_commit": safe_commit,
            "safe_root": str(self.root),
            "safe_state": status.state,
            "predecessor_pid": status.pid,
            "predecessor_nonce": status.process_nonce,
        }

    def _probe(self, trial_root: Path) -> None:
        """Prove this interpreter can load the trial tree, before stopping.

        The probe runs with the *production* data directory, exactly as the
        real trial launch will, so it validates the configuration that will be
        used rather than a different one -- and so it cannot create a stray
        data directory inside the trial worktree.
        """

        environment = dict(os.environ)
        environment["FCP_RECORDER_DATA_DIR"] = str(self.paths.data_directory)
        # Supervisor identity belongs to the running recorder, not to a probe.
        for name in (
            "FCP_RECORDER_SUPERVISOR_SESSION",
            "FCP_RECORDER_PROCESS_NONCE",
            "FCP_RECORDER_BUILD_COMMIT",
            "FCP_RECORDER_FEDERATION_KEY",
        ):
            environment.pop(name, None)
        try:
            completed = self._runner(
                [self._interpreter, "-c", PROBE_CODE],
                cwd=str(trial_root),
                env=environment,
                shell=False,
                check=False,
                capture_output=True,
                text=True,
                timeout=PROBE_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise TrialRefusedLocally(
                "trial_interpreter_unusable",
                "The recorder interpreter could not load the trial branch; "
                f"capture was not stopped ({type(exc).__name__}).",
            ) from exc
        if completed.returncode != 0:
            raise TrialRefusedLocally(
                "trial_interpreter_unusable",
                "The recorder interpreter could not load the trial branch, so "
                "capture was not stopped.",
            )

    # ---- request handling ------------------------------------------------

    def handle_trial(
        self,
        request_id: str,
        selection: TrialSelection,
        *,
        update_active: bool = False,
    ) -> dict[str, object]:
        finished = self.journal.history_entry(request_id)
        if finished is not None:
            stored = finished.get("result")
            if isinstance(stored, dict):
                write_json_atomic(self._result_path(request_id), stored)
                write_json_atomic(self.paths.trial_result_file, stored)
                return stored
        active = self.journal.active()
        if active is not None and active.get("request_id") != request_id:
            return self._write_result(
                request_id=request_id,
                state="error",
                code="trial_in_progress",
                message="Another branch trial is already running on this device.",
                record=active,
            )
        if active is not None:
            return self._write_result(
                request_id=request_id,
                state=str(active.get("stage") or TRIAL_PREPARING),
                code="trial_in_progress",
                message="This branch trial is already running on this device.",
                record=active,
            )
        if update_active:
            return self._write_result(
                request_id=request_id,
                state="error",
                code="host_update_busy",
                message="A software update activation is already running.",
            )
        running = running_trial(self.journal)
        if running is not None:
            return self._handle_restore(request_id, selection, running)
        try:
            record = self.prevalidate(selection)
        except TrialRefusedLocally as refused:
            return self._write_result(
                request_id=request_id,
                state=refused.state,
                code=refused.code,
                message=refused.message,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            # Prevalidation runs Git and an interpreter. Both can fail for local
            # reasons, and every one of those failures happens while capture is
            # still recording, so it is reported rather than raised.
            return self._write_result(
                request_id=request_id,
                state="error",
                code="trial_prevalidation_failed",
                message=(
                    "The branch trial could not be validated, so capture was "
                    f"not stopped ({type(exc).__name__})."
                ),
            )
        record["request_id"] = request_id
        self.paths.trial_stop_file.unlink(missing_ok=True)
        opened = self.journal.open({**record, "stage": TRIAL_PREPARING})
        return self._write_result(
            request_id=request_id,
            state=TRIAL_PREPARING,
            code="trial_prevalidated",
            message=(
                "The trial branch was validated and checked out beside the "
                "known-good checkout. Capture has not been stopped yet."
            ),
            record=opened,
        )

    def _handle_restore(
        self,
        request_id: str,
        selection: TrialSelection,
        running: dict[str, Any],
    ) -> dict[str, object]:
        """Return a device that is on a test branch to its pinned safe version.

        This is the same verified rollback the failure path runs, asked for
        deliberately instead of triggered by a failure. It is the only way back
        while a trial is running, which is what keeps a second trial from
        stacking on top of an unproven one.
        """

        safe_commit = running.get("safe_commit")
        if (
            not selection.is_approved_branch
            or not isinstance(safe_commit, str)
            or selection.target_commit != safe_commit
        ):
            return self._write_result(
                request_id=request_id,
                state="error",
                code="return_to_pinned_version_required",
                message=(
                    "This device is running a test branch. It can only be "
                    "returned to the exact pinned known-good version it fell "
                    "back to, and can then be sent to another branch."
                ),
                record=running,
            )
        failure, current = self.adapter.checkout_baseline()
        if failure is not None or current != safe_commit:
            return self._write_result(
                request_id=request_id,
                state="error",
                code="safe_checkout_not_intact",
                message="The pinned known-good checkout is no longer intact.",
                record=running,
            )
        try:
            status = self.supervised_recorder()
        except TrialRefusedLocally as refused:
            return self._write_result(
                request_id=request_id,
                state=refused.state,
                code=refused.code,
                message=refused.message,
                record=running,
            )
        opened = self.journal.open(
            {
                **{
                    key: running.get(key)
                    for key in (
                        "trial_id",
                        "trial_root",
                        "trial_branch",
                        "trial_commit",
                        "repository",
                        "safe_branch",
                        "safe_commit",
                        "safe_root",
                        "safe_state",
                    )
                },
                "request_id": request_id,
                "stage": TRIAL_FAILED,
                "restore": True,
                "failure_reason": (
                    "An operator returned this device to the pinned known-good "
                    "version."
                ),
                "predecessor_pid": status.pid,
                "predecessor_nonce": status.process_nonce,
            }
        )
        return self._write_result(
            request_id=request_id,
            state=TRIAL_PREPARING,
            code="trial_prevalidated",
            message=(
                "The pinned known-good version was validated. Capture has not "
                "been stopped yet."
            ),
            record=opened,
        )

    def write_refusal(
        self,
        request_id: str,
        *,
        code: str,
        message: str,
        state: str = "refused",
    ) -> dict[str, object]:
        """Report a bounded refusal that changed nothing."""

        return self._write_result(
            request_id=request_id,
            state=state,
            code=code,
            message=message,
            record=self.journal.active(),
        )

    def report_stage(
        self,
        request_id: str,
        stage: str,
        *,
        code: str,
        message: str,
    ) -> dict[str, object]:
        return self._write_result(
            request_id=request_id,
            state=stage,
            code=code,
            message=message,
            record=self.journal.active(),
        )

    def abandon(
        self,
        request_id: str,
        *,
        code: str,
        message: str,
    ) -> dict[str, object]:
        """Drop a prepared trial that was never activated.

        Reached only before any stop request took effect, so capture, the
        production checkout and recorder data are all untouched. The prepared
        worktree is left on disk and retired by the ordinary bounded prune;
        deleting a directory is never part of a failure path here.
        """

        result = self.write_refusal(request_id, code=code, message=message)
        self.paths.trial_stop_file.unlink(missing_ok=True)
        active = self.journal.active()
        if active is not None and active.get("request_id") == request_id:
            self.journal.discard_active()
        return result

    def mark_stop_requested(self, *, expires_at: str) -> dict[str, Any]:
        """Record that the recorder was asked to stop for this trial."""

        return self.journal.advance(
            TRIAL_STARTING,
            stop_requested_at=stamp(self._now()),
            activation_expires_at=expires_at,
        )

    # ---- supervisor entry points ----------------------------------------

    def plan_after_exit(self) -> TrialPlan | None:
        """Say what the supervisor must launch next, from durable state alone.

        Returns ``None`` when no trial is active, so the ordinary update path is
        reached exactly as before.
        """

        active = self.journal.active()
        if active is None:
            return None
        stage = active.get("stage")
        if stage in {TRIAL_PREPARING, TRIAL_STARTING}:
            # An operator-requested return to the pinned version travels the
            # same stages as a trial up to the stop, and the same verified
            # rollback afterwards. Only the destination differs.
            if active.get("restore"):
                return self._plan_rollback(active)
            return self._plan_trial_launch(active)
        if stage in {TRIAL_VERIFYING, TRIAL_FAILED}:
            return self._plan_rollback(active)
        if stage in {ROLLBACK_STARTING, ROLLBACK_VERIFYING}:
            return self._finish_rollback_failure(
                active,
                "The restored known-good recorder did not come back either.",
            )
        if stage == TRIAL_OPERATOR_STOPPED:
            self.journal.finish(
                TRIAL_OPERATOR_STOPPED,
                result=self._write_result(
                    request_id=str(active.get("request_id") or "trial"),
                    state=TRIAL_OPERATOR_STOPPED,
                    code="trial_operator_stopped",
                    message="An operator stopped the trial recorder; nothing was relaunched.",
                    record=active,
                ),
            )
            return TrialPlan(False, "trial", code="trial_operator_stopped")
        return None

    def _instance_gone(self, nonce: object) -> bool:
        """Whether the process instance that published ``nonce`` has exited.

        Both halves again: a recycled PID with a different nonce is a different
        process, and a matching nonce whose PID is gone is an exited one.
        """

        if not isinstance(nonce, str):
            return True
        status = self.recorder_status()
        return not (
            status.process_nonce == nonce and process_is_running(status.pid)
        )

    def _plan_trial_launch(self, record: dict[str, Any]) -> TrialPlan:
        if not self._instance_gone(record.get("predecessor_nonce")):
            return TrialPlan(False, "trial", code="recorder_still_running")
        trial_root = record.get("trial_root")
        trial_commit = record.get("trial_commit")
        if not isinstance(trial_root, str) or not isinstance(trial_commit, str):
            return self._finish_rollback_failure(
                record, "The trial record was unusable."
            )
        head = self.worktrees.head_commit(Path(trial_root))
        if head != trial_commit:
            # The prepared worktree is not the exact commit any more. Go back to
            # the pinned safe version rather than launch something unverified.
            self.journal.advance(
                TRIAL_FAILED,
                failure_reason="The prepared trial checkout was not the exact commit.",
            )
            return self._plan_rollback(self.journal.active() or record)
        self.journal.advance(TRIAL_STARTING)
        return TrialPlan(
            True,
            "trial",
            launch_root=trial_root,
            data_directory=str(self.paths.data_directory),
            build_commit=trial_commit,
            code="trial_launch",
        )

    def _plan_rollback(self, record: dict[str, Any]) -> TrialPlan:
        safe_commit = record.get("safe_commit")
        safe_root = record.get("safe_root") or str(self.root)
        if not isinstance(safe_commit, str) or not OID_RE.fullmatch(safe_commit):
            return self._finish_rollback_failure(
                record, "No exact known-good commit had been pinned."
            )
        # The production checkout was never moved during the trial, so this is a
        # re-read rather than a repair. Refusing here is still important: it is
        # the difference between relaunching the pinned version and relaunching
        # whatever happens to be checked out.
        failure, current = self.adapter.checkout_baseline()
        if failure is not None or current != safe_commit:
            return self._finish_rollback_failure(
                record,
                "The pinned known-good checkout was no longer intact.",
            )
        trial_instance = record.get("replacement_nonce") or record.get(
            "predecessor_nonce"
        )
        if not self._instance_gone(trial_instance):
            # Never start the safe version beside a trial process that is still
            # alive. Two recorders writing one data directory is worse than a
            # slow recovery.
            return TrialPlan(False, "rollback", code="trial_still_running")
        # The instance the stop marker addressed is proven gone, so the marker
        # has done its job. Leaving it could stop the restored recorder if a
        # later process ever matched it.
        self.paths.trial_stop_file.unlink(missing_ok=True)
        stage = record.get("stage")
        if stage != TRIAL_FAILED:
            record = self.journal.advance(
                TRIAL_FAILED,
                failure_reason=record.get("failure_reason")
                or "The trial recorder exited before it proved a healthy startup.",
                trial_process_stopped=True,
            )
        status = self.recorder_status()
        moment = self._now()
        # The rollback replacement must differ from the *failed trial* process,
        # not merely from the recorder that was replaced before the trial.
        self.journal.advance(
            ROLLBACK_STARTING,
            rollback_started_at=stamp(moment),
            trial_process_stopped=True,
            predecessor_pid=(
                status.pid
                if status.process_nonce == record.get("replacement_nonce")
                else record.get("predecessor_pid")
            ),
            predecessor_nonce=(
                record.get("replacement_nonce") or record.get("predecessor_nonce")
            ),
            replacement_nonce=None,
        )
        return TrialPlan(
            True,
            "rollback",
            launch_root=str(safe_root),
            data_directory=str(self.paths.data_directory),
            build_commit=safe_commit,
            code="rollback_launch",
        )

    def _finish_rollback_failure(
        self,
        record: dict[str, Any],
        message: str,
    ) -> TrialPlan:
        request_id = str(record.get("request_id") or "trial")
        result = self._write_result(
            request_id=request_id,
            state=ROLLBACK_FAILED,
            code="rollback_failed",
            message=message,
            record=record,
            recovery=self._recovery(record, restored=False),
        )
        if self.journal.active() is not None:
            self.journal.finish(ROLLBACK_FAILED, result=result, failure_reason=message)
        return TrialPlan(False, "rollback", code="rollback_failed")

    def mark_relaunched(self, process_nonce: str) -> bool:
        """Record the exact replacement the supervisor is about to start."""

        if not NONCE_RE.fullmatch(str(process_nonce)):
            raise ValueError("malformed_process_nonce")
        active = self.journal.active()
        if active is None:
            return False
        stage = active.get("stage")
        moment = self._now()
        if stage == TRIAL_STARTING:
            self.journal.advance(
                TRIAL_VERIFYING,
                replacement_nonce=str(process_nonce),
                activated_at=stamp(moment),
                deadline_at=stamp(
                    moment + timedelta(seconds=TRIAL_STARTUP_TIMEOUT_SECONDS)
                ),
            )
            return True
        if stage == ROLLBACK_STARTING:
            self.journal.advance(
                ROLLBACK_VERIFYING,
                rollback_nonce=str(process_nonce),
                replacement_nonce=str(process_nonce),
                activated_at=stamp(moment),
                deadline_at=stamp(
                    moment + timedelta(seconds=ROLLBACK_STARTUP_TIMEOUT_SECONDS)
                ),
            )
            return True
        return False

    # ---- in-process verification ----------------------------------------

    def verify_once(self) -> bool:
        """Advance what *this* process is allowed to decide. Safe to repeat.

        Runs inside the recorder. It never judges a trial: that verdict belongs
        outside the branch under test. What it does own is ending this
        process's own capture when a stop marker names it, verifying a rollback
        (which always runs from the permanent checkout), and releasing a trial
        whose stop request never took effect.
        """

        if self._stop_requested():
            return True
        active = self.journal.active()
        if active is None:
            return False
        stage = active.get("stage")
        if stage == ROLLBACK_VERIFYING:
            return self._verify_rollback(active)
        if stage in {TRIAL_PREPARING, TRIAL_STARTING}:
            return self._expire_unactivated(active)
        return False

    def _stop_requested(self) -> bool:
        """End this process's capture if a stop marker names this instance."""

        marker = read_json(self.paths.trial_stop_file)
        if marker is None:
            return False
        status = self.recorder_status()
        if not trial_stop_applies(
            marker,
            trial_id=(self.journal.latest() or {}).get("trial_id"),
            pid=int(status.pid or 0),
            process_nonce=status.process_nonce,
            supervisor_session=self.supervisor_session,
            now=self._now(),
        ):
            return False
        self._stop_capture_for_rollback()
        return True

    def evaluate_trial(self) -> str:
        """Judge an in-flight trial from the permanent checkout. Repeatable.

        This is the check a branch under test cannot omit, because it does not
        run from that branch. Returns the stage the trial is now in, or
        ``"pending"`` while it is still inside its acceptance window.
        """

        active = self.journal.active()
        if active is None:
            return "idle"
        if active.get("stage") != TRIAL_VERIFYING:
            return str(active.get("stage") or "idle")
        return self._verify_trial(active)

    def _expire_unactivated(self, record: dict[str, Any]) -> bool:
        """Release a trial whose stop request never took effect.

        A crash between preparing a trial and activating it, or a recorder that
        never consented to the stop, would otherwise leave an active record
        blocking every later trial and update. Nothing durable happened at
        either stage -- capture is still running and the checkout is untouched
        -- so the safe resolution is to let it go and say so.
        """

        moment = self._now()
        expires = parse_stamp(record.get("activation_expires_at"))
        if expires is None:
            opened = parse_stamp(record.get("created_at"))
            if opened is None:
                expires = moment
            else:
                expires = opened + timedelta(seconds=ACTIVATION_TTL_SECONDS)
        if moment < expires:
            return False
        if not self._instance_gone(record.get("predecessor_nonce")):
            # The recorder this trial named is still the running one, so it
            # never stopped and nothing was ever replaced.
            self.abandon(
                str(record.get("request_id") or "trial"),
                code="trial_activation_expired",
                message=(
                    "The recorder did not stop for the trial before the "
                    "activation expired. Capture was never interrupted."
                ),
            )
            return True
        return False

    def _verify_trial(self, record: dict[str, Any]) -> str:
        expected = str(record.get("trial_commit") or "")
        acceptance = evaluate_startup(
            self.recorder_status(),
            expectation_from(
                record,
                expected_commit=expected,
                supervisor_session=self.supervisor_session,
            ),
            now=self._now(),
        )
        request_id = str(record.get("request_id") or "trial")
        if acceptance.accepted:
            result = self._write_result(
                request_id=request_id,
                state=TRIAL_RUNNING,
                code="trial_running",
                message=(
                    "The trial branch started, rejoined the Federation and is "
                    "recording."
                ),
                record=record,
                acceptance=acceptance.to_dict(),
            )
            self.journal.finish(
                TRIAL_RUNNING, result=result, acceptance=acceptance.to_dict()
            )
            return TRIAL_RUNNING
        if not deadline_passed(record, now=self._now()):
            return "pending"
        reason = failure_reason(
            acceptance, seconds=TRIAL_STARTUP_TIMEOUT_SECONDS
        )
        failed = self.journal.advance(
            TRIAL_FAILED,
            failure_reason=reason,
            acceptance=acceptance.to_dict(),
        )
        self._write_result(
            request_id=request_id,
            state=TRIAL_FAILED,
            code="trial_startup_failed",
            message=reason,
            record=record,
            acceptance=acceptance.to_dict(),
        )
        self._request_trial_stop(failed, reason)
        return TRIAL_FAILED

    def _request_trial_stop(self, record: dict[str, Any], reason: str) -> None:
        """Ask the trial process to end its own capture, and note that we did.

        Addressed to one exact process instance. The trial process is the only
        thing that can stop it cleanly, so this is a request, not a kill -- and
        whether it was honoured is reported rather than assumed.
        """

        status = self.recorder_status()
        try:
            marker = trial_stop_request(
                trial_id=str(record.get("trial_id") or ""),
                request_id=str(record.get("request_id") or ""),
                supervisor_session=self.supervisor_session,
                recorder_pid=int(status.pid or 0),
                process_nonce=str(status.process_nonce or ""),
                reason=reason,
                created_at=self._now(),
            )
        except (TrialRefused, ValueError):
            # No live trial instance to address. The supervisor already regains
            # control when the child exits, so there is nothing to ask.
            return
        write_json_atomic(self.paths.trial_stop_file, marker)
        # The in-process actuator may not exist in the branch under test, so
        # the caller waits a bounded grace and reports if it never exited.
        self._stop_capture_for_rollback()

    def _verify_rollback(self, record: dict[str, Any]) -> bool:
        expected = str(record.get("safe_commit") or "")
        acceptance = evaluate_startup(
            self.recorder_status(),
            expectation_from(
                record,
                expected_commit=expected,
                supervisor_session=self.supervisor_session,
            ),
            now=self._now(),
        )
        request_id = str(record.get("request_id") or "trial")
        if acceptance.accepted:
            preamble = (
                ""
                if record.get("restore")
                else "The trial branch did not start. "
                + str(record.get("failure_reason") or "")
            )
            result = self._write_result(
                request_id=request_id,
                state=SAFE_RESTORED,
                code="safe_restored",
                message=(
                    preamble
                    + " The pinned known-good version is running and verified."
                ).strip(),
                record=record,
                acceptance=acceptance.to_dict(),
                recovery=self._recovery(record, restored=True, acceptance=acceptance),
            )
            self.journal.finish(SAFE_RESTORED, result=result)
            return True
        if not deadline_passed(record, now=self._now()):
            return False
        reason = failure_reason(
            acceptance, seconds=ROLLBACK_STARTUP_TIMEOUT_SECONDS
        )
        result = self._write_result(
            request_id=request_id,
            state=ROLLBACK_FAILED,
            code="rollback_failed",
            message=(
                "Automatic recovery did not complete: " + reason
            ),
            record=record,
            acceptance=acceptance.to_dict(),
            recovery=self._recovery(record, restored=False, acceptance=acceptance),
        )
        self.journal.finish(ROLLBACK_FAILED, result=result, failure_reason=reason)
        return True

    @staticmethod
    def _recovery(
        record: dict[str, Any],
        *,
        restored: bool,
        acceptance: Any = None,
    ) -> dict[str, object]:
        """The operator-facing recovery checklist, from durable evidence only."""

        return {
            "trial_process_stopped": bool(record.get("trial_process_stopped", restored)),
            "safe_version_started": restored,
            "safe_branch": record.get("safe_branch", APPROVED_BRANCH),
            "safe_commit": record.get("safe_commit"),
            "recorder_running": bool(
                restored and getattr(acceptance, "process_present", False)
            ),
            "federation_connected": bool(
                restored and getattr(acceptance, "federation_connected", False)
            ),
            "recording_resumed": bool(
                restored and getattr(acceptance, "recorder_healthy", False)
            ),
        }

    def _stop_capture_for_rollback(self) -> None:
        if self._request_capture_stop is None:
            return
        try:
            self._request_capture_stop()
        except Exception:  # noqa: BLE001 - a failed stop is retried next pass
            return

    def mark_operator_stopped(self) -> bool:
        """Record that this trial process is exiting because an operator said so."""

        active = self.journal.active()
        if active is None or active.get("stage") not in {
            TRIAL_VERIFYING,
            TRIAL_STARTING,
        }:
            return False
        self.journal.advance(TRIAL_OPERATOR_STOPPED)
        return True

    # ---- reporting -------------------------------------------------------

    def snapshot(self) -> dict[str, object]:
        """Bounded description of what this device is running, and its fallback."""

        record = self.journal.latest()
        running = running_trial(self.journal)
        value: dict[str, object] = {
            "branch": APPROVED_BRANCH,
            "trial_active": running is not None,
            "stage": None,
            "trial_branch": None,
            "trial_commit": None,
            "safe_branch": None,
            "safe_commit": None,
            "failure_reason": None,
            "running_commit": self.running_commit(),
        }
        if record is None:
            return value
        value.update(
            {
                "stage": record.get("stage"),
                "trial_branch": record.get("trial_branch"),
                "trial_commit": record.get("trial_commit"),
                "safe_branch": record.get("safe_branch"),
                "safe_commit": record.get("safe_commit"),
                "failure_reason": record.get("failure_reason"),
            }
        )
        if running is not None:
            value["branch"] = record.get("trial_branch")
        return value


__all__ = [
    "PROBE_CODE",
    "PROBE_TIMEOUT_SECONDS",
    "NativeRecorderTrialAgent",
    "TrialRefusedLocally",
]
