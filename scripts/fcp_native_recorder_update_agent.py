"""Host update steps for the native standalone MTConnect recorder.

The recorder runs the update agent inside its own process: it already knows its
data directory and repository, so a separate long-running agent would add an
interpreter to the supported startup path for no benefit.

What is left here are the two steps that genuinely cannot happen inside the
recorder, plus a bounded self-check for CI:

``--finalize``
    Fast-forward the checkout after the supervised recorder has exited. This is
    the only mode that mutates the checkout, and it refuses to run while the
    recorder process it was activated for is still alive. An ordinary update
    also enters the same checkout-scoped host-mutation boundary used by normal
    launcher builds. If that finite lock is busy, the unchanged recorder is
    relaunched and the pending update fails safely after replacement identity is
    observed; capture is not left down merely because another supported host
    operation owns the checkout.
``--mark-relaunched``
    Record the exact process-instance nonce the supervisor just started, so the
    replacement must be proven to be that process and not a survivor.
``--watch-trial``
    Judge an in-flight branch trial *from this permanent checkout*, for a
    bounded time, and ask the trial process to stop if it did not prove itself.
    This mode exists precisely because it must not run from the branch under
    test: a check living in the trial worktree is one that branch could omit,
    break, or simply predate, and a trial that never fails itself would keep an
    unproven recorder running indefinitely.
``--once``
    One bounded pass of the same state machine the recorder runs in-process.

The data directory is resolved either explicitly or, exactly as the launcher
resolves it, from the recorder arguments the supervisor was started with. No
mode accepts an executable, path, command, argument, URL, or environment value
from a Federation peer.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from catalog.federation.host_mutation import (
    HostMutationLockError,
    host_mutation_lock,
)
from catalog.federation.software_trial import (
    TRIAL_FAILED,
    TRIAL_RUNNING,
    TRIAL_STARTUP_TIMEOUT_SECONDS,
)
from catalog.mtconnect_recorder.native_identity import (
    NONCE_RE,
    SUPERVISOR_SESSION_ENV,
)
from catalog.mtconnect_recorder.native_update_agent import NativeRecorderUpdateAgent
from scripts.start_tailscale_recorder import recorder_data_directory


def _supervisor_session(value: str | None) -> str:
    candidate = (value or os.environ.get(SUPERVISOR_SESSION_ENV) or "").strip().lower()
    if not NONCE_RE.fullmatch(candidate):
        raise SystemExit(
            "A 32-character hexadecimal supervisor session is required. The "
            "recorder supervisor generates it locally; it is never supplied by "
            "a Federation peer."
        )
    return candidate


def _data_directory(
    parser: argparse.ArgumentParser,
    explicit: str | None,
    recorder_arguments: list[str],
) -> Path:
    if explicit:
        return Path(explicit).resolve()
    # Resolved by the launcher's own rule rather than re-implemented here: an
    # agent watching a different directory than the recorder writes to would
    # verify the wrong process.
    try:
        return recorder_data_directory(list(recorder_arguments))
    except (RuntimeError, OSError, ValueError):
        parser.error(
            "The recorder data directory could not be resolved. Pass "
            "--data-directory explicitly."
        )
        raise  # pragma: no cover - parser.error always exits


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--data-directory", default=None)
    parser.add_argument(
        "--recorder-arg",
        dest="recorder_arguments",
        action="append",
        default=[],
        help="One recorder argument, repeated, used only to resolve the data "
        "directory exactly as the launcher does.",
    )
    parser.add_argument("--supervisor-session", default=None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--finalize", action="store_true")
    parser.add_argument("--mark-relaunched", action="store_true")
    parser.add_argument("--watch-trial", action="store_true")
    parser.add_argument("--process-nonce", default=None)
    return parser


#: How long the watchdog waits, past the acceptance window, for a trial process
#: to honour a stop request before reporting that it never did.
STOP_GRACE_SECONDS = 60
WATCH_INTERVAL_SECONDS = 2.0


def watch_trial(
    agent: NativeRecorderUpdateAgent,
    *,
    sleep=time.sleep,
    monotonic=time.monotonic,
) -> dict:
    """Own the verdict on one in-flight trial, then exit.

    Bounded twice over: the acceptance window the trial was given, plus a grace
    period for a failed trial to end its own capture. It never kills anything --
    if the trial process ignores the stop request, that is reported rather than
    forced, because the trial writes to the real recorder data directory.
    """

    deadline = (
        monotonic()
        + TRIAL_STARTUP_TIMEOUT_SECONDS
        + STOP_GRACE_SECONDS
        + WATCH_INTERVAL_SECONDS
    )
    outcome = "idle"
    while monotonic() < deadline:
        outcome = agent.trial.evaluate_trial()
        if outcome == TRIAL_RUNNING:
            return {"watched": True, "outcome": outcome, "stopped": None}
        if outcome == TRIAL_FAILED:
            break
        if outcome != "pending":
            # Either there is no trial to judge -- the supervisor records the
            # replacement instance before starting this, so an idle journal
            # means nothing is in flight -- or it already moved on because the
            # child exited and a rollback was planned.
            return {"watched": True, "outcome": outcome, "stopped": None}
        sleep(WATCH_INTERVAL_SECONDS)
    if outcome != TRIAL_FAILED:
        return {"watched": True, "outcome": outcome, "stopped": None}
    grace = monotonic() + STOP_GRACE_SECONDS
    while monotonic() < grace:
        if not agent.trial.recorder_status().is_running():
            return {"watched": True, "outcome": outcome, "stopped": True}
        sleep(WATCH_INTERVAL_SECONDS)
    return {"watched": True, "outcome": outcome, "stopped": False}


def _unchanged_relaunch_plan(code: str) -> dict[str, object]:
    """Keep capture recoverable when checkout serialization is unavailable.

    The active update journal intentionally remains at its pre-mutation stage.
    The replacement recorder has a fresh nonce and therefore rejects the stale
    activation marker; the in-process agent then closes that pending activation
    as superseded. No target commit is claimed because the checkout never moved.
    """

    return {
        "relaunch": True,
        "mode": "update",
        "code": code,
        "target_commit": None,
        "launch_root": None,
        "data_directory": None,
        "build_commit": None,
    }


def finalize_with_host_mutation(
    agent: NativeRecorderUpdateAgent,
    *,
    repo_root: Path,
) -> dict[str, object]:
    """Finalize one transition, serializing only real production source updates."""

    update_active = agent.journal.active() is not None
    trial_active = agent.trial.journal.active() is not None
    if not update_active or trial_active:
        # A branch trial owns its prepared worktree and deliberately leaves the
        # production checkout untouched. It must not be blocked by a launcher
        # building the production checkout merely because both transitions use
        # the same supervisor exit path.
        return agent.finalize_after_exit()

    try:
        with host_mutation_lock(repo_root):
            return agent.finalize_after_exit()
    except HostMutationLockError as exc:
        # The recorder has already exited at this point. Failing the supervisor
        # here would turn harmless lock contention into capture downtime. Leave
        # source untouched and relaunch the same checkout; the fresh process
        # identity makes the pending activation fail closed on its next pass.
        return _unchanged_relaunch_plan(str(exc))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    session = _supervisor_session(args.supervisor_session)
    agent = NativeRecorderUpdateAgent(
        repository_root=Path(args.repo_root).resolve(),
        data_directory=_data_directory(
            parser,
            args.data_directory,
            args.recorder_arguments,
        ),
        supervisor_session=session,
    )

    if args.mark_relaunched:
        nonce = (args.process_nonce or "").strip().lower()
        if not NONCE_RE.fullmatch(nonce):
            parser.error("--process-nonce must be 32 hexadecimal characters.")
        print(json.dumps({"marked": agent.mark_relaunched(nonce)}))
        return 0

    if args.watch_trial:
        print(json.dumps(watch_trial(agent), sort_keys=True))
        return 0

    if args.finalize:
        outcome = finalize_with_host_mutation(
            agent,
            repo_root=Path(args.repo_root),
        )
        print(json.dumps(outcome, sort_keys=True))
        return 0 if outcome.get("relaunch") else 1

    agent.poll_once()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
