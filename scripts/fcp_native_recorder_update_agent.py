"""Host update agent for the native standalone MTConnect recorder.

This is a thin, explicit entry point. Every lifecycle rule lives in
``catalog.mtconnect_recorder.native_update_agent`` so the same state machine is
exercised by CI on Linux and Windows alike; this file only chooses a mode,
enforces one agent per recorder, and prints a bounded result.

Modes:

``(default)``
    Poll the bounded request handoff and resume any interrupted activation.
``--once``
    One bounded pass, used by CI smoke checks.
``--finalize``
    Fast-forward the checkout after the supervised recorder has exited. This is
    the only mode that mutates the checkout, and it refuses to run while the
    recorder process it was activated for is still alive.
``--mark-relaunched``
    Record the exact process-instance nonce the supervisor just started, so the
    replacement must be proven to be that process and not a survivor.

No mode accepts an executable, path, command, argument, URL, or environment
value from a Federation peer.
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

from catalog.mtconnect_recorder.native_identity import (
    NONCE_RE,
    SUPERVISOR_SESSION_ENV,
)
from catalog.mtconnect_recorder.native_update import (
    NativeRecorderUpdatePaths,
    process_is_running,
    read_json,
    write_json_atomic,
)
from catalog.mtconnect_recorder.native_update_agent import (
    NativeRecorderUpdateAgent,
)

LOCK_SCHEMA = "fcp.recorder-update-agent-lock.v1"


def _claim_single_instance(lock_file: Path) -> bool:
    """Allow exactly one agent per recorder data directory.

    A lock left behind by a crashed agent is reclaimed only after its recorded
    PID is proven gone with the same read-only probe used everywhere else.
    """

    lock_file.parent.mkdir(parents=True, exist_ok=True)
    existing = read_json(lock_file)
    if existing is not None and existing.get("schema") == LOCK_SCHEMA:
        holder = existing.get("pid")
        if holder != os.getpid() and process_is_running(holder):
            return False
    write_json_atomic(lock_file, {"schema": LOCK_SCHEMA, "pid": os.getpid()})
    return True


def _release_single_instance(lock_file: Path) -> None:
    existing = read_json(lock_file)
    if existing is not None and existing.get("pid") == os.getpid():
        lock_file.unlink(missing_ok=True)


def _supervisor_session(value: str | None) -> str:
    candidate = (value or os.environ.get(SUPERVISOR_SESSION_ENV) or "").strip().lower()
    if not NONCE_RE.fullmatch(candidate):
        raise SystemExit(
            "A 32-character hexadecimal supervisor session is required. The "
            "recorder supervisor generates it locally; it is never supplied by "
            "a Federation peer."
        )
    return candidate


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--data-directory", required=True)
    parser.add_argument("--supervisor-session", default=None)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--stop-file", default=None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--finalize", action="store_true")
    parser.add_argument("--mark-relaunched", action="store_true")
    parser.add_argument("--process-nonce", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    session = _supervisor_session(args.supervisor_session)
    root = Path(args.repo_root).resolve()
    data_directory = Path(args.data_directory).resolve()
    poll_seconds = min(max(float(args.poll_seconds), 0.1), 30.0)

    agent = NativeRecorderUpdateAgent(
        repository_root=root,
        data_directory=data_directory,
        supervisor_session=session,
    )

    if args.mark_relaunched:
        nonce = (args.process_nonce or "").strip().lower()
        if not NONCE_RE.fullmatch(nonce):
            parser.error("--process-nonce must be 32 hexadecimal characters.")
        print(json.dumps({"marked": agent.mark_relaunched(nonce)}))
        return 0

    if args.finalize:
        outcome = agent.finalize_after_exit()
        print(json.dumps(outcome, sort_keys=True))
        return 0 if outcome.get("relaunch") else 1

    paths = NativeRecorderUpdatePaths(data_directory)
    lock_file = paths.directory / "agent.lock"
    if not _claim_single_instance(lock_file):
        return 0
    # The supervisor ends a generation by creating this file rather than by
    # killing the agent, so an agent is never terminated mid-operation and the
    # next generation always starts from freshly checked-out code.
    stop_file = Path(args.stop_file).resolve() if args.stop_file else None
    try:
        while True:
            worked = agent.poll_once()
            if args.once:
                return 0
            if stop_file is not None and stop_file.exists():
                return 0
            if not worked:
                time.sleep(poll_seconds)
    finally:
        _release_single_instance(lock_file)


if __name__ == "__main__":
    raise SystemExit(main())
