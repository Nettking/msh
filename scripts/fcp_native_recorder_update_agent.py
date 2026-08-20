"""Host update steps for the native standalone MTConnect recorder.

The recorder runs the update agent inside its own process: it already knows its
data directory and repository, so a separate long-running agent would add an
interpreter to the supported startup path for no benefit.

What is left here are the two steps that genuinely cannot happen inside the
recorder, plus a bounded self-check for CI:

``--finalize``
    Fast-forward the checkout after the supervised recorder has exited. This is
    the only mode that mutates the checkout, and it refuses to run while the
    recorder process it was activated for is still alive.
``--mark-relaunched``
    Record the exact process-instance nonce the supervisor just started, so the
    replacement must be proven to be that process and not a survivor.
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
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

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
    parser.add_argument("--process-nonce", default=None)
    return parser


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

    if args.finalize:
        outcome = agent.finalize_after_exit()
        print(json.dumps(outcome, sort_keys=True))
        return 0 if outcome.get("relaunch") else 1

    agent.poll_once()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
