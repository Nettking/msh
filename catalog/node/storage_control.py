"""Issue and publish signed storage control plans for live federation nodes."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from catalog.federation.control_sync import (
    StorageControlPlan,
    StorageControlPublicationStore,
    StorageControlRelayPublisher,
)
from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_control import PhaseDControlPlane

from .client import RelayNodeClient
from .identity import IdentityStore
from .state import EnrollmentState
from .storage_agent import (
    DEFAULT_ENROLLMENT_TOKEN_ENV,
    DEFAULT_SESSION_INVITATION_ENV,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    issue = commands.add_parser("issue")
    issue.add_argument("--control-database", required=True)
    issue.add_argument("--session-id", required=True)
    issue.add_argument("--authority-state-dir", required=True)
    issue.add_argument("--authority-display-name", required=True)
    issue.add_argument("--publication-database", required=True)
    issue.add_argument("--output", required=True)

    publish = commands.add_parser("publish")
    publish.add_argument("--plan", required=True)
    publish.add_argument("--state-dir", required=True)
    publish.add_argument("--relay", required=True)
    publish.add_argument("--display-name", required=True)
    publish.add_argument("--target-node-id", action="append", required=True)
    publish.add_argument("--allow-insecure-local", action="store_true")
    publish.add_argument("--timeout", type=float, default=15.0)
    return parser


def _load_plan(path: Path | str) -> StorageControlPlan:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise FederationValidationError(
            "storage-control-plan-unavailable",
            "plan",
            "could not read the signed control plan",
        ) from exc
    except json.JSONDecodeError as exc:
        raise FederationValidationError(
            "invalid-storage-control-plan",
            "plan",
            "plan file must contain valid JSON",
        ) from exc
    return StorageControlPlan.from_dict(value)


def _issue(arguments: argparse.Namespace) -> int:
    credentials = IdentityStore(
        arguments.authority_state_dir,
        display_name=arguments.authority_display_name,
    ).load()
    control = PhaseDControlPlane(arguments.control_database)
    store = StorageControlPublicationStore(arguments.publication_database)
    plan = store.issue(control, credentials, arguments.session_id)
    output = Path(arguments.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(plan.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "publication_id": plan.publication_id,
                "publication_revision": plan.publication_revision,
                "content_hash": plan.content_hash,
                "session_id": plan.session_id,
            },
            sort_keys=True,
        )
    )
    return 0


async def _publish(arguments: argparse.Namespace) -> int:
    plan = _load_plan(arguments.plan)
    client = RelayNodeClient(
        state_directory=arguments.state_dir,
        relay_url=arguments.relay,
        display_name=arguments.display_name,
        allow_insecure_local=arguments.allow_insecure_local,
        request_timeout=arguments.timeout,
    )
    if client.node_id != plan.authority_node_id:
        raise FederationValidationError(
            "storage-control-publisher-mismatch",
            "authority_node_id",
            "local identity does not own the signed plan",
        )
    state = client.state.status()
    enrollment = state["enrollment_state"]
    enrollment_token = (
        os.environ.get(DEFAULT_ENROLLMENT_TOKEN_ENV)
        if enrollment == EnrollmentState.UNENROLLED.value
        else None
    )
    if enrollment == EnrollmentState.UNENROLLED.value and not enrollment_token:
        raise FederationValidationError(
            "storage-control-enrollment-required",
            DEFAULT_ENROLLMENT_TOKEN_ENV,
            "first publication requires a protected enrollment token",
        )
    await client.connect(enrollment_token=enrollment_token)
    try:
        joined = {item.session_id for item in client.state.joined_sessions()}
        if plan.session_id not in joined:
            invitation = os.environ.get(DEFAULT_SESSION_INVITATION_ENV)
            if not invitation:
                raise FederationValidationError(
                    "storage-control-session-invitation-required",
                    DEFAULT_SESSION_INVITATION_ENV,
                    "publisher must join the plan session before delivery",
                )
            joined_session = await client.join_session(invitation)
            if joined_session.get("session_id") != plan.session_id:
                raise FederationValidationError(
                    "storage-control-session-mismatch",
                    "session_id",
                    "invitation joined a different session",
                )
        result = await StorageControlRelayPublisher(
            client, timeout=arguments.timeout
        ).publish(plan, tuple(arguments.target_node_id))
        print(
            json.dumps(
                {
                    "publication_id": result.publication_id,
                    "publication_revision": result.publication_revision,
                    "acknowledged_node_ids": list(result.acknowledged_node_ids),
                },
                sort_keys=True,
            )
        )
    finally:
        await client.disconnect()
    return 0


def main() -> int:
    arguments = _parser().parse_args()
    try:
        if arguments.command == "issue":
            return _issue(arguments)
        return asyncio.run(_publish(arguments))
    except KeyboardInterrupt:
        return 130
    except (FederationValidationError, TimeoutError) as exc:
        if isinstance(exc, FederationValidationError):
            error = {"code": exc.code, "field": exc.field, "message": exc.message}
        else:
            error = {
                "code": "storage-control-timeout",
                "field": "target_node_id",
                "message": str(exc),
            }
        print(json.dumps({"error": error}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
