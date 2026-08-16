"""Headless Federation human-auth publication for the canonical ``fcp`` CLI.

The Federation creator keeps password material locally and publishes only signed
non-secret authority/user metadata. Joined members publish only their web origin
so browsers can be redirected to the creator for Federation SSO.
"""

from __future__ import annotations

import argparse
import json
import sys

from catalog.federation.errors import FederationOperationError
from catalog.flask_app.app import create_app
from catalog.flask_app.auth.federation import get_federated_human_auth_service


def configure_authority(base_url: str) -> dict[str, object]:
    service = get_federated_human_auth_service()
    authority = service.ensure_authority(base_url)
    if authority is None:
        raise FederationOperationError(
            "human-auth-authority-unavailable",
            "this device is not the Federation human-auth authority",
        )
    service.publish_all_users()
    return {
        "action": "authority",
        "state": "published",
        "node_id": authority.node_id,
        "base_url": authority.base_url,
        "revision": authority.revision,
    }


def configure_member(base_url: str) -> dict[str, object]:
    service = get_federated_human_auth_service()
    normalized = service.ensure_member_endpoint(base_url)
    authority = service.authority(refresh=True)
    if authority is None:
        raise FederationOperationError(
            "human-auth-authority-unavailable",
            "the joined Federation has not published its human-auth authority",
        )
    return {
        "action": "member",
        "state": "published",
        "base_url": normalized,
        "authority_node_id": authority.node_id,
        "authority_base_url": authority.base_url,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--json", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)
    authority = subparsers.add_parser("authority")
    authority.add_argument("--base-url", required=True)
    member = subparsers.add_parser("member")
    member.add_argument("--base-url", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app = create_app()
    try:
        with app.app_context():
            if args.command == "authority":
                result = configure_authority(args.base_url)
            else:
                result = configure_member(args.base_url)
    except (FederationOperationError, OSError, RuntimeError, ValueError) as exc:
        code = str(getattr(exc, "code", type(exc).__name__))
        message = str(getattr(exc, "message", str(exc) or type(exc).__name__))
        if args.json:
            print(json.dumps({"accepted": False, "error": code, "message": message}), file=sys.stderr)
        else:
            print(f"Federation human-auth bootstrap failed ({code}): {message}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    else:
        print(f"Federation human-auth {result['action']} published.")
        if result.get("authority_base_url"):
            print(f"  Authority: {result['authority_base_url']}")
        else:
            print(f"  Base URL:  {result['base_url']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
