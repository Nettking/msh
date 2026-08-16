"""Headless local capability evidence commands used by FCP acceptance.

The command reuses the installed capability inspection service.  Inspection is
read-only evidence and grants no Federation, provider, storage, or compute
authority.
"""

from __future__ import annotations

import argparse
import json
import sys

from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.flask_app.app import create_app
from catalog.node.identity import NodeIdentityStateError

from .capability_inspection_service import get_capability_inspection_service


def run_inspection() -> dict[str, object]:
    snapshot = get_capability_inspection_service().run()
    return {
        "action": "inspect",
        "state": "complete",
        "device_id": snapshot.device_id,
        "revision": snapshot.revision,
        "os_family": snapshot.os_family,
        "architecture": snapshot.architecture,
        "detected_services": list(snapshot.detected_services),
        "detected_data_sources": list(snapshot.detected_data_sources),
        "registered_handlers": list(snapshot.registered_handlers),
        "recommended_benchmark_ids": list(snapshot.recommended_benchmark_ids),
        "warnings": list(snapshot.warnings),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Headless local FCP capability evidence commands.",
        allow_abbrev=False,
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("command", choices=("inspect",))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app = create_app()
    try:
        with app.app_context():
            result = run_inspection()
    except (
        AuthenticationError,
        AuthorizationError,
        FederationOperationError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
        OSError,
        RuntimeError,
        TimeoutError,
    ) as exc:
        code = str(getattr(exc, "code", type(exc).__name__))
        message = str(getattr(exc, "message", str(exc) or type(exc).__name__))
        if args.json:
            print(
                json.dumps(
                    {"accepted": False, "error": code, "message": message},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                file=sys.stderr,
            )
        else:
            print(f"Headless capability command failed ({code}): {message}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    else:
        print("Capability inspection complete.")
        print(f"  Device:   {result['device_id']}")
        print(f"  Revision: {result['revision']}")
        services = result["detected_services"]
        sources = result["detected_data_sources"]
        print(f"  Services: {', '.join(services) if services else 'none'}")
        print(f"  Sources:  {', '.join(sources) if sources else 'none'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
