"""Headless capability inspection, benchmarking, and contribution bootstrap.

The canonical ``fcp`` host CLI invokes this module *inside* the Flask container.
That keeps provider discovery, benchmarking and contribution activation on the
same application composition as the web UI while avoiding host Python/package
drift.

A normal device joins the Federation in order to contribute.  ``bootstrap``
therefore performs the complete local sequence by default:

    inspect -> run every runnable benchmark -> enable every eligible candidate

Operators may opt out with ``--disable`` or restrict the device with ``--only``.
Explicit policy BLOCKED states still fail closed; this command does not forge a
capability that did not pass its existing prerequisites/benchmark contract.
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

from .capability_benchmark_service import get_capability_benchmark_service
from .capability_contribution_service import get_capability_contribution_service
from .capability_inspection_service import get_capability_inspection_service


def _types(value: str | None) -> frozenset[str]:
    if not value:
        return frozenset()
    return frozenset(
        item.strip().casefold()
        for item in value.split(",")
        if item.strip()
    )


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


def run_bootstrap(
    *,
    only: frozenset[str] = frozenset(),
    disabled: frozenset[str] = frozenset(),
) -> dict[str, object]:
    if only.intersection(disabled):
        overlap = ", ".join(sorted(only.intersection(disabled)))
        raise FederationValidationError(
            "conflicting-capability-selection",
            "capability_type",
            f"cannot be both --only and --disable: {overlap}",
        )

    inspection = get_capability_inspection_service()
    benchmark = get_capability_benchmark_service()
    contributions = get_capability_contribution_service()

    snapshot = inspection.run()
    benchmark_outcomes: list[dict[str, object]] = []
    unavailable: list[dict[str, str]] = []

    for item in benchmark.plan(snapshot):
        if not item.runnable:
            unavailable.append(
                {
                    "benchmark_id": item.benchmark_id,
                    "target_service_id": item.target_service_id,
                    "reason": item.unavailable_reason or "unavailable",
                }
            )
            continue
        result = benchmark.run(
            benchmark_id=item.benchmark_id,
            target_service_id=item.target_service_id,
        )
        benchmark_outcomes.append(
            {
                "benchmark_id": result.benchmark_id,
                "target_service_id": result.target_service_id,
                "state": result.state.value,
                "recommendation": result.recommendation.value,
            }
        )

    # Mark any still-reviewable runnable item explicitly skipped.  In the normal
    # path this is zero because every runnable item above was executed; keeping
    # this call makes interrupted/stale plans deterministic on rerun.
    skipped = benchmark.skip_all()

    candidates = contributions.recommend(require_benchmark_review=True)
    choices: dict[str, str] = {}
    selected: list[str] = []
    excluded: list[str] = []
    for candidate in candidates:
        capability_type = candidate.capability_type.casefold()
        enabled = (
            capability_type not in disabled
            and (not only or capability_type in only)
        )
        choices[candidate.candidate_id] = "enabled" if enabled else "disabled"
        (selected if enabled else excluded).append(candidate.candidate_id)

    intents = contributions.apply_choices(choices) if choices else ()
    return {
        "action": "bootstrap",
        "state": "complete",
        "device_id": snapshot.device_id,
        "inspection_revision": snapshot.revision,
        "benchmarks": benchmark_outcomes,
        "unavailable_benchmarks": unavailable,
        "skipped_benchmarks": skipped,
        "selected_candidates": selected,
        "excluded_candidates": excluded,
        "contributions": [
            {
                "candidate_id": intent.candidate_id,
                "desired_state": intent.desired_state.value,
                "policy_state": intent.policy_state.value,
                "activation_state": intent.activation_state.value,
                "reason": intent.reason,
            }
            for intent in intents
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Headless local FCP capability commands.",
        allow_abbrev=False,
    )
    parser.add_argument("--json", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("inspect", help="Inspect this device without changing contributions.")
    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="Inspect, benchmark, and contribute every eligible capability.",
    )
    bootstrap.add_argument(
        "--only",
        default="",
        metavar="TYPE,...",
        help="Contribute only the named capability types.",
    )
    bootstrap.add_argument(
        "--disable",
        default="",
        metavar="TYPE,...",
        help="Do not contribute the named capability types.",
    )
    return parser


def _emit(result: dict[str, object], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return
    if result["action"] == "inspect":
        print("Capability inspection complete.")
        print(f"  Device:   {result['device_id']}")
        print(f"  Revision: {result['revision']}")
        services = result["detected_services"]
        sources = result["detected_data_sources"]
        print(f"  Services: {', '.join(services) if services else 'none'}")
        print(f"  Sources:  {', '.join(sources) if sources else 'none'}")
        return

    print("Capability bootstrap complete.")
    print(f"  Device:       {result['device_id']}")
    print(f"  Benchmarks:   {len(result['benchmarks'])}")
    print(f"  Contributions:{len(result['selected_candidates'])}")
    for intent in result["contributions"]:
        print(
            "  - "
            f"{intent['candidate_id']}: {intent['activation_state']} "
            f"({intent['policy_state']})"
        )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app = create_app()
    try:
        with app.app_context():
            if args.command == "bootstrap":
                result = run_bootstrap(
                    only=_types(args.only),
                    disabled=_types(args.disable),
                )
            else:
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
        ValueError,
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

    _emit(result, as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
