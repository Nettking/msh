"""Zero-touch capability bootstrap for an already trusted Federation member.

This module composes the installed capability-first services.  It does not add a
new authority path: inspection remains evidence, benchmarks remain bounded local
checks, contribution intent still flows through the existing adapters, and
provider/storage activation remains control-plane owned.

The command is intentionally idempotent.  Once capability-first startup is
complete it reuses the persisted state and does not rerun expensive evidence on
ordinary starts.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass

from catalog.capabilities.benchmarking import BenchmarkingError
from catalog.capabilities.contributions import ContributionServiceError
from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionDesiredState,
    ContributionPolicyState,
)
from catalog.node.identity import NodeIdentityStateError

from catalog.flask_app.app import create_app

from .capability_benchmark_service import get_capability_benchmark_service
from .capability_contribution_service import get_capability_contribution_service
from .capability_inspection_service import get_capability_inspection_service
from .capability_onboarding_service import get_capability_onboarding_service
from .capability_startup_transition_service import (
    CapabilityStartupState,
    get_capability_startup_transition_service,
)


@dataclass(frozen=True)
class AutomaticCapabilityBootstrapResult:
    state: str
    inspection_revision: int
    benchmarks_run: int
    contributions_enabled: int
    contributions_disabled: int
    startup_completed: bool


def _connected_device_id() -> str:
    context = get_capability_onboarding_service().authorized_context()
    if context is None:
        raise FederationOperationError(
            "automatic-bootstrap-federation-required",
            "a trusted Federation membership is required before automatic capability bootstrap",
            "binding",
        )
    return context.credentials.identity.node_id


def _run_all_benchmarks(snapshot) -> int:
    service = get_capability_benchmark_service()
    _summary, cards, complete = service.view_model(snapshot, connected=True)
    if complete:
        return 0

    ran = 0
    for card in cards:
        if card.get("can_run") is not True:
            continue
        benchmark_id = card.get("benchmark_id")
        target_service_id = card.get("target_service_id")
        if not isinstance(benchmark_id, str) or not isinstance(target_service_id, str):
            raise FederationValidationError(
                "automatic-benchmark-plan-invalid",
                "benchmark",
                "the current benchmark plan contains an invalid target",
            )
        service.run(
            benchmark_id=benchmark_id,
            target_service_id=target_service_id,
        )
        ran += 1

    _summary, remaining, complete = service.view_model(snapshot, connected=True)
    if not complete:
        blocked = sorted(
            str(card.get("benchmark_id") or "unknown")
            for card in remaining
            if card.get("state") not in {"passed", "failed", "cancelled", "skipped"}
        )
        detail = ", ".join(blocked[:8]) or "unknown benchmark"
        raise FederationOperationError(
            "automatic-benchmarks-incomplete",
            f"all applicable benchmarks must finish before automatic activation ({detail})",
            "benchmarks",
        )
    return ran


def _enable_available_contributions(snapshot) -> tuple[int, int]:
    service = get_capability_contribution_service()
    candidates = service.recommend(require_benchmark_review=True)
    enabled = 0
    disabled = 0

    for candidate in candidates:
        # A candidate already known to be blocked is not an available service.
        # Persist an explicit disabled choice so onboarding is complete without
        # pretending that the missing prerequisite was satisfied.
        if candidate.policy_state is ContributionPolicyState.BLOCKED:
            service.apply_choices(
                {candidate.candidate_id: ContributionDesiredState.DISABLED.value}
            )
            disabled += 1
            continue

        intent = service.apply_choices(
            {candidate.candidate_id: ContributionDesiredState.ENABLED.value}
        )[0]
        # Some policy checks (notably recorder source readiness) are evaluated
        # only for the ENABLED desired state.  If that makes the candidate
        # blocked, immediately fence it and persist DISABLED.  APPROVAL_REQUIRED
        # and PENDING are deliberately kept enabled: those states represent the
        # existing control plane doing its job rather than an unavailable local
        # service.
        if (
            intent.policy_state is ContributionPolicyState.BLOCKED
            or intent.activation_state is ContributionActivationState.BLOCKED
        ):
            service.apply_choices(
                {candidate.candidate_id: ContributionDesiredState.DISABLED.value}
            )
            disabled += 1
        else:
            enabled += 1

    summary, _cards, complete = service.view_model(
        snapshot,
        connected=True,
        benchmark_complete=True,
    )
    if not complete or summary.get("state") != "complete":
        raise FederationOperationError(
            "automatic-contributions-incomplete",
            "every supported contribution must have a safe persisted decision before startup completes",
            "contributions",
        )
    return enabled, disabled


def bootstrap_capabilities() -> AutomaticCapabilityBootstrapResult:
    """Inspect, benchmark, enable available services, and complete startup once."""

    transition = get_capability_startup_transition_service()
    existing = transition.load()
    if isinstance(existing, CapabilityStartupState) and existing.completed:
        return AutomaticCapabilityBootstrapResult(
            state="already-complete",
            inspection_revision=existing.inspection_revision,
            benchmarks_run=0,
            contributions_enabled=0,
            contributions_disabled=0,
            startup_completed=True,
        )

    device_id = _connected_device_id()
    inspection = get_capability_inspection_service()
    snapshot = inspection.load()
    if snapshot is None or snapshot.device_id != device_id:
        snapshot = inspection.run()
    elif inspection.state(snapshot) != "current":
        snapshot = inspection.run()

    benchmarks_run = _run_all_benchmarks(snapshot)
    enabled, disabled = _enable_available_contributions(snapshot)
    completed = transition.complete_current()
    return AutomaticCapabilityBootstrapResult(
        state="completed",
        inspection_revision=completed.inspection_revision,
        benchmarks_run=benchmarks_run,
        contributions_enabled=enabled,
        contributions_disabled=disabled,
        startup_completed=completed.completed,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the installed zero-touch capability bootstrap once.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print one deterministic JSON result.",
    )
    return parser


def _emit(result: AutomaticCapabilityBootstrapResult, *, as_json: bool) -> None:
    value = asdict(result)
    if as_json:
        print(json.dumps(value, sort_keys=True, separators=(",", ":")))
        return
    print(f"Capability bootstrap: {result.state}")
    print(f"Inspection revision:  {result.inspection_revision}")
    print(f"Benchmarks run:       {result.benchmarks_run}")
    print(f"Services enabled:     {result.contributions_enabled}")
    print(f"Services unavailable: {result.contributions_disabled}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app = create_app()
    try:
        with app.app_context():
            result = bootstrap_capabilities()
    except (
        AuthenticationError,
        AuthorizationError,
        BenchmarkingError,
        ContributionServiceError,
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
            print(
                f"Automatic capability bootstrap failed ({code}): {message}",
                file=sys.stderr,
            )
        return 2

    _emit(result, as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AutomaticCapabilityBootstrapResult",
    "bootstrap_capabilities",
    "main",
]
