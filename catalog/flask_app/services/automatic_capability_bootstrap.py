"""Zero-touch capability bootstrap for an already trusted Federation member.

This module composes the installed capability-first services.  It does not add a
new authority path: inspection remains evidence, benchmarks remain bounded local
checks, contribution intent still flows through the existing adapters, and
provider/storage activation remains control-plane owned.

The command is intentionally idempotent.  Once capability-first startup is
complete it reuses the persisted state and does not rerun expensive evidence on
ordinary starts.  One narrow repair is required for creator storage: an existing
primary without a leader grant must refresh its bounded GREEN storage evidence so
the normal authenticated publication path can obtain the missing authority grant.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass

from catalog.capabilities.benchmarking import BenchmarkingError, evaluate_result
from catalog.capabilities.contributions import ContributionServiceError
from catalog.capabilities.storage_authority_enrollment import (
    DEFAULT_STORAGE_GROUP_ID,
    federation_storage_provider_id,
)
from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.onboarding_models import (
    BenchmarkRecommendation,
    BenchmarkState,
    ContributionActivationState,
    ContributionDesiredState,
    ContributionPolicyState,
)
from catalog.federation.phase_d_control import PhaseDControlPlane
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


_GREEN_RECOMMENDATIONS = frozenset(
    {BenchmarkRecommendation.RECOMMENDED, BenchmarkRecommendation.USABLE}
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


def _current_inspection(device_id: str):
    inspection = get_capability_inspection_service()
    snapshot = inspection.load()
    if snapshot is None or snapshot.device_id != device_id:
        return inspection.run()
    if inspection.state(snapshot) != "current":
        return inspection.run()
    return snapshot


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


def _fresh_green(result, item) -> bool:
    try:
        validity = evaluate_result(
            result,
            item.definition,
            item.dependency_inputs,
        )
    except Exception:  # noqa: BLE001 - repair evidence fails closed
        return False
    return bool(
        validity.current
        and getattr(result, "state", None) is BenchmarkState.PASSED
        and getattr(result, "recommendation", None) in _GREEN_RECOMMENDATIONS
    )


def _repair_creator_storage_authority_evidence(snapshot, *, node_id: str) -> int:
    """Refresh only a creator primary that is assigned but lacks its leader grant.

    Some pre-release builds could persist the creator's local storage assignment
    even when the storage benchmark was failed/skipped or when an upgrade changed
    the publication contract.  The relay correctly refuses to fabricate a grant
    without current GREEN evidence.  On ordinary startup, repair exactly that
    incomplete creator-primary state by running the bounded local storage check
    once and reconciling the existing intent.  No assignment or grant is written
    here; the normal authenticated Federation publication remains the authority.
    """

    contribution = get_capability_contribution_service()
    intents = {
        str(intent.candidate_id): intent
        for intent in contribution.intents()
        if isinstance(getattr(intent, "candidate_id", None), str)
        and getattr(intent, "desired_state", None) is ContributionDesiredState.ENABLED
    }
    if not intents:
        return 0

    candidates = tuple(contribution.recommend(require_benchmark_review=False))
    storage_candidates = tuple(
        candidate
        for candidate in candidates
        if getattr(candidate, "capability_type", None) == "storage"
        and getattr(candidate, "candidate_id", None) in intents
    )
    if not storage_candidates:
        return 0

    onboarding = get_capability_onboarding_service()
    context = onboarding.authorized_context()
    if context is None:
        raise FederationOperationError(
            "automatic-bootstrap-federation-required",
            "a trusted Federation membership is required before storage repair",
            "binding",
        )
    binding = getattr(context, "binding", None)
    session_id = getattr(binding, "internal_session_id", None)
    if not isinstance(session_id, str) or not session_id:
        raise FederationValidationError(
            "automatic-storage-session-missing",
            "session_id",
            "trusted Federation storage repair requires the current session",
        )
    session = context.coordinator.store.get_session(session_id)
    if session is None or getattr(session, "created_by_node_id", None) != node_id:
        return 0

    control = PhaseDControlPlane(context.coordinator.store.database)
    control_snapshot = control.snapshot(session_id)
    assignment = control_snapshot.groups.get(DEFAULT_STORAGE_GROUP_ID)
    if assignment is None:
        return 0

    repair_targets: list[str] = []
    for candidate in storage_candidates:
        envelope = getattr(candidate, "capacity_envelope", None)
        local_provider_id = (
            envelope.get("provider_id") if isinstance(envelope, dict) else None
        )
        if not isinstance(local_provider_id, str) or not local_provider_id.strip():
            continue
        provider_id = federation_storage_provider_id(node_id, local_provider_id.strip())
        if assignment.primary_provider_id != provider_id:
            continue
        active = control_snapshot.leader_grants.get(DEFAULT_STORAGE_GROUP_ID)
        if active is not None and active.get("provider_id") == provider_id:
            continue
        repair_targets.append(local_provider_id.strip())

    if not repair_targets:
        return 0

    benchmarks = get_capability_benchmark_service()
    plan = tuple(benchmarks.plan(snapshot))
    ran = 0
    for target_service_id in sorted(set(repair_targets)):
        matches = [
            item
            for item in plan
            if getattr(item, "target_service_id", None) == target_service_id
            and getattr(getattr(item, "definition", None), "capability_type", None)
            == "storage"
            and getattr(item, "runnable", False)
        ]
        if len(matches) != 1:
            raise FederationOperationError(
                "automatic-storage-benchmark-target-unavailable",
                "the assigned creator storage primary has no runnable bounded storage check",
                "benchmarks",
            )
        item = matches[0]
        result = benchmarks.run(
            benchmark_id=item.benchmark_id,
            target_service_id=item.target_service_id,
        )
        ran += 1
        if not _fresh_green(result, item):
            raise FederationOperationError(
                "automatic-storage-benchmark-not-green",
                "the assigned creator storage primary did not produce current GREEN evidence",
                "benchmarks",
            )

    # Rebind the persisted decision to the freshly reconstructed candidate.  This
    # does not grant authority; it makes the new benchmark run IDs available to
    # the publication monitor, which then carries the strict storage attestation.
    contribution.reconcile()
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
        device_id = _connected_device_id()
        snapshot = _current_inspection(device_id)
        repaired = _repair_creator_storage_authority_evidence(
            snapshot,
            node_id=device_id,
        )
        return AutomaticCapabilityBootstrapResult(
            state=(
                "repaired-storage-authority-evidence"
                if repaired
                else "already-complete"
            ),
            inspection_revision=snapshot.revision,
            benchmarks_run=repaired,
            contributions_enabled=0,
            contributions_disabled=0,
            startup_completed=True,
        )

    device_id = _connected_device_id()
    snapshot = _current_inspection(device_id)

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
