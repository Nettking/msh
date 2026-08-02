"""Contribution activation policy evaluation."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from catalog.federation.onboarding_models import (
    BenchmarkResult,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionPolicyState,
)


@dataclass(frozen=True)
class PolicyEvaluation:
    state: ContributionPolicyState
    reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", ContributionPolicyState(self.state))
        if self.reason is not None:
            value = str(self.reason).strip()
            if not value:
                raise ValueError("policy reason must be non-empty")
            object.__setattr__(self, "reason", value)


PolicyRule = Callable[
    [ContributionCandidate, tuple[BenchmarkResult, ...], ContributionDesiredState],
    PolicyEvaluation | None,
]


class ContributionPolicyEvaluator:
    """Deterministic local policy; evidence never activates by itself."""

    def __init__(self, *, rules: Sequence[PolicyRule] = ()) -> None:
        self._rules = tuple(rules)

    def evaluate(
        self,
        candidate: ContributionCandidate,
        evidence: tuple[BenchmarkResult, ...],
        desired_state: ContributionDesiredState,
    ) -> PolicyEvaluation:
        desired_state = ContributionDesiredState(desired_state)
        if candidate.missing_prerequisites:
            return PolicyEvaluation(
                ContributionPolicyState.BLOCKED,
                "Required local prerequisites are missing.",
            )
        for rule in self._rules:
            decision = rule(candidate, evidence, desired_state)
            if decision is not None:
                if not isinstance(decision, PolicyEvaluation):
                    raise TypeError("policy rules must return PolicyEvaluation or None")
                return decision
        if (
            desired_state is ContributionDesiredState.ENABLED
            and candidate.capability_type == "storage"
        ):
            return PolicyEvaluation(
                ContributionPolicyState.APPROVAL_REQUIRED,
                "Storage remains a candidate until the control plane assigns authority.",
            )
        return PolicyEvaluation(ContributionPolicyState.ALLOWED)
