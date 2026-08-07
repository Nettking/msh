"""CF4 contribution recommendation, intent, policy, and reconciliation service."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import replace
from datetime import datetime, timezone

from catalog.federation.onboarding_models import (
    BenchmarkResult,
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionIntent,
    ContributionPolicyState,
    DeviceInspectionSnapshot,
)

from .candidates import ContributionCandidateGenerator
from .errors import AdapterResolutionError, CandidateNotFoundError
from .policy import ContributionPolicyEvaluator, PolicyEvaluation
from .store import SQLiteContributionIntentStore
from .types import (
    AdapterOutcome,
    CandidateRecommendation,
    ContributionActivationAdapter,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ContributionService:
    """Keep user intent separate from every existing authority boundary."""

    def __init__(
        self,
        *,
        generator: ContributionCandidateGenerator,
        store: SQLiteContributionIntentStore,
        policy: ContributionPolicyEvaluator,
        adapters: Sequence[ContributionActivationAdapter],
        now: Callable[[], datetime] = _utc_now,
        enforce_candidate_expiry: bool = True,
    ) -> None:
        if not isinstance(enforce_candidate_expiry, bool):
            raise TypeError("enforce_candidate_expiry must be boolean")
        self._generator = generator
        self._store = store
        self._policy = policy
        self._adapters = tuple(adapters)
        self._now = now
        self._enforce_candidate_expiry = enforce_candidate_expiry
        self._recommendations: dict[str, CandidateRecommendation] = {}

    def recommend(
        self,
        inspection: DeviceInspectionSnapshot,
        benchmark_results: Iterable[BenchmarkResult],
    ) -> tuple[ContributionCandidate, ...]:
        recommendations = self._generator.generate(inspection, benchmark_results)
        self._recommendations = {
            item.candidate.candidate_id: item for item in recommendations
        }
        candidates: list[ContributionCandidate] = []
        for item in recommendations:
            existing = self._store.get(item.candidate.candidate_id)
            desired = (
                ContributionDesiredState.ASK_LATER
                if existing is None
                else existing.desired_state
            )
            evaluation = self._policy.evaluate(
                item.candidate,
                item.benchmark_results,
                desired,
            )
            candidates.append(
                replace(item.candidate, policy_state=evaluation.state)
            )
        return tuple(candidates)

    def enable(self, candidate_id: str) -> ContributionIntent:
        recommendation = self._recommendation(candidate_id)
        candidate = recommendation.candidate
        evaluation = self._policy.evaluate(
            candidate,
            recommendation.benchmark_results,
            ContributionDesiredState.ENABLED,
        )
        adapter = self._adapter(candidate)
        if evaluation.state is ContributionPolicyState.ALLOWED:
            outcome = adapter.enable(candidate)
        elif adapter.candidate_only:
            outcome = adapter.enable(candidate)
            if outcome.authority_confirmed:
                evaluation = PolicyEvaluation(ContributionPolicyState.ALLOWED)
        else:
            outcome = AdapterOutcome(
                ContributionActivationState.BLOCKED
                if evaluation.state is ContributionPolicyState.BLOCKED
                else ContributionActivationState.PENDING,
                evaluation.reason or "Activation requires an additional decision.",
            )
        return self._persist(
            candidate,
            desired=ContributionDesiredState.ENABLED,
            evaluation=evaluation,
            outcome=outcome,
        )

    def disable(self, candidate_id: str) -> ContributionIntent:
        recommendation = self._recommendation(candidate_id, require_current=False)
        candidate = recommendation.candidate
        adapter = self._adapter(candidate)
        outcome = adapter.disable(candidate)
        return self._store.transition(
            candidate=candidate,
            desired_state=ContributionDesiredState.DISABLED,
            policy_state=ContributionPolicyState.ALLOWED,
            activation_state=ContributionActivationState.INACTIVE,
            reason=outcome.reason,
            decided_at=self._now(),
        )

    def ask_later(self, candidate_id: str) -> ContributionIntent:
        recommendation = self._recommendation(candidate_id, require_current=False)
        candidate = recommendation.candidate
        outcome = self._adapter(candidate).disable(candidate)
        return self._store.transition(
            candidate=candidate,
            desired_state=ContributionDesiredState.ASK_LATER,
            policy_state=ContributionPolicyState.ALLOWED,
            activation_state=ContributionActivationState.INACTIVE,
            reason=outcome.reason,
            decided_at=self._now(),
        )

    def suspend(self, candidate_id: str, *, reason: str) -> ContributionIntent:
        recommendation = self._recommendation(candidate_id, require_current=False)
        candidate = recommendation.candidate
        current = self._store.get(candidate_id)
        desired = (
            ContributionDesiredState.ASK_LATER
            if current is None
            else current.desired_state
        )
        policy_state = (
            ContributionPolicyState.NOT_EVALUATED
            if current is None
            else current.policy_state
        )
        outcome = self._adapter(candidate).suspend(candidate, reason=reason)
        return self._store.transition(
            candidate=candidate,
            desired_state=desired,
            policy_state=policy_state,
            activation_state=ContributionActivationState.SUSPENDED,
            reason=outcome.reason or reason,
            decided_at=self._now(),
        )

    def reconcile(self) -> tuple[ContributionIntent, ...]:
        reconciled: list[ContributionIntent] = []
        for current in self._store.list_intents():
            recommendation = self._recommendations.get(current.candidate_id)
            if recommendation is None:
                reconciled.append(current)
                continue
            candidate = recommendation.candidate
            adapter = self._adapter(candidate)
            if (
                self._enforce_candidate_expiry
                and candidate.expires_at <= self._now()
                and current.desired_state is ContributionDesiredState.ENABLED
            ):
                outcome = adapter.suspend(
                    candidate,
                    reason="Contribution inspection or benchmark evidence expired.",
                )
                reconciled.append(
                    self._persist(
                        candidate,
                        desired=current.desired_state,
                        evaluation=PolicyEvaluation(current.policy_state),
                        outcome=outcome,
                    )
                )
                continue
            if current.desired_state in {
                ContributionDesiredState.DISABLED,
                ContributionDesiredState.ASK_LATER,
            }:
                outcome = adapter.reconcile(
                    candidate,
                    desired_state=ContributionDesiredState.DISABLED,
                )
                evaluation = PolicyEvaluation(ContributionPolicyState.ALLOWED)
            else:
                evaluation = self._policy.evaluate(
                    candidate,
                    recommendation.benchmark_results,
                    current.desired_state,
                )
                if evaluation.state is ContributionPolicyState.ALLOWED:
                    outcome = adapter.reconcile(
                        candidate,
                        desired_state=current.desired_state,
                    )
                elif adapter.candidate_only:
                    outcome = adapter.reconcile(
                        candidate,
                        desired_state=current.desired_state,
                    )
                    if outcome.authority_confirmed:
                        evaluation = PolicyEvaluation(
                            ContributionPolicyState.ALLOWED
                        )
                else:
                    outcome = AdapterOutcome(
                        ContributionActivationState.BLOCKED
                        if evaluation.state is ContributionPolicyState.BLOCKED
                        else ContributionActivationState.PENDING,
                        evaluation.reason
                        or "Activation requires an additional decision.",
                    )
            reconciled.append(
                self._persist(
                    candidate,
                    desired=current.desired_state,
                    evaluation=evaluation,
                    outcome=outcome,
                )
            )
        return tuple(reconciled)

    def _persist(
        self,
        candidate: ContributionCandidate,
        *,
        desired: ContributionDesiredState,
        evaluation: PolicyEvaluation,
        outcome: AdapterOutcome,
    ) -> ContributionIntent:
        reason = outcome.reason or evaluation.reason
        if outcome.activation_state is ContributionActivationState.BLOCKED and not reason:
            reason = "Activation was blocked by policy."
        return self._store.transition(
            candidate=candidate,
            desired_state=desired,
            policy_state=evaluation.state,
            activation_state=outcome.activation_state,
            reason=reason,
            decided_at=self._now(),
        )

    def _recommendation(
        self,
        candidate_id: str,
        *,
        require_current: bool = True,
    ) -> CandidateRecommendation:
        try:
            recommendation = self._recommendations[candidate_id]
        except KeyError as exc:
            raise CandidateNotFoundError(
                f"candidate is not in the current recommendation set: {candidate_id}"
            ) from exc
        if (
            require_current
            and self._enforce_candidate_expiry
            and recommendation.candidate.expires_at <= self._now()
        ):
            raise CandidateNotFoundError(f"candidate expired: {candidate_id}")
        return recommendation

    def _adapter(
        self,
        candidate: ContributionCandidate,
    ) -> ContributionActivationAdapter:
        matches = tuple(
            adapter for adapter in self._adapters if adapter.supports(candidate)
        )
        if len(matches) != 1:
            raise AdapterResolutionError(
                f"expected exactly one adapter for {candidate.candidate_id}, "
                f"found {len(matches)}"
            )
        return matches[0]
