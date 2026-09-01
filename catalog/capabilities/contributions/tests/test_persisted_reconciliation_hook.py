from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.contributions.policy import ContributionPolicyEvaluator
from catalog.capabilities.contributions.service import ContributionService
from catalog.capabilities.contributions.store import SQLiteContributionIntentStore
from catalog.capabilities.contributions.types import (
    AdapterOutcome,
    CandidateRecommendation,
)
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionPolicyState,
)

NOW = datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc)


def _candidate() -> ContributionCandidate:
    return ContributionCandidate(
        candidate_id="candidate-persisted-hook",
        device_id="node-persisted-hook",
        capability_type="compute",
        capability_protocol="fcp-test",
        display_label="Persisted hook",
        inspection_revision=1,
        benchmark_run_ids=(),
        capacity_envelope={"kind": "registered-compute-handler"},
        missing_prerequisites=(),
        policy_state=ContributionPolicyState.ALLOWED,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


class _Adapter:
    candidate_only = False

    def __init__(self) -> None:
        self.persisted = []
        self.fail_projection = False

    def supports(self, _candidate) -> bool:
        return True

    def enable(self, _candidate) -> AdapterOutcome:
        return AdapterOutcome(ContributionActivationState.ACTIVE)

    def disable(self, _candidate) -> AdapterOutcome:
        return AdapterOutcome(ContributionActivationState.INACTIVE)

    def suspend(self, _candidate, *, reason: str) -> AdapterOutcome:
        return AdapterOutcome(ContributionActivationState.SUSPENDED, reason)

    def reconcile(self, _candidate, *, desired_state) -> AdapterOutcome:
        return (
            self.disable(_candidate)
            if desired_state is ContributionDesiredState.DISABLED
            else self.enable(_candidate)
        )

    def reconcile_persisted(self, candidate, intent) -> None:
        persisted = self._store.get(candidate.candidate_id)
        assert persisted == intent
        self.persisted.append(intent)
        if self.fail_projection:
            raise RuntimeError("projection unavailable")


def _service(tmp_path: Path):
    candidate = _candidate()
    adapter = _Adapter()
    store = SQLiteContributionIntentStore(tmp_path / "contributions.sqlite3")
    adapter._store = store
    service = ContributionService(
        generator=object(),  # recommend() is intentionally not needed in this focused test
        store=store,
        policy=ContributionPolicyEvaluator(),
        adapters=(adapter,),
        now=lambda: NOW,
    )
    service._recommendations[candidate.candidate_id] = CandidateRecommendation(
        candidate=candidate,
        logical_service_id="persisted-hook",
        benchmark_results=(),
    )
    return candidate, adapter, store, service


def test_post_persist_hook_receives_store_assigned_decision_revision(tmp_path: Path) -> None:
    candidate, adapter, store, service = _service(tmp_path)

    enabled = service.enable(candidate.candidate_id)
    disabled = service.disable(candidate.candidate_id)

    assert enabled.decision_revision == 1
    assert disabled.decision_revision == 2
    assert [item.decision_revision for item in adapter.persisted] == [1, 2]
    assert store.get(candidate.candidate_id) == disabled


def test_projection_failure_leaves_durable_intent_for_reconciliation_retry(
    tmp_path: Path,
) -> None:
    candidate, adapter, store, service = _service(tmp_path)
    adapter.fail_projection = True

    with pytest.raises(RuntimeError, match="projection unavailable"):
        service.enable(candidate.candidate_id)

    persisted = store.get(candidate.candidate_id)
    assert persisted is not None
    assert persisted.activation_state is ContributionActivationState.ACTIVE
    assert persisted.decision_revision == 1
