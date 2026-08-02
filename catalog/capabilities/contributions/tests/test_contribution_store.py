from __future__ import annotations

from datetime import datetime, timedelta, timezone

from catalog.capabilities.contributions import SQLiteContributionIntentStore
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionPolicyState,
)

NOW = datetime(2026, 8, 2, 20, tzinfo=timezone.utc)


def candidate() -> ContributionCandidate:
    return ContributionCandidate(
        candidate_id="candidate-one",
        device_id="device-one",
        capability_type="recorder",
        capability_protocol="mtconnect",
        display_label="Recorder",
        inspection_revision=1,
        benchmark_run_ids=(),
        capacity_envelope={},
        missing_prerequisites=(),
        policy_state=ContributionPolicyState.NOT_EVALUATED,
        created_at=NOW,
        expires_at=NOW + timedelta(hours=1),
    )


def test_intent_store_is_restart_safe_and_idempotent(tmp_path) -> None:
    path = tmp_path / "intents.sqlite3"
    with SQLiteContributionIntentStore(path) as store:
        first = store.transition(
            candidate=candidate(),
            desired_state=ContributionDesiredState.ENABLED,
            policy_state=ContributionPolicyState.ALLOWED,
            activation_state=ContributionActivationState.ACTIVE,
            reason=None,
            decided_at=NOW,
        )
        duplicate = store.transition(
            candidate=candidate(),
            desired_state=ContributionDesiredState.ENABLED,
            policy_state=ContributionPolicyState.ALLOWED,
            activation_state=ContributionActivationState.ACTIVE,
            reason=None,
            decided_at=NOW + timedelta(seconds=1),
        )
        assert first == duplicate
        assert first.decision_revision == 1

    with SQLiteContributionIntentStore(path) as reopened:
        loaded = reopened.get("candidate-one")
        assert loaded == first
        disabled = reopened.transition(
            candidate=candidate(),
            desired_state=ContributionDesiredState.DISABLED,
            policy_state=ContributionPolicyState.ALLOWED,
            activation_state=ContributionActivationState.INACTIVE,
            reason="Fenced",
            decided_at=NOW + timedelta(seconds=2),
        )
        assert disabled.decision_revision == 2
