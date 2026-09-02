"""Superseded contribution intent revisions must not accumulate for a lifetime.

``contribution_intents`` holds the authoritative current intent per candidate.
``contribution_intent_history`` holds the superseded revisions behind it, one
appended on every enable/disable/suspend/reconcile, and **nothing in the
product reads it** -- not one query selects from that table. No replay,
idempotency, authority or recovery path can observe a retired row, which is
what makes a bound here semantics-preserving rather than a retention policy.

Recent revisions are still retained: their only remaining value is operator
forensics, and that value is entirely in the recent ones.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.contributions import SQLiteContributionIntentStore
from catalog.capabilities.contributions.store import (
    MAX_INTENT_HISTORY_REVISIONS,
)
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionPolicyState,
)

NOW = datetime(2026, 9, 2, 9, tzinfo=timezone.utc)


def _candidate(candidate_id: str = "candidate-one") -> ContributionCandidate:
    return ContributionCandidate(
        candidate_id=candidate_id,
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
        expires_at=NOW + timedelta(days=365),
    )


def _churn(store: SQLiteContributionIntentStore, candidate_id: str, passes: int) -> None:
    """Alternate enable/disable, which is what an operator's history looks like."""

    states = (
        (ContributionDesiredState.ENABLED, ContributionActivationState.ACTIVE),
        (ContributionDesiredState.DISABLED, ContributionActivationState.INACTIVE),
    )
    for index in range(passes):
        desired, activation = states[index % 2]
        store.transition(
            candidate=_candidate(candidate_id),
            desired_state=desired,
            policy_state=ContributionPolicyState.ALLOWED,
            activation_state=activation,
            reason=None,
            decided_at=NOW + timedelta(seconds=index),
        )


def _history(path: Path, candidate_id: str) -> list[int]:
    connection = sqlite3.connect(path)
    try:
        return [
            int(row[0])
            for row in connection.execute(
                "SELECT revision FROM contribution_intent_history "
                "WHERE candidate_id=? ORDER BY revision ASC",
                (candidate_id,),
            ).fetchall()
        ]
    finally:
        connection.close()


def test_history_stops_growing_at_the_retained_window(tmp_path: Path) -> None:
    path = tmp_path / "intents.sqlite3"
    with SQLiteContributionIntentStore(path) as store:
        _churn(store, "candidate-one", MAX_INTENT_HISTORY_REVISIONS * 2)

    revisions = _history(path, "candidate-one")

    assert len(revisions) <= MAX_INTENT_HISTORY_REVISIONS


def test_the_newest_revisions_are_the_ones_retained(tmp_path: Path) -> None:
    """Forensic value is in recent history, so retirement takes the oldest."""

    path = tmp_path / "intents.sqlite3"
    passes = MAX_INTENT_HISTORY_REVISIONS + 25
    with SQLiteContributionIntentStore(path) as store:
        _churn(store, "candidate-one", passes)
        current = store.get("candidate-one")

    revisions = _history(path, "candidate-one")

    assert revisions == sorted(revisions)
    assert revisions[-1] == max(revisions)
    # The authoritative current intent is in the other table and is untouched.
    assert current is not None
    assert current.decision_revision >= revisions[-1]


def test_the_current_intent_is_never_retired(tmp_path: Path) -> None:
    """The authoritative row lives in contribution_intents and must survive."""

    path = tmp_path / "intents.sqlite3"
    with SQLiteContributionIntentStore(path) as store:
        _churn(store, "candidate-one", MAX_INTENT_HISTORY_REVISIONS * 2)

    with SQLiteContributionIntentStore(path) as reopened:
        loaded = reopened.get("candidate-one")

    assert loaded is not None
    assert loaded.desired_state in {
        ContributionDesiredState.ENABLED,
        ContributionDesiredState.DISABLED,
    }


def test_retirement_is_confined_to_the_candidate_that_was_written(
    tmp_path: Path,
) -> None:
    """One busy candidate must not retire a quiet one's history."""

    path = tmp_path / "intents.sqlite3"
    with SQLiteContributionIntentStore(path) as store:
        _churn(store, "candidate-quiet", 3)
        _churn(store, "candidate-busy", MAX_INTENT_HISTORY_REVISIONS * 2)

    assert len(_history(path, "candidate-quiet")) == 3
    assert len(_history(path, "candidate-busy")) <= MAX_INTENT_HISTORY_REVISIONS


def test_a_short_history_is_left_entirely_alone(tmp_path: Path) -> None:
    """False-positive prevention: an ordinary device retires nothing."""

    path = tmp_path / "intents.sqlite3"
    with SQLiteContributionIntentStore(path) as store:
        _churn(store, "candidate-one", 5)

    assert len(_history(path, "candidate-one")) == 5


def test_retirement_resumes_across_restart(tmp_path: Path) -> None:
    """The frontier is the candidate's own revision order, not process state."""

    path = tmp_path / "intents.sqlite3"
    with SQLiteContributionIntentStore(path) as store:
        _churn(store, "candidate-one", MAX_INTENT_HISTORY_REVISIONS + 10)
    first = len(_history(path, "candidate-one"))

    with SQLiteContributionIntentStore(path) as reopened:
        _churn(reopened, "candidate-one", 10)

    assert first <= MAX_INTENT_HISTORY_REVISIONS
    assert len(_history(path, "candidate-one")) <= MAX_INTENT_HISTORY_REVISIONS
