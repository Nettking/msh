"""A small, safe read model over federation execution learning.

The point of this module is diagnosis, not dashboards: how much the federation
has learned, what it currently believes, how confident it is, and why it
recently chose what it chose. Everything returned is plain JSON built from
already-public scheduling metadata.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .policy import EfficiencyPolicy, expected_completion_millis
from .profiles import ANY, EvidenceTier, LearnedProfile
from .store import SQLiteExecutionLearningStore

LEARNING_SNAPSHOT_SCHEMA = "fcp.efficiency-learning-snapshot.v1"

DEFAULT_PROFILE_LIMIT = 25
DEFAULT_DECISION_LIMIT = 10


def _profile_view(
    profile: LearnedProfile, *, now: datetime, policy: EfficiencyPolicy
) -> dict[str, Any]:
    estimate = profile.estimate(
        now,
        half_life_seconds=policy.half_life_seconds,
        tier=EvidenceTier.MEASURED_EXACT,
    )
    cost = expected_completion_millis(estimate, policy=policy)
    return {
        "capability_type": profile.key.capability_type,
        "workload_class": profile.key.workload_class,
        "node_id": profile.key.node_id,
        "capability_id": profile.key.capability_id,
        "environment_fingerprint": profile.key.environment_fingerprint,
        "observations": profile.observation_count,
        "evidence": round(estimate.evidence, 3),
        "success_probability": round(estimate.success_probability, 4),
        "expected_total_millis": (
            None
            if estimate.expected_total_millis is None
            else round(estimate.expected_total_millis, 1)
        ),
        "expected_cost_millis": None if cost is None else round(cost, 1),
        "last_observed_at": (
            None
            if profile.last_observed_at is None
            else profile.last_observed_at.isoformat().replace("+00:00", "Z")
        ),
    }


def preferred_nodes(
    store: SQLiteExecutionLearningStore,
    *,
    now: datetime,
    policy: EfficiencyPolicy | None = None,
    limit: int = DEFAULT_PROFILE_LIMIT,
) -> tuple[dict[str, Any], ...]:
    """Which node the federation currently prefers per workload class.

    This reports what the *evidence* says. The scheduler additionally applies
    hard constraints and bounded exploration, so a preferred node is not a
    guarantee that the next job goes there.
    """

    active = policy or store.policy
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for profile in store.profiles(include_pooled=False):
        if profile.key.environment_fingerprint == ANY:
            continue
        view = _profile_view(profile, now=now, policy=active)
        if view["expected_cost_millis"] is None:
            continue
        grouped.setdefault(
            (profile.key.capability_type, profile.key.workload_class), []
        ).append(view)
    results: list[dict[str, Any]] = []
    for (capability_type, workload_class), views in sorted(grouped.items()):
        ranked = sorted(views, key=lambda item: item["expected_cost_millis"])
        best = ranked[0]
        results.append(
            {
                "capability_type": capability_type,
                "workload_class": workload_class,
                "preferred_node_id": best["node_id"],
                "preferred_capability_id": best["capability_id"],
                "expected_cost_millis": best["expected_cost_millis"],
                "success_probability": best["success_probability"],
                "evidence": best["evidence"],
                "observations": best["observations"],
                "candidates_compared": len(ranked),
            }
        )
    results.sort(key=lambda item: (-item["observations"], item["workload_class"]))
    return tuple(results[: max(1, int(limit))])


def learning_snapshot(
    store: SQLiteExecutionLearningStore | None,
    *,
    now: datetime,
    policy: EfficiencyPolicy | None = None,
    profile_limit: int = DEFAULT_PROFILE_LIMIT,
    decision_limit: int = DEFAULT_DECISION_LIMIT,
) -> dict[str, Any]:
    """Return one bounded snapshot, or an explicit empty one when unavailable."""

    empty: dict[str, Any] = {
        "schema": LEARNING_SNAPSHOT_SCHEMA,
        "available": False,
        "observation_count": 0,
        "nodes": [],
        "preferred": [],
        "profiles": [],
        "recent_decisions": [],
        "policy": (policy or EfficiencyPolicy()).to_dict(),
    }
    if store is None:
        return empty
    try:
        active = policy or store.policy
        profiles = [
            _profile_view(profile, now=now, policy=active)
            for profile in store.profiles(
                include_pooled=False, limit=max(1, int(profile_limit))
            )
            if profile.key.environment_fingerprint != ANY
        ]
        return {
            "schema": LEARNING_SNAPSHOT_SCHEMA,
            "available": True,
            "observation_count": store.observation_count(),
            "nodes": list(store.node_summary()),
            "preferred": list(
                preferred_nodes(
                    store, now=now, policy=active, limit=profile_limit
                )
            ),
            "profiles": profiles,
            "recent_decisions": list(
                store.recent_decisions(limit=max(1, int(decision_limit)))
            ),
            "policy": active.to_dict(),
        }
    except Exception:  # noqa: BLE001 - observability must never break a page
        return empty


__all__ = [
    "DEFAULT_DECISION_LIMIT",
    "DEFAULT_PROFILE_LIMIT",
    "LEARNING_SNAPSHOT_SCHEMA",
    "learning_snapshot",
    "preferred_nodes",
]
