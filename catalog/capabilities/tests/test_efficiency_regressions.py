"""Regression coverage for execution-efficiency consistency and final selection."""

from __future__ import annotations

from datetime import timedelta

from catalog.capabilities.efficiency import (
    DecayedStatistic,
    EfficiencyPolicy,
    ExecutionEnvironment,
    LearnedProviderRanker,
    PerformanceEstimator,
    ProfileKey,
    default_workload_descriptor,
)
from catalog.capabilities.provider_reports import ProviderSelectionPolicy
from catalog.capabilities.provider_selection import select_provider
from catalog.capabilities.tests.efficiency_harness import (
    CAPABILITY_TYPE,
    NOW,
    build_job,
    build_observation,
    build_report,
    build_store,
)


def _profile_key(node_id: str) -> ProfileKey:
    descriptor = default_workload_descriptor(build_job())
    return ProfileKey(
        capability_type=CAPABILITY_TYPE,
        workload_class=descriptor.class_key("exact"),
        node_id=node_id,
        capability_id=f"cap-{node_id}",
        environment_fingerprint=ExecutionEnvironment(model_id="qwen3:4b").fingerprint,
    )


def _teach(store, node_id: str, *, total_millis: int, count: int = 8) -> None:
    workload = default_workload_descriptor(build_job())
    environment = ExecutionEnvironment(model_id="qwen3:4b")
    for index in range(count):
        store.record(
            build_observation(
                node_id=node_id,
                capability_id=f"cap-{node_id}",
                observed_at=NOW + timedelta(minutes=index),
                total_millis=total_millis,
                workload=workload,
                environment=environment,
                job_id=f"job-{node_id}-{index}",
            )
        )


def test_persisted_decision_matches_operator_preferred_final_selection(tmp_path) -> None:
    policy = EfficiencyPolicy(exploration_ratio=0.0)
    store = build_store(tmp_path, policy=policy)
    _teach(store, "node-a", total_millis=9_000)
    _teach(store, "node-b", total_millis=1_000)
    ranker = LearnedProviderRanker(
        PerformanceEstimator(store, policy=policy),
        policy=policy,
        decision_sink=store.record_decision,
    )
    evaluated_at = NOW + timedelta(minutes=20)
    selection = select_provider(
        build_job(),
        (
            build_report(
                capability_id="cap-node-a", node_id="node-a", now=evaluated_at
            ),
            build_report(
                capability_id="cap-node-b", node_id="node-b", now=evaluated_at
            ),
        ),
        evaluated_at=evaluated_at,
        policy=ProviderSelectionPolicy(preferred_node_id="node-a"),
        ranker=ranker,
    )

    assert selection.selected_node_id == "node-a"
    assert selection.ranking is not None
    assert selection.ranking.details["selected_node_id"] == "node-a"
    assert selection.ranking.details["operator_preference_applied"] is True
    assert selection.ranking.details["candidates"][0]["selected"] is True
    stored = store.recent_decisions(limit=1)
    assert stored[0]["selected_node_id"] == "node-a"
    assert stored[0]["operator_preference_applied"] is True


def test_decayed_statistic_is_independent_of_arrival_order() -> None:
    half_life = 60 * 60
    chronological = DecayedStatistic()
    chronological = chronological.update(1.0, at=NOW, half_life_seconds=half_life)
    chronological = chronological.update(
        7.0, at=NOW + timedelta(hours=1), half_life_seconds=half_life
    )
    chronological = chronological.update(
        3.0, at=NOW + timedelta(hours=2), half_life_seconds=half_life
    )

    delayed = DecayedStatistic()
    delayed = delayed.update(1.0, at=NOW, half_life_seconds=half_life)
    delayed = delayed.update(
        3.0, at=NOW + timedelta(hours=2), half_life_seconds=half_life
    )
    delayed = delayed.update(
        7.0, at=NOW + timedelta(hours=1), half_life_seconds=half_life
    )

    assert delayed.last_at == chronological.last_at
    assert delayed.weight == chronological.weight
    assert delayed.mean == chronological.mean
    assert delayed.mean_square == chronological.mean_square


def test_out_of_order_delivery_matches_chronological_rebuild(tmp_path) -> None:
    chronological = build_store(tmp_path, name="chronological.sqlite3")
    delayed = build_store(tmp_path, name="delayed.sqlite3")
    values = (
        (NOW, 1_000, "job-early"),
        (NOW + timedelta(hours=1), 7_000, "job-middle"),
        (NOW + timedelta(hours=2), 3_000, "job-late"),
    )

    observations = [
        build_observation(
            node_id="node-a",
            capability_id="cap-node-a",
            observed_at=observed_at,
            total_millis=total,
            job_id=job_id,
        )
        for observed_at, total, job_id in values
    ]
    for observation in observations:
        chronological.record(observation)
    for observation in (observations[0], observations[2], observations[1]):
        delayed.record(observation)

    expected = {
        profile.key.canonical(): profile.to_dict()
        for profile in chronological.profiles()
    }
    actual = {
        profile.key.canonical(): profile.to_dict() for profile in delayed.profiles()
    }
    assert actual == expected

    delayed.rebuild_profiles()
    rebuilt = {
        profile.key.canonical(): profile.to_dict() for profile in delayed.profiles()
    }
    assert rebuilt == expected


def test_retention_pruning_removes_samples_from_derived_profiles(tmp_path) -> None:
    store = build_store(tmp_path, max_observations=5)
    for index in range(12):
        store.record(
            build_observation(
                node_id="node-a",
                capability_id="cap-node-a",
                observed_at=NOW + timedelta(minutes=index),
                total_millis=1_000 + index,
                job_id=f"job-{index}",
            )
        )

    assert store.observation_count() == 5
    profile = store.profile(_profile_key("node-a"))
    assert profile is not None
    assert profile.observation_count == 5
    before = {item.key.canonical(): item.to_dict() for item in store.profiles()}
    store.rebuild_profiles()
    after = {item.key.canonical(): item.to_dict() for item in store.profiles()}
    assert after == before
