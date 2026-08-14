"""Learned ranking: cold start, hard constraints, exploration and fallback."""

from __future__ import annotations

from datetime import timedelta

from catalog.capabilities.efficiency import (
    EfficiencyPolicy,
    EvidenceTier,
    ExecutionEnvironment,
    LearnedProviderRanker,
    PerformanceEstimator,
    SchedulingObjective,
    default_workload_descriptor,
    exploration_slot,
    learning_snapshot,
    preferred_nodes,
)
from catalog.capabilities.efficiency.ranking import (
    REASON_EVIDENCE,
    REASON_EXPLORATION,
)
from catalog.capabilities.provider_reports import (
    ProviderSelectionPolicy,
    ProviderStatus,
)
from catalog.capabilities.provider_selection import select_provider

from catalog.capabilities.tests.efficiency_harness import (
    NOW,
    build_job,
    build_observation,
    build_report,
    build_store,
)

DETERMINISTIC = EfficiencyPolicy(exploration_ratio=0.0)
ENVIRONMENT = ExecutionEnvironment(model_id="qwen3:4b")


def _ranker(store, *, policy=None, sink=None):
    active = policy or store.policy
    estimator = PerformanceEstimator(store, policy=active)
    return LearnedProviderRanker(estimator, policy=active, decision_sink=sink)


def _reports(*nodes, now=NOW, **kwargs):
    """Reports are short-lived, so they are always minted for the instant
    the selection under test is evaluated at."""

    return tuple(
        build_report(capability_id=f"cap-{node}", node_id=node, now=now, **kwargs)
        for node in nodes
    )


def _teach(store, node, *, count, total_millis, succeeded=True, job=None, start=0.0):
    workload = default_workload_descriptor(job or build_job())
    for index in range(count):
        store.record(
            build_observation(
                node_id=node,
                capability_id=f"cap-{node}",
                observed_at=NOW + timedelta(minutes=start + index),
                total_millis=total_millis,
                succeeded=succeeded,
                workload=workload,
                environment=ENVIRONMENT,
                job_id=f"job-{node}-{int(start)}-{index}",
            )
        )


def test_cold_start_keeps_the_existing_deterministic_order(tmp_path) -> None:
    store = build_store(tmp_path)
    job = build_job()
    reports = _reports("node-b", "node-a")

    baseline = select_provider(job, reports, evaluated_at=NOW)
    learned = select_provider(
        job, reports, evaluated_at=NOW, ranker=_ranker(store)
    )
    assert learned.selected_capability_id == baseline.selected_capability_id
    assert learned.ranking is None
    assert learned.reason == "highest-ranked-eligible-provider"


def test_a_clearly_better_node_becomes_preferred(tmp_path) -> None:
    store = build_store(tmp_path, policy=DETERMINISTIC)
    job = build_job()
    _teach(store, "node-a", count=20, total_millis=8_000)
    _teach(store, "node-b", count=20, total_millis=2_000)

    later = NOW + timedelta(minutes=30)
    selection = select_provider(
        job,
        _reports("node-a", "node-b", now=later),
        evaluated_at=later,
        ranker=_ranker(store, policy=DETERMINISTIC),
    )
    assert selection.selected_node_id == "node-b"
    assert selection.reason == REASON_EVIDENCE
    assert selection.ranking is not None
    # node-a is lexicographically first, so the base scheduler would pick it.
    assert (
        select_provider(
            job, _reports("node-a", "node-b"), evaluated_at=NOW
        ).selected_node_id
        == "node-a"
    )


def test_incompatible_nodes_are_never_selected_however_fast(tmp_path) -> None:
    store = build_store(tmp_path, policy=DETERMINISTIC)
    job = build_job()
    _teach(store, "node-fast", count=30, total_millis=100)
    _teach(store, "node-slow", count=30, total_millis=9_000)

    later = NOW + timedelta(minutes=40)
    reports = (
        build_report(
            capability_id="cap-node-fast",
            node_id="node-fast",
            now=later,
            status=ProviderStatus.DRAINING,
        ),
        build_report(
            capability_id="cap-node-slow", node_id="node-slow", now=later
        ),
    )
    selection = select_provider(
        job,
        reports,
        evaluated_at=later,
        ranker=_ranker(store, policy=DETERMINISTIC),
    )
    assert selection.selected_node_id == "node-slow"
    assert "cap-node-fast" in selection.rejected
    assert selection.rejected["cap-node-fast"] == ("provider-status-draining",)


def test_capability_requirements_still_gate_a_learned_favourite(tmp_path) -> None:
    store = build_store(tmp_path, policy=DETERMINISTIC)
    job = build_job()
    _teach(store, "node-fast", count=30, total_millis=100)
    _teach(store, "node-slow", count=30, total_millis=9_000)

    later = NOW + timedelta(minutes=40)
    reports = (
        build_report(
            capability_id="cap-node-fast",
            node_id="node-fast",
            now=later,
            attributes={"model": "other-model", "job_kind": "summarize"},
        ),
        build_report(
            capability_id="cap-node-slow", node_id="node-slow", now=later
        ),
    )
    selection = select_provider(
        job,
        reports,
        evaluated_at=later,
        ranker=_ranker(store, policy=DETERMINISTIC),
    )
    assert selection.selected_node_id == "node-slow"
    assert "requirement-mismatch:requirements.model" in selection.rejected[
        "cap-node-fast"
    ]


def test_failures_move_preference_away_from_a_fast_but_unreliable_node(
    tmp_path,
) -> None:
    store = build_store(tmp_path, policy=DETERMINISTIC)
    job = build_job()
    _teach(store, "node-a", count=20, total_millis=1_000, succeeded=False)
    _teach(store, "node-b", count=20, total_millis=3_000, succeeded=True)

    later = NOW + timedelta(minutes=30)
    selection = select_provider(
        job,
        _reports("node-a", "node-b", now=later),
        evaluated_at=later,
        ranker=_ranker(store, policy=DETERMINISTIC),
    )
    # node-a is three times faster per run but almost always fails, so its
    # failure-aware expected cost is far worse.
    assert selection.selected_node_id == "node-b"


def test_recent_regression_overturns_a_stale_preference(tmp_path) -> None:
    policy = EfficiencyPolicy(exploration_ratio=0.0, half_life_seconds=1_800)
    store = build_store(tmp_path, policy=policy)
    job = build_job()
    _teach(store, "node-a", count=25, total_millis=1_000, start=0)
    _teach(store, "node-b", count=25, total_millis=4_000, start=0)

    early_at = NOW + timedelta(minutes=30)
    early = select_provider(
        job,
        _reports("node-a", "node-b", now=early_at),
        evaluated_at=early_at,
        ranker=_ranker(store, policy=policy),
    )
    assert early.selected_node_id == "node-a"

    # node-a starts thermally throttling a day later.
    _teach(store, "node-a", count=12, total_millis=12_000, start=1_440)
    _teach(store, "node-b", count=12, total_millis=4_000, start=1_440)
    later_at = NOW + timedelta(minutes=1_500)
    later = select_provider(
        job,
        _reports("node-a", "node-b", now=later_at),
        evaluated_at=later_at,
        ranker=_ranker(store, policy=policy),
    )
    assert later.selected_node_id == "node-b"


def test_a_version_change_does_not_reuse_old_assumptions_at_full_weight(
    tmp_path,
) -> None:
    store = build_store(tmp_path, policy=DETERMINISTIC)
    job = build_job()
    _teach(store, "node-a", count=30, total_millis=500)
    estimator = PerformanceEstimator(store, policy=DETERMINISTIC)
    workload = default_workload_descriptor(job)
    later = NOW + timedelta(minutes=45)

    same_version = estimator.estimate(
        workload=workload,
        node_id="node-a",
        capability_id="cap-node-a",
        environment_fingerprint=ENVIRONMENT.fingerprint,
        now=later,
    )
    upgraded = estimator.estimate(
        workload=workload,
        node_id="node-a",
        capability_id="cap-node-a",
        environment_fingerprint=ExecutionEnvironment(
            model_id="qwen3:4b", software_version="2.0.0"
        ).fingerprint,
        now=later,
    )
    assert same_version.tier is EvidenceTier.MEASURED_EXACT
    assert upgraded.tier is EvidenceTier.MEASURED_LEGACY_ENVIRONMENT
    assert upgraded.evidence < same_version.evidence * 0.5

    strict = EfficiencyPolicy(
        exploration_ratio=0.0, reuse_cross_environment_evidence=False
    )
    strict_estimate = PerformanceEstimator(store, policy=strict).estimate(
        workload=workload,
        node_id="node-a",
        capability_id="cap-node-a",
        environment_fingerprint=ExecutionEnvironment(
            model_id="qwen3:4b", software_version="2.0.0"
        ).fingerprint,
        now=later,
    )
    assert strict_estimate.tier is EvidenceTier.FEDERATION_PRIOR


def test_a_new_node_receives_bounded_exploration(tmp_path) -> None:
    policy = EfficiencyPolicy(exploration_ratio=0.25, exploration_bonus=1.0)
    store = build_store(tmp_path, policy=policy)
    _teach(store, "node-a", count=40, total_millis=1_000)

    ranker = _ranker(store, policy=policy)
    later = NOW + timedelta(minutes=60)
    reports = _reports("node-a", "node-new", now=later)
    probes = 0
    for index in range(200):
        job = build_job(job_id=f"job-{index:04d}")
        selection = select_provider(
            job,
            reports,
            evaluated_at=later,
            ranker=ranker,
        )
        if selection.selected_node_id == "node-new":
            probes += 1
    # The unmeasured node must be tried, but only inside the configured budget.
    assert probes > 0
    assert probes <= int(200 * policy.exploration_ratio) + 1


def test_exploration_is_disabled_and_deterministic_at_ratio_zero(tmp_path) -> None:
    store = build_store(tmp_path, policy=DETERMINISTIC)
    _teach(store, "node-a", count=40, total_millis=1_000)
    ranker = _ranker(store, policy=DETERMINISTIC)
    later = NOW + timedelta(minutes=60)
    reports = _reports("node-a", "node-new", now=later)

    chosen = set()
    for index in range(100):
        job = build_job(job_id=f"job-{index:04d}")
        assert not exploration_slot(job.job_id, policy=DETERMINISTIC)
        selection = select_provider(
            job, reports, evaluated_at=later, ranker=ranker
        )
        chosen.add(selection.selected_node_id)
        assert selection.reason == REASON_EVIDENCE
    assert chosen == {"node-a"}


def test_exploration_marks_the_decision_it_changed(tmp_path) -> None:
    policy = EfficiencyPolicy(exploration_ratio=0.5, exploration_bonus=2.0)
    store = build_store(tmp_path, policy=policy)
    _teach(store, "node-a", count=40, total_millis=1_000)
    ranker = _ranker(store, policy=policy)
    later = NOW + timedelta(minutes=60)
    reports = _reports("node-a", "node-new", now=later)

    reasons = set()
    for index in range(50):
        selection = select_provider(
            build_job(job_id=f"job-{index:04d}"),
            reports,
            evaluated_at=later,
            ranker=ranker,
        )
        reasons.add(selection.reason)
        if selection.reason == REASON_EXPLORATION:
            assert selection.ranking.details["exploration"] is True
            assert selection.ranking.details["exploration_changed_selection"] is True
    assert reasons == {REASON_EVIDENCE, REASON_EXPLORATION}


def test_an_operator_node_preference_outranks_learned_evidence(tmp_path) -> None:
    store = build_store(tmp_path, policy=DETERMINISTIC)
    job = build_job()
    _teach(store, "node-a", count=20, total_millis=9_000)
    _teach(store, "node-b", count=20, total_millis=1_000)

    later = NOW + timedelta(minutes=30)
    selection = select_provider(
        job,
        _reports("node-a", "node-b", now=later),
        evaluated_at=later,
        policy=ProviderSelectionPolicy(preferred_node_id="node-a"),
        ranker=_ranker(store, policy=DETERMINISTIC),
    )
    assert selection.selected_node_id == "node-a"
    assert selection.reason == "preferred-node-provider-selected"


def test_a_broken_ranker_falls_back_to_existing_behaviour(tmp_path) -> None:
    job = build_job()
    reports = _reports("node-a", "node-b")
    baseline = select_provider(job, reports, evaluated_at=NOW)

    class Raising:
        def rank(self, job, candidates, *, evaluated_at):
            raise RuntimeError("learned state is unavailable")

    class Forging:
        def rank(self, job, candidates, *, evaluated_at):
            from catalog.capabilities.provider_selection import CandidateRanking

            return CandidateRanking(
                ordered_capability_ids=("cap-not-a-candidate",),
                reason="forged",
            )

    for ranker in (Raising(), Forging(), None):
        selection = select_provider(
            job, reports, evaluated_at=NOW, ranker=ranker
        )
        assert selection.selected_capability_id == baseline.selected_capability_id
        assert selection.ranking is None


def test_a_decision_records_candidates_predictions_and_rejections(
    tmp_path,
) -> None:
    store = build_store(tmp_path, policy=DETERMINISTIC)
    job = build_job()
    _teach(store, "node-a", count=18, total_millis=4_200)
    _teach(store, "node-b", count=18, total_millis=7_800)

    ranker = _ranker(store, policy=DETERMINISTIC, sink=store.record_decision)
    later = NOW + timedelta(minutes=30)
    reports = (
        *_reports("node-a", "node-b", now=later),
        build_report(
            capability_id="cap-node-c",
            node_id="node-c",
            now=later,
            status=ProviderStatus.UNAVAILABLE,
        ),
    )
    selection = select_provider(
        job,
        reports,
        evaluated_at=later,
        ranker=ranker,
    )
    details = selection.ranking.details
    assert details["selected_node_id"] == "node-a"
    assert details["objective"] == SchedulingObjective.FASTEST_COMPLETION.value
    assert [item["node_id"] for item in details["candidates"]] == [
        "node-a",
        "node-b",
    ]
    assert details["candidates"][0]["observation_count"] == 18
    assert details["candidates"][0]["tier"] == EvidenceTier.MEASURED_EXACT.value
    assert details["rejected"]["cap-node-c"] == ["provider-status-unavailable"]
    assert "4.2 s" in details["explanation"] and "7.8 s" in details["explanation"]

    stored = store.recent_decisions(limit=5)
    assert stored and stored[0]["job_id"] == job.job_id


def test_the_projection_reports_learning_and_preferences(tmp_path) -> None:
    store = build_store(tmp_path, policy=DETERMINISTIC)
    _teach(store, "node-a", count=6, total_millis=4_000)
    _teach(store, "node-b", count=6, total_millis=1_000)

    now = NOW + timedelta(minutes=30)
    snapshot = learning_snapshot(store, now=now, policy=DETERMINISTIC)
    assert snapshot["available"] is True
    assert snapshot["observation_count"] == 12
    assert {item["node_id"] for item in snapshot["nodes"]} == {"node-a", "node-b"}

    preferred = preferred_nodes(store, now=now, policy=DETERMINISTIC)
    assert preferred
    assert preferred[0]["preferred_node_id"] == "node-b"
    assert preferred[0]["candidates_compared"] == 2

    assert learning_snapshot(None, now=now)["available"] is False
