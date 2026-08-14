"""End-to-end demonstration that the federation actually learns.

Four heterogeneous simulated nodes each win a different workload class, and no
node wins them all:

* ``gpu-node`` is excellent on long-context work but its startup overhead
  dominates tiny jobs,
* ``cpu-node`` is the best analytical worker and poor at long generation,
* ``balanced-node`` is unremarkable everywhere and best only for tiny jobs,
* ``flaky-node`` looks quick per attempt but fails half the time.

The scheduler is told none of this. It starts with the repository's existing
deterministic ordering, gathers evidence from real (simulated) executions, and
is measured on the wall-clock cost it actually incurs.
"""

from __future__ import annotations

import math
import random
from datetime import timedelta

from catalog.capabilities.efficiency import (
    EfficiencyPolicy,
    ExecutionEnvironment,
    ExecutionMeasurements,
    LearnedProviderRanker,
    PerformanceEstimator,
    preferred_nodes,
)
from catalog.capabilities.jobs import (
    ArtifactReference,
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.provider_selection import select_provider

from catalog.capabilities.tests.efficiency_harness import (
    CAPABILITY_TYPE,
    NOW,
    PROTOCOL,
    PROTOCOL_VERSION,
    SESSION,
    build_observation,
    build_report,
    build_store,
)

SEED = 20_260_814
JOB_COUNT = 600

#: workload -> (job kind, input size in bytes)
WORKLOADS = {
    "tiny": ("classify", 2_000),
    "long": ("summarize", 500_000),
    "analysis": ("analyze", 4_000_000),
}

#: node -> workload -> mean milliseconds. Ground truth the scheduler cannot see.
TRUE_MEANS = {
    "gpu-node": {"tiny": 3_000, "long": 4_000, "analysis": 12_000},
    "cpu-node": {"tiny": 1_200, "long": 20_000, "analysis": 2_500},
    "balanced-node": {"tiny": 900, "long": 12_000, "analysis": 6_000},
    "flaky-node": {"tiny": 800, "long": 6_000, "analysis": 3_000},
}
TRUE_SUCCESS = {
    "gpu-node": 0.98,
    "cpu-node": 0.98,
    "balanced-node": 0.97,
    "flaky-node": 0.50,
}
NODES = tuple(sorted(TRUE_MEANS))

#: The node a perfectly informed scheduler would choose per workload.
BEST_NODE = {"tiny": "balanced-node", "long": "gpu-node", "analysis": "cpu-node"}

ENVIRONMENT = ExecutionEnvironment(model_id="qwen3:4b")


def _job(index: int, workload: str) -> JobContract:
    job_kind, size = WORKLOADS[workload]
    job_id = f"sim-job-{index:05d}"
    return JobContract(
        job_id=job_id,
        session_id=SESSION,
        request_id=f"request-{job_id}",
        idempotency_key=f"{SESSION}:{job_id}",
        capability=CapabilityRequirement(
            capability_type=CAPABILITY_TYPE,
            protocol=PROTOCOL,
            protocol_version=PROTOCOL_VERSION,
            requirements={"model": "qwen3:4b", "job_kind": job_kind},
        ),
        inputs=(
            ArtifactReference(
                reference_id=f"{job_id}-input",
                session_id=SESSION,
                schema_name="fcp.prompt.v1",
                media_type="application/json",
                content_hash="sha256:" + f"{index:064x}",
                size_bytes=size,
            ),
        ),
        outputs=(),
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=(1,),
            retryable_error_codes=("worker-handler-failed",),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=3_600,
            queue_timeout_seconds=600,
            start_timeout_seconds=600,
            run_timeout_seconds=3_600,
            cancellation_grace_seconds=5,
        ),
    )


def _attempt(rng: random.Random, node: str, workload: str) -> tuple[int, bool]:
    """Sample one execution: lognormal duration plus a reliability draw."""

    mean = TRUE_MEANS[node][workload]
    duration = int(mean * math.exp(rng.gauss(0.0, 0.2)))
    return max(1, duration), rng.random() < TRUE_SUCCESS[node]


def _run(*, ranker, store=None) -> tuple[int, list[tuple[str, str]]]:
    """Execute the same job sequence and return wall-clock cost and choices.

    A failed attempt is retried once on the same node, which is what the
    existing retry policy would do, so unreliability shows up as real cost.
    """

    rng = random.Random(SEED)
    workload_rng = random.Random(SEED + 1)
    total_millis = 0
    choices: list[tuple[str, str]] = []
    for index in range(JOB_COUNT):
        workload = workload_rng.choice(list(WORKLOADS))
        moment = NOW + timedelta(minutes=index)
        job = _job(index, workload)
        reports = tuple(
            build_report(
                capability_id=f"cap-{node}",
                node_id=node,
                now=moment,
                attributes=dict(job.capability.requirements),
            )
            for node in NODES
        )
        selection = select_provider(
            job, reports, evaluated_at=moment, ranker=ranker
        )
        node = selection.selected_node_id
        assert node is not None
        choices.append((workload, node))

        duration, succeeded = _attempt(rng, node, workload)
        total_millis += duration
        if not succeeded:
            retry_duration, _retry_ok = _attempt(rng, node, workload)
            total_millis += retry_duration
        if store is not None:
            store.record(
                build_observation(
                    node_id=node,
                    capability_id=f"cap-{node}",
                    observed_at=moment,
                    job_id=job.job_id,
                    environment=ENVIRONMENT,
                    workload=_descriptor(job),
                    measurements=ExecutionMeasurements(
                        succeeded=succeeded,
                        error_code=None if succeeded else "worker-handler-failed",
                        execution_millis=duration,
                        total_millis=duration,
                    ),
                )
            )
    return total_millis, choices


def _descriptor(job):
    from catalog.capabilities.efficiency import default_workload_descriptor

    return default_workload_descriptor(job)


def _share_on_best(choices: list[tuple[str, str]]) -> float:
    if not choices:
        return 0.0
    hits = sum(1 for workload, node in choices if node == BEST_NODE[workload])
    return hits / len(choices)


def test_the_federation_learns_where_to_send_each_workload(tmp_path) -> None:
    policy = EfficiencyPolicy(exploration_ratio=0.1, exploration_bonus=0.7)
    store = build_store(tmp_path, policy=policy)
    estimator = PerformanceEstimator(store, policy=policy)
    ranker = LearnedProviderRanker(
        estimator, policy=policy, decision_sink=store.record_decision
    )

    baseline_millis, baseline_choices = _run(ranker=None)
    learned_millis, learned_choices = _run(ranker=ranker, store=store)

    # 1. The default scheduler is workload-blind: one node takes everything.
    assert len(set(node for _workload, node in baseline_choices)) == 1

    # 2. It starts out no better than the default and ends up much better.
    early = _share_on_best(learned_choices[:50])
    late = _share_on_best(learned_choices[-150:])
    assert early < 0.55
    assert late > 0.75
    assert late > early + 0.3

    # 3. The cost the federation actually paid drops substantially.
    assert learned_millis < baseline_millis * 0.7

    # 4. And it lands close to a perfectly informed scheduler.
    oracle_millis = sum(
        TRUE_MEANS[BEST_NODE[workload]][workload]
        for workload, _node in learned_choices
    )
    assert learned_millis < oracle_millis * 1.45

    # 5. The learned preference matches the ground truth per workload class.
    preferences = {
        entry["workload_class"]: entry["preferred_node_id"]
        for entry in preferred_nodes(
            store, now=NOW + timedelta(minutes=JOB_COUNT), policy=policy
        )
    }
    for workload, (job_kind, _size) in WORKLOADS.items():
        matching = [
            node
            for workload_class, node in preferences.items()
            if f"/{job_kind}/" in workload_class
        ]
        assert matching, f"no learned preference for {job_kind}"
        assert matching[0] == BEST_NODE[workload]

    # 6. The unreliable node never becomes anyone's preference.
    assert "flaky-node" not in set(preferences.values())

    # 7. Learning is visible and inspectable rather than implicit.
    assert store.observation_count() == JOB_COUNT
    decisions = store.recent_decisions(limit=5)
    assert decisions and decisions[0]["candidates"]


def test_learning_survives_a_restart_and_keeps_its_preferences(tmp_path) -> None:
    policy = EfficiencyPolicy(exploration_ratio=0.0)
    store = build_store(tmp_path, policy=policy)
    for index in range(40):
        moment = NOW + timedelta(minutes=index)
        for node in NODES:
            store.record(
                build_observation(
                    node_id=node,
                    capability_id=f"cap-{node}",
                    observed_at=moment,
                    job_id=f"warm-{node}-{index}",
                    total_millis=TRUE_MEANS[node]["long"],
                    workload=_descriptor(_job(index, "long")),
                    environment=ENVIRONMENT,
                )
            )

    job = _job(9_999, "long")
    moment = NOW + timedelta(minutes=45)
    reports = tuple(
        build_report(
            capability_id=f"cap-{node}",
            node_id=node,
            now=moment,
            attributes=dict(job.capability.requirements),
        )
        for node in NODES
    )

    def _select(active_store):
        estimator = PerformanceEstimator(active_store, policy=policy)
        ranker = LearnedProviderRanker(estimator, policy=policy)
        return select_provider(
            job, reports, evaluated_at=moment, ranker=ranker
        ).selected_node_id

    before = _select(store)
    reopened = build_store(tmp_path, policy=policy)
    assert _select(reopened) == before == BEST_NODE["long"]
