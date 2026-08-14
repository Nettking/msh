"""Durable learning state: persistence, rebuild, decay, retention, corruption."""

from __future__ import annotations

import sqlite3
from datetime import timedelta

import pytest

from catalog.capabilities.efficiency import (
    EfficiencyPolicy,
    EvidenceTier,
    ExecutionEnvironment,
    ExecutionMeasurements,
    FederationObservationIngest,
    ProfileKey,
    SQLiteExecutionLearningStore,
    enforce_observation_authority,
    observation_event_payload,
)
from catalog.federation.errors import (
    AuthorizationError,
    FederationValidationError,
)

from catalog.capabilities.tests.efficiency_harness import (
    CAPABILITY_TYPE,
    NOW,
    SESSION,
    build_observation,
    build_store,
    build_workload,
)

WORKLOAD = build_workload()
EXACT_CLASS = WORKLOAD.class_key("exact")


def _key(node_id: str, *, environment_fingerprint: str | None = None) -> ProfileKey:
    return ProfileKey(
        capability_type=CAPABILITY_TYPE,
        workload_class=EXACT_CLASS,
        node_id=node_id,
        capability_id=f"cap-{node_id}",
        environment_fingerprint=(
            environment_fingerprint
            or ExecutionEnvironment(model_id="qwen3:4b").fingerprint
        ),
    )


def _record(store, node_id, *, offset_hours=0.0, total_millis=1_000, succeeded=True):
    return store.record(
        build_observation(
            node_id=node_id,
            capability_id=f"cap-{node_id}",
            observed_at=NOW + timedelta(hours=offset_hours),
            total_millis=total_millis,
            succeeded=succeeded,
        )
    )


def test_observations_and_profiles_survive_a_restart(tmp_path) -> None:
    store = build_store(tmp_path)
    for index in range(5):
        assert _record(store, "node-a", offset_hours=index, total_millis=2_000)

    reopened = SQLiteExecutionLearningStore(tmp_path / "efficiency.sqlite3")
    assert reopened.observation_count() == 5
    profile = reopened.profile(_key("node-a"))
    assert profile is not None
    assert profile.observation_count == 5
    estimate = profile.estimate(
        NOW + timedelta(hours=5),
        half_life_seconds=reopened.half_life_seconds,
        tier=EvidenceTier.MEASURED_EXACT,
    )
    assert estimate.expected_total_millis == pytest.approx(2_000, rel=1e-6)


def test_repeated_observations_sharpen_the_estimate(tmp_path) -> None:
    store = build_store(tmp_path)
    _record(store, "node-a", offset_hours=0, total_millis=2_000)
    single = store.profile(_key("node-a"))
    assert single is not None
    first_evidence = single.evidence(NOW, half_life_seconds=store.half_life_seconds)

    for index in range(1, 12):
        _record(store, "node-a", offset_hours=index * 0.1, total_millis=2_000)
    many = store.profile(_key("node-a"))
    assert many is not None
    later = NOW + timedelta(hours=1.1)
    assert many.evidence(later, half_life_seconds=store.half_life_seconds) > (
        first_evidence * 5
    )
    assert many.metrics["total_millis"].stddev == pytest.approx(0.0, abs=1e-3)


def test_failures_lower_the_learned_success_probability(tmp_path) -> None:
    store = build_store(tmp_path)
    for index in range(8):
        _record(store, "node-a", offset_hours=index * 0.1, succeeded=True)
    healthy = store.profile(_key("node-a"))
    assert healthy is not None
    before = healthy.success_probability(
        NOW + timedelta(hours=1), half_life_seconds=store.half_life_seconds
    )

    for index in range(8, 16):
        _record(store, "node-a", offset_hours=index * 0.1, succeeded=False)
    degraded = store.profile(_key("node-a"))
    assert degraded is not None
    after = degraded.success_probability(
        NOW + timedelta(hours=2), half_life_seconds=store.half_life_seconds
    )
    assert before > 0.8
    assert after < before
    assert after < 0.65


def test_recent_evidence_outweighs_stale_evidence(tmp_path) -> None:
    policy = EfficiencyPolicy(half_life_seconds=3_600)
    store = build_store(tmp_path, policy=policy)
    for index in range(20):
        _record(store, "node-a", offset_hours=index * 0.01, total_millis=1_000)
    for index in range(5):
        _record(
            store,
            "node-a",
            offset_hours=48 + index * 0.01,
            total_millis=9_000,
        )
    profile = store.profile(_key("node-a"))
    assert profile is not None
    # Twenty stale fast runs must not outvote five recent slow ones.
    assert profile.metrics["total_millis"].mean > 8_000


def test_rebuild_reproduces_profiles_from_raw_observations(tmp_path) -> None:
    store = build_store(tmp_path)
    for index in range(6):
        _record(store, "node-a", offset_hours=index, total_millis=1_500 + index)
        _record(store, "node-b", offset_hours=index, total_millis=4_000)
    original = {
        profile.key.canonical(): profile.to_dict() for profile in store.profiles()
    }
    assert original

    count = store.rebuild_profiles()
    rebuilt = {
        profile.key.canonical(): profile.to_dict() for profile in store.profiles()
    }
    assert count == len(rebuilt)
    assert rebuilt == original


def test_a_replayed_execution_is_not_counted_twice(tmp_path) -> None:
    store = build_store(tmp_path)
    observation = build_observation(
        node_id="node-a",
        capability_id="cap-node-a",
        observed_at=NOW,
        job_id="job-replay",
    )
    assert store.record(observation) is True
    assert store.record(observation) is False
    assert store.observation_count() == 1
    profile = store.profile(_key("node-a"))
    assert profile is not None and profile.observation_count == 1


def test_retention_bounds_the_observation_history(tmp_path) -> None:
    store = build_store(tmp_path, max_observations=5)
    for index in range(12):
        _record(store, "node-a", offset_hours=index)
    assert store.observation_count() == 5


def test_a_corrupt_profile_row_reads_as_missing_and_is_repairable(tmp_path) -> None:
    store = build_store(tmp_path)
    for index in range(3):
        _record(store, "node-a", offset_hours=index)
    key = _key("node-a")
    assert store.profile(key) is not None

    with sqlite3.connect(tmp_path / "efficiency.sqlite3") as connection:
        connection.execute(
            "UPDATE capability_execution_profiles SET profile_json='{}' "
            "WHERE profile_key=?",
            (key.canonical(),),
        )
    assert store.profile(key) is None

    store.rebuild_profiles()
    repaired = store.profile(key)
    assert repaired is not None and repaired.observation_count == 3


def test_expiring_stale_observations_forgets_them(tmp_path) -> None:
    policy = EfficiencyPolicy(max_observation_age_seconds=3_600)
    store = build_store(tmp_path, policy=policy)
    _record(store, "node-a", offset_hours=0)
    _record(store, "node-a", offset_hours=10)
    removed = store.expire_stale_observations(now=NOW + timedelta(hours=10))
    assert removed == 1
    assert store.observation_count() == 1


def test_partial_measurements_still_produce_a_profile(tmp_path) -> None:
    store = build_store(tmp_path)
    store.record(
        build_observation(
            node_id="node-a",
            capability_id="cap-node-a",
            observed_at=NOW,
            measurements=ExecutionMeasurements(succeeded=True, execution_millis=750),
        )
    )
    profile = store.profile(_key("node-a"))
    assert profile is not None
    assert "total_millis" in profile.metrics
    assert "memory_peak_bytes" not in profile.metrics


# ----------------------------------------------------------------------
# federation ingest
# ----------------------------------------------------------------------


def test_a_member_may_report_only_its_own_executions(tmp_path) -> None:
    store = build_store(tmp_path)
    ingest = FederationObservationIngest(store)
    payload = observation_event_payload(
        build_observation(
            node_id="node-a", capability_id="cap-node-a", observed_at=NOW
        )
    )

    outcome = ingest.apply(
        actor_node_id="node-a", session_id=SESSION, payload=payload, now=NOW
    )
    assert outcome.accepted and outcome.reason == "recorded"

    with pytest.raises(AuthorizationError):
        ingest.apply(
            actor_node_id="node-b", session_id=SESSION, payload=payload, now=NOW
        )
    assert store.observation_count() == 1


def test_a_forged_node_id_inside_the_observation_is_rejected(tmp_path) -> None:
    payload = observation_event_payload(
        build_observation(
            node_id="node-a", capability_id="cap-node-a", observed_at=NOW
        )
    )
    payload["observation"]["node_id"] = "node-b"
    with pytest.raises(AuthorizationError):
        enforce_observation_authority(actor_node_id="node-a", payload=payload)


def test_ingest_rejects_cross_session_and_implausible_timestamps(tmp_path) -> None:
    store = build_store(tmp_path)
    ingest = FederationObservationIngest(store)
    payload = observation_event_payload(
        build_observation(
            node_id="node-a", capability_id="cap-node-a", observed_at=NOW
        )
    )

    with pytest.raises(AuthorizationError):
        ingest.apply(
            actor_node_id="node-a",
            session_id="other-session",
            payload=payload,
            now=NOW,
        )

    future = ingest.apply(
        actor_node_id="node-a",
        session_id=SESSION,
        payload=payload,
        now=NOW - timedelta(hours=1),
    )
    assert not future.accepted and future.reason == "observation-from-future"

    stale = ingest.apply(
        actor_node_id="node-a",
        session_id=SESSION,
        payload=payload,
        now=NOW + timedelta(days=400),
    )
    assert not stale.accepted and stale.reason == "observation-too-old"
    assert store.observation_count() == 0


def test_ingest_rejects_a_malformed_payload(tmp_path) -> None:
    store = build_store(tmp_path)
    ingest = FederationObservationIngest(store)
    with pytest.raises(FederationValidationError):
        ingest.apply(
            actor_node_id="node-a",
            session_id=SESSION,
            payload={"schema": "fcp.execution-observation.v1"},
            now=NOW,
        )
