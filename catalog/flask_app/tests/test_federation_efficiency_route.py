"""The read-only Federation efficiency-learning surface."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from catalog.capabilities.efficiency import (
    EfficiencyPolicy,
    ExecutionEnvironment,
    ExecutionMeasurements,
    ExecutionObservation,
    SQLiteExecutionLearningStore,
    WorkloadDescriptor,
    observation_id_for,
)
from catalog.flask_app.app import create_app
from catalog.flask_app.auth.policy import permission_for
from catalog.flask_app.federation_routes import EFFICIENCY_STORE_CONFIG_KEY

NOW = datetime(2026, 8, 14, 9, 0, tzinfo=timezone.utc)
SESSION = "session-efficiency"


def _observation(node_id: str, index: int, total_millis: int) -> ExecutionObservation:
    job_id = f"job-{node_id}-{index}"
    return ExecutionObservation(
        observation_id=observation_id_for(
            session_id=SESSION, job_id=job_id, attempt_id="attempt-1"
        ),
        session_id=SESSION,
        job_id=job_id,
        attempt_id="attempt-1",
        node_id=node_id,
        capability_id=f"cap-{node_id}",
        workload=WorkloadDescriptor(
            capability_type="language-model", job_kind="summarize"
        ),
        environment=ExecutionEnvironment(model_id="qwen3:4b"),
        measurements=ExecutionMeasurements(
            succeeded=True, total_millis=total_millis, execution_millis=total_millis
        ),
        observed_at=NOW + timedelta(minutes=index),
    )


def test_the_route_reports_an_unavailable_snapshot_without_a_store(tmp_path) -> None:
    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    response = client.get("/federation/efficiency.json")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["available"] is False
    assert payload["observation_count"] == 0
    assert response.headers["Cache-Control"] == "no-store"


def test_the_route_reports_learned_preferences(tmp_path) -> None:
    store = SQLiteExecutionLearningStore(
        tmp_path / "efficiency.sqlite3",
        policy=EfficiencyPolicy(exploration_ratio=0.0),
    )
    for index in range(5):
        store.record(_observation("node-a", index, 6_000))
        store.record(_observation("node-b", index, 1_500))

    app = create_app()
    app.config.update(TESTING=True)
    app.config[EFFICIENCY_STORE_CONFIG_KEY] = store
    client = app.test_client()

    payload = client.get("/federation/efficiency.json").get_json()
    assert payload["available"] is True
    assert payload["observation_count"] == 10
    assert {item["node_id"] for item in payload["nodes"]} == {"node-a", "node-b"}
    assert payload["preferred"][0]["preferred_node_id"] == "node-b"
    assert payload["policy"]["objective"] == "fastest-completion"


def test_the_route_is_covered_by_the_existing_read_permission() -> None:
    # It lives on the Federation blueprint, so it inherits federation.read
    # rather than introducing an unreviewed authorization path.
    assert (
        permission_for("federation_web.efficiency_learning", "GET")
        == "federation.read"
    )
    assert permission_for("federation_web.efficiency_learning", "POST") is None
