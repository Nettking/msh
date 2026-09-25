"""Contract evidence cannot be manufactured from absent or retained telemetry."""
from __future__ import annotations

import json
from copy import deepcopy

import pytest

from scripts.acceptance import v1_recorder_observability as obs

CANDIDATE = "a" * 40
PROVENANCE = {"candidate_sha": CANDIDATE, "runtime_generation": "b" * 32,
              "supervisor_generation": None, "pid": 12}


def heartbeat(second=1):
    stamp = f"2026-09-25T19:00:{second:02d}+00:00"
    operations = []
    for index, kind in enumerate(obs.OPERATIONS):
        recovery = kind == "recorder-recovery"
        operations.append({
            "operation": kind, "operation_id": f"{second * 2 + index:032x}",
            "operation_sequence": second * 2 + index, "provenance": dict(PROVENANCE),
            "observation_loss_count": 0,
            "outcome": "completed", "error_type": None,
            "started_at_utc": stamp, "ended_at_utc": stamp,
            "started_at_monotonic_ns": second * 100, "ended_at_monotonic_ns": second * 100 + 10,
            "duration_ns": 10,
            "context": {"source_alias": "c" * 64, "next_sequence_before": 1, "agent_instance_id": 123,
                        "session_id": "session-test", "node_id": "node-test", "storage_group": "group-test"},
            "progress": {"next_sequence_after": 2, "advanced_sequences": 1} if recovery else
                        {key: 0 for key in ("scanned_batches", "eligible_batches", "publication_chunks",
                                             "enqueued", "already_enqueued", "quarantined")},
        })
    workers = {name: {
        **PROVENANCE, "worker_generation": f"{index + 1:032x}", "generation_current": True,
        "generation_overlap": False, "supervisor": "compose", "session_id": "session-test",
        "node_id": "node-test", "alive": True, "healthy": True, "started": True,
        "stop_requested": False, "state": "alive", "consecutive_failures": 0,
        "last_cycle_outcome": "completed", "observed_at_utc": stamp,
        "observed_at_monotonic_ns": second * 100 + 15,
        "future_pending": True, "event_loop_thread_alive": True, "event_loop_running": True,
    } for index, name in enumerate(obs.MANAGED_WORKERS)}
    return {"workers": workers, "acceptance_observability": {
        "schema": obs.PRODUCT_SCHEMA, "available": True, "provenance": dict(PROVENANCE),
        "observed_at_utc": stamp, "observed_at_monotonic_ns": second * 100 + 20,
        "dropped_updates": 0, "evicted_operations": 0, "retention_truncated": False,
        "max_operations": 16, "operations": operations,
    }}


def review(status, **kwargs):
    return obs.evidence(status, expected_candidate=CANDIDATE, expected_session="session-test", **kwargs)


def test_actual_completed_spans_and_workers_preserve_source_provenance():
    raw = heartbeat()
    result = review(raw)
    assert result["status"] == "pass"
    assert result["source"] == raw
    assert result["physical_assertion_pass"] is False
    assert result["durations"]["recorder-recovery"]["completed_operations"][0]["duration_ns"] == 10


@pytest.mark.parametrize("state", [None, {}, {"workers": {}}, {"acceptance_observability": {"available": True}}])
def test_absent_evidence_is_not_zero_or_pass(state):
    assert review(state)["status"] == "unavailable"


@pytest.mark.parametrize("change", [
    {"outcome": "failed"}, {"outcome": "interrupted"}, {"outcome": "incomplete"},
    {"duration_ns": None}, {"duration_ns": 0}, {"duration_ns": True},
    {"operation_id": None}, {"operation_sequence": 0}, {"progress": {}},
    {"ended_at_monotonic_ns": 90}, {"error_type": "ValueError"},
])
def test_incomplete_or_inconsistent_operation_is_missing(change):
    raw = heartbeat()
    raw["acceptance_observability"]["operations"][0].update(change)
    result = review(raw)
    assert result["durations"]["recorder-recovery"]["status"] == "unavailable"
    assert result["status"] != "pass"


def test_actual_zero_span_requires_matching_explicit_boundaries():
    raw = heartbeat()
    op = raw["acceptance_observability"]["operations"][0]
    op.update(ended_at_monotonic_ns=op["started_at_monotonic_ns"], duration_ns=0)
    assert review(raw)["status"] == "pass"


@pytest.mark.parametrize("change", [
    {"dropped_updates": 1}, {"retention_truncated": True}, {"evicted_operations": 1},
    {"operations": [None]}, {"operations": [{"operation": "unknown"}]},
])
def test_missing_coverage_is_unavailable(change):
    raw = heartbeat()
    raw["acceptance_observability"].update(change)
    assert review(raw)["status"] == "unavailable"


def test_real_new_completion_after_observation_loss_restores_measurement():
    raw = heartbeat()
    raw["acceptance_observability"]["dropped_updates"] = 1
    for op in raw["acceptance_observability"]["operations"]:
        op["observation_loss_count"] = 1
    result = review(raw)
    assert result["status"] == "pass"
    assert result["dropped_updates"] == 1


@pytest.mark.parametrize("key,value", [("candidate_sha", "d" * 40), ("runtime_generation", "e" * 32), ("pid", 99)])
def test_stale_operation_identity_cannot_satisfy_current_runtime(key, value):
    raw = heartbeat()
    raw["acceptance_observability"]["operations"][0]["provenance"][key] = value
    assert review(raw)["status"] == "unavailable"


@pytest.mark.parametrize("change", [
    {"alive": False, "healthy": False, "state": "failed"},
    {"stop_requested": True, "healthy": False, "state": "stopping"},
    {"generation_current": False}, {"generation_overlap": True}, {"worker_generation": None},
    {"session_id": "session-old"}, {"observed_at_monotonic_ns": 999},
    {"observed_at_utc": "2026-09-25T20:00:00Z"},
])
def test_stopped_failed_stale_or_future_worker_cannot_pass(change):
    raw = heartbeat()
    raw["workers"]["managed_companion"].update(change)
    assert review(raw)["status"] != "pass"


def test_managed_recovery_distinguishes_alive_from_healthy():
    raw = heartbeat()
    raw["workers"]["managed_companion"].update(healthy=False, state="recovering",
        consecutive_failures=1, last_cycle_outcome="retrying")
    result = review(raw)
    assert result["worker_health"]["status"] == "pass"
    assert result["worker_health"]["all_expected_alive"] is True
    assert result["worker_health"]["all_expected_healthy"] is False


def test_previous_run_or_runtime_is_not_reused():
    assert review(heartbeat(), not_before_utc="2026-09-25T20:00:00Z")["status"] == "unavailable"
    assert review(heartbeat(), expected_runtime_generation="f" * 32)["status"] == "unavailable"


@pytest.mark.parametrize("index", [0, 1])
def test_operation_session_must_match_current_managed_worker(index):
    raw = heartbeat()
    raw["acceptance_observability"]["operations"][index]["context"]["session_id"] = "previous-session"
    assert review(raw)["status"] == "unavailable"


def test_pending_future_cannot_hide_dead_publication_loop():
    raw = heartbeat()
    raw["workers"]["recorder_publication"]["event_loop_thread_alive"] = False
    assert review(raw)["status"] == "unavailable"


@pytest.mark.parametrize("workers", [None, [], 1, "missing"])
def test_malformed_worker_collection_is_explicitly_unavailable(workers):
    raw = heartbeat()
    raw["workers"] = workers
    assert review(raw)["status"] == "unavailable"


def test_series_run_boundary_is_preserved_after_raw_revalidation():
    result = obs.evidence_series([review(heartbeat(1)), review(heartbeat(2))], expected_candidate=CANDIDATE,
                                 not_before_utc="2026-09-25T20:00:00Z")
    assert result["status"] == "unavailable"


def test_current_operation_series_preserves_raw_and_explicit_baseline():
    result = obs.evidence_series([review(heartbeat(1)), review(heartbeat(2))],
                                 expected_candidate=CANDIDATE, expected_session="session-test")
    assert result["status"] == "pass"
    assert len(result["durations"]["recorder-recovery"]["samples"]) == 1
    assert result["physical_assertion_pass"] is False


def test_old_completion_cannot_be_reused_for_next_sample():
    first, later = heartbeat(1), heartbeat(2)
    later["acceptance_observability"]["operations"] = first["acceptance_observability"]["operations"]
    result = obs.evidence_series([review(first), review(later)], expected_candidate=CANDIDATE)
    assert result["durations"]["recorder-recovery"]["status"] == "unavailable"
    assert result["status"] == "unavailable"


def test_repeated_snapshot_or_forged_derived_pass_is_not_accepted():
    first = review(heartbeat())
    assert obs.evidence_series([first, first], expected_candidate=CANDIDATE)["status"] == "unavailable"
    fake = deepcopy(first)
    fake["source"] = {}
    assert obs.evidence_series([first, fake], expected_candidate=CANDIDATE)["status"] == "unavailable"


def test_evidence_file_records_all_values_and_missing_file_fails_closed(tmp_path):
    path = tmp_path / "status.json"
    assert obs.read_evidence(path, expected_candidate=CANDIDATE)["status"] == "unavailable"
    path.write_text(json.dumps(heartbeat()), encoding="utf-8")
    result = obs.read_evidence(path, expected_candidate=CANDIDATE)
    assert result["status"] == "pass"
    assert result["source"]["acceptance_observability"]["operations"]
    assert set(result["source"]["workers"]) == set(obs.MANAGED_WORKERS)


@pytest.mark.parametrize("series", ["recorder", "publication", "orphan"])
def test_truthy_missing_series_is_not_campaign_pass(monkeypatch, tmp_path, series):
    from scripts.acceptance import v1_physical_probes as probes
    context = probes.ProbeContext(tmp_path, tmp_path, CANDIDATE, "test-host", "posix", "test", "P12", series,
                                  run_id="current-run", options={"series": series})
    sample = {"extras": {series: {"status": "unavailable"}}}
    monkeypatch.setattr(probes, "_sample_packets", lambda _context: [sample, sample])
    outcome = probes._probe_campaign_series(context)
    assert outcome.status == "unavailable"


@pytest.mark.parametrize("series", ["recorder", "publication", "orphan"])
def test_current_observations_flow_into_campaign_evidence(monkeypatch, tmp_path, series):
    from scripts.acceptance import v1_physical_probes as probes
    context = probes.ProbeContext(tmp_path, tmp_path, CANDIDATE, "test-host", "posix", "test", "P12", series,
                                  run_id="current-run", options={"series": series})
    samples = [{"extras": {"recorder": {"acceptance_observability": review(heartbeat(second))},
                           "publication": {"entries": 1}, "orphan": {"files": 0}}} for second in (1, 2)]
    monkeypatch.setattr(probes, "_sample_packets", lambda _context: samples)
    result = probes._probe_campaign_series(context)
    assert result.status == "pass"
    assert result.detail["product_observations"]["sample_count"] == 2
