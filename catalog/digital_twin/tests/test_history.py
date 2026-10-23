"""Handwritten synthetic histories; never discovered from production data."""

from __future__ import annotations

import copy
import gc
import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from catalog.digital_twin.adapters import InputRegistry, InputSource
from catalog.digital_twin.contracts import (
    ROLES,
    TwinError,
    identifier,
    revision,
    timestamp,
    validate,
)
from catalog.digital_twin.projection import metrics
from catalog.digital_twin.records import TwinStore
from catalog.flask_app.services.process_twin_service import ProcessTwinService
from catalog.mtconnect_recorder.canonical.observation import CanonicalObservation
from catalog.mtconnect_recorder.canonical.store import (
    CanonicalObservationStore,
    ProjectedBatchRecord,
)
from catalog.mtconnect_recorder.operational.projection import project_agent_instance
from catalog.mtconnect_recorder.operational.store import OperationalSegmentationStore


def observation(
    sequence,
    value,
    kind="EXECUTION",
    *,
    device="device-synthetic",
    second=None,
    category="EVENT",
):
    return CanonicalObservation.from_recorder_record(
        {
            "sequence": sequence,
            "machine_id": "machine-synthetic",
            "device_uuid": device,
            "category": category,
            "observation_type": kind.title(),
            "type": kind,
            "data_item_id": f"{device}-{kind}",
            "stream_component": "Path",
            "stream_component_id": "path-test",
            "stream_component_name": "PATH-TEST",
            "native_value": value,
            "attributes": {
                "timestamp": timestamp(
                    (second if second is not None else sequence) * 1_000_000
                )
            },
        },
        source_name="source-synthetic",
        agent_instance_id=1,
        raw_batch_sha256="a" * 64,
    )


def seed_canonical(root, rows=None):
    rows = rows or [
        observation(1, "MAIN", "PROGRAM"),
        observation(2, "7", "TOOL_NUMBER"),
        observation(3, "ACTIVE"),
        observation(4, "FEED_HOLD"),
        observation(5, "ACTIVE"),
        observation(6, "READY"),
    ]
    canonical = CanonicalObservationStore(root / "canonical.sqlite3")
    canonical.record_batch(
        observations=rows,
        batch=ProjectedBatchRecord(
            source_key=rows[0].source_key,
            raw_sha256="a" * 64,
            source_name="source-synthetic",
            agent_instance_id=1,
            manifest_path="synthetic.manifest",
            raw_path="synthetic.xml",
            manifest_schema="synthetic",
            first_sequence=rows[0].sequence,
            last_sequence=rows[-1].sequence,
            manifest_observation_count=len(rows),
            projected_count=len(rows),
            status="projected",
            reason=None,
            detail=None,
            projected_at=timestamp(0),
        ),
    )
    operational = OperationalSegmentationStore(root / "operational.sqlite3")
    project_agent_instance(
        rows, operational, source_key=rows[0].source_key, agent_instance_id=1
    )
    # The existing owner store uses connection context managers that commit
    # without closing. Finish fixture-owned writers before the read-only proof;
    # a later garbage-collection WAL checkpoint is not a DT source write.
    gc.collect()
    return rows, canonical, operational


@pytest.fixture
def history(tmp_path):
    rows, canonical, operational = seed_canonical(tmp_path / "source")
    allowed = {"actor": True, "other": False}
    source = InputSource(
        identifier("in"),
        revision("synthetic-binding"),
        "CANONICAL_DEVICE",
        "Synthetic process",
        tmp_path / "source",
        (canonical.database_path, operational.database_path),
        {
            "source_key": rows[0].source_key,
            "agent_instance_id": 1,
            "device_key": "uuid:device-synthetic",
        },
        lambda actor, record: allowed.get(actor, False),
        utc_verified=True,
        origin_status="KNOWN",
    )
    registry = InputRegistry([source])
    service = ProcessTwinService(tmp_path / "private-twins", registry)
    service.store.initialize()
    return service, source, allowed, rows


def prepare_request(source, policy="SOURCE_UTC", operator=None):
    return {
        "schema": "fcp.process_twin.prepare.v1",
        "input_id": source.input_id,
        "expected_binding_revision": source.binding_revision,
        "operator_input_id": operator.input_id if operator else None,
        "expected_operator_binding_revision": operator.binding_revision
        if operator
        else None,
        "time_policy": policy,
    }


def prepare(service, source, policy="SOURCE_UTC", operator=None):
    return service.execute(
        "actor",
        "PREPARE_DATASET",
        prepare_request(source, policy, operator),
        identifier("cmd"),
    )[0]


def definition_request(dataset):
    return {
        "schema": "fcp.process_twin.definition.v1",
        "display_name": "Synthetic process",
        "counterpart_kind": "PRODUCTION_PROCESS",
        "counterpart_description": "Handwritten process fixture",
        "process_boundary": "One synthetic device",
        "dataset_revision_id": dataset["dataset_revision_id"],
        "goals": ["PROCESS_HISTORY", "PROVENANCE"],
        "properties": [
            "EXECUTION_HISTORY",
            "OPERATIONAL_EPISODES",
            "OPERATOR_RECORDS",
            "EVIDENCE",
        ],
        "role_profile": ROLES.copy(),
        "validity_scope": "Historical description only",
    }


def definition(service, dataset):
    return service.execute(
        "actor", "CREATE_TWIN", definition_request(dataset), identifier("cmd")
    )[0]


def snapshot_request(definition, dataset, a=0, b=10_000_000):
    return {
        "schema": "fcp.process_twin.snapshot_request.v1",
        "definition_revision_id": definition["definition_revision_id"],
        "dataset_revision_ids": [dataset["dataset_revision_id"]],
        "window": {"start_at": timestamp(a), "end_at": timestamp(b)},
        "data_mode": "HISTORICAL",
    }


def build(history):
    service, source, *_ = history
    data = prepare(service, source)
    twin = definition(service, data)
    request = snapshot_request(twin, data)
    snapshot = service.execute(
        "actor", "BUILD_SNAPSHOT", request, identifier("cmd"), target=twin["twin_id"]
    )[0]
    return data, twin, request, snapshot


def error_code(code, callable):
    with pytest.raises(TwinError) as caught:
        callable()
    assert caught.value.code == code
    return caught.value


def test_real_owner_integration_immutable_content_and_read_only_sources(history):
    service, source, _, _ = history
    before = [hashlib.sha256(p.read_bytes()).digest() for p in source.paths]
    data, twin, request, snapshot = build(history)
    assert snapshot["summary"]["active_us"] == 2_000_000
    assert snapshot["summary"]["feed_hold_us"] == 1_000_000
    assert snapshot["counts"]["operator"] is None
    assert snapshot["model_validity"] == "NOT_ASSESSED"
    assert snapshot["services"]["feedback"]["reason_codes"] == ["NOT_IN_RELEASE"]
    again = service.execute(
        "actor", "BUILD_SNAPSHOT", request, identifier("cmd"), target=twin["twin_id"]
    )[0]
    assert again["snapshot_id"] != snapshot["snapshot_id"]
    assert again["content_revision"] == snapshot["content_revision"]
    timeline = service.timeline(
        snapshot["snapshot_id"], "actor", view="execution", limit=1
    )
    assert timeline["total"] > 1 and timeline["has_more"]
    next_page = service.timeline(
        snapshot["snapshot_id"],
        "actor",
        view="execution",
        limit=1,
        cursor=timeline["next_cursor"],
    )
    assert next_page["items"][0]["row_id"] != timeline["items"][0]["row_id"]
    assert data["integrity_status"] == "VERIFIED"
    assert before == [hashlib.sha256(p.read_bytes()).digest() for p in source.paths]


def test_append_does_not_mutate_prepared_history(history):
    service, source, _, rows = history
    _, twin, request, old = build(history)
    seed_canonical(source.root, rows + [observation(7, "ACTIVE")])
    new = service.execute(
        "actor", "BUILD_SNAPSHOT", request, identifier("cmd"), target=twin["twin_id"]
    )[0]
    assert new["content_revision"] == old["content_revision"]
    assert new["last_observed_at"] == timestamp(6_000_000)


def test_mismatched_projection_is_preserved_and_not_rebuilt(history):
    service, source, *_ = history
    with sqlite3.connect(source.paths[1]) as c:
        c.execute(
            "UPDATE segmentation_projection_runs SET input_fingerprint='mismatch'"
        )
    error_code("PROJECTION_REVISION_MISMATCH", lambda: prepare(service, source))
    assert not service.store.objects("dataset")
    with sqlite3.connect(source.paths[1]) as c:
        assert (
            c.execute(
                "SELECT input_fingerprint FROM segmentation_projection_runs"
            ).fetchone()[0]
            == "mismatch"
        )


def test_unknown_source_schema_never_initializes_or_migrates(history):
    service, source, *_ = history
    with sqlite3.connect(source.paths[0]) as c:
        c.execute("UPDATE fcp_schema_versions SET version=999")
    before = source.paths[0].read_bytes()
    error_code("INPUT_SCHEMA_UNSUPPORTED", lambda: prepare(service, source))
    assert source.paths[0].read_bytes() == before


def test_missing_source_does_not_create_database(history):
    service, source, *_ = history
    missing = source.root / "not-created.sqlite3"
    replacement = replace(source, paths=(missing, source.paths[1]))
    service.registry.sources[source.input_id] = replacement
    error_code("SOURCE_UNAVAILABLE", lambda: prepare(service, replacement))
    assert not missing.exists()


def test_no_source_alias_fallback_and_no_new_namespace_union(history):
    service, source, *_ = history
    other = replace(source, input_id=identifier("in"))
    service.registry.sources[other.input_id] = other
    a, b = prepare(service, source), prepare(service, other)
    assert a["dataset_revision_id"] != b["dataset_revision_id"]
    twin = definition(service, a)
    error_code(
        "BINDING_MISMATCH",
        lambda: service.execute(
            "actor",
            "BUILD_SNAPSHOT",
            snapshot_request(twin, b),
            identifier("cmd"),
            target=twin["twin_id"],
        ),
    )


def test_same_key_replay_preserves_original_and_changed_request_conflicts(history):
    service, source, *_ = history
    dataset = prepare(service, source)
    payload = definition_request(dataset)
    key = identifier("cmd")
    first = service.execute("actor", "CREATE_TWIN", copy.deepcopy(payload), key)
    changed = dict(payload, display_name="Revised synthetic process")
    service.execute(
        "actor",
        "REVISE_TWIN",
        {
            "expected_revision_id": first[0]["definition_revision_id"],
            "definition": changed,
        },
        identifier("cmd"),
        target=first[0]["twin_id"],
    )
    assert service.execute("actor", "CREATE_TWIN", payload, key) == first
    error_code(
        "IDEMPOTENCY_CONFLICT",
        lambda: service.execute("actor", "CREATE_TWIN", changed, key),
    )


def test_revoke_access_fences_manifest_timeline_evidence_and_replay(history):
    service, _source, allowed, _ = history
    _, twin, request, snapshot = build(history)
    key = identifier("cmd")
    replay = service.execute(
        "actor", "BUILD_SNAPSHOT", request, key, target=twin["twin_id"]
    )[0]
    eid = service.timeline(snapshot["snapshot_id"], "actor", view="execution")["items"][
        0
    ]["evidence_refs"][0]
    allowed["actor"] = False
    for operation in (
        lambda: service.snapshot(snapshot["snapshot_id"], "actor"),
        lambda: service.timeline(snapshot["snapshot_id"], "actor"),
        lambda: service.detail(snapshot["snapshot_id"], "actor", "evidence", eid),
        lambda: service.execute(
            "actor", "BUILD_SNAPSHOT", request, key, target=twin["twin_id"]
        ),
    ):
        error_code("OBJECT_NOT_FOUND", operation)
    assert replay["snapshot_id"]


def test_index_loss_and_explicit_rebuild_preserve_manifest(history, monkeypatch):
    service, _, *_ = history
    _, _, _, snapshot = build(history)
    saved = service.store.index.with_suffix(".preserved")
    service.store.index.rename(saved)
    assert service.snapshot(snapshot["snapshot_id"], "actor") == snapshot
    error_code(
        "VIEW_INDEX_UNAVAILABLE",
        lambda: service.timeline(snapshot["snapshot_id"], "actor"),
    )
    assert not service.store.index.exists()
    request = {
        "schema": "fcp.process_twin.rebuild.v1",
        "expected_content_revision": snapshot["content_revision"],
    }
    service.execute(
        "actor",
        "REBUILD_INDEX",
        request,
        identifier("cmd"),
        target=snapshot["snapshot_id"],
    )
    assert service.snapshot(snapshot["snapshot_id"], "actor") == snapshot
    assert service.timeline(snapshot["snapshot_id"], "actor")["items"]
    assert saved.exists()


def test_rebuild_with_different_logic_cannot_publish(history, monkeypatch):
    from catalog.digital_twin import projection

    service, *_ = history
    _, _, _, snapshot = build(history)
    original = projection.build

    def changed(*args):
        result = original(*args)
        result["content_revision"] = revision("different logic")
        return result

    monkeypatch.setattr(projection, "build", changed)
    error_code(
        "REBUILD_DIFFERENT_RESULT",
        lambda: service.execute(
            "actor",
            "REBUILD_INDEX",
            {
                "schema": "fcp.process_twin.rebuild.v1",
                "expected_content_revision": snapshot["content_revision"],
            },
            identifier("cmd"),
            target=snapshot["snapshot_id"],
        ),
    )
    assert service.snapshot(snapshot["snapshot_id"], "actor") == snapshot


@pytest.mark.parametrize(
    "key,value,code",
    [
        ("data_mode", "LIVE", "DATA_MODE_NOT_SUPPORTED"),
        ("data_mode", "REPLAY", "DATA_MODE_NOT_SUPPORTED"),
        ("data_mode", "UNKNOWN", "INVALID_REQUEST"),
        ("actor", "not-server-actor", "INVALID_REQUEST"),
    ],
)
def test_snapshot_validation_has_no_implicit_fallback(history, key, value, code):
    service, source, *_ = history
    dataset = prepare(service, source)
    twin = definition(service, dataset)
    request = snapshot_request(twin, dataset)
    request[key] = value
    error_code(
        code,
        lambda: service.execute(
            "actor",
            "BUILD_SNAPSHOT",
            request,
            identifier("cmd"),
            target=twin["twin_id"],
        ),
    )
    assert not service.store.objects("snapshot")


@pytest.mark.parametrize(
    "removed",
    list(
        snapshot_request(
            {"definition_revision_id": revision("d")},
            {"dataset_revision_id": revision("s")},
        )
    ),
)
def test_missing_snapshot_field_is_rejected(removed):
    payload = snapshot_request(
        {"definition_revision_id": revision("d")},
        {"dataset_revision_id": revision("s")},
    )
    del payload[removed]
    error_code("INVALID_REQUEST", lambda: validate("BUILD_SNAPSHOT", payload))


def test_clipping_metrics_do_not_scale_episode_totals_or_double_count_hierarchy():
    spans = [
        {
            "start_observed_at_us": s * 1_000_000,
            "end_observed_at_us": e * 1_000_000,
            "state": state,
        }
        for s, e, state in [
            (8, 12, "ACTIVE"),
            (12, 15, "FEED_HOLD"),
            (15, 17, "UNKNOWN"),
            (17, 23, "ACTIVE"),
        ]
    ]
    result = metrics(spans, 10_000_000, 20_000_000)
    assert result["covered_duration_us"] == 10_000_000
    assert result["active_us"] == 5_000_000
    assert result["feed_hold_us"] == 3_000_000
    assert result["unknown_state_us"] == 2_000_000
    assert result["active_share"] == 0.5
    assert metrics([], 0, 10)["active_us"] is None
    assert (
        metrics(
            [
                {
                    "start_observed_at_us": 0,
                    "end_observed_at_us": 10,
                    "state": "FEED_HOLD",
                }
            ],
            0,
            10,
        )["active_us"]
        == 0
    )


def test_unresolved_time_publishes_no_placed_primary_history(history):
    service, source, *_ = history
    dataset = prepare(service, source, "UNRESOLVED")
    twin = definition(service, dataset)
    snapshot = service.execute(
        "actor",
        "BUILD_SNAPSHOT",
        snapshot_request(twin, dataset),
        identifier("cmd"),
        target=twin["twin_id"],
    )[0]
    assert snapshot["services"]["history"]["state"] == "UNRESOLVED_CONTEXT"
    assert snapshot["summary"]["active_us"] is None
    assert snapshot["counts"]["unplaced_primary_records"] == 6
    assert service.timeline(snapshot["snapshot_id"], "actor")["items"] == []


def test_cursor_cannot_change_scope_view_limit_or_principal(history):
    service, *_ = history
    _, _, _, snapshot = build(history)
    page = service.timeline(snapshot["snapshot_id"], "actor", view="execution", limit=1)
    for kwargs in [{"view": "episodes", "limit": 1}, {"view": "execution", "limit": 2}]:
        error_code(
            "INVALID_CURSOR",
            lambda kwargs=kwargs: service.timeline(
                snapshot["snapshot_id"], "actor", cursor=page["next_cursor"], **kwargs
            ),
        )
    error_code(
        "INVALID_CURSOR",
        lambda: service.timeline(
            snapshot["snapshot_id"],
            "actor",
            view="execution",
            limit=1,
            cursor="altered",
        ),
    )


def test_failed_build_replays_original_error_and_preserves_atomicity(
    history, monkeypatch
):
    from catalog.digital_twin import projection

    service, source, *_ = history
    dataset = prepare(service, source)
    twin = definition(service, dataset)
    request = snapshot_request(twin, dataset)
    key = identifier("cmd")
    called = []

    def fail(*args):
        called.append(True)
        raise TwinError("BUILD_DEADLINE_EXCEEDED", 504)

    monkeypatch.setattr(projection, "build", fail)
    for _ in range(2):
        error_code(
            "BUILD_DEADLINE_EXCEEDED",
            lambda: service.execute(
                "actor", "BUILD_SNAPSHOT", request, key, target=twin["twin_id"]
            ),
        )
    assert len(called) == 1
    assert service.store.objects("snapshot") == []
    assert (
        service.command("actor", operation="BUILD_SNAPSHOT", key=key)["status"]
        == "FAILED"
    )


def test_simultaneous_idempotency_has_one_published_object(history):
    service, source, *_ = history
    dataset = prepare(service, source)
    request = definition_request(dataset)
    key = identifier("cmd")

    def worker():
        try:
            return service.execute("actor", "CREATE_TWIN", copy.deepcopy(request), key)[
                0
            ]["twin_id"]
        except TwinError as error:
            assert error.code == "COMMAND_RUNNING"
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: worker(), range(2)))
    assert len({value for value in results if value}) == 1
    assert len(service.store.objects("definition")) == 1


def test_get_never_creates_missing_private_store(tmp_path):
    store = TwinStore(tmp_path / "absent")
    error_code("STORE_UNAVAILABLE", lambda: store.objects("definition"))
    assert not store.directory.exists()


def test_new_window_at_exact_limit_and_one_microsecond_over():
    payload = snapshot_request(
        {"definition_revision_id": revision("d")},
        {"dataset_revision_id": revision("s")},
        0,
        2_678_400_000_000,
    )
    validate("BUILD_SNAPSHOT", payload)
    payload["window"]["end_at"] = timestamp(2_678_400_000_001)
    error_code("INVALID_TIME_WINDOW", lambda: validate("BUILD_SNAPSHOT", payload))


def test_resource_callbacks_preserve_owner_results_and_can_stop_iteration(history):
    from catalog.mtconnect_recorder.operational.projection import _input_fingerprint
    from catalog.mtconnect_recorder.operational.roles import resolve_device_roles
    from catalog.mtconnect_recorder.operational.timeline import (
        build_device_timeline_reports,
    )

    rows = history[3]
    calls = []

    def check():
        calls.append(True)

    for function in (
        build_device_timeline_reports,
        resolve_device_roles,
        _input_fingerprint,
    ):
        assert function(rows, check=check) == function(rows)
    assert len(calls) > len(rows)

    def expired():
        raise TwinError("BUILD_DEADLINE_EXCEEDED", 504)

    for function in (
        build_device_timeline_reports,
        resolve_device_roles,
        _input_fingerprint,
    ):
        error_code(
            "BUILD_DEADLINE_EXCEEDED",
            lambda function=function: function(rows, check=expired),
        )


def test_deadline_is_bound_to_committed_command_reservation(history, monkeypatch):
    service, source, *_ = history
    original = service._perform

    def inspect_budget(*args, **kwargs):
        actor, _, _, _, command_id, budget, *_ = args
        command = service.store.command(actor=actor, command_id=command_id)
        assert budget.started == command["mono"]
        return original(*args, **kwargs)

    monkeypatch.setattr(service, "_perform", inspect_budget)
    prepare(service, source)
