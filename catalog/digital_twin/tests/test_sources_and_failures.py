"""Synthetic legacy/operator data and failure-specific historical guarantees."""

from __future__ import annotations

import json
import time
from dataclasses import replace
from types import SimpleNamespace

from catalog.digital_twin.adapters import InputRegistry, InputSource, read_canonical
from catalog.digital_twin.contracts import (
    MAX_ARTIFACT,
    MAX_INPUT,
    MAX_RECORDS,
    Budget,
    identifier,
    revision,
    timestamp,
)
from catalog.digital_twin.tests.test_history import (
    build,
    definition,
    definition_request,
    error_code,
    observation,
    prepare,
    prepare_request,
    seed_canonical,
    snapshot_request,
)
from catalog.flask_app.services.process_twin_inputs import InventoryInputRegistry
from catalog.flask_app.services.process_twin_service import ProcessTwinService

pytest_plugins = ("catalog.digital_twin.tests.test_history",)


def legacy(tmp_path, paths=1):
    root = tmp_path / "source"
    root.mkdir()
    contents = "".join(
        json.dumps(
            {
                "timestamp": timestamp(n * 1_000_000),
                "machine_id": "synthetic",
                "execution": "ACTIVE" if n % 2 else "STOPPED",
            }
        )
        + "\n"
        for n in range(31)
    )
    files = []
    for n in range(paths):
        path = root / f"synthetic-{n}.jsonl"
        path.write_text(contents, encoding="utf-8")
        files.append(path)
    source = InputSource(
        identifier("in"),
        revision("legacy binding"),
        "LEGACY_JSONL",
        "Synthetic legacy",
        root,
        tuple(files),
        {"machine_id": "synthetic"},
        lambda actor, record: actor == "actor",
        utc_verified=True,
        origin_status="KNOWN",
    )
    service = ProcessTwinService(tmp_path / "twins", InputRegistry([source]))
    service.store.initialize()
    return service, source


def legacy_snapshot(service, source, policy="SOURCE_UTC"):
    data = prepare(service, source, policy)
    payload = definition_request(data)
    payload["properties"].remove("OPERATIONAL_EPISODES")
    twin = service.execute("actor", "CREATE_TWIN", payload, identifier("cmd"))[0]
    request = snapshot_request(twin, data, 0, 86_400_000_000)
    result = service.execute(
        "actor", "BUILD_SNAPSHOT", request, identifier("cmd"), target=twin["twin_id"]
    )[0]
    return data, twin, request, result


def test_legacy_reuses_owner_daily_summary_without_new_episodes(tmp_path):
    service, source = legacy(tmp_path)
    _, _, _, snapshot = legacy_snapshot(service, source)
    assert snapshot["summary"]["covered_duration_us"] == 30_000_000
    assert snapshot["summary"]["active_us"] == 15_000_000
    assert snapshot["summary"]["feed_hold_us"] is None
    assert snapshot["counts"]["episodes"] is None
    assert service.timeline(snapshot["snapshot_id"], "actor")["view"] == "legacy_daily"
    error_code(
        "VIEW_NOT_SUPPORTED",
        lambda: service.timeline(snapshot["snapshot_id"], "actor", view="episodes"),
    )
    assert snapshot["services"]["simulation"]["reason_codes"] == ["NOT_IN_RELEASE"]


def test_known_identical_recording_copies_count_once_and_unknown_overlap_abstains(
    tmp_path,
):
    service, source = legacy(tmp_path, 2)
    _, _, _, snapshot = legacy_snapshot(service, source)
    assert snapshot["summary"]["covered_duration_us"] == 30_000_000
    source.paths[1].write_text(source.paths[1].read_text() + "\n", encoding="utf-8")
    error_code("OVERLAP_UNRESOLVED", lambda: prepare(service, source))


def test_symlink_escape_is_not_opened(tmp_path):
    service, source = legacy(tmp_path)
    outside = tmp_path / "outside.jsonl"
    outside.write_text("synthetic-secret", encoding="utf-8")
    escaped = replace(source, paths=(outside,))
    service.registry.sources[source.input_id] = escaped
    error_code("SOURCE_BINDING_UNAVAILABLE", lambda: prepare(service, escaped))


def test_legacy_subday_and_assumption_are_explicit(tmp_path):
    service, source = legacy(tmp_path)
    _, twin, request, snapshot = legacy_snapshot(service, source, "ASSUME_UTC")
    assert "UTC_ASSUMED" in snapshot["services"]["history"]["reason_codes"]
    request["window"]["end_at"] = timestamp(100)
    error_code(
        "LEGACY_WINDOW_NOT_DAY_ALIGNED",
        lambda: service.execute(
            "actor",
            "BUILD_SNAPSHOT",
            request,
            identifier("cmd"),
            target=twin["twin_id"],
        ),
    )


def operator_source(
    history, tmp_path, records, links=(), record_bindings=None, *, local=True
):
    service, _machine, *_ = history
    path = tmp_path / "human" / "synthetic.json"
    path.parent.mkdir()
    path.write_text(
        json.dumps(
            {
                "schema": "fcp.process_twin.operator_read.v1",
                "records": records,
                "links": list(links),
            }
        ),
        encoding="utf-8",
    )
    allowed = {row["id"]: True for row in records}
    source = InputSource(
        identifier("in"),
        revision("operator binding"),
        "OPERATOR_RECORDS",
        "Synthetic operator",
        path.parent,
        (path,),
        {"record_bindings": record_bindings or {}},
        lambda actor, record: (
            actor == "actor" and (record is None or allowed.get(record, False))
        ),
        utc_verified=True,
        local_authoritative=local,
    )
    service.registry.sources[source.input_id] = source
    return source, allowed


def test_operator_recorded_time_is_not_event_time_and_no_action_is_invented(
    history, tmp_path
):
    service, source, *_ = history
    operator, _ = operator_source(
        history,
        tmp_path,
        [
            {
                "id": "note",
                "captured_at": timestamp(4_000_000),
                "worked": "yes",
                "outcome": "better",
                "review_status": "reusable",
            }
        ],
    )
    data = prepare(service, source, operator=operator)
    twin = definition(service, data)
    snapshot = service.execute(
        "actor",
        "BUILD_SNAPSHOT",
        snapshot_request(twin, data),
        identifier("cmd"),
        target=twin["twin_id"],
    )[0]
    assert snapshot["counts"]["operator"] == 0
    assert snapshot["counts"]["unplaced_operator_records"] == 1
    assert snapshot["services"]["operator_knowledge"]["state"] == "INSUFFICIENT_DATA"


def test_scoped_human_record_stays_unlinked_and_revoke_fences_replay(history, tmp_path):
    service, source, *_ = history
    record = {
        "id": "note",
        "decision_time": timestamp(4_000_000),
        "observation": "synthetic-secret",
        "outcome": "better",
        "worked": "yes",
    }
    operator, allowed = operator_source(
        history, tmp_path, [record], record_bindings={"note": source.binding}
    )
    key = identifier("cmd")
    request = prepare_request(source, operator=operator)
    data = service.execute("actor", "PREPARE_DATASET", request, key)[0]
    twin = definition(service, data)
    snapshot = service.execute(
        "actor",
        "BUILD_SNAPSHOT",
        snapshot_request(twin, data),
        identifier("cmd"),
        target=twin["twin_id"],
    )[0]
    page = service.timeline(snapshot["snapshot_id"], "actor", view="operator")
    assert page["total"] == 1
    detail = service.detail(
        snapshot["snapshot_id"], "actor", "record", page["items"][0]["record_id"]
    )
    assert detail["link_status"] == "UNLINKED"
    assert detail["fields"]["outcome"] == "better"
    assert "execution_status" not in detail
    allowed["note"] = False
    error_code(
        "OBJECT_NOT_FOUND",
        lambda: service.timeline(snapshot["snapshot_id"], "actor", view="operator"),
    )
    error_code(
        "OBJECT_NOT_FOUND",
        lambda: service.execute("actor", "PREPARE_DATASET", request, key),
    )


def test_network_authority_collection_never_uses_local_file_as_fallback(
    history, tmp_path
):
    service, source, *_ = history
    operator, _ = operator_source(history, tmp_path, [{"id": "note"}], local=False)
    error_code(
        "SOURCE_BINDING_UNAVAILABLE",
        lambda: prepare(service, source, operator=operator),
    )
    assert (
        service.inputs("actor")[
            1 if service.inputs("actor")[0]["kind"] != "OPERATOR_RECORDS" else 0
        ]["can_prepare"]
        is False
    )


def test_corrupt_evidence_is_not_replaced_by_current_text(history):
    service, *_ = history
    snapshot = build(history)[-1]
    eid = service.timeline(snapshot["snapshot_id"], "actor", view="execution")["items"][
        0
    ]["evidence_refs"][0]
    internal = snapshot["snapshot_id"] + ":" + eid
    with service.store.connection(write=True) as c:
        value = json.loads(
            c.execute("SELECT payload FROM objects WHERE id=?", (internal,)).fetchone()[
                0
            ]
        )
        value["content"] = "different synthetic text"
        c.execute(
            "UPDATE objects SET payload=? WHERE id=?", (json.dumps(value), internal)
        )
    evidence = service.detail(snapshot["snapshot_id"], "actor", "evidence", eid)
    assert evidence["state"] == "SOURCE_UNAVAILABLE" and evidence["content"] is None


def test_page_cursor_remains_bound_to_frozen_selection(history):
    service, *_ = history
    items = [{"id": n} for n in range(1001)]
    one = service.page(items, "actor", "test-selection", limit=500)
    two = service.page(
        items, "actor", "test-selection", limit=500, cursor=one["next_cursor"]
    )
    three = service.page(
        items, "actor", "test-selection", limit=500, cursor=two["next_cursor"]
    )
    assert [len(v["items"]) for v in (one, two, three)] == [500, 500, 1]
    assert three["next_cursor"] is None
    error_code(
        "LIST_CHANGED",
        lambda: service.page(
            items + [{"id": 1001}],
            "actor",
            "test-selection",
            limit=500,
            cursor=one["next_cursor"],
        ),
    )


def test_low_storage_fails_before_reservation_and_preserves_everything(
    history, monkeypatch
):
    from catalog.digital_twin import records

    service, source, *_ = history
    monkeypatch.setattr(
        records.shutil, "disk_usage", lambda path: SimpleNamespace(free=603_979_775)
    )
    error_code("INSUFFICIENT_STORAGE", lambda: prepare(service, source))
    with service.store.connection() as c:
        assert c.execute("SELECT count(*) FROM commands").fetchone()[0] == 0


def test_expired_worker_is_fenced_and_clock_disagreement_cannot_take_over(history):
    service, source, *_ = history
    scopes = [{"input_id": source.input_id}]
    cmd = service.store.reserve(
        "actor", "PREPARE_DATASET", identifier("cmd"), "synthetic", scopes, True
    )
    with service.store.connection(write=True) as c:
        c.execute("UPDATE commands SET wall=wall-151,mono=mono-151 WHERE id=?", (cmd,))
    assert service.command("actor", command_id=cmd)["status"] == "FAILED"
    error_code(
        "BUILD_ABANDONED",
        lambda: service.store.finish(
            cmd,
            "actor",
            {},
            201,
            None,
            budget=Budget(),
            before_commit=lambda: None,
            scopes=scopes,
        ),
    )
    with service.store.connection(write=True) as c:
        c.execute("UPDATE commands SET mono=? WHERE id=?", (time.monotonic() + 10, cmd))
    error_code("CLOCK_UNSAFE", lambda: service.command("actor", command_id=cmd))


def test_inventory_adapter_reads_existing_bindings_without_registering_or_granting(
    tmp_path,
):
    root = tmp_path / "data"
    path = root / "source_config" / "machines_and_sensors.json"
    path.parent.mkdir(parents=True)
    entry = {
        "input_id": identifier("in"),
        "machine_id": "m",
        "kind": "LEGACY_JSONL",
        "relative_paths": ["existing.jsonl"],
        "binding": {"machine_id": "existing-id"},
        "clock_binding": "UTC_NORMALIZED",
        "origin_status": "KNOWN",
    }
    payload = {
        "schema": "fcp.source_inventory.v1",
        "machines": [{"id": "m", "name": "Synthetic"}],
        "process_twin_inputs": [entry],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    before = path.read_bytes()
    registry = InventoryInputRegistry(root, lambda actor, record: actor == "actor")
    assert registry.list("other") == []
    assert registry.list("actor")[0]["can_prepare"] is False
    assert registry.get(entry["input_id"], "actor").binding == entry["binding"]
    assert path.read_bytes() == before
    assert not (root / "existing.jsonl").exists()


def test_view_row_loss_is_not_reported_as_a_short_successful_page(history):
    service, *_ = history
    snapshot = build(history)[-1]
    with service.store.connection(write=True, index=True) as c:
        c.execute(
            "DELETE FROM views.rows WHERE snapshot=? AND view='episodes'",
            (snapshot["snapshot_id"],),
        )
    error_code(
        "VIEW_INDEX_UNAVAILABLE",
        lambda: service.timeline(snapshot["snapshot_id"], "actor"),
    )


def test_actual_bounds_are_not_invented_from_capture_time(history, tmp_path):
    service, source, *_ = history
    data = prepare(service, source)
    assert data["first_observed_at"] == timestamp(1_000_000)
    assert data["last_observed_at"] == timestamp(6_000_000)
    assert prepare(service, source, "UNRESOLVED")["first_observed_at"] is None
    (tmp_path / "legacy").mkdir()
    legacy_service, legacy_source = legacy(tmp_path / "legacy")
    legacy_data, _, _, snapshot = legacy_snapshot(legacy_service, legacy_source)
    assert (
        legacy_data["first_observed_at"]
        == snapshot["first_observed_at"]
        == timestamp(0)
    )
    assert (
        legacy_data["last_observed_at"]
        == snapshot["last_observed_at"]
        == timestamp(30_000_000)
    )


def test_existing_many_to_many_links_preserve_revision_and_uncertain_time(
    history, tmp_path
):
    service, source, *_ = history
    content = read_canonical(source, Budget())
    episode = content["episodes"][0]
    records = [
        {
            "id": "note",
            "occurred_at": timestamp(4_000_000),
            "time_status": "ASSUMED",
            "observation": "Synthetic note",
        },
        {
            "id": "other",
            "occurred_at": timestamp(4_000_000),
            "time_status": "UNRESOLVED",
        },
    ]
    links = [
        {
            "record_id": record["id"],
            "input_id": source.input_id,
            "segment_id": episode["segment_id"],
            "segment_revision": episode["fingerprint"],
            "status": "CONFIRMED",
            "relation_kind": "CONTEXT_FOR",
        }
        for record in records
    ]
    operator, _ = operator_source(history, tmp_path, records, links)
    data = prepare(service, source, operator=operator)
    twin = definition(service, data)
    snapshot = service.execute(
        "actor",
        "BUILD_SNAPSHOT",
        snapshot_request(twin, data),
        identifier("cmd"),
        target=twin["twin_id"],
    )[0]
    page = service.timeline(snapshot["snapshot_id"], "actor", view="operator")
    assert len(page["items"]) == 2
    for row in page["items"]:
        detail = service.detail(
            snapshot["snapshot_id"], "actor", "record", row["record_id"]
        )
        assert detail["time_status"] in ("ASSUMED", "UNRESOLVED")
        assert row["original_start_at"] is None
        assert detail["link_status"] == "CONFIRMED"
    assert (
        len(service.timeline(snapshot["snapshot_id"], "actor")["items"][0]["link_refs"])
        == 2
    )


def test_other_device_cannot_satisfy_selected_context_or_leak_payload(tmp_path):
    rows = [
        observation(1, "MAIN", "PROGRAM"),
        observation(2, "other-private", "PROGRAM", device="another"),
        observation(3, "7", "TOOL_NUMBER"),
        observation(4, "ACTIVE"),
        observation(5, "READY"),
    ]
    rows, canonical, operational = seed_canonical(tmp_path / "source", rows)
    source = InputSource(
        identifier("in"),
        revision("multi-device"),
        "CANONICAL_DEVICE",
        "Synthetic",
        tmp_path / "source",
        (canonical.database_path, operational.database_path),
        {
            "source_key": rows[0].source_key,
            "agent_instance_id": 1,
            "device_key": "uuid:device-synthetic",
        },
        lambda actor, record: actor == "actor",
        utc_verified=True,
    )
    service = ProcessTwinService(tmp_path / "twins", InputRegistry([source]))
    service.store.initialize()
    data = prepare(service, source)
    twin = definition(service, data)
    snapshot = service.execute(
        "actor",
        "BUILD_SNAPSHOT",
        snapshot_request(twin, data),
        identifier("cmd"),
        target=twin["twin_id"],
    )[0]
    assert "SEQUENCE_GAP" not in snapshot["services"]["history"]["reason_codes"]
    for value, _ in service.store.objects("evidence", snapshot["snapshot_id"]):
        assert "other-private" not in json.dumps(value)


def test_bad_registry_entry_is_server_unavailability_not_client_error(tmp_path):
    root = tmp_path / "data"
    path = root / "source_config" / "machines_and_sensors.json"
    path.parent.mkdir(parents=True)
    entry = {
        "input_id": "bad",
        "machine_id": "m",
        "kind": "LEGACY_JSONL",
        "relative_paths": ["synthetic.jsonl"],
        "binding": {},
    }
    path.write_text(
        json.dumps(
            {
                "schema": "fcp.source_inventory.v1",
                "machines": [{"id": "m", "name": "Synthetic"}],
                "process_twin_inputs": [entry],
            }
        ),
        encoding="utf-8",
    )
    error_code(
        "INPUT_REGISTRY_UNAVAILABLE",
        lambda: InventoryInputRegistry(root, lambda actor, record: True).list("actor"),
    )


def test_one_existing_record_can_reference_two_original_episodes(history, tmp_path):
    service, source, *_ = history
    root = tmp_path / "two-episodes"
    _, canonical, operational = seed_canonical(
        root,
        [
            observation(1, "MAIN", "PROGRAM"),
            observation(2, "1", "TOOL_NUMBER"),
            observation(3, "ACTIVE"),
            observation(4, "2", "TOOL_NUMBER"),
            observation(5, "READY"),
        ],
    )
    source = replace(
        source, root=root, paths=(canonical.database_path, operational.database_path)
    )
    service.registry.sources[source.input_id] = source
    episodes = read_canonical(source, Budget())["episodes"]
    assert len(episodes) == 2
    links = [
        {
            "record_id": "one-note",
            "input_id": source.input_id,
            "segment_id": e["segment_id"],
            "segment_revision": e["fingerprint"],
            "status": "CONFIRMED",
            "relation_kind": "CONTEXT_FOR",
        }
        for e in episodes
    ]
    operator, _ = operator_source(
        history,
        tmp_path,
        [{"id": "one-note", "observation": "Synthetic shared context"}],
        links,
    )
    data = prepare(service, source, operator=operator)
    twin = definition(service, data)
    snapshot = service.execute(
        "actor",
        "BUILD_SNAPSHOT",
        snapshot_request(twin, data),
        identifier("cmd"),
        target=twin["twin_id"],
    )[0]
    page = service.timeline(snapshot["snapshot_id"], "actor", view="operator")
    assert page["total"] == 1
    record = service.detail(
        snapshot["snapshot_id"], "actor", "record", page["items"][0]["record_id"]
    )
    assert len(record["link_refs"]) == 2
    assert record["time_status"] == "UNRESOLVED"


def test_resource_boundaries_and_deadline_do_not_publish(history, monkeypatch):
    from catalog.digital_twin import contracts

    for field, maximum, code in (
        ("records", MAX_RECORDS, "RECORD_LIMIT_EXCEEDED"),
        ("source_bytes", MAX_INPUT, "INPUT_LIMIT_EXCEEDED"),
    ):
        budget = Budget()
        budget.check(**{field: maximum})
        error_code(code, lambda budget=budget, field=field: budget.check(**{field: 1}))
    budget = Budget()
    monkeypatch.setattr(contracts, "rss_bytes", lambda: budget.rss + MAX_ARTIFACT + 1)
    error_code("MEMORY_LIMIT_EXCEEDED", budget.check)
    monkeypatch.undo()
    service, source, *_ = history
    data = prepare(service, source)
    twin = definition(service, data)
    original = service._perform

    def expired_build(*args, **kwargs):
        result = original(*args, **kwargs)
        args[5].started -= 121
        return result

    monkeypatch.setattr(service, "_perform", expired_build)
    error_code(
        "BUILD_DEADLINE_EXCEEDED",
        lambda: service.execute(
            "actor",
            "BUILD_SNAPSHOT",
            snapshot_request(twin, data),
            identifier("cmd"),
            target=twin["twin_id"],
        ),
    )
    assert service.store.objects("snapshot") == []


def test_busy_builder_does_not_reserve_a_second_key(history):
    service, source, *_ = history
    first = identifier("cmd")
    second = identifier("cmd")
    service.store.reserve(
        "actor",
        "PREPARE_DATASET",
        first,
        "synthetic",
        [{"input_id": source.input_id}],
        True,
    )
    error_code(
        "BUILD_BUSY",
        lambda: service.execute(
            "actor", "PREPARE_DATASET", prepare_request(source), second
        ),
    )
    assert (
        service.store.command(actor="actor", operation="PREPARE_DATASET", key=second)
        is None
    )


def test_hidden_record_links_do_not_change_visible_revision(history, tmp_path):
    service, source, *_ = history
    operator, allowed = operator_source(
        history,
        tmp_path,
        [{"id": "visible"}, {"id": "hidden"}],
        [
            {
                "record_id": "hidden",
                "input_id": source.input_id,
                "segment_id": "hidden-private",
            }
        ],
    )
    allowed["hidden"] = False
    first = prepare(service, source, operator=operator)
    raw = json.loads(operator.paths[0].read_text(encoding="utf-8"))
    raw["links"][0]["segment_id"] = "changed-hidden-private"
    operator.paths[0].write_text(json.dumps(raw), encoding="utf-8")
    second = prepare(service, source, operator=operator)
    assert first["dataset_revision_id"] == second["dataset_revision_id"]
