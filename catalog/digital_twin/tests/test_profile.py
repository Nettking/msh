"""B15's fixed synthetic profile; no production-derived test data."""

from __future__ import annotations

import gc
import json
import time

from flask import Flask

from catalog.digital_twin.adapters import InputRegistry, InputSource
from catalog.digital_twin.contracts import identifier, revision, timestamp
from catalog.digital_twin.tests.test_history import (
    definition,
    observation,
    prepare,
    seed_canonical,
    snapshot_request,
)
from catalog.flask_app import process_twin_routes as routes
from catalog.flask_app.auth import policy
from catalog.flask_app.auth.models import Role, User
from catalog.flask_app.services.process_twin_service import ProcessTwinService


def test_fixed_synthetic_episode_page_profile(tmp_path, monkeypatch, record_property):
    rows = []
    for sequence in range(1, 100_001):
        if sequence == 1:
            value, kind = "MAIN", "PROGRAM"
        elif sequence == 2:
            value, kind = "0", "TOOL_NUMBER"
        elif sequence == 3:
            value, kind = "ACTIVE", "EXECUTION"
        elif sequence == 100_000:
            value, kind = "READY", "EXECUTION"
        elif sequence % 10 == 0:
            value, kind = str(sequence // 10), "TOOL_NUMBER"
        else:
            value, kind = str(sequence), "SPINDLE_SPEED"
        rows.append(
            observation(
                sequence,
                value,
                kind,
                category="SAMPLE" if kind == "SPINDLE_SPEED" else "EVENT",
            )
        )
    rows, canonical, operational = seed_canonical(tmp_path / "source", rows)
    assert len(operational.episodes()) == 10_000
    gc.collect()
    machine = InputSource(
        identifier("in"),
        revision("synthetic profile binding"),
        "CANONICAL_DEVICE",
        "Synthetic profile",
        tmp_path / "source",
        (canonical.database_path, operational.database_path),
        {
            "source_key": rows[0].source_key,
            "agent_instance_id": 1,
            "device_key": "uuid:device-synthetic",
        },
        lambda actor, record: actor == "actor",
        utc_verified=True,
        origin_status="KNOWN",
    )
    records = [
        {
            "id": f"handwritten-{n}",
            "decision_time": timestamp((n + 4) * 1_000_000),
            "observation": "Synthetic profile note",
        }
        for n in range(100)
    ]
    human_path = tmp_path / "source" / "human.json"
    human_path.write_text(
        json.dumps(
            {
                "schema": "fcp.process_twin.operator_read.v1",
                "records": records,
                "links": [],
            }
        ),
        encoding="utf-8",
    )
    human = InputSource(
        identifier("in"),
        revision("synthetic operator binding"),
        "OPERATOR_RECORDS",
        "Synthetic profile operator",
        tmp_path / "source",
        (human_path,),
        {"record_bindings": {r["id"]: machine.binding for r in records}},
        lambda actor, record: actor == "actor",
        utc_verified=True,
    )
    service = ProcessTwinService(
        tmp_path / "private-twins", InputRegistry([machine, human])
    )
    service.store.initialize()
    started = time.perf_counter()
    dataset = prepare(service, machine, operator=human)
    record_property("synthetic_prepare_seconds", time.perf_counter() - started)
    twin = definition(service, dataset)
    request = snapshot_request(twin, dataset, 0, 100_001_000_000)
    started = time.perf_counter()
    snapshot = service.execute(
        "actor", "BUILD_SNAPSHOT", request, identifier("cmd"), target=twin["twin_id"]
    )[0]
    record_property("synthetic_build_seconds", time.perf_counter() - started)
    record_property("synthetic_content_revision", snapshot["content_revision"])
    assert snapshot["counts"]["episodes"] == 10_000
    assert snapshot["counts"]["operator"] == 100
    app = Flask(__name__)
    app.config.update(
        FCP_AUTH_ENABLED=True, FCP_PROCESS_TWIN_STORE=service.store.directory
    )
    principal = User(
        id=1,
        email="synthetic@example.com",
        active=True,
        fs_uniquifier="actor",
        roles=[Role(name="synthetic-reader", permissions=["data.read"])],
    )
    monkeypatch.setattr(routes, "current_user", principal)
    monkeypatch.setattr(policy, "current_user", principal)
    app.before_request(policy.enforce_human_authorization)
    app.register_blueprint(routes.process_twin_api)
    app.extensions["process_twin_input_registry"] = service.registry
    client = app.test_client()
    path = routes.API + "/snapshots/" + snapshot["snapshot_id"] + "/timeline?limit=100"
    for _ in range(10):
        response = client.get(path)
        assert (
            response.status_code == 200 and len(response.json["data"]["items"]) == 100
        )
    latencies = []
    for _ in range(100):
        started = time.perf_counter()
        response = client.get(path)
        assert response.status_code == 200
        assert response.data
        latencies.append(time.perf_counter() - started)
    p95 = sorted(latencies)[94]
    record_property("synthetic_profile_p95_seconds", p95)
    assert p95 < 2, f"Synthetic page p95 exceeded two seconds: {p95:.3f}"
