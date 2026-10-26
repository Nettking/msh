"""HTTP contract tests with synthetic principals and real CSRF validation."""

from __future__ import annotations

import pytest
from flask import Flask, session
from flask_wtf import CSRFProtect
from flask_wtf.csrf import generate_csrf

from catalog.digital_twin.adapters import InputRegistry
from catalog.digital_twin.contracts import identifier
from catalog.digital_twin.tests.test_history import (
    build,
    definition_request,
    prepare_request,
)
from catalog.flask_app import process_twin_routes as routes
from catalog.flask_app.auth import policy
from catalog.flask_app.routes import web

pytest_plugins = ("catalog.digital_twin.tests.test_history",)


class SyntheticHuman:
    is_authenticated = True
    is_active = True

    def __init__(self):
        self.permissions = {"data.read", "analysis.run", "workflow.run"}

    def get_id(self):
        return "actor"

    def has_permission(self, permission):
        return permission in self.permissions


@pytest.fixture
def api(history, monkeypatch, tmp_path):
    service, _source, _allowed, _rows = history
    human = SyntheticHuman()
    monkeypatch.setattr(routes, "current_user", human)
    monkeypatch.setattr(policy, "current_user", human)
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "synthetic-test-only-session-secret"
    app.config.update(
        TESTING=True,
        FCP_AUTH_ENABLED=True,
        WTF_CSRF_CHECK_DEFAULT=False,
        FCP_PROCESS_TWIN_STORE=service.store.directory,
        FCP_DATA_ROOT=tmp_path / "data",
    )
    CSRFProtect(app)
    app.context_processor(
        lambda: {"capability_startup_flags": {"completed": True, "workbench": True}}
    )
    app.before_request(policy.enforce_human_authorization)
    app.register_blueprint(web)
    routes.install_process_twins(app)
    app.extensions["process_twin_input_registry"] = service.registry
    client = app.test_client()
    with app.test_request_context():
        token = generate_csrf()
        raw_token = session["csrf_token"]
    with client.session_transaction() as saved:
        saved["csrf_token"] = raw_token
    headers = {
        "Content-Type": "application/json",
        "X-CSRFToken": token,
        "Idempotency-Key": identifier("cmd"),
    }
    return app, client, headers, human, history


def test_post_uses_real_csrf_and_server_identity(api):
    _, client, headers, _, history = api
    source = history[1]
    response = client.post(
        routes.API + "/dataset-revisions", headers=headers, json=prepare_request(source)
    )
    assert response.status_code == 201
    data = response.json["data"]
    assert data["created_by"] == "actor"
    assert response.headers["Location"].startswith(routes.API + "/dataset-revisions/")
    duplicate = client.post(
        routes.API + "/dataset-revisions", headers=headers, json=prepare_request(source)
    )
    assert duplicate.status_code == 201
    assert duplicate.json["data"] == data
    assert duplicate.headers["Cache-Control"] == "private, no-store"


@pytest.mark.parametrize(
    "case,status,code",
    [
        ("anonymous", 401, "AUTHENTICATION_REQUIRED"),
        ("viewer", 403, "PERMISSION_REQUIRED"),
        ("csrf", 400, "CSRF_INVALID"),
        ("disabled-auth", 503, "AUTHENTICATION_UNAVAILABLE"),
    ],
)
def test_security_has_priority_over_bad_json_and_no_reservation(
    api, case, status, code
):
    app, client, headers, human, history = api
    if case == "anonymous":
        human.is_authenticated = False
    if case == "viewer":
        human.permissions = {"data.read"}
    if case == "csrf":
        headers.pop("X-CSRFToken")
    if case == "disabled-auth":
        app.config["FCP_AUTH_ENABLED"] = False
    response = client.post(
        routes.API + "/dataset-revisions", data="not-json", headers=headers
    )
    assert response.status_code == status
    assert response.json["error"]["code"] == code
    with history[0].store.connection() as c:
        assert c.execute("SELECT count(*) FROM commands").fetchone()[0] == 0


@pytest.mark.parametrize(
    "payload,code",
    [
        (b'{"schema":1,"schema":2}', "INVALID_JSON"),
        (b'{"actor":"synthetic-secret"}', "INVALID_REQUEST"),
    ],
)
def test_invalid_json_or_extra_fields_never_echo_values(api, payload, code):
    _, client, headers, *_ = api
    response = client.post(
        routes.API + "/dataset-revisions", data=payload, headers=headers
    )
    assert response.status_code == 400
    assert response.json["error"]["code"] == code
    assert b"synthetic-secret" not in response.data


def test_body_limit_and_media_type_precede_schema(api):
    _, client, headers, *_ = api
    response = client.post(
        routes.API + "/dataset-revisions", data=b" " * 65_537, headers=headers
    )
    assert response.status_code == 413
    assert response.json["error"]["code"] == "REQUEST_TOO_LARGE"
    headers["Content-Type"] = "text/plain"
    response = client.post(
        routes.API + "/dataset-revisions", data=b"{}", headers=headers
    )
    assert response.status_code == 415


def test_unknown_and_future_routes_have_no_fallback(api):
    _, client, headers, *_ = api
    for path in ("/cases", "/analysis/baselines", "/unknown"):
        response = client.post(routes.API + path, data="{}", headers=headers)
        assert response.status_code == 404
        assert response.json["error"]["code"] == "ROUTE_NOT_FOUND"
        assert response.headers["Cache-Control"] == "private, no-store"
    wrong = client.delete(routes.API + "/twins")
    assert wrong.status_code == 405
    assert "GET" in wrong.headers["Allow"]


@pytest.mark.parametrize(
    "query",
    [
        "?limit=0",
        "?limit=01",
        "?limit=501",
        "?limit=1&limit=2",
        "?start_at=synthetic-secret",
        "?view=episodes&view=operator",
    ],
)
def test_query_contract_no_private_filter_values_in_url(api, query):
    _, client, *_ = api
    response = client.get(routes.API + "/twins" + query)
    assert response.status_code == 400
    assert b"synthetic-secret" not in response.data


def test_empty_registry_differs_from_unavailable_registry(api):
    app, client, *_ = api
    app.extensions["process_twin_input_registry"] = InputRegistry([])
    response = client.get(routes.API + "/inputs")
    assert response.status_code == 200 and response.json["data"]["total"] == 0
    app.extensions.pop("process_twin_input_registry")
    response = client.get(routes.API + "/inputs")
    assert response.status_code == 503
    assert response.json["error"]["code"] == "INPUT_REGISTRY_UNAVAILABLE"


def test_manifest_read_does_not_rebuild_missing_index(api):
    _, client, _, _, history = api
    snapshot = build(history)[-1]
    history[0].store.index.rename(history[0].store.index.with_suffix(".preserved"))
    response = client.get(routes.API + "/snapshots/" + snapshot["snapshot_id"])
    assert response.status_code == 200
    assert response.json["data"] == snapshot
    assert response.json["meta"]["technical_state"] == "PARTIAL"
    response = client.get(
        routes.API + "/snapshots/" + snapshot["snapshot_id"] + "/timeline"
    )
    assert response.status_code == 503
    assert not history[0].store.index.exists()


def test_policy_covers_every_new_route_and_options_is_read_only(api):
    app, client, _, human, _ = api
    new_rules = [
        rule
        for rule in app.url_map.iter_rules()
        if rule.endpoint.startswith("process_twin_")
    ]
    for rule in new_rules:
        for method in rule.methods:
            assert policy.permission_for(rule.endpoint, method) is not None
    human.permissions = {"data.read"}
    response = client.options(routes.API + "/twins")
    assert response.status_code == 204
    assert "POST" in response.headers["Allow"]
    assert (
        policy.permission_for("process_twin_api.create_twin", "POST") == "workflow.run"
    )
    assert (
        policy.permission_for("process_twin_api.build_snapshot", "POST")
        == "analysis.run"
    )


def test_private_page_uses_local_assets_and_no_automatic_post(api):
    _, client, *_ = api
    response = client.get("/process-twins")
    assert response.status_code == 200
    text = response.get_data(as_text=True)
    assert "Prosesstvilling" in text
    assert "cdn.jsdelivr.net" not in text
    assert "no-referrer" == response.headers["Referrer-Policy"]
    assert "data-csrf=" in text
    script = client.get("/static/js/process-twin.js").get_data(as_text=True)
    assert "localStorage" not in script
    assert "setInterval" not in script
    assert "innerHTML" not in script


def test_command_lookup_is_actor_and_permission_scoped(api):
    _, client, headers, human, history = api
    response = client.post(
        routes.API + "/dataset-revisions",
        headers=headers,
        json=prepare_request(history[1]),
    )
    command = response.json["data"]["command_id"]
    assert client.get(routes.API + "/commands/" + command).status_code == 200
    human.permissions = {"data.read"}
    assert client.get(routes.API + "/commands/" + command).status_code == 403


def test_unsafe_definition_markup_stays_data(api):
    _, client, headers, _, history = api
    data = client.post(
        routes.API + "/dataset-revisions",
        headers=headers,
        json=prepare_request(history[1]),
    ).json["data"]
    payload = definition_request(data)
    payload["display_name"] = "<script>synthetic-secret</script>"
    headers["Idempotency-Key"] = identifier("cmd")
    result = client.post(routes.API + "/twins", headers=headers, json=payload)
    assert result.status_code == 201
    assert result.json["data"]["display_name"] == payload["display_name"]
    assert "definition" not in result.json["data"]
    # Rendering uses external DOM textContent; no definition appears in markup.
    page = client.get("/process-twins")
    assert b"synthetic-secret" not in page.data


def test_analysis_revoke_before_commit_is_not_saved_as_success(api, monkeypatch):
    _, client, headers, human, history = api
    original = routes.ProcessTwinService._perform

    def revoke(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        human.permissions.remove("analysis.run")
        return result

    monkeypatch.setattr(routes.ProcessTwinService, "_perform", revoke)
    result = client.post(
        routes.API + "/dataset-revisions",
        headers=headers,
        json=prepare_request(history[1]),
    )
    assert result.status_code == 403
    assert result.json["error"]["code"] == "PERMISSION_REQUIRED"
    assert history[0].store.objects("dataset") == []


@pytest.fixture
def route_contract(api, tmp_path):
    from catalog.digital_twin.contracts import timestamp
    from catalog.digital_twin.tests.test_history import (
        definition,
        prepare,
        snapshot_request,
    )
    from catalog.digital_twin.tests.test_sources_and_failures import operator_source

    _app, _client, _headers, _human, history = api
    service, source, *_ = history
    operator, _ = operator_source(
        history,
        tmp_path,
        [
            {
                "id": "note",
                "decision_time": timestamp(4_000_000),
                "observation": "Synthetic note",
            }
        ],
        record_bindings={"note": source.binding},
    )
    dataset = prepare(service, source, operator=operator)
    twin = definition(service, dataset)
    key = identifier("cmd")
    body = snapshot_request(twin, dataset)
    snapshot = service.execute(
        "actor", "BUILD_SNAPSHOT", body, key, target=twin["twin_id"]
    )[0]
    sid = snapshot["snapshot_id"]
    tid = twin["twin_id"]
    episode = service.timeline(sid, "actor")["items"][0]
    record = service.timeline(sid, "actor", view="operator")["items"][0]
    reads = [
        ("/inputs", {}),
        ("/dataset-revisions/" + dataset["dataset_revision_id"], {}),
        ("/twins", {}),
        (f"/twins/{tid}/revisions/" + twin["definition_revision_id"], {}),
        (f"/twins/{tid}/snapshots", {}),
        (f"/snapshots/{sid}", {}),
        (f"/snapshots/{sid}/timeline", {}),
        (f"/snapshots/{sid}/episodes/" + episode["row_id"], {}),
        (f"/snapshots/{sid}/records/" + record["record_id"], {}),
        (f"/snapshots/{sid}/links", {}),
        (f"/snapshots/{sid}/evidence/" + record["evidence_refs"][0], {}),
        ("/commands/" + snapshot["command_id"], {}),
        ("/commands/by-key?operation=BUILD_SNAPSHOT", {"Idempotency-Key": key}),
    ]
    revised = definition_request(dataset)
    revised["display_name"] = "Synthetic revised process"
    writes = [
        ("/dataset-revisions", prepare_request(source, operator=operator)),
        ("/twins", definition_request(dataset)),
        (
            f"/twins/{tid}/revisions",
            {
                "expected_revision_id": twin["definition_revision_id"],
                "definition": revised,
            },
        ),
        (f"/twins/{tid}/snapshots", body),
        (
            f"/snapshots/{sid}/rebuild-index",
            {
                "schema": "fcp.process_twin.rebuild.v1",
                "expected_content_revision": snapshot["content_revision"],
            },
        ),
    ]
    return api, reads, writes


@pytest.mark.parametrize("index", range(13))
def test_every_read_route_has_current_auth_and_read_only_contract(
    route_contract, index
):
    api, reads, _ = route_contract
    _, client, _, human, history = api
    path, headers = reads[index]
    store = history[0].store
    before = store.primary.read_bytes()
    response = client.get(routes.API + path, headers=headers)
    assert response.status_code == 200
    assert response.json["schema"] == "fcp.process_twin.response.v1"
    assert "no-store" in response.headers["Cache-Control"]
    assert store.primary.read_bytes() == before
    invalid_path = path + ("&" if "?" in path else "?") + "unsupported=synthetic"
    assert client.get(routes.API + invalid_path, headers=headers).status_code == 400
    human.is_authenticated = False
    denied = client.get(routes.API + path, headers=headers)
    assert denied.status_code == 401
    assert store.primary.read_bytes() == before
    human.is_authenticated = True
    history[2]["actor"] = False
    revoked = client.get(routes.API + path, headers=headers)
    if index in (0, 2):
        assert revoked.status_code == 200
        assert all(
            item.get("input_id") != history[1].input_id
            for item in revoked.json["data"]["items"]
        )
        if index == 2:
            assert revoked.json["data"]["items"] == []
    else:
        assert (
            revoked.status_code == 404
            and revoked.json["error"]["code"] == "OBJECT_NOT_FOUND"
        )
    assert store.primary.read_bytes() == before


@pytest.mark.parametrize("index", range(5))
def test_every_write_route_accepts_exact_body_and_rejects_missing_fields(
    route_contract, index
):
    api, _, writes = route_contract
    _, client, headers, _, history = api
    path, body = writes[index]
    before = history[0].store.primary.read_bytes()
    for field in body:
        missing = {key: value for key, value in body.items() if key != field}
        invalid = client.post(routes.API + path, headers=headers, json=missing)
        assert invalid.status_code == 400
        assert invalid.json["error"]["field"] == field
        assert history[0].store.primary.read_bytes() == before
    response = client.post(routes.API + path, headers=headers, json=body)
    assert response.status_code == (200 if index == 4 else 201)
    assert response.headers["Location"].startswith(routes.API + "/")
    after = history[0].store.primary.read_bytes()
    history[2]["actor"] = False
    revoked = client.post(routes.API + path, headers=headers, json=body)
    assert revoked.status_code == 404
    assert history[0].store.primary.read_bytes() == after
    history[2]["actor"] = True
    human = api[3]
    human.is_authenticated = False
    assert client.post(routes.API + path, headers=headers, json=body).status_code == 401
    human.is_authenticated = True
    no_csrf = {key: value for key, value in headers.items() if key != "X-CSRFToken"}
    assert (
        client.post(routes.API + path, headers=no_csrf, json=body).json["error"]["code"]
        == "CSRF_INVALID"
    )
    assert history[0].store.primary.read_bytes() == after


def _bv_snapshot(api, tmp_path, *, rows=None, records=None, window=(0, 10_000_000)):
    from dataclasses import replace

    from catalog.digital_twin.contracts import revision
    from catalog.digital_twin.tests.test_history import seed_canonical, snapshot_request
    from catalog.digital_twin.tests.test_sources_and_failures import operator_source

    _, client, headers, _, history = api
    service, source, *_ = history
    if rows is not None:
        root = tmp_path / "bv-source"
        _, canonical, operational = seed_canonical(root, rows)
        source = replace(
            source,
            root=root,
            paths=(canonical.database_path, operational.database_path),
            binding_revision=revision("synthetic BV binding"),
        )
        service.registry.sources[source.input_id] = source
    operator = (
        operator_source(
            history,
            tmp_path,
            records,
            record_bindings={record["id"]: source.binding for record in records},
        )[0]
        if records is not None
        else None
    )

    def post(path, body):
        response = client.post(
            routes.API + path,
            headers={**headers, "Idempotency-Key": identifier("cmd")},
            json=body,
        )
        assert response.status_code == 201
        return response.json["data"]

    data = post("/dataset-revisions", prepare_request(source, operator=operator))
    twin = post("/twins", definition_request(data))
    snapshot = post(
        f"/twins/{twin['twin_id']}/snapshots", snapshot_request(twin, data, *window)
    )
    return data, twin, snapshot


def test_bv14_half_open_point_window_through_http(api, tmp_path):
    from catalog.digital_twin.contracts import timestamp

    records = [
        {
            "id": str(value),
            "occurred_at": timestamp(value),
            "observation": "Synthetic point",
        }
        for value in (10, 19, 20)
    ]
    _, _, snapshot = _bv_snapshot(api, tmp_path, records=records, window=(10, 20))
    client = api[1]
    sid = snapshot["snapshot_id"]
    result = client.get(routes.API + f"/snapshots/{sid}/timeline?view=operator").json[
        "data"
    ]
    assert result["total"] == 2
    assert {row["original_start_at"] for row in result["items"]} == {
        timestamp(10),
        timestamp(19),
    }
    assert snapshot["counts"]["unplaced_operator_records"] == 1


def test_bv15_original_episode_boundaries_survive_clipping_through_http(api, tmp_path):
    from catalog.digital_twin.contracts import timestamp
    from catalog.digital_twin.tests.test_history import observation

    rows = [
        observation(1, "MAIN", "PROGRAM", second=6),
        observation(2, "7", "TOOL_NUMBER", second=7),
        observation(3, "ACTIVE", second=8),
        observation(4, "FEED_HOLD", second=12),
        observation(5, "ACTIVE", second=17),
        observation(6, "READY", second=23),
    ]
    _, _, snapshot = _bv_snapshot(
        api, tmp_path, rows=rows, window=(10_000_000, 20_000_000)
    )
    result = (
        api[1]
        .get(routes.API + f"/snapshots/{snapshot['snapshot_id']}/timeline")
        .json["data"]
    )
    row = result["items"][0]
    assert (row["original_start_at"], row["original_end_at"]) == (
        timestamp(8_000_000),
        timestamp(23_000_000),
    )
    assert (row["visible_start_at"], row["visible_end_at"]) == (
        timestamp(10_000_000),
        timestamp(20_000_000),
    )
    assert row["clipped_start"] is row["clipped_end"] is True
    assert snapshot["summary"]["covered_duration_us"] == 10_000_000
    assert snapshot["summary"]["active_us"] == 5_000_000


def test_bv17_empty_window_has_null_measurements_and_explicit_reason(api, tmp_path):
    _, _, snapshot = _bv_snapshot(api, tmp_path, window=(50_000_000, 50_000_010))
    assert snapshot["summary"]["covered_duration_us"] == 0
    assert snapshot["summary"]["uncovered_duration_us"] == 10
    assert snapshot["summary"]["active_us"] is None
    assert snapshot["summary"]["active_share"] is None
    assert snapshot["services"]["history"]["state"] == "INSUFFICIENT_DATA"
    assert snapshot["services"]["history"]["reason_codes"] == ["EMPTY_WINDOW"]


def test_bv19_agent_gap_on_other_device_remains_visible_without_payload_leak(
    api, tmp_path
):
    from catalog.digital_twin.tests.test_history import observation

    rows = [
        observation(1, "MAIN", "PROGRAM"),
        observation(2, "7", "TOOL_NUMBER"),
        observation(
            4, "synthetic-private-other-device", "PROGRAM", device="another-device"
        ),
        observation(5, "ACTIVE"),
        observation(6, "READY"),
    ]
    _, _, snapshot = _bv_snapshot(api, tmp_path, rows=rows)
    response = api[1].get(
        routes.API + f"/snapshots/{snapshot['snapshot_id']}/timeline?view=gaps"
    )
    assert response.status_code == 200
    assert response.json["data"]["total"] == 1
    assert "SEQUENCE_GAP" in snapshot["services"]["history"]["reason_codes"]
    assert b"synthetic-private-other-device" not in response.data


def test_bv24_changed_preserved_content_is_rejected_without_source_fallback(api):
    import json

    from catalog.digital_twin.tests.test_history import (
        definition,
        prepare,
        snapshot_request,
    )

    _, client, headers, _, history = api
    service, source, *_ = history
    data = prepare(service, source)
    twin = definition(service, data)
    stored, _ = service.store.object(data["dataset_revision_id"], "dataset")
    content, _ = service.store.object(stored["content_ref"], "dataset_content")
    content["origin_status"] = "UNKNOWN"
    with service.store.connection(write=True) as connection:
        connection.execute(
            "UPDATE objects SET payload=? WHERE id=?",
            (json.dumps(content), stored["content_ref"]),
        )
    response = client.post(
        routes.API + f"/twins/{twin['twin_id']}/snapshots",
        headers=headers,
        json=snapshot_request(twin, data),
    )
    assert response.status_code == 409
    assert response.json["error"]["code"] == "SOURCE_CHANGED"
    assert service.store.objects("snapshot") == []


def test_bv43_and_bv46_stale_revision_and_original_revision_replay(api, tmp_path):
    data, twin, snapshot = _bv_snapshot(api, tmp_path)
    _, client, headers, _, _ = api
    path = routes.API + f"/twins/{twin['twin_id']}/revisions"

    def change(expected, name, key):
        definition = definition_request(data)
        definition["display_name"] = name
        body = {"expected_revision_id": expected, "definition": definition}
        response = client.post(
            path, headers={**headers, "Idempotency-Key": key}, json=body
        )
        return response, body

    first_key = identifier("cmd")
    first, body = change(
        twin["definition_revision_id"], "Synthetic revision B", first_key
    )
    assert first.status_code == 201
    second, _ = change(
        first.json["data"]["definition_revision_id"],
        "Synthetic revision C",
        identifier("cmd"),
    )
    assert second.status_code == 201
    stale, _ = change(
        twin["definition_revision_id"], "Synthetic stale proposal", identifier("cmd")
    )
    assert (
        stale.status_code == 409 and stale.json["error"]["code"] == "DEFINITION_CHANGED"
    )
    replay = client.post(
        path, headers={**headers, "Idempotency-Key": first_key}, json=body
    )
    assert replay.status_code == 201 and replay.json["data"] == first.json["data"]
    assert replay.headers["Location"] == first.headers["Location"]
    assert api[4][0].snapshot(snapshot["snapshot_id"], "actor") == snapshot


def test_definition_reversions_preserve_revision_chain_and_snapshot(api, tmp_path):
    data, original, snapshot = _bv_snapshot(api, tmp_path)
    _, client, headers, _, history = api
    path = routes.API + f"/twins/{original['twin_id']}/revisions"
    chain = [original]
    commands = []
    for name in (
        "Synthetic revision B",
        original["display_name"],
        "Synthetic revision B",
    ):
        body = {
            "expected_revision_id": chain[-1]["definition_revision_id"],
            "definition": {**definition_request(data), "display_name": name},
        }
        key = identifier("cmd")
        response = client.post(
            path, headers={**headers, "Idempotency-Key": key}, json=body
        )
        assert response.status_code == 201
        current = response.json["data"]
        assert current["previous_revision_id"] == chain[-1]["definition_revision_id"]
        assert current["definition_revision_id"] not in {
            row["definition_revision_id"] for row in chain
        }
        chain.append(current)
        commands.append((key, body, current, response.headers["Location"]))

    for row in chain:
        stored = client.get(
            routes.API
            + f"/twins/{original['twin_id']}/revisions/{row['definition_revision_id']}"
        )
        assert stored.status_code == 200 and stored.json["data"] == row
    for key, body, expected, location in commands:
        replay = client.post(
            path, headers={**headers, "Idempotency-Key": key}, json=body
        )
        assert replay.status_code == 201 and replay.json["data"] == expected
        assert replay.headers["Location"] == location
    assert (
        history[0].store.latest_revision(original["twin_id"])
        == chain[-1]["definition_revision_id"]
    )
    assert history[0].snapshot(snapshot["snapshot_id"], "actor") == snapshot


def test_bv36_actual_episode_pages_have_exact_totals_and_terminal_cursor(api, tmp_path):
    from catalog.digital_twin.tests.test_history import observation

    rows = [
        observation(1, "MAIN", "PROGRAM"),
        observation(2, "0", "TOOL_NUMBER"),
        observation(3, "ACTIVE"),
    ]
    rows.extend(
        observation(index, str(index - 3), "TOOL_NUMBER") for index in range(4, 1004)
    )
    rows.append(observation(1004, "READY"))
    _, _, snapshot = _bv_snapshot(api, tmp_path, rows=rows, window=(0, 1_005_000_000))
    client = api[1]
    base = routes.API + f"/snapshots/{snapshot['snapshot_id']}/timeline?limit=500"
    first = client.get(base).json["data"]
    second = client.get(base + "&cursor=" + first["next_cursor"]).json["data"]
    third = client.get(base + "&cursor=" + second["next_cursor"]).json["data"]
    assert [len(page["items"]) for page in (first, second, third)] == [500, 500, 1]
    assert [page["total"] for page in (first, second, third)] == [1001] * 3
    assert [page["has_more"] for page in (first, second, third)] == [True, True, False]
    assert third["next_cursor"] is None


def test_bv38_bv47_bv49_available_history_zero_active_and_lost_index(api, tmp_path):
    from catalog.digital_twin.tests.test_history import observation

    rows = [
        observation(1, "MAIN", "PROGRAM"),
        observation(2, "7", "TOOL_NUMBER"),
        observation(3, "FEED_HOLD"),
        observation(6, "FEED_HOLD"),
    ]
    # Sequence continuity is Agent-wide; retain intervening observations.
    rows[3:3] = [
        observation(4, "10", "SPINDLE_SPEED", category="SAMPLE"),
        observation(5, "11", "SPINDLE_SPEED", category="SAMPLE"),
    ]
    _, _, snapshot = _bv_snapshot(
        api, tmp_path, rows=rows, window=(3_000_000, 6_000_000)
    )
    assert snapshot["summary"]["active_us"] == 0
    assert snapshot["summary"]["active_share"] == 0
    assert snapshot["services"]["history"]["state"] == "AVAILABLE"
    assert snapshot["model_validity"] == "NOT_ASSESSED"
    assert snapshot["services"]["comparison"]["reason_codes"] == ["NOT_IN_RELEASE"]
    index = api[4][0].store.index
    index.rename(index.with_suffix(".preserved-lost-index"))
    response = api[1].get(routes.API + f"/snapshots/{snapshot['snapshot_id']}")
    assert response.status_code == 200
    assert response.json["data"]["services"] == snapshot["services"]
    assert response.json["meta"]["technical_state"] == "PARTIAL"
    assert response.json["meta"]["technical_reason_codes"] == ["VIEW_INDEX_UNAVAILABLE"]
    timeline = api[1].get(routes.API + f"/snapshots/{snapshot['snapshot_id']}/timeline")
    assert (
        timeline.status_code == 503
        and timeline.json["error"]["code"] == "VIEW_INDEX_UNAVAILABLE"
    )
    assert not index.exists()


def test_bv06_nested_required_fields_use_fixed_field_paths_without_reservation(
    route_contract,
):
    import copy

    api, _, writes = route_contract
    _, client, headers, _, history = api
    before = history[0].store.primary.read_bytes()
    # Nested paths are selected from the server schema, never echoed client keys.
    for index, prefix in (
        (1, "role_profile"),
        (2, "definition.role_profile"),
        (3, "window"),
    ):
        path, original = writes[index]
        for field in (
            ("start_at", "end_at")
            if prefix == "window"
            else ("observer", "analyst", "decision_maker", "executor")
        ):
            payload = copy.deepcopy(original)
            node = payload
            for part in prefix.split("."):
                node = node[part]
            node.pop(field)
            response = client.post(routes.API + path, headers=headers, json=payload)
            assert response.status_code == 400
            assert response.json["error"]["field"] == prefix + "." + field
            assert history[0].store.primary.read_bytes() == before


@pytest.mark.parametrize(
    "case,code",
    [
        ("empty-window", "INVALID_TIME_WINDOW"),
        ("two-datasets", "MULTI_DATASET_NOT_SUPPORTED"),
    ],
)
def test_bv08_bv10_no_reservation_or_implicit_dataset_union(route_contract, case, code):
    import copy

    api, _, writes = route_contract
    _, client, headers, _, history = api
    path, body = writes[3]
    payload = copy.deepcopy(body)
    if case == "empty-window":
        payload["window"]["end_at"] = payload["window"]["start_at"]
    else:
        payload["dataset_revision_ids"] *= 2
    before = history[0].store.primary.read_bytes()
    response = client.post(routes.API + path, headers=headers, json=payload)
    assert response.status_code == 422 and response.json["error"]["code"] == code
    assert history[0].store.primary.read_bytes() == before


def test_bv26_snapshot_replay_returns_original_location_and_rejects_changed_window(
    route_contract,
):
    import copy

    from catalog.digital_twin.contracts import timestamp

    api, reads, writes = route_contract
    _, client, headers, _, history = api
    path, body = writes[3]
    key = reads[-1][1]["Idempotency-Key"]
    before = len(history[0].store.objects("snapshot"))
    response = client.post(
        routes.API + path, headers={**headers, "Idempotency-Key": key}, json=body
    )
    assert response.status_code == 201
    assert (
        response.headers["Location"]
        == routes.API + "/snapshots/" + response.json["data"]["snapshot_id"]
    )
    payload = copy.deepcopy(body)
    payload["window"]["end_at"] = timestamp(9_000_000)
    conflict = client.post(
        routes.API + path, headers={**headers, "Idempotency-Key": key}, json=payload
    )
    assert (
        conflict.status_code == 409
        and conflict.json["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    )
    assert len(history[0].store.objects("snapshot")) == before


def test_bv25_concurrent_http_snapshot_commands_publish_exactly_once(route_contract):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    api, _, writes = route_contract
    app, client, headers, _, history = api
    path, body = writes[3]
    key = identifier("cmd")
    cookie = client.get_cookie(app.config["SESSION_COOKIE_NAME"])
    barrier = Barrier(2)
    before = len(history[0].store.objects("snapshot"))

    def send():
        local = app.test_client()
        local.set_cookie(app.config["SESSION_COOKIE_NAME"], cookie.value)
        barrier.wait(timeout=5)
        return local.post(
            routes.API + path, headers={**headers, "Idempotency-Key": key}, json=body
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda _: send(), range(2)))
    assert all(response.status_code in (201, 409) for response in responses)
    assert any(response.status_code == 201 for response in responses)
    for response in responses:
        if response.status_code == 409:
            assert response.json["error"]["code"] == "COMMAND_RUNNING"
    assert len(history[0].store.objects("snapshot")) == before + 1


def test_bv07_unknown_and_revoked_object_have_the_same_private_error(route_contract):
    from catalog.digital_twin.contracts import revision

    api, reads, _ = route_contract
    _, client, _, _, history = api
    unknown = client.get(
        routes.API + "/dataset-revisions/" + revision("unknown synthetic dataset")
    )
    history[2]["actor"] = False
    revoked = client.get(routes.API + reads[1][0])
    assert unknown.status_code == revoked.status_code == 404
    assert unknown.json["error"] == revoked.json["error"]


def test_bv44_operator_text_stays_in_authorized_json_and_never_reaches_network_or_logs(
    api, tmp_path, monkeypatch, caplog
):
    import requests

    from catalog.digital_twin.contracts import timestamp

    def no_network(*args, **kwargs):
        pytest.fail("Historical note handling attempted an external request")

    monkeypatch.setattr(requests.sessions.Session, "request", no_network)
    marker = "synthetic-secret <script>throw new Error('must remain text')</script>"
    _, _, snapshot = _bv_snapshot(
        api,
        tmp_path,
        records=[
            {"id": "note", "occurred_at": timestamp(4_000_000), "observation": marker}
        ],
    )
    client = api[1]
    sid = snapshot["snapshot_id"]
    row = client.get(routes.API + f"/snapshots/{sid}/timeline?view=operator").json[
        "data"
    ]["items"][0]
    record = client.get(routes.API + f"/snapshots/{sid}/records/{row['record_id']}")
    assert (
        record.status_code == 200
        and record.json["data"]["fields"]["observation"] == marker
    )
    evidence = client.get(
        routes.API + f"/snapshots/{sid}/evidence/{row['evidence_refs'][0]}"
    )
    assert (
        evidence.status_code == 200
        and "synthetic-secret" in evidence.json["data"]["content"]
    )
    assert evidence.mimetype == "application/json"
    assert "synthetic-secret" not in evidence.request.url
    assert "synthetic-secret" not in caplog.text
    page = client.get("/process-twins")
    assert "synthetic-secret" not in page.text and "cdn.jsdelivr.net" not in page.text
    assert page.headers["Referrer-Policy"] == "no-referrer"
