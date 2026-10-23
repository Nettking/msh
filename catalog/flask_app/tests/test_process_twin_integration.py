"""Real app, login and existing inventory; every file and user is synthetic."""

from __future__ import annotations

import json
import os
import re
from types import SimpleNamespace

import pytest
from flask_security import hash_password

from catalog.digital_twin.contracts import identifier
from catalog.digital_twin.tests.test_history import prepare_request, seed_canonical
from catalog.flask_app import app as application
from catalog.flask_app import capability_product_routes, process_twin_routes
from catalog.flask_app.auth.models import Role, User, db
from catalog.flask_app.auth.policy import audit_route_policy, is_process_twin_path
from catalog.flask_app.conftest import owned_app_workers


@pytest.fixture
def real_app(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for key in tuple(os.environ):
        if key.startswith(("FCP_", "CAPABILITY_", "FEDERATED_")):
            monkeypatch.delenv(key)
    monkeypatch.setenv(
        "FCP_FLASK_SECRET", "synthetic-isolated-session-secret-0000000000"
    )
    monkeypatch.setenv(
        "FCP_PASSWORD_SALT", "synthetic-isolated-password-salt-0000000000"
    )
    monkeypatch.setenv("FCP_AUTH_SECRET_DIR", str(tmp_path / "auth"))
    monkeypatch.setenv("FCP_AUTH_DATABASE", str(tmp_path / "auth" / "users.sqlite3"))
    monkeypatch.setenv("FCP_DATA_ROOT", str(tmp_path / "data"))
    # App construction must not alter the process-wide runtime/identity supplier.
    monkeypatch.setattr(
        application,
        "get_runtime_manager",
        lambda: SimpleNamespace(
            mark_app_started=lambda: None, requires_startup_choice=lambda: False
        ),
    )
    monkeypatch.setattr(
        application, "register_identity_supplier", lambda supplier: None
    )
    with owned_app_workers():
        app = application.create_app()
        app.config.update(TESTING=True)
        app.config["FCP_PROCESS_TWIN_STORE"] = tmp_path / "private-twins"
        with app.app_context():
            role = db.session.query(Role).filter_by(name="operator").one()
            user = User(
                email="synthetic@example.com",
                password=hash_password("Synthetic-test-password-42!"),
                active=True,
                fs_uniquifier=identifier("row"),
                roles=[role],
            )
            db.session.add(user)
            db.session.commit()
            user_id = user.id
        yield app, user_id
        with app.app_context():
            db.session.remove()
            db.engine.dispose()


def login(app):
    client = app.test_client()
    response = client.get("/login")
    token = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', response.text)[1]
    response = client.post(
        "/login",
        data={
            "email": "synthetic@example.com",
            "password": "Synthetic-test-password-42!",
            "csrf_token": token,
        },
    )
    assert response.status_code == 302
    page = client.get("/process-twins")
    assert page.status_code == 200
    token = re.search(r'data-csrf="([^"]+)"', page.text)[1]
    return client, {"X-CSRFToken": token, "Idempotency-Key": identifier("cmd")}


def test_real_app_route_inventory_and_get_do_not_start_runtime(real_app, monkeypatch):
    app, _ = real_app
    client, _ = login(app)

    def forbidden(*args, **kwargs):
        pytest.fail("Historical request tried to access runtime or recorder status")

    monkeypatch.setattr(application, "get_runtime_manager", forbidden)
    monkeypatch.setattr(application, "run_startup_contribution_reconcile", forbidden)
    monkeypatch.setattr(
        capability_product_routes, "get_recorder_control_service", forbidden
    )
    assert client.get("/process-twins").status_code == 200
    assert client.get("/api/process-twins/v1/inputs").status_code == 503
    assert not app.config["FCP_PROCESS_TWIN_STORE"].exists()
    assert not audit_route_policy(app)
    assert is_process_twin_path("/api/process-twins/v1/not-supported")
    assert not is_process_twin_path("/api/process-twins/v10")


def test_real_inventory_current_auth_revoke_rolls_back_publication(
    real_app, tmp_path, monkeypatch
):
    app, user_id = real_app
    rows, canonical, operational = seed_canonical(tmp_path / "data" / "source")
    entry = {
        "input_id": identifier("in"),
        "machine_id": "synthetic",
        "kind": "CANONICAL_DEVICE",
        "relative_paths": [
            str(p.relative_to(tmp_path / "data"))
            for p in (canonical.database_path, operational.database_path)
        ],
        "binding": {
            "source_key": rows[0].source_key,
            "agent_instance_id": 1,
            "device_key": "uuid:device-synthetic",
        },
        "clock_binding": "UTC_NORMALIZED",
        "origin_status": "KNOWN",
    }
    inventory = tmp_path / "data" / "source_config" / "machines_and_sensors.json"
    inventory.parent.mkdir(parents=True)
    inventory.write_text(
        json.dumps(
            {
                "schema": "fcp.source_inventory.v1",
                "machines": [{"id": "synthetic", "name": "Synthetic process"}],
                "process_twin_inputs": [entry],
            }
        ),
        encoding="utf-8",
    )
    assert app.test_cli_runner().invoke(args=["process-twin-init"]).exit_code == 0
    client, headers = login(app)
    source = client.get("/api/process-twins/v1/inputs").json["data"]["items"][0]
    request = prepare_request(SimpleNamespace(**source))
    original = process_twin_routes.ProcessTwinService._perform

    def revoke_after_read(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        # A separate DB transaction models an administrator's concurrent revoke.
        with db.engine.begin() as connection:
            connection.execute(
                db.text("DELETE FROM roles_users WHERE user_id=:id"), {"id": user_id}
            )
        return result

    monkeypatch.setattr(
        process_twin_routes.ProcessTwinService, "_perform", revoke_after_read
    )
    response = client.post(
        "/api/process-twins/v1/dataset-revisions", headers=headers, json=request
    )
    assert response.status_code == 404
    from catalog.digital_twin.records import TwinStore

    store = TwinStore(app.config["FCP_PROCESS_TWIN_STORE"])
    with store.connection() as connection:
        assert connection.execute("SELECT count(*) FROM objects").fetchone()[0] == 0
        assert (
            connection.execute("SELECT status FROM commands").fetchone()[0] == "FAILED"
        )
