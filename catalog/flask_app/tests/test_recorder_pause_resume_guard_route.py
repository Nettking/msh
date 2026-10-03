from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from flask import Flask

from catalog.flask_app import server_setup_routes as routes
from catalog.flask_app.services.recorder_control_service import RecorderControlService


def _armed_service(tmp_path) -> tuple[RecorderControlService, dict[str, str]]:
    service = RecorderControlService(
        control_path=tmp_path / "control.json",
        status_path=tmp_path / "status.json",
        log_path=tmp_path / "recorder.log",
    )
    token = secrets.token_urlsafe(32)
    operation_id = "a" * 32
    binding_sha256 = "b" * 64
    service.control_path.parent.mkdir(parents=True, exist_ok=True)
    service.control_path.write_text(
        __import__("json").dumps(
            {
                "schema": "fcp.mtconnect_recorder.control.v1",
                "enabled": False,
                "operation_id": operation_id,
                "resume_guard": {
                    "operation_id": operation_id,
                    "prior_control_operation_id": "c" * 32,
                    "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
                    "runtime_binding_sha256": binding_sha256,
                    "deadline_utc": (
                        datetime.now(timezone.utc) + timedelta(minutes=10)
                    ).isoformat(),
                },
            }
        ),
        encoding="utf-8",
    )
    return service, {
        "X-FCP-Recorder-Resume-Token": token,
        "X-FCP-Recorder-Resume-Operation": operation_id,
        "X-FCP-Recorder-Runtime-Binding-SHA256": binding_sha256,
    }


def test_one_time_guard_passes_only_for_local_start_route(
    tmp_path, monkeypatch
) -> None:
    service, headers = _armed_service(tmp_path)
    monkeypatch.setattr(routes, "get_recorder_control_service", lambda: service)
    app = Flask(__name__)

    with app.test_request_context(
        "/server-setup/recording/start",
        method="POST",
        headers=headers,
        environ_base={"REMOTE_ADDR": "127.0.0.1"},
    ):
        assert routes._is_local_resume_guard_request()
        routes._require_setup_csrf()

    with app.test_request_context(
        "/server-setup/recording/start",
        method="POST",
        headers={**headers, "Host": "recorder.example"},
        environ_base={"REMOTE_ADDR": "192.168.1.20"},
    ):
        assert not routes._is_local_resume_guard_request()

    with app.test_request_context(
        "/server-setup/recording/stop",
        method="POST",
        headers=headers,
        environ_base={"REMOTE_ADDR": "127.0.0.1"},
    ):
        assert not routes._is_local_resume_guard_request()


def test_host_loopback_guard_accepts_docker_bridge_peer_with_bound_token(
    tmp_path, monkeypatch
) -> None:
    service, headers = _armed_service(tmp_path)
    monkeypatch.setattr(routes, "get_recorder_control_service", lambda: service)
    app = Flask(__name__)

    with app.test_request_context(
        "/server-setup/recording/start",
        method="POST",
        headers={**headers, "Host": "127.0.0.1:5000"},
        environ_base={"REMOTE_ADDR": "172.20.0.1"},
    ):
        assert routes._is_local_resume_guard_request()


def test_guard_route_credential_stops_working_after_control_owner_changes(
    tmp_path, monkeypatch
) -> None:
    service, headers = _armed_service(tmp_path)
    service.set_enabled(True, type("C", (), {"recorder_sources": "IG=http://127.0.0.1:5000"})())
    monkeypatch.setattr(routes, "get_recorder_control_service", lambda: service)
    app = Flask(__name__)

    with app.test_request_context(
        "/server-setup/recording/start",
        method="POST",
        headers=headers,
        environ_base={"REMOTE_ADDR": "127.0.0.1"},
    ):
        assert not routes._is_local_resume_guard_request()


def test_supported_start_route_consumes_runtime_bound_guard_and_changes_control(
    tmp_path, monkeypatch
) -> None:
    service, headers = _armed_service(tmp_path)
    config = SimpleNamespace(recorder_sources="IG=http://127.0.0.1:5000")
    monkeypatch.setattr(routes, "get_recorder_control_service", lambda: service)
    monkeypatch.setattr(routes, "load_capability_config", lambda: config)
    monkeypatch.setattr(routes, "_recorder_authorized", lambda: True)
    app = Flask(__name__)
    app.register_blueprint(routes.server_setup_web)

    response = app.test_client().post(
        "/server-setup/recording/start",
        data={"next": "/"},
        headers={**headers, "Accept": "application/json"},
        environ_base={"REMOTE_ADDR": "127.0.0.1"},
    )

    assert response.status_code == 200
    assert response.json["ok"] is True
    assert response.json["requested_enabled"] is True
    assert response.json["operation_id"] != "a" * 32
    control = __import__("json").loads(service.control_path.read_text(encoding="utf-8"))
    assert control["enabled"] is True
    assert control["requested_by"] == "automatic-resume-guard"
    assert "resume_guard" not in control


def test_supported_start_route_keeps_current_authority_check(
    tmp_path, monkeypatch
) -> None:
    service, headers = _armed_service(tmp_path)
    monkeypatch.setattr(routes, "get_recorder_control_service", lambda: service)
    monkeypatch.setattr(routes, "load_capability_config", lambda: SimpleNamespace(
        recorder_sources="IG=http://127.0.0.1:5000"
    ))
    monkeypatch.setattr(routes, "_recorder_authorized", lambda: False)
    app = Flask(__name__)
    app.register_blueprint(routes.server_setup_web)

    response = app.test_client().post(
        "/server-setup/recording/start",
        data={"next": "/"},
        headers={**headers, "Accept": "application/json"},
        environ_base={"REMOTE_ADDR": "127.0.0.1"},
    )

    assert response.status_code == 400
    assert response.json["ok"] is False
    control = __import__("json").loads(service.control_path.read_text(encoding="utf-8"))
    assert control["enabled"] is False
    assert control["operation_id"] == "a" * 32
