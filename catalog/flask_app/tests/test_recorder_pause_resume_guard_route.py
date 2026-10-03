from __future__ import annotations

import hashlib
import secrets
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from flask import Flask

from catalog.flask_app import server_setup_routes as routes
from catalog.flask_app.services.recorder_control_service import RecorderControlService
from catalog.mtconnect_recorder import runtime as recorder_runtime


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


def test_guard_credential_revalidation_race_never_falls_through_to_plain_start(
    tmp_path, monkeypatch
) -> None:
    service, headers = _armed_service(tmp_path)
    config = SimpleNamespace(recorder_sources="IG=http://127.0.0.1:5000")
    monkeypatch.setattr(routes, "get_recorder_control_service", lambda: service)
    monkeypatch.setattr(routes, "load_capability_config", lambda: config)
    monkeypatch.setattr(routes, "_recorder_authorized", lambda: True)
    validations = iter((True, False))
    monkeypatch.setattr(
        routes, "_is_local_resume_guard_request", lambda: next(validations)
    )
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


def test_supported_stop_and_guarded_start_drive_recorder_worker(
    tmp_path, monkeypatch
) -> None:
    control_path = tmp_path / "control.json"
    status_path = tmp_path / "status.json"
    service = RecorderControlService(
        control_path=control_path,
        status_path=status_path,
        log_path=tmp_path / "recorder.log",
    )
    config = SimpleNamespace(recorder_sources="MACHINE-ALPHA=http://agent.invalid:5000")
    prior_operation_id = "b" * 32
    pause_operation_id = "a" * 32
    runtime_binding_sha256 = "c" * 64
    resume_token = secrets.token_urlsafe(32)
    guard_deadline = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    service.set_enabled(True, config, operation_id=prior_operation_id)

    monkeypatch.setattr(routes, "get_recorder_control_service", lambda: service)
    monkeypatch.setattr(routes, "load_capability_config", lambda: config)
    monkeypatch.setattr(routes, "_recorder_authorized", lambda: True)
    monkeypatch.setattr(routes, "_require_setup_csrf", lambda: None)
    monkeypatch.setattr(recorder_runtime, "MANAGED_MODE", True)
    monkeypatch.setattr(recorder_runtime, "CONTROL_FILE", control_path)
    monkeypatch.setattr(recorder_runtime, "CONFIG_FILE", tmp_path / "config.json")
    monkeypatch.setattr(recorder_runtime, "STATUS_FILE", status_path)
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(
        recorder_runtime,
        "_managed_configuration",
        lambda _payload: ({"MACHINE-ALPHA": "http://agent.invalid:5000"}, 0.2),
    )

    recorder = recorder_runtime.RecorderRuntime()
    capture_started = threading.Event()
    monkeypatch.setattr(
        recorder,
        "capture_source",
        lambda source, _url: capture_started.set()
        or recorder_runtime.CaptureResult(source, True, "", transaction_complete=True),
    )
    app = Flask(__name__)
    app.register_blueprint(routes.server_setup_web)
    client = app.test_client()

    try:
        recorder.refresh_configuration(force=True)
        stop_response = client.post(
            "/server-setup/recording/stop",
            data={
                "next": "/",
                "operation_id": pause_operation_id,
                "expected_control_operation_id": prior_operation_id,
                "resume_guard_token_sha256": hashlib.sha256(
                    resume_token.encode()
                ).hexdigest(),
                "resume_guard_runtime_binding_sha256": runtime_binding_sha256,
                "resume_guard_deadline_utc": guard_deadline,
            },
            headers={"Accept": "application/json"},
        )
        assert stop_response.status_code == 200
        recorder.refresh_configuration(force=True)
        recorder.acknowledge_capture_pause()
        recorder.publish_status(force=True)
        paused = __import__("json").loads(status_path.read_text(encoding="utf-8"))
        assert paused["capture_control"]["acknowledged_operation_id"] == pause_operation_id
        assert paused["capture_control"]["durable_boundary"] is True
        assert recorder.capture_schedule_count == 0

        start_response = client.post(
            "/server-setup/recording/start",
            data={"next": "/"},
            headers={
                "Accept": "application/json",
                "X-FCP-Recorder-Resume-Token": resume_token,
                "X-FCP-Recorder-Resume-Operation": pause_operation_id,
                "X-FCP-Recorder-Runtime-Binding-SHA256": runtime_binding_sha256,
            },
            environ_base={"REMOTE_ADDR": "127.0.0.1"},
        )
        assert start_response.status_code == 200
        assert start_response.json["ok"] is True

        recorder.refresh_configuration(force=True)
        recorder.run_fetch_cycle()
        assert capture_started.wait(timeout=2)
        assert recorder.capture_schedule_count == 1
        for _base_url, future in list(recorder._capture_futures.values()):
            future.result(timeout=2)
        recorder._harvest_capture_results()
        assert recorder._capture_outcomes["MACHINE-ALPHA"] is True
    finally:
        recorder.executor.shutdown(wait=True, cancel_futures=False)
        recorder_runtime.unregister_stop_target(recorder)
