from __future__ import annotations

import json
import hashlib
import secrets
from datetime import datetime, timedelta, timezone

import pytest

from catalog.flask_app.services.capability_config_service import CapabilityConfig
from catalog.flask_app.services.recorder_control_service import (
    RecorderControlError,
    RecorderControlService,
)


def _config(
    *,
    sources: str = "IG500=http://192.168.200.251:5000/current",
) -> CapabilityConfig:
    return CapabilityConfig(
        ai_provider_mode="local",
        ai_provider_name="This computer",
        ai_profile="laptop-standard",
        ai_model="llama3.2:3b",
        ollama_base_url="http://ollama:11434",
        recorder_sources=sources,
        recorder_poll_interval="0.2",
        recorder_include_condition=False,
        updated_at="test",
    )


def _service(tmp_path) -> RecorderControlService:
    return RecorderControlService(
        control_path=tmp_path / "control.json",
        status_path=tmp_path / "status.json",
        log_path=tmp_path / "recorder.log",
    )


def test_start_and_stop_write_durable_desired_state(tmp_path) -> None:
    service = _service(tmp_path)
    config = _config()

    ok, _ = service.set_enabled(True, config)
    assert ok is True
    start = json.loads(service.control_path.read_text(encoding="utf-8"))
    assert start["enabled"] is True
    assert start["operation_id"]

    ok, _ = service.set_enabled(False, config)
    assert ok is True
    stop = json.loads(service.control_path.read_text(encoding="utf-8"))
    assert stop["enabled"] is False
    assert stop["operation_id"]
    assert stop["operation_id"] != start["operation_id"]


def test_start_requires_recorder_source_not_legacy_role(tmp_path) -> None:
    service = _service(tmp_path)

    with pytest.raises(RecorderControlError, match="MTConnect source"):
        service.set_enabled(True, _config(sources=""))

    roleless = type(
        "RolelessRecorderConfig",
        (),
        {"recorder_sources": "IG500=http://192.168.200.251:5000"},
    )()
    ok, _ = service.set_enabled(True, roleless)
    assert ok is True


def test_status_reports_recording_from_fresh_worker_heartbeat(tmp_path) -> None:
    service = _service(tmp_path)
    config = _config()
    service.set_enabled(True, config)

    service.status_path.write_text(
        json.dumps(
            {
                "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                "state": "recording",
                "message": "Polling one source.",
                "sources": ["IG500"],
                "records_written": 12,
                "records_buffered": 1,
            }
        ),
        encoding="utf-8",
    )

    status = service.status(config)

    assert status["worker_alive"] is True
    assert status["requested_enabled"] is True
    assert status["running"] is True
    assert status["state"] == "recording"
    assert status["records_written"] == 12


def test_status_distinguishes_requested_recording_from_offline_worker(tmp_path) -> None:
    service = _service(tmp_path)
    config = _config()
    service.set_enabled(True, config)

    status = service.status(config)

    assert status["requested_enabled"] is True
    assert status["worker_alive"] is False
    assert status["running"] is False
    assert status["state"] == "offline"


def test_disabled_request_is_not_reported_stopped_without_correlated_drain_ack(
    tmp_path,
) -> None:
    service = _service(tmp_path)
    config = _config()
    service.set_enabled(False, config)
    control = json.loads(service.control_path.read_text(encoding="utf-8"))
    service.status_path.write_text(
        json.dumps(
            {
                "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                "state": "stopped",
                "records_buffered": 0,
                "last_flush_at": "2026-10-03T01:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    status = service.status(config)

    assert status["state"] == "draining"
    assert status["pause_proven"] is False
    assert status["control_operation_id"] == control["operation_id"]


def test_pause_requires_exact_ack_no_scheduling_zero_inflight_and_durable_boundary(
    tmp_path,
) -> None:
    service = _service(tmp_path)
    config = _config()
    service.set_enabled(False, config)
    control = json.loads(service.control_path.read_text(encoding="utf-8"))
    operation_id = control["operation_id"]
    service.status_path.write_text(
        json.dumps(
            {
                "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                "state": "stopped",
                "capture_control": {
                    "operation_id": operation_id,
                    "requested_enabled": False,
                    "capture_scheduling": False,
                    "inflight_capture_tasks": 0,
                    "acknowledged_operation_id": operation_id,
                    "acknowledged_at": datetime.now(timezone.utc).isoformat(),
                    "durable_boundary": True,
                },
            }
        ),
        encoding="utf-8",
    )

    status = service.status(config)
    assert status["state"] == "stopped"
    assert status["pause_proven"] is True

    payload = json.loads(service.status_path.read_text(encoding="utf-8"))
    payload["capture_control"]["operation_id"] = "another-pause"
    service.status_path.write_text(json.dumps(payload), encoding="utf-8")
    assert service.status(config)["pause_proven"] is False

    payload["capture_control"]["operation_id"] = operation_id
    payload["capture_control"]["inflight_capture_tasks"] = True
    service.status_path.write_text(json.dumps(payload), encoding="utf-8")
    assert service.status(config)["pause_proven"] is False

    payload["capture_control"]["inflight_capture_tasks"] = 0
    payload["capture_control"]["durable_boundary"] = False
    service.status_path.write_text(json.dumps(payload), encoding="utf-8")
    assert service.status(config)["pause_proven"] is False


def test_resume_guard_is_one_time_runtime_bound_and_compare_and_set(tmp_path) -> None:
    service = _service(tmp_path)
    config = _config()
    token = secrets.token_urlsafe(32)
    operation_id = "a" * 32
    prior_operation_id = "c" * 32
    binding_sha256 = "b" * 64
    guard = {
        "operation_id": operation_id,
        "prior_control_operation_id": prior_operation_id,
        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "runtime_binding_sha256": binding_sha256,
        "deadline_utc": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
    }

    service.set_enabled(True, config, operation_id=prior_operation_id)
    service.set_enabled(
        False,
        config,
        operation_id=operation_id,
        expected_current_operation_id=prior_operation_id,
        resume_guard=guard,
    )

    assert service.validate_resume_guard(
        token=token,
        operation_id=operation_id,
        runtime_binding_sha256=binding_sha256,
    )
    assert not service.validate_resume_guard(
        token=token,
        operation_id=operation_id,
        runtime_binding_sha256="c" * 64,
    )
    service.set_enabled(
        True,
        config,
        expected_pause_operation_id=operation_id,
        resume_guard_token=token,
        runtime_binding_sha256=binding_sha256,
    )
    resumed = json.loads(service.control_path.read_text(encoding="utf-8"))
    assert resumed["enabled"] is True
    assert resumed["operation_id"] != operation_id
    assert "resume_guard" not in resumed
    assert not service.validate_resume_guard(
        token=token,
        operation_id=operation_id,
        runtime_binding_sha256=binding_sha256,
    )


def test_resume_guard_cannot_override_a_newer_operator_decision(tmp_path) -> None:
    service = _service(tmp_path)
    config = _config()
    token = secrets.token_urlsafe(32)
    operation_id = "d" * 32
    prior_operation_id = "f" * 32
    binding_sha256 = "e" * 64
    guard = {
        "operation_id": operation_id,
        "prior_control_operation_id": prior_operation_id,
        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "runtime_binding_sha256": binding_sha256,
        "deadline_utc": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
    }
    service.set_enabled(True, config, operation_id=prior_operation_id)
    service.set_enabled(
        False,
        config,
        operation_id=operation_id,
        expected_current_operation_id=prior_operation_id,
        resume_guard=guard,
    )
    service.set_enabled(True, config)
    newer_decision = json.loads(service.control_path.read_text(encoding="utf-8"))

    with pytest.raises(RecorderControlError, match="ownership changed"):
        service.set_enabled(
            True,
            config,
            expected_pause_operation_id=operation_id,
            resume_guard_token=token,
            runtime_binding_sha256=binding_sha256,
        )

    assert json.loads(service.control_path.read_text(encoding="utf-8")) == newer_decision


def test_prepared_pause_cannot_overwrite_a_newer_control_operation(tmp_path) -> None:
    service = _service(tmp_path)
    config = _config()
    original_operation_id = "1" * 32
    planned_operation_id = "2" * 32
    service.set_enabled(True, config, operation_id=original_operation_id)
    service.set_enabled(True, config)
    newer_decision = json.loads(service.control_path.read_text(encoding="utf-8"))

    with pytest.raises(RecorderControlError, match="ownership changed"):
        service.set_enabled(
            False,
            config,
            operation_id=planned_operation_id,
            expected_current_operation_id=original_operation_id,
        )

    assert json.loads(service.control_path.read_text(encoding="utf-8")) == newer_decision


def test_reused_control_operation_identity_is_rejected(tmp_path) -> None:
    service = _service(tmp_path)
    config = _config()
    operation_id = "3" * 32
    service.set_enabled(True, config, operation_id=operation_id)

    with pytest.raises(RecorderControlError, match="identities must be unique"):
        service.set_enabled(False, config, operation_id=operation_id)

    control = json.loads(service.control_path.read_text(encoding="utf-8"))
    assert control["enabled"] is True
    assert control["operation_id"] == operation_id


def test_status_keeps_configured_sources_visible_while_offline(tmp_path) -> None:
    service = _service(tmp_path)
    service.status_path.write_text(
        json.dumps(
            {
                "source_status": {
                    "REMOVED-MACHINE": {
                        "base_url": "http://192.168.200.250:5000",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    status = service.status(
        _config(
            sources=(
                "DEMO01=http://192.168.200.101:5000;"
                "MAZAK-M7ZDA13010Z=http://192.168.200.249:5000"
            )
        )
    )

    assert list(status["source_status"]) == [
        "DEMO01",
        "MAZAK-M7ZDA13010Z",
    ]
    assert status["source_status"]["DEMO01"]["state"] == "offline"
    assert status["source_status"]["DEMO01"]["base_url"] == (
        "http://192.168.200.101:5000"
    )


def test_status_normalizes_malformed_runtime_values(tmp_path) -> None:
    service = _service(tmp_path)
    service.status_path.write_text(
        json.dumps(
            {
                "records_written": "unknown",
                "records_buffered": {"bad": "value"},
                "source_status": {
                    "IG500": {
                        "next_sequence": "not-a-number",
                        "agent_last_sequence": [],
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    status = service.status(_config())

    assert status["records_written"] == 0
    assert status["records_buffered"] is None
    assert status["source_status"]["IG500"]["next_sequence"] is None
    assert status["source_status"]["IG500"]["agent_last_sequence"] is None


def test_web_status_excludes_debug_paths_and_log_tail(tmp_path) -> None:
    service = _service(tmp_path)
    service.log_path.write_text("secret diagnostic text", encoding="utf-8")

    payload = service.web_status(_config())

    assert payload["schema"] == "fcp.recorder.web_status.v1"
    assert payload["poll_after_ms"] == 2000
    assert payload["sources"][0]["source_name"] == "IG500"
    assert "control_path" not in payload
    assert "status_path" not in payload
    assert "log_path" not in payload
    assert "log_tail" not in payload
