from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from catalog.flask_app.services.recorder_control_service import (
    RecorderControlError,
    RecorderControlService,
)
from catalog.flask_app.services.server_setup_service import ServerSetupSettings


def _settings(
    *,
    mode: str = "full-server",
    sources: str = "IG500=http://192.168.200.251:5000/current",
) -> ServerSetupSettings:
    return ServerSetupSettings(
        configured=True,
        user_setup_complete=True,
        deployment_mode=mode,
        ai_enabled=False,
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
    settings = _settings()

    ok, _ = service.set_enabled(True, settings)
    assert ok is True
    assert json.loads(service.control_path.read_text(encoding="utf-8"))["enabled"] is True

    ok, _ = service.set_enabled(False, settings)
    assert ok is True
    assert json.loads(service.control_path.read_text(encoding="utf-8"))["enabled"] is False


def test_start_requires_recorder_role_and_source(tmp_path) -> None:
    service = _service(tmp_path)

    with pytest.raises(RecorderControlError):
        service.set_enabled(True, _settings(mode="web-workbench"))

    with pytest.raises(RecorderControlError):
        service.set_enabled(True, _settings(sources=""))


def test_status_reports_recording_from_fresh_worker_heartbeat(tmp_path) -> None:
    service = _service(tmp_path)
    settings = _settings()
    service.set_enabled(True, settings)

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

    status = service.status(settings)

    assert status["worker_alive"] is True
    assert status["requested_enabled"] is True
    assert status["running"] is True
    assert status["state"] == "recording"
    assert status["records_written"] == 12


def test_status_distinguishes_requested_recording_from_offline_worker(tmp_path) -> None:
    service = _service(tmp_path)
    settings = _settings()
    service.set_enabled(True, settings)

    status = service.status(settings)

    assert status["requested_enabled"] is True
    assert status["worker_alive"] is False
    assert status["running"] is False
    assert status["state"] == "offline"


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
        _settings(
            sources=(
                "M8015RW221N=http://192.168.200.101:5000;"
                "MAZAK-M7ZDA13010Z=http://192.168.200.249:5000"
            )
        )
    )

    assert list(status["source_status"]) == [
        "M8015RW221N",
        "MAZAK-M7ZDA13010Z",
    ]
    assert status["source_status"]["M8015RW221N"]["state"] == "offline"
    assert status["source_status"]["M8015RW221N"]["base_url"] == (
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

    status = service.status(_settings())

    assert status["records_written"] == 0
    assert status["records_buffered"] == 0
    assert status["source_status"]["IG500"]["next_sequence"] is None
    assert (
        status["source_status"]["IG500"]["agent_last_sequence"] is None
    )


def test_web_status_excludes_debug_paths_and_log_tail(tmp_path) -> None:
    service = _service(tmp_path)
    service.log_path.write_text("secret diagnostic text", encoding="utf-8")

    payload = service.web_status(_settings())

    assert payload["schema"] == "msh.recorder.web_status.v1"
    assert payload["poll_after_ms"] == 2000
    assert payload["sources"][0]["source_name"] == "IG500"
    assert "control_path" not in payload
    assert "status_path" not in payload
    assert "log_path" not in payload
    assert "log_tail" not in payload
