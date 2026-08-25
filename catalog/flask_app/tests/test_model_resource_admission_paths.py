from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest

from catalog.federation.host_resources import (
    DEFAULT_CRITICAL_FREE_BYTES,
    DEFAULT_PRESSURE_FREE_BYTES,
)
from catalog.flask_app.services import capability_ai_service
from catalog.flask_app.services.capability_config_service import CapabilityConfig
from catalog.flask_app.services.host_model_install_handoff import (
    HOST_OPERATION_STALE_SECONDS,
    MODEL_REQUEST_ID,
    HostModelInstallHandoff,
    MODEL_REQUEST_SCHEMA,
)

ROOT = Path(__file__).resolve().parents[3]


def _config(*, mode: str = "local") -> CapabilityConfig:
    return CapabilityConfig(
        ai_provider_mode=mode,
        ai_provider_name="Provider" if mode == "connected" else "",
        ai_profile="laptop-standard",
        ai_model="llama3.2:3b",
        ollama_base_url=(
            "http://192.168.1.50:11434" if mode == "connected" else "http://ollama:11434"
        ),
        recorder_sources="",
        recorder_poll_interval="0.2",
        recorder_include_condition=False,
        updated_at="2026-08-25T19:00:00Z",
    )


def test_browser_local_pull_is_a_declarative_host_request(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    ok, message = capability_ai_service.pull_ollama_model(_config())

    assert ok is True
    assert "queued" in message.lower()
    request_file = tmp_path / "data" / "federation" / "update-agent" / "request.json"
    request = json.loads(request_file.read_text(encoding="utf-8"))
    assert request["schema"] == MODEL_REQUEST_SCHEMA
    assert request["request_id"] == MODEL_REQUEST_ID == "host-model-install"
    assert request["action"] == "install"
    assert request["model"] == "llama3.2:3b"
    assert request["target"] == "ollama"
    assert "command" not in request
    assert "arguments" not in request


def test_connected_provider_is_never_mutated_from_the_consumer_host(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    ok, message = capability_ai_service.pull_ollama_model(_config(mode="connected"))

    assert ok is False
    assert "cannot enforce that host's disk reserve" in message
    assert not (tmp_path / "data" / "federation" / "update-agent" / "request.json").exists()


def test_model_handoff_serializes_with_pending_host_update_request(tmp_path) -> None:
    handoff = HostModelInstallHandoff(tmp_path)
    handoff.request_file.parent.mkdir(parents=True, exist_ok=True)
    handoff.request_file.write_text("{}", encoding="utf-8")

    ok, message = handoff.queue(model="llama3.2:3b")

    assert ok is False
    assert "already processing" in message


def test_model_handoff_does_not_queue_behind_active_host_operation(tmp_path) -> None:
    handoff = HostModelInstallHandoff(tmp_path)
    handoff.directory.mkdir(parents=True, exist_ok=True)
    processing = handoff.directory / "processing-active.json"
    processing.write_text("{}", encoding="utf-8")

    ok, message = handoff.queue(model="llama3.2:3b")

    assert ok is False
    assert "already processing" in message
    assert not handoff.request_file.exists()


def test_stale_processing_claim_cannot_permanently_fence_model_repair(tmp_path) -> None:
    handoff = HostModelInstallHandoff(tmp_path)
    handoff.directory.mkdir(parents=True, exist_ok=True)
    processing = handoff.directory / "processing-interrupted.json"
    processing.write_text("{}", encoding="utf-8")
    stale = 1.0
    os.utime(processing, (stale, stale))

    ok, message = handoff.queue(model="llama3.2:3b")

    assert HOST_OPERATION_STALE_SECONDS > 0
    assert ok is True
    assert "queued" in message.lower()
    assert handoff.request_file.exists()


def test_supported_model_install_paths_have_no_direct_pull_bypass() -> None:
    start_cmd = (ROOT / "start.cmd").read_text(encoding="utf-8")
    start_sh = (ROOT / "start.sh").read_text(encoding="utf-8")
    setup = (ROOT / "setup_fcp.py").read_text(encoding="utf-8")
    ai_service = (
        ROOT / "catalog/flask_app/services/capability_ai_service.py"
    ).read_text(encoding="utf-8")
    command_setup = (ROOT / "catalog/command_setup.py").read_text(encoding="utf-8")
    headless = (ROOT / "headless_fcp.py").read_text(encoding="utf-8")
    model_pull = (ROOT / "catalog/federation/model_resource_pull.py").read_text(
        encoding="utf-8"
    )

    assert "fcp_model_pull.ps1" in start_cmd
    assert "catalog.federation.model_resource_pull" in start_sh
    assert "admitted_model_pull" in setup
    assert "env=env" in setup
    assert "/api/pull" not in ai_service
    assert "/api/pull" not in command_setup
    assert "ollama-pull" not in start_cmd
    assert "ollama-pull" not in start_sh
    assert '"ollama-pull"' not in setup
    assert '"ollama-pull"' not in headless
    assert 'environment.setdefault("COMPOSE_PROJECT_NAME", "fcp")' in model_pull


def test_update_agents_treat_ollama_as_optional_and_under_host_admission() -> None:
    windows = (ROOT / "scripts/windows/fcp_update_agent.ps1").read_text(
        encoding="utf-8"
    )
    posix = (ROOT / "scripts/posix/fcp_update_agent.py").read_text(encoding="utf-8")

    assert "fcp.host-model-install-request.v1" in windows
    assert "fcp.host-model-install-request.v1" in posix
    assert "fcp_model_pull.ps1" in windows
    assert "catalog.federation.model_resource_pull" in posix
    assert "'compose', 'up', '-d', 'relay', 'recorder'" in windows
    assert '["docker", "compose", "up", "-d", "relay", "recorder"]' in posix
    assert "'compose', 'up', '-d', 'relay', 'ollama', 'recorder'" not in windows
    assert '"relay",\n                "ollama",\n                "recorder"' not in posix
    assert "AI remains optional" in windows
    assert "AI remains optional" in posix


def test_windows_and_python_model_runners_share_pressure_and_stop_contract() -> None:
    windows = (ROOT / "scripts/windows/fcp_model_pull.ps1").read_text(
        encoding="utf-8"
    )
    python_runner = (ROOT / "catalog/federation/model_resource_pull.py").read_text(
        encoding="utf-8"
    )

    assert "$CriticalFreeBytes = [int64]10737418240" in windows
    assert "$PressureFreeBytes = [int64]12884901888" in windows
    assert DEFAULT_CRITICAL_FREE_BYTES == 10737418240
    assert DEFAULT_PRESSURE_FREE_BYTES == 12884901888

    assert "docker_data.vhdx" in windows
    assert "ext4.vhdx" in windows
    assert "Get-FreeBytes $backingPath" in windows
    assert "return [int64]-1" in windows
    assert "Test-ModelWriterStopped" in windows
    assert "'compose', 'stop', '--timeout', '5', $Service" in windows
    assert "'compose', 'stop', '--timeout', '15', $Service" in windows
    assert "'compose', 'kill'" not in windows
    assert "could not prove" in windows
    assert "exit 3" in windows

    assert "{{.DockerRootDir}}" in python_runner
    assert "Docker.raw" in python_runner
    assert "docker_data.vhdx" in python_runner
    assert "writer_stop_unverified" in python_runner
    assert '["docker", "compose", "stop", "--timeout", "15", target.service]' in python_runner
    assert '["docker", "compose", "kill", target.service]' not in python_runner


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell parser check")
def test_windows_model_pull_script_has_valid_powershell_syntax() -> None:
    path = ROOT / "scripts" / "windows" / "fcp_model_pull.ps1"
    escaped_path = str(path).replace("'", "''")
    command = (
        "$errors = $null; "
        "[System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped_path}', [ref]$null, [ref]$errors) | Out-Null; "
        "if ($errors.Count -gt 0) { "
        "$errors | ForEach-Object { Write-Error $_.Message }; exit 1 }"
    )
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
