from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

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
    MODEL_REQUEST_SCHEMA,
    HostModelInstallHandoff,
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


@pytest.fixture
def services_model_install_app(tmp_path, monkeypatch):
    from catalog.flask_app.app import create_app
    from catalog.flask_app.services.capability_config_service import (
        save_capability_config,
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FCP_SKIP_ORCHESTRATION", "1")
    save_capability_config(_config())
    app = create_app()
    app.config.update(TESTING=True)
    return app, app.test_client(), tmp_path / "data/federation/update-agent/request.json"


def _rendered_model_install_token(client) -> str:
    response = client.get("/federation/services")
    assert response.status_code == 200
    form = re.search(
        r'<form\b[^>]*action="/server-setup/pull-model"[^>]*>.*?</form>',
        response.get_data(as_text=True),
        flags=re.DOTALL,
    )
    assert form is not None
    assert 'method="post"' in form.group()
    token = re.search(r'name="_csrf_token"\s+value="([^"]+)"', form.group())
    assert token is not None
    with client.session_transaction() as browser_session:
        assert token.group(1) == browser_session["mtconnect_discovery_csrf_token"]
    return token.group(1)


def test_services_model_install_get_renders_local_form_without_installing(
    services_model_install_app, monkeypatch,
) -> None:
    from catalog.flask_app import server_setup_routes

    _app, client, request_file = services_model_install_app

    def unexpected_install(*_args, **_kwargs):
        raise AssertionError("GET must never install or queue a model")

    monkeypatch.setattr(server_setup_routes, "pull_ollama_model", unexpected_install)
    monkeypatch.setattr(HostModelInstallHandoff, "queue", unexpected_install)

    assert _rendered_model_install_token(client)
    page = client.get("/federation/services").get_data(as_text=True)
    assert "Install llama3.2:3b" in page
    assert not request_file.exists()


def test_services_model_install_connected_provider_has_no_install_form(
    services_model_install_app,
) -> None:
    from catalog.flask_app.services.capability_config_service import (
        save_capability_config,
    )

    _app, client, request_file = services_model_install_app
    save_capability_config(_config(mode="connected"))

    response = client.get("/federation/services")

    assert response.status_code == 200
    assert b'action="/server-setup/pull-model"' not in response.data
    assert not request_file.exists()


def test_services_model_install_normal_form_queues_configured_model(
    services_model_install_app,
) -> None:
    _app, client, request_file = services_model_install_app
    token = _rendered_model_install_token(client)
    assert not request_file.exists()

    response = client.post("/server-setup/pull-model", data={"_csrf_token": token})

    assert response.status_code == 303
    assert response.location == "/federation"
    request = json.loads(request_file.read_text(encoding="utf-8"))
    assert request["schema"] == MODEL_REQUEST_SCHEMA
    assert request["request_id"] == MODEL_REQUEST_ID
    assert request["action"] == "install"
    assert request["model"] == "llama3.2:3b"
    assert request["target"] == "ollama"
    assert "command" not in request
    assert "arguments" not in request


@pytest.mark.parametrize("supplied_token", [None, "wrong-token"])
def test_services_model_install_rejects_missing_or_wrong_csrf(
    services_model_install_app, supplied_token,
) -> None:
    _app, client, request_file = services_model_install_app
    _rendered_model_install_token(client)
    data = {} if supplied_token is None else {"_csrf_token": supplied_token}

    response = client.post("/server-setup/pull-model", data=data)

    assert response.status_code == 303
    assert not request_file.exists()
    with client.session_transaction() as browser_session:
        assert ("error", "The configuration form expired. Reload FCP and try again.") in (
            browser_session["_flashes"]
        )


def test_services_model_install_corrupt_config_hides_install_form(
    services_model_install_app, tmp_path,
) -> None:
    _app, client, request_file = services_model_install_app
    (tmp_path / "data/capabilities/config.json").write_text("{", encoding="utf-8")

    response = client.get("/federation/services")

    assert response.status_code == 200
    assert b'action="/server-setup/pull-model"' not in response.data
    assert not request_file.exists()


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
    connected_docs = (ROOT / "docs/connected_capabilities.md").read_text(
        encoding="utf-8"
    )
    server_docs = (ROOT / "docs/server_setup.md").read_text(encoding="utf-8")
    explainer_docs = (ROOT / "docs/ai_explainer.md").read_text(encoding="utf-8")

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
    assert "catalog.federation.model_resource_pull" in connected_docs
    assert "catalog.federation.model_resource_pull" in server_docs
    assert "catalog.federation.model_resource_pull" in explainer_docs
    assert "run --rm model-provider-install" not in connected_docs
    assert "run --rm model-provider-install" not in server_docs
    assert "run --rm ollama-pull" not in server_docs
    assert "run --rm ollama-pull" not in explainer_docs


def test_update_agents_treat_ollama_as_optional_and_under_host_admission() -> None:
    windows = (ROOT / "scripts/windows/fcp_update_engine.ps1").read_text(
        encoding="utf-8"
    )
    posix = (ROOT / "scripts/posix/fcp_update_engine.py").read_text(encoding="utf-8")

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
    windows_resource = (ROOT / "scripts/windows/fcp_docker_resource.ps1").read_text(
        encoding="utf-8"
    )
    python_runner = (ROOT / "catalog/federation/model_resource_pull.py").read_text(
        encoding="utf-8"
    )
    python_resource = (ROOT / "catalog/federation/docker_resources.py").read_text(
        encoding="utf-8"
    )

    assert "$CriticalFreeBytes = [int64]10737418240" in windows
    assert "$PressureFreeBytes = [int64]12884901888" in windows
    assert DEFAULT_CRITICAL_FREE_BYTES == 10737418240
    assert DEFAULT_PRESSURE_FREE_BYTES == 12884901888

    assert "fcp_docker_resource.ps1" in windows
    assert "Get-FcpDockerBackingPath -RepoRoot $RepoRoot" in windows
    assert "Get-FcpResourceFreeBytes -BackingPath $backingPath" in windows
    assert "docker_data.vhdx" in windows_resource
    assert "ext4.vhdx" in windows_resource
    assert "{{.OSType}}|{{.DockerRootDir}}" in windows_resource
    assert "return [int64]-1" in windows_resource
    assert "Test-ModelWriterStopped" in windows
    assert "'compose', 'stop', '--timeout', '5', $Service" in windows
    assert "'compose', 'stop', '--timeout', '15', $Service" in windows
    assert "'compose', 'kill'" not in windows
    assert "could not prove" in windows
    assert "exit 3" in windows

    assert "from .docker_resources import docker_backing_resource_path" in python_runner
    assert "_docker_backing_resource_path = docker_backing_resource_path" in python_runner
    assert "{{.DockerRootDir}}" in python_resource
    assert "Docker.raw" in python_resource
    assert "docker_data.vhdx" in python_resource
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


@pytest.mark.skipif(os.name != "nt", reason="Windows native process exit status")
@pytest.mark.parametrize("pull_exit, expected_exit", [(0, 0), (7, 1)])
def test_windows_model_pull_preserves_native_exit_status(
    tmp_path, pull_exit: int, expected_exit: int,
) -> None:
    # The Windows CI temp root uses an extended prefix unsupported by PS 5 -File.
    tmp_path = Path(str(tmp_path).removeprefix("\\\\?\\"))
    script = tmp_path / "fcp_model_pull.ps1"
    shutil.copyfile(ROOT / "scripts/windows/fcp_model_pull.ps1", script)
    (tmp_path / "fcp_docker_resource.ps1").write_text(
        "function Get-FcpDockerBackingPath { param([string]$RepoRoot); $RepoRoot }\n"
        "function Get-FcpResourceFreeBytes { param([string]$BackingPath); [int64]30000000000 }\n",
        encoding="utf-8",
    )
    # Only this fixture's batch file handles Docker commands; no engine is used.
    (tmp_path / "docker.cmd").write_text(
        "@echo off\n"
        'if "%~2"=="exec" goto show\n'
        'if "%~2"=="--profile" goto pull\n'
        "exit /b 90\n"
        ":show\n"
        'if exist "%FCP_TEST_MODEL_MARKER%" exit /b 0\n'
        "exit /b 1\n"
        ":pull\n"
        "ping -n 2 127.0.0.1 > nul\n"
        'if "%FCP_TEST_PULL_EXIT%"=="0" echo installed>"%FCP_TEST_MODEL_MARKER%"\n'
        "exit /b %FCP_TEST_PULL_EXIT%\n",
        encoding="ascii",
    )
    environment = dict(os.environ)
    environment["PATH"] = str(tmp_path) + os.pathsep + environment["PATH"]
    environment["FCP_TEST_MODEL_MARKER"] = str(tmp_path / "installed.marker")
    environment["FCP_TEST_PULL_EXIT"] = str(pull_exit)
    environment["PSMODULEPATH"] = str(
        Path(environment["SYSTEMROOT"]) / "System32/WindowsPowerShell/v1.0/Modules"
    )
    powershell = Path(environment["SYSTEMROOT"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    completed = subprocess.run(
        [str(powershell), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(script), "-RepoRoot", str(tmp_path), "-Model", "fixture-model"],
        env=environment, capture_output=True, text=True, timeout=30, check=False,
    )
    assert completed.returncode == expected_exit, completed.stderr or completed.stdout
    if pull_exit == 0:
        assert "installed and verified" in completed.stdout
    else:
        assert "exited with code 7" in completed.stderr
