from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import first_federation_start as first


ROOT = Path(__file__).resolve().parents[3]
HELPER = ROOT / "scripts" / "posix" / "stop_fcp_for_fresh_reset.py"
BUILD_COMMIT = "0123456789abcdef0123456789abcdef01234567"


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _fake_host_tools(tmp_path: Path, calls: Path) -> dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_executable(
        bin_dir / "git",
        "#!/bin/sh\n"
        'case "$1" in\n'
        f"  rev-parse) echo {BUILD_COMMIT} ;;\n"
        "  status) : ;;\n"
        "esac\n",
    )
    _write_executable(
        bin_dir / "docker",
        "#!/bin/sh\n"
        f'printf "docker %s\\n" "$*" >> "{calls}"\n'
        "exit 0\n",
    )
    _write_executable(
        bin_dir / "python3",
        "#!/bin/sh\n"
        f'printf "python3 %s\\n" "$*" >> "{calls}"\n'
        'if [ "$1 $2" = "-m catalog.federation.host_build" ]; then\n'
        f"  echo {BUILD_COMMIT}\n"
        "fi\n"
        "exit 0\n",
    )
    _write_executable(
        bin_dir / "tailscale",
        "#!/bin/sh\n"
        'if [ "$1" = "ip" ]; then echo 100.78.187.87; fi\n'
        "exit 0\n",
    )
    env = os.environ.copy()
    env["PATH"] = str(bin_dir) + os.pathsep + env["PATH"]
    # These execution fixtures exercise the child launcher body with fake host
    # tools. The real outer lease process is covered independently by the B04
    # host-mutation tests and cannot be emulated by this intentionally tiny fake
    # Python executable.
    env["FCP_HOST_MUTATION_LEASE_ACTIVE"] = "1"
    return env


def _make_fake_docker(
    tmp_path: Path,
    *,
    exit_codes: tuple[int, ...],
    calls: Path,
) -> Path:
    docker = tmp_path / "docker"
    state = tmp_path / "docker-call-index"
    state.write_text("0", encoding="utf-8")
    cases = "\n".join(
        f"  {index}) code={code} ;;" for index, code in enumerate(exit_codes)
    )
    fallback = exit_codes[-1]
    _write_executable(
        docker,
        "#!/bin/sh\n"
        f'n=$(cat "{state}")\n'
        f'printf "project=%s args=%s\\n" "${{COMPOSE_PROJECT_NAME:-}}" "$*" >> "{calls}"\n'
        'case "$n" in\n'
        f"{cases}\n"
        f"  *) code={fallback} ;;\n"
        "esac\n"
        f'expr "$n" + 1 > "{state}"\n'
        'exit "$code"\n',
    )
    return docker


def _fake_start_tools_with_ollama_failure(tmp_path: Path, calls: Path) -> dict[str, str]:
    env = _fake_host_tools(tmp_path, calls)
    docker = Path(env["PATH"].split(os.pathsep, maxsplit=1)[0]) / "docker"
    _write_executable(
        docker,
        "#!/bin/sh\n"
        f'printf "docker %s\\n" "$*" >> "{calls}"\n'
        'if [ "$*" = "compose up -d ollama" ]; then exit 1; fi\n'
        "exit 0\n",
    )
    return env


@pytest.mark.skipif(os.name == "nt", reason="POSIX launcher execution")
def test_posix_tailscale_fresh_path_reaches_verified_reset_before_discovery(
    tmp_path: Path,
) -> None:
    calls = tmp_path / "calls.log"
    env = _fake_host_tools(tmp_path, calls)

    completed = subprocess.run(
        ["sh", str(ROOT / "start-tailscale.sh"), "--fresh"],
        cwd=ROOT,
        env=env,
        input="RESET\n",
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "FRESH DEVICE INSTALL" in completed.stdout
    assert "Unknown option: --fresh" not in completed.stderr

    trace = calls.read_text(encoding="utf-8")
    build = "python3 -m catalog.federation.host_build"
    stop = f"python3 {ROOT / 'scripts' / 'posix' / 'stop_fcp_for_fresh_reset.py'}"
    reset = (
        "docker compose run --rm --no-deps --entrypoint python flask "
        "-m catalog.flask_app.services.device_state_reset"
    )
    verify = (
        "docker compose run --rm --no-deps --entrypoint python flask "
        "-m catalog.flask_app.services.device_state_reset --verify-fresh"
    )
    services = "docker compose up -d relay recorder"
    discovery = f"python3 {ROOT / 'scripts' / 'zero_touch_federation_start.py'}"
    assert trace.index(build) < trace.index(stop) < trace.index(reset) < trace.index(verify)
    assert trace.index(verify) < trace.index(services) < trace.index(discovery)


@pytest.mark.skipif(os.name == "nt", reason="POSIX launcher execution")
def test_posix_start_fresh_requires_confirmation_before_reset(tmp_path: Path) -> None:
    calls = tmp_path / "calls.log"
    env = _fake_host_tools(tmp_path, calls)

    completed = subprocess.run(
        ["sh", str(ROOT / "start.sh"), "--fresh"],
        cwd=ROOT,
        env=env,
        input="NO\n",
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "Type RESET to continue:" in completed.stdout
    assert "Fresh install cancelled. No state was removed." in completed.stdout
    assert "device_state_reset" not in calls.read_text(encoding="utf-8")


@pytest.mark.skipif(os.name == "nt", reason="POSIX launcher execution")
def test_posix_start_survives_optional_ollama_start_failure(tmp_path: Path) -> None:
    calls = tmp_path / "calls.log"
    env = _fake_start_tools_with_ollama_failure(tmp_path, calls)
    env["FCP_SUPPRESS_BROWSER"] = "1"
    env["FCP_DATA_DIR"] = str(tmp_path / "data")
    env["FCP_RESULTS_DIR"] = str(tmp_path / "results")

    completed = subprocess.run(
        ["sh", str(ROOT / "start.sh")],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    trace = calls.read_text(encoding="utf-8")
    build = "python3 -m catalog.federation.host_build"
    required = "docker compose up -d relay recorder"
    flask = "docker compose up -d flask"
    ollama = "docker compose up -d ollama"
    assert trace.index(build) < trace.index(required) < trace.index(flask) < trace.index(ollama)
    assert "WARNING: Ollama is unavailable; core FCP remains running." in completed.stderr
    assert "AI capability:        unavailable; core FCP is healthy" in completed.stdout


@pytest.mark.skipif(os.name == "nt", reason="POSIX launcher execution")
def test_posix_first_federation_uses_same_fresh_launcher(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run(command, **kwargs):
        calls.append({"command": command, **kwargs})
        return type("Completed", (), {"returncode": 0})()

    monkeypatch.setattr(first.subprocess, "run", fake_run)

    first._run_fresh_reset()

    assert calls[0]["command"] == ["sh", str(first.ROOT / "start.sh"), "--fresh"]
    assert calls[0]["input"] == "RESET\n"
    assert calls[0]["env"]["FCP_SUPPRESS_BROWSER"] == "1"


def test_posix_fresh_uses_shared_reset_contract_and_never_deletes_volumes() -> None:
    start = (ROOT / "start.sh").read_text(encoding="utf-8")
    helper = HELPER.read_text(encoding="utf-8")

    assert "--fresh) MODE=fresh" in start
    assert "catalog.flask_app.services.device_state_reset" in start
    assert "--verify-fresh" in start
    assert start.index("build_core_images") < start.index("stop_fcp_for_fresh_reset.py")
    assert start.index("stop_fcp_for_fresh_reset.py") < start.index(
        "catalog.flask_app.services.device_state_reset"
    )
    assert start.index("--verify-fresh") < start.index(
        "docker compose up -d relay recorder"
    )
    assert "--no-deps --build --entrypoint python flask" not in start
    assert '["compose", "down", "--remove-orphans", "--timeout", "10"]' in helper
    assert '["compose", "kill"]' in helper
    assert "docker compose down -v" not in start
    assert "docker volume prune" not in start
    assert "volume rm" not in helper


def test_posix_normal_and_resume_modes_remain_non_destructive() -> None:
    start = (ROOT / "start.sh").read_text(encoding="utf-8")

    assert "--resume) MODE=resume" in start
    assert 'if [ "$MODE" = resume ]; then' in start
    assert 'if [ "$MODE" = fresh ]; then' in start
    assert start.index('if [ "$MODE" = fresh ]; then') < start.index(
        'AGENT_DIR="$FCP_DATA_DIR/federation/update-agent"'
    )


def test_posix_core_starts_before_optional_model_installation() -> None:
    start = (ROOT / "start.sh").read_text(encoding="utf-8")
    required = "docker compose up -d relay recorder"
    flask = "docker compose up -d flask"
    ollama = "docker compose up -d ollama"
    model = "python3 -m catalog.federation.model_resource_pull"

    assert start.index(required) < start.index(flask) < start.index(ollama) < start.index(model)
    assert "--target ollama --model \"$FCP_AI_MODEL\"" in start
    assert "host resource admission" in start
    assert "FCP_AI_DEGRADED=0" in start
    assert "FCP_AI_DEGRADED=1" in start
    assert "WARNING: Ollama is unavailable; core FCP remains running." in start
    assert (
        "WARNING: AI capability is unavailable or resource-paused; core FCP remains running."
        in start
    )
    assert "AI capability:        unavailable; core FCP is healthy" in start
    assert "ollama-pull" not in start


def test_posix_optional_model_failure_path_has_no_fatal_exit() -> None:
    start = (ROOT / "start.sh").read_text(encoding="utf-8")
    optional_block = start.split(
        'FCP_AI_DEGRADED=0\necho "Starting optional Ollama service ..."',
        maxsplit=1,
    )[1].split("\nBASE_URL=", maxsplit=1)[0]

    assert "if ! docker compose up -d ollama; then" in optional_block
    assert "python3 -m catalog.federation.model_resource_pull" in optional_block
    assert "ollama-pull" not in optional_block
    assert "ollama show" not in optional_block
    assert "exit 1" not in optional_block


def test_posix_shutdown_helper_clean_path_is_project_scoped(tmp_path: Path) -> None:
    calls = tmp_path / "docker.log"
    docker = _make_fake_docker(tmp_path, exit_codes=(0,), calls=calls)
    env = os.environ.copy()
    env["COMPOSE_PROJECT_NAME"] = "fcp"

    completed = subprocess.run(
        [sys.executable, str(HELPER), "--docker-executable", str(docker)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert calls.read_text(encoding="utf-8").splitlines() == [
        "project=fcp args=compose down --remove-orphans --timeout 10"
    ]


def test_posix_shutdown_helper_recovers_without_volume_deletion(tmp_path: Path) -> None:
    calls = tmp_path / "docker.log"
    docker = _make_fake_docker(tmp_path, exit_codes=(1, 0, 0), calls=calls)
    env = os.environ.copy()
    env["COMPOSE_PROJECT_NAME"] = "fcp"

    completed = subprocess.run(
        [sys.executable, str(HELPER), "--docker-executable", str(docker)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    trace = calls.read_text(encoding="utf-8")
    assert trace.splitlines() == [
        "project=fcp args=compose down --remove-orphans --timeout 10",
        "project=fcp args=compose kill",
        "project=fcp args=compose down --remove-orphans --timeout 5",
    ]
    assert "volume" not in trace.casefold()


def test_posix_shutdown_helper_fails_closed_when_cleanup_fails(tmp_path: Path) -> None:
    calls = tmp_path / "docker.log"
    docker = _make_fake_docker(tmp_path, exit_codes=(1, 1, 1), calls=calls)

    completed = subprocess.run(
        [sys.executable, str(HELPER), "--docker-executable", str(docker)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 1
    assert "Device state was not reset" in completed.stderr


@pytest.mark.skipif(os.name == "nt", reason="POSIX shell syntax check")
def test_posix_start_scripts_have_valid_sh_syntax() -> None:
    for path in (ROOT / "start.sh", ROOT / "start-tailscale.sh"):
        completed = subprocess.run(
            ["sh", "-n", str(path)],
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stderr or completed.stdout
