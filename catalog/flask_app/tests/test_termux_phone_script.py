import os
import shlex
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PHONE_SCRIPT = REPO_ROOT / "termux" / "fcp-phone.sh"
SETUP_SCRIPT = REPO_ROOT / "termux" / "setup-phone.sh"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


def _phone_environment(tmp_path: Path, marker: Path) -> dict[str, str]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _write_executable(
        fake_bin / "proot-distro",
        """#!/usr/bin/env bash
if [[ "${1:-}" == "list" ]]; then
    printf '%s\\n' 'fcp-phone'
    exit 0
fi
exit 1
""",
    )
    _write_executable(
        fake_bin / "curl",
        """#!/usr/bin/env bash
[[ -f "$FAKE_HTTP_MARKER" ]]
""",
    )

    state_dir = tmp_path / "state"
    proc_root = tmp_path / "proc"
    (state_dir / "results").mkdir(parents=True)
    proc_root.mkdir()
    return {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "FAKE_HTTP_MARKER": str(marker),
        "FCP_PHONE_STATE": str(state_dir),
        "FCP_PHONE_PROC_ROOT": str(proc_root),
        "FCP_PHONE_STOP_WAIT_SECONDS": "2",
        "FCP_PHONE_KILL_WAIT_SECONDS": "0",
    }


def _setup_environment(tmp_path: Path) -> tuple[dict[str, str], Path, Path]:
    fake_bin = tmp_path / "setup-bin"
    fake_bin.mkdir()
    proot_log = tmp_path / "proot.log"
    pkg_log = tmp_path / "pkg.log"
    _write_executable(
        fake_bin / "proot-distro",
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_PROOT_LOG"
if [[ "${1:-}" == "list" ]]; then
    printf '%s\n' 'fcp-phone'
fi
exit 0
""",
    )
    _write_executable(
        fake_bin / "curl",
        """#!/usr/bin/env bash
exit 1
""",
    )
    _write_executable(
        fake_bin / "pkg",
        """#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_PKG_LOG"
exit 0
""",
    )
    # setup-phone.sh probes for git, ssh, curl and proot-distro before deciding
    # whether a package refresh is needed. Only presence is checked, never
    # behavior. Without a stub, the outcome depends on whether the host running
    # the tests happens to have ssh installed: GitHub runners do, minimal
    # containers do not, so the same assertion passed in CI and failed locally.
    # Stub it so the test controls the prerequisite set it is asserting about.
    _write_executable(
        fake_bin / "ssh",
        """#!/usr/bin/env bash
exit 0
""",
    )

    state_dir = tmp_path / "setup-state"
    (state_dir / "data").mkdir(parents=True)
    (state_dir / "results").mkdir()
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "PREFIX": "/data/data/com.termux/files/usr",
        "FAKE_PROOT_LOG": str(proot_log),
        "FAKE_PKG_LOG": str(pkg_log),
        "FCP_PHONE_STATE": str(state_dir),
    }
    return environment, proot_log, pkg_log


def test_stop_finds_a_session_started_before_pid_tracking(tmp_path: Path) -> None:
    marker = tmp_path / "http-ready"
    environment = _phone_environment(tmp_path, marker)
    proc_root = Path(environment["FCP_PHONE_PROC_ROOT"])
    process_dir = proc_root / "321"
    process_dir.mkdir()
    (process_dir / "cmdline").write_bytes(
        b"proot\0--rootfs=/tmp/containers/fcp-phone/rootfs\0python\0"
    )

    shell_program = f"""
source {shlex.quote(str(PHONE_SCRIPT))}
proot_distro_supports_command() {{ return 1; }}
tracked_server_pid() {{ return 1; }}
signal_process_tree() {{ printf '%s %s\\n' "$1" "$2"; }}
signal_phone_sessions TERM
"""
    result = subprocess.run(
        ["bash", "-c", shell_program],
        env=environment,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "321 TERM\n"


def test_stop_does_not_claim_success_while_http_responds(tmp_path: Path) -> None:
    marker = tmp_path / "http-ready"
    marker.touch()
    environment = _phone_environment(tmp_path, marker)
    environment["FCP_PHONE_STOP_WAIT_SECONDS"] = "0"

    result = subprocess.run(
        ["bash", str(PHONE_SCRIPT), "stop"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert result.returncode == 1
    assert "FCP stopped." not in result.stdout
    assert "stop was not confirmed" in result.stderr


def test_phone_setup_uses_capability_config_without_creating_a_new_role() -> None:
    script = SETUP_SCRIPT.read_text(encoding="utf-8")
    phone_script = PHONE_SCRIPT.read_text(encoding="utf-8")

    assert 'DATA_DIR/capabilities/config.json' in script
    assert 'DATA_DIR/server_setup/server_settings.json' in script
    assert "--migrate-legacy-phone-bootstrap" in script
    assert "--profile workbench --no-ai" in script
    assert "--browser-setup-pending" not in script
    assert "phone setup no longer creates a device role" in script
    assert 'BUILD_SIGNATURE_FILE="$STATE_DIR/runtime-build.signature"' in script
    assert '"$(git -C "$ROOT" hash-object Dockerfile)"' in script
    assert '"$(git -C "$ROOT" hash-object requirements.txt)"' in script
    assert '--bind "$ROOT:/app"' in script
    assert '--bind "$ROOT:/app"' in phone_script


def test_existing_compatible_container_uses_fast_setup_without_pkg_or_build(tmp_path: Path) -> None:
    environment, proot_log, pkg_log = _setup_environment(tmp_path)

    result = subprocess.run(
        ["bash", str(SETUP_SCRIPT), "--update"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Runtime dependencies are unchanged" in result.stdout
    assert "no dependency downloads were needed" in result.stdout
    assert not pkg_log.exists()
    commands = proot_log.read_text(encoding="utf-8")
    assert "\nbuild " not in f"\n{commands}"
    assert '--bind ' + str(REPO_ROOT) + ":/app" in commands
    assert (Path(environment["FCP_PHONE_STATE"]) / "runtime-build.signature").is_file()


def test_changed_runtime_signature_triggers_rebuild_without_pkg_refresh(tmp_path: Path) -> None:
    environment, proot_log, pkg_log = _setup_environment(tmp_path)
    signature_path = Path(environment["FCP_PHONE_STATE"]) / "runtime-build.signature"
    signature_path.write_text("old-runtime\n", encoding="utf-8")

    result = subprocess.run(
        ["bash", str(SETUP_SCRIPT), "--update"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Dockerfile or requirements.txt changed" in result.stdout
    commands = proot_log.read_text(encoding="utf-8")
    assert "remove fcp-phone" in commands
    assert "build -t fcp-phone:latest --install-as fcp-phone" in commands
    assert not pkg_log.exists()
    assert signature_path.read_text(encoding="utf-8") != "old-runtime\n"


def test_update_stops_before_pull_and_requests_automatic_restart(tmp_path: Path) -> None:
    marker = tmp_path / "http-ready"
    environment = _phone_environment(tmp_path, marker)
    shell_program = f"""
source {shlex.quote(str(PHONE_SCRIPT))}
container_exists() {{ return 0; }}
http_ready() {{ return 0; }}
stop_server() {{ echo stop; }}
start_server() {{ echo restart-after-failure; }}
git() {{ echo "git $*"; return 0; }}
bash() {{ echo "setup restart=${{FCP_PHONE_RESTART_AFTER_SETUP:-false}} $*"; }}
main update
"""

    result = subprocess.run(
        ["bash", "-c", shell_program],
        env=environment,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "stop",
        "git pull --ff-only",
        "setup restart=true termux/setup-phone.sh --update",
    ]


@pytest.mark.parametrize("sample", ["absent", "readme-only", "synthetic-jsonl"])
@pytest.mark.parametrize("command", ["setup", "demo-reset"])
def test_phone_demo_handles_archives_without_telemetry(
    tmp_path: Path, sample: str, command: str
) -> None:
    environment, _, _ = _setup_environment(tmp_path)
    source = tmp_path / "source"
    (source / "termux").mkdir(parents=True)
    for relative in ("termux/setup-phone.sh", "termux/fcp-phone.sh", "Dockerfile", "requirements.txt"):
        (source / relative).write_bytes((REPO_ROOT / relative).read_bytes())
    environment["FCP_PHONE_STATE"] = str(source)
    environment["FAKE_DEMO_ROOT"] = str(source)
    if sample != "absent":
        (source / "example-data").mkdir()
        (source / "example-data/README.md").write_text("No public capture.\n", encoding="utf-8")
    if sample == "synthetic-jsonl":
        (source / "example-data/sample.jsonl").write_text('{"synthetic":true}\n', encoding="utf-8")

    # Execute only the real guest copy command inside this disposable fixture.
    # Other PRoot calls are capability probes/configuration stubs, as above.
    _write_executable(
        tmp_path / "setup-bin/proot-distro",
        """#!/usr/bin/env bash
if [[ "${1:-}" == "list" ]]; then
    printf '%s\\n' 'fcp-phone'
elif [[ "${1:-}" == "login" && "${!#}" == *'cp -a example-data/'* ]]; then
    cd "$FAKE_DEMO_ROOT" || exit 1
    exec bash -c "${!#}"
fi
exit 0
""",
    )
    existing = source / "data/demo/existing.jsonl"
    if command == "demo-reset":
        existing.parent.mkdir(parents=True)
        existing.write_text('{"preserve":true}\n', encoding="utf-8")
    args = (
        ["bash", str(source / "termux/setup-phone.sh"), "--update"]
        if command == "setup"
        else ["bash", str(source / "termux/fcp-phone.sh"), "demo-reset"]
    )
    result = subprocess.run(args, env=environment, capture_output=True, text=True, timeout=10, check=False)

    if sample == "synthetic-jsonl":
        assert result.returncode == 0, result.stderr
        assert (source / "data/demo/sample.jsonl").read_text(encoding="utf-8") == '{"synthetic":true}\n'
        assert not existing.exists()
        expected_message = "Copied example data" if command == "setup" else "Demo data restored"
        assert expected_message in result.stdout
    elif command == "demo-reset":
        assert result.returncode == 2
        assert existing.read_text(encoding="utf-8") == '{"preserve":true}\n'
        assert "existing demo data were preserved" in result.stderr
        assert "Demo data restored" not in result.stdout
    else:
        assert result.returncode == 0, result.stderr
        assert "Bundled telemetry is unavailable" in result.stdout
        assert "Copied example data" not in result.stdout
        assert not (source / "data/demo").exists()
        assert (source / "demo-data.checked").is_file()
