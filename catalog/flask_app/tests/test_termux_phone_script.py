import os
import shlex
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
PHONE_SCRIPT = REPO_ROOT / "termux" / "msh-phone.sh"
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
    printf '%s\\n' 'msh-phone'
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
        "MSH_PHONE_STATE": str(state_dir),
        "MSH_PHONE_PROC_ROOT": str(proc_root),
        "MSH_PHONE_STOP_WAIT_SECONDS": "2",
        "MSH_PHONE_KILL_WAIT_SECONDS": "0",
    }


def test_stop_finds_a_session_started_before_pid_tracking(tmp_path: Path) -> None:
    marker = tmp_path / "http-ready"
    environment = _phone_environment(tmp_path, marker)
    proc_root = Path(environment["MSH_PHONE_PROC_ROOT"])
    process_dir = proc_root / "321"
    process_dir.mkdir()
    (process_dir / "cmdline").write_bytes(
        b"proot\0--rootfs=/tmp/containers/msh-phone/rootfs\0python\0"
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
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "321 TERM\n"


def test_stop_does_not_claim_success_while_http_responds(tmp_path: Path) -> None:
    marker = tmp_path / "http-ready"
    marker.touch()
    environment = _phone_environment(tmp_path, marker)
    environment["MSH_PHONE_STOP_WAIT_SECONDS"] = "0"

    result = subprocess.run(
        ["bash", str(PHONE_SCRIPT), "stop"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 1
    assert "MSH stopped." not in result.stdout
    assert "stop was not confirmed" in result.stderr


def test_phone_rebuild_preserves_saved_connected_capabilities() -> None:
    script = SETUP_SCRIPT.read_text(encoding="utf-8")

    assert 'DATA_DIR/server_setup/server_settings.json' in script
    assert "Preserving existing browser setup, including connected capabilities." in script
