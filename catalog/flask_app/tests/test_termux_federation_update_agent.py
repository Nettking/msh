from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
CODEC_PATH = REPO_ROOT / "termux" / "fcp_phone_update_codec.py"
AGENT_PATH = REPO_ROOT / "termux" / "fcp-phone-update-agent.sh"
PHONE_PATH = REPO_ROOT / "termux" / "fcp-phone.sh"

spec = importlib.util.spec_from_file_location("fcp_phone_update_codec", CODEC_PATH)
assert spec is not None and spec.loader is not None
codec = importlib.util.module_from_spec(spec)
spec.loader.exec_module(codec)


def _request(*, action: str = "check", target: str = "a" * 40) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    value: dict[str, object] = {
        "schema": codec.REQUEST_SCHEMA,
        "request_id": "fed-phone-test",
        "action": action,
        "repository": "Nettking/msh",
        "branch": "main",
        "target_commit": target,
        "created_at": now.isoformat().replace("+00:00", "Z"),
        "expires_at": (now + timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
    }
    if action == "apply":
        value["activate_after"] = (now + timedelta(seconds=2)).isoformat().replace(
            "+00:00", "Z"
        )
    return value


def test_codec_accepts_only_bounded_exact_commit_requests(tmp_path: Path) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(_request(action="apply")), encoding="utf-8")

    request_id, action, target, activate_epoch = codec.parse_request(request_path)

    assert request_id == "fed-phone-test"
    assert action == "apply"
    assert target == "a" * 40
    assert activate_epoch > 0

    invalid = _request()
    invalid["repository"] = "someone/else"
    request_path.write_text(json.dumps(invalid), encoding="utf-8")
    with pytest.raises(ValueError, match="unapproved_source"):
        codec.parse_request(request_path)

    invalid = _request(target="main")
    request_path.write_text(json.dumps(invalid), encoding="utf-8")
    with pytest.raises(ValueError, match="malformed_target"):
        codec.parse_request(request_path)


def test_codec_writes_durable_per_request_result(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    target = "b" * 40

    codec.write_result(
        result_path,
        request_id="fed-phone-result",
        action="apply",
        state="runtime_verified",
        current=target,
        target=target,
        running=target,
        code="updated",
        message="verified",
    )

    latest = json.loads(result_path.read_text(encoding="utf-8"))
    assert latest["schema"] == codec.RESULT_SCHEMA
    assert latest["running_commit"] == target
    request_results = list(tmp_path.glob("result-*.json"))
    assert len(request_results) == 1
    assert json.loads(request_results[0].read_text(encoding="utf-8")) == latest


def test_termux_agent_keeps_the_same_fail_closed_git_boundary() -> None:
    script = AGENT_PATH.read_text(encoding="utf-8")

    assert 'APPROVED_BRANCH="main"' in script
    assert "https://github.com/nettking/msh" in script
    assert "git@github.com:nettking/msh" in script
    assert 'git -C "$ROOT" fetch --no-tags origin "$APPROVED_BRANCH"' in script
    assert 'git -C "$ROOT" merge --ff-only "$target"' in script
    assert 'git -C "$ROOT" status --porcelain=v1 --untracked-files=all' in script
    assert 'FCP_PHONE_RESTART_AFTER_SETUP=true bash "$SETUP_SCRIPT" --update' in script
    assert 'write_result "$request_id" "$action" "runtime_verified"' in script

    forbidden = ("git reset", "git clean", "git checkout", "git rebase", "eval ", "sh -c")
    for command in forbidden:
        assert command not in script


def test_phone_start_bootstraps_agent_and_records_only_ready_runtime() -> None:
    script = PHONE_PATH.read_text(encoding="utf-8")

    assert 'UPDATE_AGENT_PID_FILE="$RESULTS_DIR/termux-update-agent.pid"' in script
    assert 'RUNNING_COMMIT_FILE="$RESULTS_DIR/termux-running-commit"' in script
    assert 'nohup bash "$ROOT/termux/fcp-phone-update-agent.sh"' in script
    assert 'rm -f "$PID_FILE" "$RUNNING_COMMIT_FILE"' in script

    start_body = script.split("start_server() {", 1)[1].split("\n}\n\nrun_guest()", 1)[0]
    assert start_body.index("start_update_agent") < start_body.index("start_server_process")
    assert start_body.index("if http_ready; then") < start_body.index("record_running_commit")


def test_termux_update_shells_are_syntactically_valid() -> None:
    for path in (AGENT_PATH, PHONE_PATH):
        result = subprocess.run(
            ["bash", "-n", str(path)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
