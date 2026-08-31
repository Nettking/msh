from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_posix_resume_failure_restores_previous_flask_before_reporting_failure() -> None:
    text = _text("scripts/posix/fcp_update_agent_runner.py")
    assert 'FLASK_STOP_COMMAND = ["docker", "compose", "stop", "flask"]' in text
    assert "self.flask_stopped = True" in text
    assert "restore_previous_flask_runtime" in text
    assert 'code="activation_recovered"' in text
    assert "previous Flask runtime" in text


def test_windows_resume_failure_restores_previous_flask_before_reporting_failure() -> None:
    runner = _text("scripts/windows/fcp_update_agent_runner.ps1")
    proxy = _text("scripts/windows/fcp_docker_build_proxy.cmd")
    assert "Restore-PreviousFlaskRuntime" in runner
    assert "activation_recovered" in runner
    assert "previous Flask runtime" in runner
    assert "flask-stopped" in proxy


def test_restore_success_requires_runtime_health() -> None:
    posix = _text("scripts/posix/fcp_update_agent_runner.py")
    windows = _text("scripts/windows/fcp_update_agent_runner.ps1")
    assert "wait_runtime" in posix[posix.index("def restore_previous_flask_runtime") :]
    assert "Test-RuntimeUsable" in windows


def test_failed_restore_becomes_explicit_activation_required() -> None:
    posix = _text("scripts/posix/fcp_update_agent_runner.py")
    windows = _text("scripts/windows/fcp_update_agent_runner.ps1")
    assert 'code="activation_restore_failed"' in posix
    assert "-Code 'activation_restore_failed'" in windows


def test_missing_engine_result_after_activation_phase_is_recoverable() -> None:
    posix = _text("scripts/posix/fcp_update_agent_runner.py")
    windows = _text("scripts/windows/fcp_update_agent_runner.ps1")
    assert "allow_missing_result=True" in posix
    assert "AllowMissingResult" in windows
    assert "New-SyntheticActivationResult" in windows


def test_posix_runtime_verification_failure_has_bounded_activation_recovery_state() -> None:
    text = _text("scripts/posix/fcp_update_agent_runner.py")
    assert "self.target_flask_started = True" in text
    assert 'state="activation_required"' in text
    assert 'code="activation_required"' in text


def test_windows_runtime_verification_failure_has_bounded_activation_recovery_state() -> None:
    runner = _text("scripts/windows/fcp_update_agent_runner.ps1")
    proxy = _text("scripts/windows/fcp_docker_build_proxy.cmd")
    assert "-State 'activation_required'" in runner
    assert "-Code 'activation_required'" in runner
    flask_start = proxy[proxy.index("\n:flask_start\n") : proxy.index("\n:controlled_build\n")]
    assert flask_start.index("echo target-started") < flask_start.index('"%FCP_REAL_DOCKER_EXE%" %*')


def test_recovery_correlates_result_to_exact_apply_request_on_both_platforms() -> None:
    posix = _text("scripts/posix/fcp_update_agent_runner.py")
    windows = _text("scripts/windows/fcp_update_agent_runner.ps1")
    assert "expected_request_id" in posix
    assert 'result.get("request_id") != expected_request_id' in posix
    assert "ExpectedRequestId" in windows
    assert "ExpectedTargetCommit" in windows


def test_recovery_is_bounded() -> None:
    posix = _text("scripts/posix/fcp_update_agent_runner.py")
    windows = _text("scripts/windows/fcp_update_agent_runner.ps1")
    assert posix.count('["docker", "compose", "start", "flask"]') == 1
    assert windows.count("@('compose', 'start', 'flask')") == 1


def test_activation_recovery_never_introduces_source_rollback() -> None:
    for text in (
        _text("scripts/posix/fcp_update_agent_runner.py"),
        _text("scripts/windows/fcp_update_agent_runner.ps1"),
    ):
        lowered = text.lower()
        assert "reset --hard" not in lowered
        assert "git clean" not in lowered
        assert "git stash" not in lowered
        assert "checkout --" not in lowered
