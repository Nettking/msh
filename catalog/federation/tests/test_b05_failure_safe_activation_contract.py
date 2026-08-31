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
    assert 'state="error"' in text
    assert 'code="activation_recovered"' in text
    assert text.index("restore_previous_flask_runtime") < text.index(
        'code="activation_recovered"'
    )


def test_windows_resume_failure_restores_previous_flask_before_reporting_failure() -> None:
    runner = _text("scripts/windows/fcp_update_agent_runner.ps1")
    proxy = _text("scripts/windows/fcp_docker_build_proxy.cmd")

    assert "FCP_ACTIVATION_PHASE_FILE" in runner
    assert "Restore-PreviousFlaskRuntime" in runner
    assert "activation_recovered" in runner
    assert "flask-stopped" in proxy
    assert 'compose" if /I "%~2"=="stop"' in proxy
    assert runner.index("Restore-PreviousFlaskRuntime") < runner.index(
        "-Code 'activation_recovered'"
    )


def test_posix_runtime_verification_failure_has_bounded_activation_recovery_state() -> None:
    text = _text("scripts/posix/fcp_update_agent_runner.py")

    assert "self.target_flask_started = True" in text
    assert "record_activation_recovery" in text
    assert 'state="activation_required"' in text
    assert 'code="activation_required"' in text
    assert "retry the same apply" in text


def test_windows_runtime_verification_failure_has_bounded_activation_recovery_state() -> None:
    runner = _text("scripts/windows/fcp_update_agent_runner.ps1")
    proxy = _text("scripts/windows/fcp_docker_build_proxy.cmd")

    assert "Write-ActivationRecovery" in runner
    assert "-State 'activation_required'" in runner
    assert "-Code 'activation_required'" in runner
    assert "retry the same apply" in runner
    assert "target-started" in proxy
    # The marker is written before forwarding `compose up -d flask`, because a
    # failed Compose invocation may already have replaced the old container.
    assert proxy.index("echo target-started") < proxy.index(
        '"%FCP_REAL_DOCKER_EXE%" %*', proxy.index(":flask_start")
    )


def test_activation_recovery_never_introduces_source_rollback() -> None:
    texts = (
        _text("scripts/posix/fcp_update_agent_runner.py"),
        _text("scripts/windows/fcp_update_agent_runner.ps1"),
    )
    for text in texts:
        lowered = text.lower()
        assert "reset --hard" not in lowered
        assert "git clean" not in lowered
        assert "git stash" not in lowered
        assert "checkout --" not in lowered
