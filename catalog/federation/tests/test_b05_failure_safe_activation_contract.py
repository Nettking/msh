from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_posix_resume_failure_restores_previous_flask_before_reporting_failure() -> None:
    text = _text("scripts/posix/fcp_update_engine.py")
    stop = text.index('["docker", "compose", "stop", "flask"]')
    resume = text.index('"catalog.flask_app.services.existing_setup_resume"', stop)
    failure = text.index('raise RuntimeError(f"resume_failed:{resume.returncode}")', resume)
    recovery_window = text[resume:failure]

    assert "restore_previous_flask_runtime" in recovery_window, (
        "after the update agent stops the previously usable Flask runtime, a failed "
        "target resume currently falls straight into host_update_failed without "
        "restarting the previous runtime"
    )


def test_windows_resume_failure_restores_previous_flask_before_reporting_failure() -> None:
    text = _text("scripts/windows/fcp_update_engine.ps1")
    stop = text.index("Invoke-External 'docker' @('compose', 'stop', 'flask')")
    resume = text.index("'catalog.flask_app.services.existing_setup_resume'", stop)
    failure = text.index('throw "resume_failed:$($resume.ExitCode)"', resume)
    recovery_window = text[resume:failure]

    assert "Restore-PreviousFlaskRuntime" in recovery_window, (
        "the Windows activation path has the same post-stop resume-failure window: "
        "the old Flask runtime is stopped but no restoration is attempted"
    )


def test_posix_runtime_verification_failure_has_bounded_activation_recovery_state() -> None:
    text = _text("scripts/posix/fcp_update_engine.py")
    start = text.index('["docker", "compose", "up", "-d", "flask"]')
    verify = text.index("running = wait_runtime(root, target)", start)
    tail = text[verify : text.index("return True", verify)]

    assert "activation_required" in tail and "record_activation_recovery" in tail, (
        "once target Flask has replaced the previous container, runtime verification "
        "failure currently has no explicit bounded activation-recovery state"
    )


def test_windows_runtime_verification_failure_has_bounded_activation_recovery_state() -> None:
    text = _text("scripts/windows/fcp_update_engine.ps1")
    start = text.index("Invoke-External 'docker' @('compose', 'up', '-d', 'flask')")
    verify = text.index("$running = Wait-RuntimeVerified $target", start)
    tail = text[verify : text.index("return $true", verify)]

    assert "activation_required" in tail and "Write-ActivationRecovery" in tail, (
        "Windows likewise collapses a post-replacement verification failure into "
        "generic host_update_failed instead of a deterministic recovery state"
    )
