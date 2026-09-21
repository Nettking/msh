"""Contract tests for the Windows/POSIX P01-P12 preparation wrappers.

The wrappers are the first thing a release operator runs on a physical host, so
they have to stay aligned with the harness they drive: the same host profiles,
the same non-timed scenario set, and no implicit start of a timed P07/P12 run.
"""

from __future__ import annotations

import re
from pathlib import Path

from scripts.acceptance.v1_physical_campaign import HOST_PROFILES
from scripts.acceptance.v1_physical_campaign_contract import SCENARIOS

WRAPPERS = Path("scripts/acceptance")
POSIX = WRAPPERS / "v1_prepare_linux.sh"
WINDOWS = WRAPPERS / "v1_prepare_windows.ps1"

DECLARED_PROFILES = sorted(HOST_PROFILES - {"unspecified"})
TIMED_SCENARIOS = sorted(
    scenario
    for scenario, spec in SCENARIOS.items()
    if spec.minimum_elapsed_seconds > 0
)
UNTIMED_SCENARIOS = sorted(set(SCENARIOS) - set(TIMED_SCENARIOS))


def _posix() -> str:
    return POSIX.read_text(encoding="utf-8")


def _windows() -> str:
    return WINDOWS.read_text(encoding="utf-8")


def test_both_wrappers_require_a_declared_host_profile() -> None:
    posix = _posix()
    windows = _windows()
    for profile in DECLARED_PROFILES:
        assert profile in posix, f"POSIX wrapper does not offer profile {profile}"
        assert profile in windows, f"Windows wrapper does not offer profile {profile}"
    assert "--profile" in posix
    assert "--profile" in windows
    assert "unspecified" not in posix
    assert "unspecified" not in windows


def test_both_wrappers_drive_the_checked_in_runner() -> None:
    for text in (_posix(), _windows()):
        assert "scripts.acceptance.v1_physical_runner" in text
        assert "scripts.acceptance.v1_physical_campaign" in text
        assert "scripts.acceptance.v1_physical_campaign_strict" in text


def test_wrapper_automation_covers_every_non_timed_scenario() -> None:
    posix = re.search(r"UNTIMED_SCENARIOS=\(([^)]*)\)", _posix())
    assert posix is not None
    posix_scenarios = sorted(posix.group(1).split())
    windows = re.search(r"\$UntimedScenarios = @\(([^)]*)\)", _windows())
    assert windows is not None
    windows_scenarios = sorted(
        item.strip().strip('"') for item in windows.group(1).split(",")
    )
    assert posix_scenarios == UNTIMED_SCENARIOS
    assert windows_scenarios == UNTIMED_SCENARIOS


def test_wrappers_can_sample_a_named_timed_run() -> None:
    posix = _posix()
    windows = _windows()
    assert "sample-scenario" in posix
    assert 'sample_run_id="${7:-}"' in posix
    assert '"$sample_scenario"' in posix
    assert '"$sample_run_id"' in posix
    assert "$SampleScenario" in windows
    assert "$SampleRunId" in windows
    assert '"--run-id", $SampleRunId' in windows


def _executable_lines(text: str) -> str:
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )


def test_wrappers_never_start_a_timed_run_implicitly() -> None:
    for text in (_posix(), _windows()):
        executable = _executable_lines(text)
        assert " begin " not in executable
        assert '"begin"' not in executable
        for scenario in TIMED_SCENARIOS:
            for pattern in (
                rf"--scenario\s+\"?{scenario}\"?",
                rf"\"--scenario\",\s*\"{scenario}\"",
            ):
                assert re.search(pattern, executable) is None, (
                    f"a wrapper drives timed scenario {scenario} directly"
                )


def test_wrappers_validate_through_the_strict_release_decision() -> None:
    assert "strict validate --commit" in _posix()
    assert 'Invoke-Strict @("validate"' in _windows()


def test_wrappers_forward_the_explicit_runtime_binding_when_configured() -> None:
    assert "FCP_RUNTIME_BINDING" in _posix()
    assert "--runtime-binding" in _posix()
    assert "FCP_RUNTIME_BINDING" in _windows()
    assert "--runtime-binding" in _windows()
