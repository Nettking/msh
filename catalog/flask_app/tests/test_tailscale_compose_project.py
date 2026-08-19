"""Regression coverage for Compose project identity across host zero-touch orchestration."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_windows_tailscale_launcher_keeps_host_and_child_on_fcp_project() -> None:
    script = (ROOT / "start-tailscale.cmd").read_text(encoding="utf-8")
    project = 'if not defined COMPOSE_PROJECT_NAME set "COMPOSE_PROJECT_NAME=fcp"'

    assert project in script
    project_index = script.index(project)
    assert project_index < script.index("scripts\\first_federation_start.py")
    assert project_index < script.index('call "%~dp0start.cmd"')
    assert project_index < script.index("scripts\\zero_touch_federation_start.py")


def test_posix_tailscale_launcher_exports_same_fcp_project_to_host_orchestration() -> None:
    script = (ROOT / "start-tailscale.sh").read_text(encoding="utf-8")
    project = ': "${COMPOSE_PROJECT_NAME:=fcp}"'
    exported = "export COMPOSE_PROJECT_NAME"

    assert project in script
    assert exported in script
    project_index = script.index(project)
    export_index = script.index(exported)
    assert project_index < export_index
    assert export_index < script.index("scripts/first_federation_start.py")
    assert export_index < script.index('sh "$ROOT/start.sh"')
    assert export_index < script.index("scripts/zero_touch_federation_start.py")
