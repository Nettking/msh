from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
    ProcessResourceAdmission,
)
from catalog.runner import script_exec
from catalog.runner.script_catalog import ScriptOption


NOW = datetime(2026, 8, 31, tzinfo=timezone.utc)


def _measurement(*, free_bytes: int) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id="script-resource",
        observed_at=NOW,
        total_bytes=10_000_000,
        free_bytes=free_bytes,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )


def _admission(free_bytes: int) -> ProcessResourceAdmission:
    return ProcessResourceAdmission(
        thresholds=PressureThresholds(
            critical_free_bytes=100,
            pressure_free_bytes=200,
            warning_free_bytes=300,
            critical_free_inodes=0,
            pressure_free_inodes=0,
            warning_free_inodes=0,
            max_measurement_age_seconds=60,
        ),
        measurer=lambda _path: _measurement(free_bytes=free_bytes),
        clock=lambda: NOW,
    )


def _script_option(key: str = "bounded_script") -> ScriptOption:
    return ScriptOption(
        number=1,
        key=key,
        script_path=Path("catalog/runner/tests/bounded_script.py"),
        description=key,
        category="Simple",
    )


def test_direct_script_path_refuses_before_creating_run_tree(tmp_path: Path) -> None:
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    metadata = {
        "paths": {"runs_dir": "runs"},
        "scripts": {"bounded_script": {"status": "not_run"}},
    }

    with pytest.raises(HostResourceRefused):
        script_exec.execute_script_for_session(
            session_dir=session_dir,
            metadata=metadata,
            script=_script_option(),
            resource_admission=_admission(102),
        )

    assert not (session_dir / "runs").exists()


def test_running_script_is_terminated_when_persistent_output_exceeds_bound(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "run"
    workspace.mkdir()
    script = tmp_path / "writer.py"
    script.write_text(
        "from pathlib import Path\n"
        "Path('output.bin').write_bytes(b'x' * 4096)\n"
        "while True: pass\n",
        encoding="utf-8",
    )

    with pytest.raises(script_exec.ScriptWorkspaceLimitExceeded):
        script_exec.run_script(
            script,
            workspace,
            max_workspace_bytes=1024,
            max_workspace_inodes=32,
        )

    assert (workspace / "output.bin").stat().st_size == 4096
