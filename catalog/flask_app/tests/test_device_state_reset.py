from __future__ import annotations

from pathlib import Path

import pytest

from catalog.flask_app.services.device_state_reset import (
    planned_reset_targets,
    reset_device_state,
)


def _write(path: Path, content: str = "state") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_sqlite_state(path: Path) -> None:
    _write(path)
    _write(Path(f"{path}-wal"))
    _write(Path(f"{path}-shm"))


def test_reset_follows_configured_paths_and_preserves_operational_data(
    tmp_path: Path,
) -> None:
    app_root = tmp_path / "app"
    data_root = app_root / "data"
    relay_root = tmp_path / "relay-state"

    identity = data_root / "custom-state" / "identity"
    onboarding = data_root / "custom-state" / "onboarding.sqlite3"
    transition = data_root / "custom-state" / "transition.sqlite3"
    benchmark = data_root / "custom-state" / "benchmark.sqlite3"
    contribution = data_root / "custom-state" / "contribution.sqlite3"
    coordinator = relay_root / "custom" / "control.sqlite3"
    remote_pairing = data_root / "custom-state" / "pairing.json"
    settings = data_root / "server_setup" / "server_settings.json"

    _write(identity / "identity.json")
    for database in (
        onboarding,
        transition,
        benchmark,
        contribution,
        coordinator,
    ):
        _write_sqlite_state(database)
    _write(remote_pairing)
    _write(settings)

    preserved = (
        data_root / "sources" / "machine" / "jsonl" / "telemetry.jsonl",
        data_root / "source_config" / "sources.json",
        data_root / "source_state" / "recorder_checkpoint.json",
        data_root / "operator_strategy_records" / "note.json",
        app_root / "results" / "workflows" / "result.json",
        tmp_path / "ollama-model-volume" / "model.bin",
    )
    for path in preserved:
        _write(path, "preserve")

    environ = {
        "MSH_FEDERATION_NODE_STATE_DIR": str(identity),
        "MSH_FEDERATION_ONBOARDING_DATABASE": str(onboarding),
        "MSH_FEDERATION_TRANSITION_DATABASE": str(transition),
        "MSH_FEDERATION_BENCHMARK_DATABASE": str(benchmark),
        "MSH_FEDERATION_CONTRIBUTION_DATABASE": str(contribution),
        "MSH_FEDERATION_COORDINATOR_DATABASE": str(coordinator),
        "MSH_FEDERATION_REMOTE_PAIRING_PATH": str(remote_pairing),
    }

    removed = reset_device_state(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )

    assert removed
    assert not identity.exists()
    for database in (
        onboarding,
        transition,
        benchmark,
        contribution,
        coordinator,
    ):
        assert not database.exists()
        assert not Path(f"{database}-wal").exists()
        assert not Path(f"{database}-shm").exists()
    assert not remote_pairing.exists()
    assert not settings.exists()

    for path in preserved:
        assert path.read_text(encoding="utf-8") == "preserve"


def test_reset_rejects_configured_paths_outside_bounded_mounts(
    tmp_path: Path,
) -> None:
    app_root = tmp_path / "app"
    relay_root = tmp_path / "relay-state"
    outside = tmp_path / "outside" / "identity"

    with pytest.raises(RuntimeError, match="Refusing to remove"):
        planned_reset_targets(
            environ={"MSH_FEDERATION_NODE_STATE_DIR": str(outside)},
            app_root=app_root,
            relay_root=relay_root,
        )
