from __future__ import annotations

from pathlib import Path

from catalog.flask_app.services.device_state_reset import (
    reset_device_state,
    verify_fresh_application_state,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_fresh_reset_never_deletes_git_tracked_root_scaffolding(tmp_path: Path) -> None:
    app_root = tmp_path / "app"
    data_root = app_root / "data"
    results_root = app_root / "results"
    relay_root = tmp_path / "relay-state"

    scaffolding = {
        data_root / ".gitkeep": "data-gitkeep",
        data_root / "README.md": "data-readme",
        results_root / ".gitkeep": "results-gitkeep",
        results_root / "README.md": "results-readme",
    }
    for path, content in scaffolding.items():
        _write(path, content)

    mutable_data = data_root / "auth" / "users.sqlite3"
    mutable_result = results_root / "analysis.json"
    mutable_relay = relay_root / "control.sqlite3"
    _write(mutable_data, "mutable-user-state")
    _write(mutable_result, "mutable-result")
    _write(mutable_relay, "mutable-relay")

    environ = {"FCP_FEDERATION_COORDINATOR_DATABASE": str(mutable_relay)}
    removed = reset_device_state(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )

    for path, expected in scaffolding.items():
        assert path.read_text(encoding="utf-8") == expected
        assert path not in removed

    assert not mutable_data.exists()
    assert not mutable_result.exists()
    assert not mutable_relay.exists()
    assert verify_fresh_application_state(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    ) == ()


def test_scaffolding_name_is_not_a_nested_mutable_state_escape(tmp_path: Path) -> None:
    app_root = tmp_path / "app"
    data_root = app_root / "data"
    relay_root = tmp_path / "relay-state"

    _write(data_root / ".gitkeep", "root-scaffold")
    nested_mutable = data_root / "auth" / "README.md"
    _write(nested_mutable, "mutable-auth-state")

    environ = {
        "FCP_FEDERATION_COORDINATOR_DATABASE": str(relay_root / "control.sqlite3")
    }
    reset_device_state(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )

    assert (data_root / ".gitkeep").exists()
    assert not nested_mutable.exists()
