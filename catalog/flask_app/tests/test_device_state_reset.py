from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from catalog.flask_app.services.device_state_reset import (
    planned_reset_targets,
    reset_device_state,
    verify_fresh_application_state,
)


def _write(path: Path, content: str = "state") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_sqlite_state(path: Path) -> None:
    _write(path)
    _write(Path(f"{path}-wal"))
    _write(Path(f"{path}-shm"))


def _write_raw_recording(raw_path: Path, payload: bytes) -> Path:
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(raw_path, "wb") as handle:
        handle.write(payload)
    manifest = raw_path.with_suffix(".manifest.json")
    manifest.write_text(
        json.dumps(
            {
                "raw_file": str(raw_path),
                "raw_sha256": hashlib.sha256(payload).hexdigest(),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return manifest


def _snapshot_tree(root: Path) -> dict[str, bytes]:
    if not root.exists():
        return {}
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def _isolated_environ(relay_root: Path) -> dict[str, str]:
    """Keep unit tests independent from FCP_* variables on the CI runner."""

    return {
        "FCP_FEDERATION_COORDINATOR_DATABASE": str(relay_root / "control.sqlite3")
    }


def test_fresh_reset_removes_all_mutable_state_and_preserves_recordings_only(
    tmp_path: Path,
) -> None:
    app_root = tmp_path / "app"
    data_root = app_root / "data"
    results_root = app_root / "results"
    relay_root = tmp_path / "relay-state"

    identity = data_root / "custom-state" / "identity"
    onboarding = data_root / "custom-state" / "onboarding.sqlite3"
    transition = data_root / "custom-state" / "transition.sqlite3"
    benchmark = data_root / "custom-state" / "benchmark.sqlite3"
    contribution = data_root / "custom-state" / "contribution.sqlite3"
    coordinator = relay_root / "custom" / "control.sqlite3"
    remote_pairing = data_root / "custom-state" / "pairing.json"
    auth_database = data_root / "auth" / "users.sqlite3"
    auth_secret_dir = data_root / "auth"

    _write(identity / "identity.json")
    for database in (
        onboarding,
        transition,
        benchmark,
        contribution,
        coordinator,
        auth_database,
    ):
        _write_sqlite_state(database)
    _write(remote_pairing)
    _write(auth_secret_dir / "flask-secret", "old-session-secret")
    _write(auth_secret_dir / "password-salt", "old-password-salt")

    mutable = (
        data_root / "federation" / "legacy" / "binding.json",
        data_root / "server_setup" / "server_settings.json",
        data_root / "source_config" / "sources.json",
        data_root / "source_state" / "mtconnect_recorder_state.json",
        data_root / "source_state" / "mtconnect_recorder_control.json",
        data_root / "source_state" / "mtconnect_recorder_status.json",
        data_root / "source_state" / "mtconnect_recorder.log",
        data_root / "capabilities" / "config.json",
        data_root / "operator_strategy_records" / "note.json",
        data_root / "digital_twin" / "projection.json",
        data_root / "activity" / "events.jsonl",
        data_root / "jobs" / "job.json",
        results_root / "workflows" / "result.json",
        results_root / "analyses" / "analysis.json",
        relay_root / "legacy-authority.sqlite3",
    )
    for path in mutable:
        _write(path)

    recordings = data_root / "sources"
    _write(
        recordings / "legacy-machine" / "jsonl" / "telemetry.jsonl",
        "legacy-recording",
    )
    _write(
        recordings / "legacy-machine" / "observations" / "batch.jsonl",
        "normalized-recording",
    )
    _write_raw_recording(
        recordings
        / "mtconnect_recorder"
        / "raw"
        / "machine-a"
        / "instance-a"
        / "2026-08-07"
        / "seq-1-2-deadbeef.xml.gz",
        b"<MTConnectStreams>immutable raw payload</MTConnectStreams>",
    )
    recording_snapshot = _snapshot_tree(recordings)

    # Deployment/source files are outside the mounted application-state roots and
    # are not part of the reset surface.
    _write(app_root / ".env", "deployment-config")
    external_model = tmp_path / "ollama-model-volume" / "model.bin"
    _write(external_model, "model")

    environ = {
        "FCP_FEDERATION_NODE_STATE_DIR": str(identity),
        "FCP_FEDERATION_ONBOARDING_DATABASE": str(onboarding),
        "FCP_FEDERATION_TRANSITION_DATABASE": str(transition),
        "FCP_FEDERATION_BENCHMARK_DATABASE": str(benchmark),
        "FCP_FEDERATION_CONTRIBUTION_DATABASE": str(contribution),
        "FCP_FEDERATION_COORDINATOR_DATABASE": str(coordinator),
        "FCP_FEDERATION_REMOTE_PAIRING_PATH": str(remote_pairing),
        "FCP_AUTH_DATABASE": str(auth_database),
        "FCP_AUTH_SECRET_DIR": str(auth_secret_dir),
    }

    removed = reset_device_state(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    )

    assert removed
    assert _snapshot_tree(recordings) == recording_snapshot
    assert set(path.name for path in data_root.iterdir()) == {"sources"}
    assert results_root.exists()
    assert list(results_root.iterdir()) == []
    assert relay_root.exists()
    assert list(relay_root.iterdir()) == []
    assert (app_root / ".env").read_text(encoding="utf-8") == "deployment-config"
    assert external_model.read_text(encoding="utf-8") == "model"

    assert verify_fresh_application_state(
        environ=environ,
        app_root=app_root,
        relay_root=relay_root,
    ) == ()


def test_verify_fresh_detects_any_mutable_residue(tmp_path: Path) -> None:
    app_root = tmp_path / "app"
    relay_root = tmp_path / "relay-state"
    _write(
        app_root / "data" / "sources" / "machine" / "jsonl" / "telemetry.jsonl",
        "recording",
    )
    _write(app_root / "data" / "auth" / "users.sqlite3", "old-user")
    _write(app_root / "results" / "analysis.json", "old-analysis")
    _write(relay_root / "control.sqlite3", "old-federation")

    problems = verify_fresh_application_state(
        environ=_isolated_environ(relay_root),
        app_root=app_root,
        relay_root=relay_root,
    )

    assert any("mutable data still exists" in problem for problem in problems)
    assert any("analysis/job/result state still exists" in problem for problem in problems)
    assert any("relay/Federation authority still exists" in problem for problem in problems)
    assert any("human authentication database still exists" in problem for problem in problems)


def test_corrupt_recording_blocks_reset_before_any_mutable_state_is_deleted(
    tmp_path: Path,
) -> None:
    app_root = tmp_path / "app"
    relay_root = tmp_path / "relay-state"
    mutable = app_root / "data" / "auth" / "users.sqlite3"
    _write(mutable, "must-survive-failed-reset")

    raw_path = (
        app_root
        / "data"
        / "sources"
        / "mtconnect_recorder"
        / "raw"
        / "machine"
        / "instance"
        / "2026-08-07"
        / "seq-1-1-bad.xml.gz"
    )
    manifest = _write_raw_recording(raw_path, b"valid raw bytes")
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["raw_sha256"] = "0" * 64
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="digest mismatch"):
        reset_device_state(
            environ=_isolated_environ(relay_root),
            app_root=app_root,
            relay_root=relay_root,
        )

    assert mutable.read_text(encoding="utf-8") == "must-survive-failed-reset"
    assert raw_path.exists()
    assert manifest.exists()


def test_reset_rejects_configured_paths_outside_bounded_mounts_before_deletion(
    tmp_path: Path,
) -> None:
    app_root = tmp_path / "app"
    relay_root = tmp_path / "relay-state"
    outside = tmp_path / "outside" / "identity"
    mutable = app_root / "data" / "auth" / "users.sqlite3"
    _write(mutable, "keep-on-validation-failure")
    environ = _isolated_environ(relay_root)
    environ["FCP_FEDERATION_NODE_STATE_DIR"] = str(outside)

    with pytest.raises(RuntimeError, match="Refusing to remove"):
        planned_reset_targets(
            environ=environ,
            app_root=app_root,
            relay_root=relay_root,
        )

    assert mutable.read_text(encoding="utf-8") == "keep-on-validation-failure"


def test_reset_rejects_mutable_state_configured_inside_recording_corpus(
    tmp_path: Path,
) -> None:
    app_root = tmp_path / "app"
    relay_root = tmp_path / "relay-state"
    collision = app_root / "data" / "sources" / "auth.sqlite3"
    environ = _isolated_environ(relay_root)
    environ["FCP_AUTH_DATABASE"] = str(collision)

    with pytest.raises(RuntimeError, match="overlaps preserved recordings"):
        planned_reset_targets(
            environ=environ,
            app_root=app_root,
            relay_root=relay_root,
        )
