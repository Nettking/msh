from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import backup_recovery as backup
from catalog.federation.host_resources import FilesystemMeasurement


def _measurement(resource_id: str, *, free_bytes: int = 10**12) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id=resource_id,
        observed_at=datetime.now(timezone.utc),
        total_bytes=free_bytes * 2,
        free_bytes=free_bytes,
        total_inodes=1_000_000,
        free_inodes=900_000,
        available=True,
        error_code=None,
    )


def _layout(tmp_path: Path) -> backup.BackupLayout:
    data = tmp_path / "data"
    results = tmp_path / "results"
    data.mkdir(exist_ok=True)
    results.mkdir(exist_ok=True)
    return backup.BackupLayout(
        data_dir=data,
        results_dir=results,
        relay_volume="fcp_relay_state",
        relay_container="relay-container",
        relay_image="sha256:relay-image",
    )


def test_destination_on_a_source_backing_resource_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    layout = _layout(tmp_path)
    external = tmp_path / "external"
    external.mkdir()
    destination = external / "backup"

    monkeypatch.setattr(backup, "docker_backing_resource_path", lambda _root: repo)
    monkeypatch.setattr(backup, "measure_filesystem", lambda _path: _measurement("same"))

    with pytest.raises(backup.BackupError, match="backup_destination_not_independent"):
        backup._preflight_destination(
            repo,
            destination,
            layout,
            backup.TreeEstimate(1024, 3),
        )


def test_destination_capacity_is_proven_before_quiescence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    layout = _layout(tmp_path)
    external = tmp_path / "external"
    external.mkdir()
    destination = external / "backup"
    events: list[str] = []

    monkeypatch.setattr(backup, "_require_clean_checkout", lambda _root: None)
    monkeypatch.setattr(backup, "_git_commit", lambda _root: "a" * 40)
    monkeypatch.setattr(backup, "_resolve_layout", lambda _root: layout)
    monkeypatch.setattr(backup, "_refuse_live_native_recorder", lambda _layout: None)
    monkeypatch.setattr(
        backup,
        "_local_estimate",
        lambda _root, _layout: backup.TreeEstimate(100, 1),
    )
    monkeypatch.setattr(
        backup,
        "_scan_relay_volume",
        lambda _root, _layout: backup.TreeEstimate(200, 2),
    )

    def preflight(*_args, **_kwargs):
        events.append("preflight")
        return backup.CapacityRequirement(1000, 10)

    monkeypatch.setattr(backup, "_preflight_destination", preflight)

    @contextmanager
    def lock(*_args, **_kwargs):
        events.append("lock-enter")
        yield
        events.append("lock-exit")

    monkeypatch.setattr(backup, "host_mutation_lock", lock)
    monkeypatch.setattr(backup, "_quiesce_responder", lambda _data: events.append("responder-stop"))
    monkeypatch.setattr(backup, "_stop_compose_and_prove", lambda _root: events.append("compose-stop"))
    monkeypatch.setattr(backup, "_create_destination", lambda _dest: events.append("create"))
    monkeypatch.setattr(backup, "_copy_local_state", lambda *_args: events.append("copy-local"))
    monkeypatch.setattr(backup, "_copy_relay", lambda *_args: events.append("copy-relay"))
    monkeypatch.setattr(backup, "_integrity_checks", lambda _dest: [])
    monkeypatch.setattr(backup, "_complete_manifest", lambda *_args, **_kwargs: events.append("complete"))
    monkeypatch.setattr(backup, "verify_backup", lambda _dest: {"source_commit": "a" * 40})

    backup.create_quiesced_backup(repo, destination)

    assert events[0] == "preflight"
    assert events.index("preflight") < events.index("lock-enter")
    assert events.index("compose-stop") < events.index("create")
    assert events.count("preflight") == 2
    assert events.index("copy-local") < events.index("complete")
    assert events.index("copy-relay") < events.index("complete")


def test_a_failed_copy_never_implicitly_restarts_fcp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    layout = _layout(tmp_path)
    external = tmp_path / "external"
    external.mkdir()
    destination = external / "backup"
    events: list[str] = []

    monkeypatch.setattr(backup, "_require_clean_checkout", lambda _root: None)
    monkeypatch.setattr(backup, "_git_commit", lambda _root: "b" * 40)
    monkeypatch.setattr(backup, "_resolve_layout", lambda _root: layout)
    monkeypatch.setattr(backup, "_refuse_live_native_recorder", lambda _layout: None)
    monkeypatch.setattr(
        backup,
        "_local_estimate",
        lambda *_args: backup.TreeEstimate(1, 1),
    )
    monkeypatch.setattr(
        backup,
        "_scan_relay_volume",
        lambda *_args: backup.TreeEstimate(1, 1),
    )
    monkeypatch.setattr(
        backup,
        "_preflight_destination",
        lambda *_args: backup.CapacityRequirement(10, 10),
    )

    @contextmanager
    def lock(*_args, **_kwargs):
        yield

    monkeypatch.setattr(backup, "host_mutation_lock", lock)
    monkeypatch.setattr(backup, "_quiesce_responder", lambda _data: None)
    monkeypatch.setattr(backup, "_stop_compose_and_prove", lambda _root: events.append("stopped"))
    monkeypatch.setattr(backup, "_create_destination", lambda _dest: events.append("created"))
    monkeypatch.setattr(backup, "_copy_local_state", lambda *_args: events.append("local-copied"))

    def fail_relay(*_args):
        events.append("relay-copy-failed")
        raise backup.BackupError("relay_copy_failed")

    monkeypatch.setattr(backup, "_copy_relay", fail_relay)

    with pytest.raises(backup.BackupError, match="relay_copy_failed"):
        backup.create_quiesced_backup(repo, destination)

    assert events == ["stopped", "created", "local-copied", "relay-copy-failed"]


def test_relay_helper_cannot_pull_or_use_the_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    captured: list[str] = []

    def command(argv, **_kwargs):
        captured.extend(argv)
        return json.dumps({"bytes": 12, "files": 3})

    monkeypatch.setattr(backup, "_require_command", command)

    assert backup._scan_relay_volume(tmp_path, layout) == backup.TreeEstimate(12, 3)
    assert "--pull=never" in captured
    assert "--network=none" in captured
    assert layout.relay_image in captured


def test_live_native_recorder_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    status_file = layout.data_dir / "source_state" / "mtconnect_recorder_status.json"
    status_file.parent.mkdir()
    status_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        backup.native_update,
        "read_json",
        lambda _path: {"native_runtime": {"schema": "present"}},
    )
    monkeypatch.setattr(
        backup.native_update,
        "read_recorder_status",
        lambda _path: SimpleNamespace(present=True, is_running=lambda: True),
    )

    with pytest.raises(backup.BackupError, match="native_recorder_active"):
        backup._refuse_live_native_recorder(layout)


def test_matching_responder_is_stopped_by_process_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    record = data / backup.PID_RELATIVE
    record.parent.mkdir(parents=True)
    record.write_text(
        json.dumps(
            {
                "schema": backup.PROCESS_RECORD_SCHEMA,
                "pid": 4242,
                "start_token": "token",
            }
        ),
        encoding="utf-8",
    )
    tokens = iter(["token", None])
    monkeypatch.setattr(backup, "process_start_token", lambda _pid: next(tokens))
    terminated: list[tuple[int, str]] = []
    monkeypatch.setattr(
        backup,
        "terminate_process_if_same_instance",
        lambda pid, token: terminated.append((pid, token)) or True,
    )

    backup._quiesce_responder(data)

    assert terminated == [(4242, "token")]


def test_stale_responder_record_never_authorizes_termination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    record = data / backup.PID_RELATIVE
    record.parent.mkdir(parents=True)
    record.write_text(
        json.dumps(
            {
                "schema": backup.PROCESS_RECORD_SCHEMA,
                "pid": 4242,
                "start_token": "old-token",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(backup, "process_start_token", lambda _pid: "replacement-token")

    def forbidden(*_args):
        raise AssertionError("a reused PID must never be terminated")

    monkeypatch.setattr(backup, "terminate_process_if_same_instance", forbidden)

    backup._quiesce_responder(data)


def test_sqlite_backup_copy_must_pass_quick_check(tmp_path: Path) -> None:
    database = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE example(value TEXT NOT NULL)")
    connection.execute("INSERT INTO example(value) VALUES ('ok')")
    connection.commit()
    connection.close()

    assert backup._integrity_checks(tmp_path) == ["state.sqlite3"]


def test_incomplete_backup_is_never_accepted(tmp_path: Path) -> None:
    backup_root = tmp_path / "backup"
    backup_root.mkdir()
    (backup_root / backup.BACKUP_INCOMPLETE_NAME).write_text("incomplete\n", encoding="utf-8")

    with pytest.raises(backup.BackupError, match="backup_incomplete"):
        backup.verify_backup(backup_root)


def test_complete_manifest_is_verified_against_sqlite_inventory(tmp_path: Path) -> None:
    backup_root = tmp_path / "backup"
    backup_root.mkdir()
    database = backup_root / "state.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE example(value INTEGER)")
    connection.commit()
    connection.close()
    checks = backup._integrity_checks(backup_root)
    manifest = {
        "schema": backup.BACKUP_SCHEMA,
        "complete": True,
        "source_commit": "c" * 40,
        "sqlite_quick_check": checks,
        "sqlite_inventory_sha256": backup._directory_digest(backup_root),
    }
    (backup_root / backup.BACKUP_MANIFEST_NAME).write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    assert backup.verify_backup(backup_root)["source_commit"] == "c" * 40


def test_tree_scan_refuses_symlinked_content(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    link = source / "link.txt"
    try:
        link.symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("host does not permit symlink creation")

    with pytest.raises(backup.BackupError, match="backup_source_contains_link"):
        backup._scan_tree(source)


def test_compose_layout_uses_effective_bind_and_volume_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "configured-data"
    results = tmp_path / "configured-results"
    data.mkdir()
    results.mkdir()
    config = {
        "services": {
            "flask": {
                "volumes": [
                    {"type": "bind", "source": str(data), "target": backup.DATA_TARGET},
                    {
                        "type": "bind",
                        "source": str(results),
                        "target": backup.RESULTS_TARGET,
                    },
                ]
            },
            "relay": {
                "volumes": [
                    {
                        "type": "volume",
                        "source": "custom_relay",
                        "target": backup.RELAY_TARGET,
                    }
                ]
            },
        }
    }
    monkeypatch.setattr(backup, "_compose_config", lambda _root: config)
    monkeypatch.setattr(backup, "_container_id", lambda _root, _service: "relay-id")
    monkeypatch.setattr(backup, "_relay_image", lambda _root, _container: "relay-image")

    layout = backup._resolve_layout(tmp_path)

    assert layout.data_dir == data.resolve()
    assert layout.results_dir == results.resolve()
    assert layout.relay_volume == "custom_relay"
