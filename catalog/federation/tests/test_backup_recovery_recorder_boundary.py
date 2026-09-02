from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import backup_recovery as backup


def _layout(tmp_path: Path) -> backup.BackupLayout:
    data = tmp_path / "data"
    data.mkdir()
    return backup.BackupLayout(
        data_dir=data,
        results_dir=None,
        relay_volume="fcp_relay_state",
        relay_container="relay-container",
        relay_image="sha256:relay-image",
    )


def _identity(
    *,
    pid: int = 1,
    process_nonce: str | None,
    supervisor_session: str | None,
) -> dict[str, object]:
    return {
        "schema": backup.native_update.NATIVE_RUNTIME_SCHEMA,
        "runtime_type": backup.native_update.NATIVE_RUNTIME_TYPE,
        "pid": pid,
        "process_nonce": process_nonce,
        "supervisor_session": supervisor_session,
        "build_commit": None,
    }


def test_compose_managed_recorder_heartbeat_is_left_for_compose_quiescence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    monkeypatch.setattr(
        backup.native_update,
        "read_json",
        lambda _path: {
            "managed": True,
            "native_runtime": _identity(
                process_nonce=None,
                supervisor_session=None,
            ),
        },
    )

    def forbidden_status_read(_path: Path) -> object:
        raise AssertionError(
            "a Compose heartbeat must be quiesced through Docker, not host PID identity"
        )

    monkeypatch.setattr(
        backup.native_update,
        "read_recorder_status",
        forbidden_status_read,
    )

    backup._refuse_live_native_recorder(layout)


def test_direct_managed_host_recorder_is_not_mistaken_for_compose(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    monkeypatch.setattr(
        backup.native_update,
        "read_json",
        lambda _path: {
            "managed": True,
            "native_runtime": _identity(
                pid=4242,
                process_nonce=None,
                supervisor_session=None,
            ),
        },
    )
    monkeypatch.setattr(
        backup.native_update,
        "read_recorder_status",
        lambda _path: SimpleNamespace(present=True, is_running=lambda: True),
    )

    with pytest.raises(backup.BackupError, match="native_recorder_active"):
        backup._refuse_live_native_recorder(layout)


def test_supervised_native_recorder_is_still_refused_when_managed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    monkeypatch.setattr(
        backup.native_update,
        "read_json",
        lambda _path: {
            "managed": True,
            "native_runtime": _identity(
                pid=4242,
                process_nonce="a" * 32,
                supervisor_session="b" * 32,
            ),
        },
    )
    monkeypatch.setattr(
        backup.native_update,
        "read_recorder_status",
        lambda _path: SimpleNamespace(present=True, is_running=lambda: True),
    )

    with pytest.raises(backup.BackupError, match="native_recorder_active"):
        backup._refuse_live_native_recorder(layout)


def test_malformed_managed_native_identity_still_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    monkeypatch.setattr(
        backup.native_update,
        "read_json",
        lambda _path: {
            "managed": True,
            "native_runtime": _identity(
                pid=4242,
                process_nonce=None,
                supervisor_session="not-a-valid-supervisor-session",
            ),
        },
    )
    monkeypatch.setattr(
        backup.native_update,
        "read_recorder_status",
        lambda _path: SimpleNamespace(present=False, is_running=lambda: False),
    )

    with pytest.raises(backup.BackupError, match="native_recorder_status_unreadable"):
        backup._refuse_live_native_recorder(layout)
