from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.mtconnect_recorder import managed_service
from catalog.mtconnect_recorder.managed_service import (
    ManagedRecorderFederationRuntime,
)


class FakeNode:
    def __init__(self, *, paired: bool, calls: list[object], **kwargs: object) -> None:
        self.paired = paired
        self.calls = calls
        self.calls.append(("node", kwargs))

    def has_saved_membership(self) -> bool:
        self.calls.append("has-membership")
        return self.paired

    def bootstrap(self):
        self.calls.append("bootstrap")
        return SimpleNamespace(
            node_id="node-recorder",
            federation_id="federation-one",
            session_id="session-one",
        )

    def stop(self, *, timeout: float = 3.0) -> None:
        self.calls.append(("node-stop", timeout))


class FakeControl:
    def __init__(self, node: object, *, data_directory: Path) -> None:
        self.node = node
        self.data_directory = data_directory
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self, *, timeout: float = 3.0) -> None:
        del timeout
        self.stopped = True


def test_companion_waits_for_pairing_without_touching_capture(tmp_path: Path) -> None:
    calls: list[object] = []

    runtime = ManagedRecorderFederationRuntime(
        data_directory=tmp_path,
        node_factory=lambda **kwargs: FakeNode(
            paired=False,
            calls=calls,
            **kwargs,
        ),
        control_factory=FakeControl,
        source_names_loader=lambda _path: ("Mazak",),
    )

    assert runtime.connect_once() == "waiting_for_pairing"
    assert "bootstrap" not in calls
    assert runtime._node is None
    assert any(
        isinstance(value, tuple) and value[0] == "node-stop"
        for value in calls
    )


def test_companion_reconnects_saved_membership_and_starts_control(
    tmp_path: Path,
) -> None:
    calls: list[object] = []

    runtime = ManagedRecorderFederationRuntime(
        data_directory=tmp_path,
        device_name="FCP MTConnect recorder",
        node_factory=lambda **kwargs: FakeNode(
            paired=True,
            calls=calls,
            **kwargs,
        ),
        control_factory=FakeControl,
        source_names_loader=lambda _path: ("M8015RW221N", "MAZAK-M7ZDA13010Z"),
    )

    assert runtime.connect_once() == "connected"
    assert "bootstrap" in calls
    assert runtime._node is not None
    assert runtime._control is not None
    assert runtime._control.started is True

    node_call = next(
        value
        for value in calls
        if isinstance(value, tuple) and value[0] == "node"
    )
    kwargs = node_call[1]
    assert kwargs["source_names"] == ("M8015RW221N", "MAZAK-M7ZDA13010Z")

    runtime.stop()
    assert runtime._node is None
    assert runtime._control is None


def test_managed_entrypoint_starts_capture_even_without_pairing_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class Companion:
        def __init__(self, **_kwargs: object) -> None:
            events.append("companion-created")

        def start(self) -> None:
            events.append("companion-started")

        def stop(self) -> None:
            events.append("companion-stopped")

    monkeypatch.setenv("FCP_RECORDER_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(
        managed_service,
        "ensure_recorder_upgrade_config",
        lambda **_kwargs: events.append("upgrade-config"),
    )

    def capture() -> str:
        events.append("capture")
        return "done"

    result = managed_service.run_managed_recorder(
        capture_runner=capture,
        companion_factory=Companion,
    )

    assert result == "done"
    assert events == [
        "upgrade-config",
        "companion-created",
        "companion-started",
        "capture",
        "companion-stopped",
    ]


def test_managed_entrypoint_stops_companion_if_capture_exits_with_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class Companion:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            events.append("start")

        def stop(self) -> None:
            events.append("stop")

    monkeypatch.setenv("FCP_RECORDER_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(
        managed_service,
        "ensure_recorder_upgrade_config",
        lambda **_kwargs: None,
    )

    def capture() -> None:
        raise RuntimeError("capture-stopped")

    with pytest.raises(RuntimeError, match="capture-stopped"):
        managed_service.run_managed_recorder(
            capture_runner=capture,
            companion_factory=Companion,
        )

    assert events == ["start", "stop"]


def test_compose_recorder_uses_managed_federation_entrypoint() -> None:
    repository_root = Path(__file__).resolve().parents[3]
    compose = (repository_root / "docker-compose.yml").read_text(encoding="utf-8")

    assert (
        'entrypoint: ["python", "-m", '
        '"catalog.mtconnect_recorder.managed_service"]'
        in compose
    )
