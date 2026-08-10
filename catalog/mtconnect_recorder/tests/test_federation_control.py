from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.flask_app.services.capability_config_service import (
    default_capability_config,
    load_capability_config,
    parse_recorder_sources,
    save_capability_config,
    update_recorder_config,
)
from catalog.flask_app.services.mtconnect_discovery_service import (
    MtconnectDiscoveryError,
)
from catalog.mtconnect_recorder import federation_control as control_module
from catalog.mtconnect_recorder.federation_control import (
    RecorderFederationControlWorker,
    infer_private_scan_cidr,
)


class _Runtime:
    def __init__(self) -> None:
        self.fail = False
        self.events: list[tuple[str, dict, str]] = []

    def append_session_event(
        self,
        _remote,
        *,
        session_id: str,
        event_type: str,
        payload: dict,
        request_id: str,
    ) -> None:
        if self.fail:
            raise RuntimeError("relay unavailable")
        assert session_id == "session-one"
        self.events.append((event_type, payload, request_id))


class _Node:
    def __init__(self) -> None:
        self.runtime = _Runtime()
        self.source_names: tuple[str, ...] = ()
        self.announcements: list[tuple[str, ...]] = []
        self.remote_store = SimpleNamespace(load=lambda: None)

    def _announce(self, _remote) -> None:
        self.announcements.append(self.source_names)


def _remote():
    return SimpleNamespace(
        relay_url="ws://192.168.1.10:8765",
        binding=SimpleNamespace(
            device_id="node-recorder",
            internal_session_id="session-one",
        ),
    )


def _write_scan(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema": "fcp.mtconnect.discovery.v1",
                "status": "complete",
                "results": [
                    {
                        "source_id": "source-new",
                        "source_name": "new-machine",
                        "display_name": "New machine",
                        "base_url": "http://192.168.1.22:5000",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_source_change_uses_latest_scan_and_updates_managed_recorder(tmp_path: Path) -> None:
    config_path = tmp_path / "capabilities" / "config.json"
    config = update_recorder_config(
        default_capability_config(),
        recorder_sources="old-machine=http://192.168.1.21:5000",
    )
    save_capability_config(config, config_path)

    node = _Node()
    worker = RecorderFederationControlWorker(node, data_directory=tmp_path)
    _write_scan(worker.discovery.scan_path)
    state = {"last_scan_id": "scan-one"}

    event_type, report = worker._change_sources(
        {
            "request_id": "change-one",
            "scan_id": "scan-one",
            "add_source_ids": ("source-new",),
            "remove_source_names": ("old-machine",),
        },
        remote=_remote(),
        state=state,
    )

    sources = parse_recorder_sources(load_capability_config(config_path).recorder_sources)
    assert sources == {"new-machine": "http://192.168.1.22:5000"}
    assert event_type == "recorder.control.sources.reported"
    assert report["configured_source_names"] == ["new-machine"]
    assert node.source_names == ("new-machine",)
    assert node.announcements == [("new-machine",)]

    control = json.loads(worker.control_path.read_text(encoding="utf-8"))
    assert control["enabled"] is True
    marker = json.loads(worker.auto_config_path.read_text(encoding="utf-8"))
    assert marker["initial_selection_complete"] is True


def test_stale_scan_cannot_add_sources(tmp_path: Path) -> None:
    config_path = tmp_path / "capabilities" / "config.json"
    save_capability_config(default_capability_config(), config_path)
    worker = RecorderFederationControlWorker(_Node(), data_directory=tmp_path)
    _write_scan(worker.discovery.scan_path)

    with pytest.raises(MtconnectDiscoveryError):
        worker._change_sources(
            {
                "request_id": "change-stale",
                "scan_id": "older-scan",
                "add_source_ids": ("source-new",),
                "remove_source_names": (),
            },
            remote=_remote(),
            state={"last_scan_id": "latest-scan"},
        )


def test_report_checkpoint_advances_only_after_relay_accepts_report(tmp_path: Path) -> None:
    node = _Node()
    worker = RecorderFederationControlWorker(node, data_directory=tmp_path)
    state = {
        "last_revision": 4,
        "pending_report": {
            "revision": 5,
            "request_id": "scan-one",
            "event_type": "recorder.control.scan.reported",
            "payload": {"schema": "fcp.recorder-control.v1"},
        },
    }

    node.runtime.fail = True
    with pytest.raises(RuntimeError):
        worker._flush_pending(_remote(), state)
    assert state["last_revision"] == 4
    assert state["pending_report"] is not None

    node.runtime.fail = False
    worker._flush_pending(_remote(), state)
    assert state["last_revision"] == 5
    assert state["pending_report"] is None
    assert len(node.runtime.events) == 1


def test_private_scan_cidr_inference_is_bounded_to_one_slash24(monkeypatch) -> None:
    monkeypatch.setattr(
        control_module,
        "_route_address",
        lambda _host, _port: "192.168.42.17",
    )

    assert infer_private_scan_cidr() == "192.168.42.0/24"
