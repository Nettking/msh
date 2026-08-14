from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import start_recorder
from catalog.federation.errors import FederationOperationError
from catalog.flask_app.services.capability_config_service import (
    load_capability_config,
    parse_recorder_sources,
    update_recorder_config,
)


class _Discovery:
    merge_calls = 0

    def __init__(self, *, scan_path, config_path, checkpoint_path) -> None:
        self.scan_path = Path(scan_path)
        self.config_path = Path(config_path)
        self.checkpoint_path = Path(checkpoint_path)

    def recommended_cidr(self, _config) -> str:
        return ""

    def scan(self, cidr: str, *, port: int):
        assert cidr == "192.168.55.0/24"
        assert port == 5000
        return {
            "machines_found": 1,
            "agents_found": 1,
            "results": [
                {
                    "source_id": "source-one",
                    "source_name": "machine-one",
                    "display_name": "Machine one",
                    "base_url": "http://192.168.55.20:5000",
                }
            ],
        }

    def merge_selected_results(self, config, selected):
        type(self).merge_calls += 1
        assert selected == ["source-one"]
        return update_recorder_config(
            config,
            recorder_sources="machine-one=http://192.168.55.20:5000",
        )


def test_pairing_key_can_be_the_only_positional_argument(monkeypatch) -> None:
    monkeypatch.delenv("FCP_RECORDER_FEDERATION_KEY", raising=False)
    parser = start_recorder.build_parser()
    args = parser.parse_args(["FCP1-example-pairing-code"])

    pairing_key, sources = start_recorder._classify_inputs(parser, args)

    assert pairing_key == "FCP1-example-pairing-code"
    assert sources == []


def test_required_federation_refuses_silent_local_only_start(monkeypatch) -> None:
    class NoMembershipNode:
        instance = None

        def __init__(self, **_kwargs) -> None:
            self.stopped = False
            type(self).instance = self

        def has_saved_membership(self) -> bool:
            return False

        def stop(self) -> None:
            self.stopped = True

    monkeypatch.setattr(start_recorder, "RecorderFederationNode", NoMembershipNode)
    args = start_recorder.build_parser().parse_args(["--require-federation"])

    with pytest.raises(FederationOperationError) as excinfo:
        start_recorder._start_federation(
            args=args,
            pairing_key=None,
            data_dir=Path("data"),
            parsed_sources=[],
        )

    assert excinfo.value.code == "recorder-pairing-required"
    assert NoMembershipNode.instance.stopped is True


def test_required_data_sharing_waits_for_ready_publication(monkeypatch) -> None:
    class SharingNode:
        instance = None

        def __init__(self, **_kwargs) -> None:
            self.waited = None
            type(self).instance = self

        def has_saved_membership(self) -> bool:
            return True

        def bootstrap(self, _pairing_key):
            return SimpleNamespace(
                node_id="node-recorder",
                federation_id="federation-one",
                session_id="session-one",
                storage_group=None,
            )

        def wait_until_sharing_ready(self, *, timeout_seconds):
            self.waited = timeout_seconds
            return SimpleNamespace(
                node_id="node-recorder",
                federation_id="federation-one",
                session_id="session-one",
                storage_group="telemetry",
            )

        def stop(self) -> None:
            pass

    monkeypatch.setattr(start_recorder, "RecorderFederationNode", SharingNode)
    args = start_recorder.build_parser().parse_args(
        ["--require-data-sharing", "--sharing-timeout", "7"]
    )

    node = start_recorder._start_federation(
        args=args,
        pairing_key=None,
        data_dir=Path("data"),
        parsed_sources=[],
    )

    assert node is SharingNode.instance
    assert SharingNode.instance.waited == 7.0


def test_unexpected_sharing_failure_still_stops_federation_node(monkeypatch) -> None:
    class FailingNode:
        instance = None

        def __init__(self, **_kwargs) -> None:
            self.stopped = False
            type(self).instance = self

        def has_saved_membership(self) -> bool:
            return True

        def bootstrap(self, _pairing_key):
            return SimpleNamespace(storage_group=None)

        def wait_until_sharing_ready(self, *, timeout_seconds):
            assert timeout_seconds == 45.0
            raise ValueError("unexpected readiness defect")

        def stop(self) -> None:
            self.stopped = True

    monkeypatch.setattr(start_recorder, "RecorderFederationNode", FailingNode)
    args = start_recorder.build_parser().parse_args(["--require-data-sharing"])

    with pytest.raises(ValueError, match="unexpected readiness defect"):
        start_recorder._start_federation(
            args=args,
            pairing_key=None,
            data_dir=Path("data"),
            parsed_sources=[],
        )

    assert FailingNode.instance.stopped is True


@pytest.mark.parametrize("timeout", ["nan", "inf", "601"])
def test_start_recorder_rejects_unbounded_sharing_timeout(timeout: str) -> None:
    with pytest.raises(SystemExit) as excinfo:
        start_recorder.main(["--sharing-timeout", timeout])

    assert excinfo.value.code == 2


@pytest.mark.parametrize("timeout", ["nan", "inf", "121"])
def test_start_recorder_rejects_unbounded_federation_timeout(timeout: str) -> None:
    with pytest.raises(SystemExit) as excinfo:
        start_recorder.main(["--federation-timeout", timeout])

    assert excinfo.value.code == 2


def test_first_start_auto_scan_selects_discovered_sources(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _Discovery.merge_calls = 0
    monkeypatch.setattr(start_recorder, "MtconnectDiscoveryService", _Discovery)
    monkeypatch.setattr(
        start_recorder,
        "infer_private_scan_cidr",
        lambda: "192.168.55.0/24",
    )
    parser = start_recorder.build_parser()
    args = parser.parse_args(["--data-dir", str(tmp_path)])

    _config, sources = start_recorder._prepare_recorder_configuration(
        parser=parser,
        args=args,
        data_dir=tmp_path,
        manual_sources=[],
    )

    assert sources == [("machine-one", "http://192.168.55.20:5000")]
    assert _Discovery.merge_calls == 1
    configured = parse_recorder_sources(
        load_capability_config(
            tmp_path / "capabilities" / "config.json"
        ).recorder_sources
    )
    assert configured == {"machine-one": "http://192.168.55.20:5000"}
    control = json.loads(
        (tmp_path / "source_state" / "mtconnect_recorder_control.json").read_text(
            encoding="utf-8"
        )
    )
    assert control["enabled"] is True


def test_completed_empty_selection_is_not_repopulated_on_restart(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _Discovery.merge_calls = 0
    monkeypatch.setattr(start_recorder, "MtconnectDiscoveryService", _Discovery)
    monkeypatch.setattr(
        start_recorder,
        "infer_private_scan_cidr",
        lambda: "192.168.55.0/24",
    )
    marker = tmp_path / "source_state" / "mtconnect_recorder_autoconfig.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        json.dumps(
            {
                "schema": "fcp.mtconnect_recorder.autoconfig.v1",
                "initial_selection_complete": True,
            }
        ),
        encoding="utf-8",
    )
    parser = start_recorder.build_parser()
    args = parser.parse_args(["--data-dir", str(tmp_path)])

    _config, sources = start_recorder._prepare_recorder_configuration(
        parser=parser,
        args=args,
        data_dir=tmp_path,
        manual_sources=[],
    )

    assert sources == []
    assert _Discovery.merge_calls == 0
    control = json.loads(
        (tmp_path / "source_state" / "mtconnect_recorder_control.json").read_text(
            encoding="utf-8"
        )
    )
    assert control["enabled"] is False
