from __future__ import annotations

import json
from pathlib import Path

import start_recorder
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
