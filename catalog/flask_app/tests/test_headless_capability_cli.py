from __future__ import annotations

from types import SimpleNamespace

from catalog.flask_app.services import headless_capability_cli as cli


def test_headless_inspection_uses_existing_read_only_service(monkeypatch) -> None:
    snapshot = SimpleNamespace(
        device_id="node-a",
        revision=3,
        os_family="linux",
        architecture="x86_64",
        detected_services=("ollama-configured",),
        detected_data_sources=("mtconnect-recorder-source",),
        registered_handlers=("python",),
        recommended_benchmark_ids=("ollama", "mtconnect"),
        warnings=("example warning",),
    )
    service = SimpleNamespace(run=lambda: snapshot)
    monkeypatch.setattr(cli, "get_capability_inspection_service", lambda: service)

    result = cli.run_inspection()

    assert result == {
        "action": "inspect",
        "state": "complete",
        "device_id": "node-a",
        "revision": 3,
        "os_family": "linux",
        "architecture": "x86_64",
        "detected_services": ["ollama-configured"],
        "detected_data_sources": ["mtconnect-recorder-source"],
        "registered_handlers": ["python"],
        "recommended_benchmark_ids": ["ollama", "mtconnect"],
        "warnings": ["example warning"],
    }
