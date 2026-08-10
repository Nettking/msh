from __future__ import annotations

import json
from typing import Any

import pytest

from catalog.capabilities.benchmarking import (
    BenchmarkRegistry,
    BenchmarkRunner,
    BenchmarkRunRequest,
    SQLiteBenchmarkResultStore,
)
from catalog.capabilities.benchmarking.adapters import (
    OllamaBenchmarkAdapter,
    OllamaProbeTarget,
    register_concrete_adapters,
)
from catalog.capabilities.benchmarking.adapters.ollama import OllamaProbeError
from catalog.federation.onboarding_models import BenchmarkState
from catalog.flask_app.app import create_app
from catalog.flask_app.services import capability_recovery_adapters


def _ollama_adapter(app) -> OllamaBenchmarkAdapter:
    with app.app_context():
        factory = app.config["CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS"]
        adapters = factory()
    matches = [
        adapter for adapter in adapters if isinstance(adapter, OllamaBenchmarkAdapter)
    ]
    assert len(matches) == 1
    return matches[0]


@pytest.mark.parametrize(
    ("containerized", "expected_url"),
    [
        (True, "http://ollama:11434"),
        (False, "http://127.0.0.1:11434"),
    ],
)
def test_optional_local_ollama_check_uses_runtime_reachable_endpoint(
    tmp_path,
    monkeypatch,
    containerized: bool,
    expected_url: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    monkeypatch.delenv("FCP_AI_MODEL", raising=False)
    monkeypatch.setattr(
        capability_recovery_adapters,
        "_running_in_container",
        lambda: containerized,
    )
    app = create_app()
    app.config["CAPABILITY_ONBOARDING_SETUP_LOADER"] = lambda: {
        "configured": True,
        "user_setup_complete": True,
        "deployment_mode": "web-workbench",
        "ai_enabled": False,
        "ai_provider_mode": "local",
        "ai_model": "llama3.2:3b",
        "ollama_base_url": "http://ollama:11434",
    }

    adapter = _ollama_adapter(app)
    target = adapter._targets["ollama-configured"]

    assert target.base_url == expected_url
    assert target.model == "llama3.2:3b"


def _run_ollama(
    tmp_path,
    *,
    requester,
):
    target = OllamaProbeTarget(
        service_id="ollama-runtime-parity",
        display_label="Runtime parity Ollama",
        base_url="http://127.0.0.1:11434",
        model="tiny-model",
    )
    adapter = OllamaBenchmarkAdapter((target,), requester=requester)
    registry = BenchmarkRegistry()
    register_concrete_adapters(registry, (adapter,))
    store = SQLiteBenchmarkResultStore(tmp_path / "runtime-parity.sqlite3")
    try:
        result = BenchmarkRunner(registry, store).run(
            BenchmarkRunRequest(
                run_id="runtime-parity-run",
                device_id="runtime-parity-device",
                benchmark_id=adapter.definition.benchmark_id,
                target_service_id=target.service_id,
                dependency_inputs=adapter.dependency_inputs(target.service_id),
                available_prerequisites=frozenset({"ollama-model-available"}),
            )
        )
    finally:
        store.close()
    return adapter, result


def test_ollama_benchmark_allows_bounded_cold_model_start(tmp_path) -> None:
    timeouts: list[float] = []

    def requester(
        method: str,
        url: str,
        payload: dict[str, Any] | None,
        timeout: float,
        maximum: int,
    ) -> dict[str, Any]:
        assert method == "POST"
        assert url.endswith("/api/chat")
        assert payload is not None
        assert maximum > 0
        timeouts.append(timeout)
        return {
            "message": {"content": "OK"},
            "eval_count": 1,
            "eval_duration": 100_000_000,
        }

    adapter, result = _run_ollama(tmp_path, requester=requester)

    assert adapter.definition.max_duration_seconds == 120
    assert len(timeouts) == 1
    assert 100 < timeouts[0] <= 120
    assert result.state is BenchmarkState.PASSED


def test_ollama_timeout_failure_is_actionable_and_private(tmp_path) -> None:
    def requester(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise OllamaProbeError("Ollama did not respond within the benchmark limit")

    _adapter, result = _run_ollama(tmp_path, requester=requester)

    assert result.state is BenchmarkState.FAILED
    assert result.diagnostics == (
        "Ollama did not respond within the benchmark limit",
    )
    serialized = json.dumps(result.to_dict())
    assert "127.0.0.1" not in serialized
    assert "11434" not in serialized
