from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from catalog.capabilities.benchmarking import (
    BenchmarkRegistry,
    BenchmarkRunRequest,
    BenchmarkRunner,
    CancellationToken,
    DeviceInspector,
    SQLiteBenchmarkResultStore,
    evaluate_result,
)
from catalog.capabilities.benchmarking.adapters import (
    AuthenticatedNetworkPathAdapter,
    AuthenticatedPathSample,
    AuthenticatedPathTarget,
    MtconnectSourceAdapter,
    OllamaBenchmarkAdapter,
    OllamaProbeTarget,
    RegisteredComputeHandlerAdapter,
    StorageCandidateAdapter,
    StorageCandidateTarget,
    register_concrete_adapters,
)
from catalog.federation.onboarding_models import BenchmarkState


def _runner(tmp_path: Any, adapter: Any) -> tuple[BenchmarkRunner, SQLiteBenchmarkResultStore]:
    registry = BenchmarkRegistry()
    register_concrete_adapters(registry, (adapter,))
    store = SQLiteBenchmarkResultStore(tmp_path / "benchmarks.sqlite3")
    return BenchmarkRunner(registry, store), store


def _request(
    adapter: Any,
    *,
    run_id: str,
    target_id: str,
    dependency_inputs: dict[str, Any],
    prerequisite: str,
    timeout_seconds: float | None = None,
    cancellation_token: CancellationToken | None = None,
) -> BenchmarkRunRequest:
    return BenchmarkRunRequest(
        run_id=run_id,
        device_id="device-test",
        benchmark_id=adapter.definition.benchmark_id,
        target_service_id=target_id,
        dependency_inputs=dependency_inputs,
        available_prerequisites=frozenset({prerequisite}),
        timeout_seconds=timeout_seconds,
        cancellation_token=cancellation_token,
    )


def test_ollama_inspection_and_bounded_inference_do_not_leak_endpoint(tmp_path: Any) -> None:
    calls: list[tuple[str, str, dict[str, Any] | None, float, int]] = []

    def requester(
        method: str,
        url: str,
        payload: dict[str, Any] | None,
        timeout: float,
        maximum: int,
    ) -> dict[str, Any]:
        calls.append((method, url, payload, timeout, maximum))
        if method == "GET":
            return {"models": [{"name": "tiny-model"}]}
        assert payload is not None
        assert len(json.dumps(payload).encode("utf-8")) < 4096
        return {
            "message": {"content": "OK"},
            "eval_count": 1,
            "eval_duration": 100_000_000,
        }

    target = OllamaProbeTarget(
        service_id="ollama-local",
        display_label="Local Ollama",
        base_url="http://127.0.0.1:11434",
        model="tiny-model",
        service_version="0.9.0",
        max_response_bytes=2048,
    )
    adapter = OllamaBenchmarkAdapter((target,), requester=requester)
    registry = BenchmarkRegistry()
    registration = register_concrete_adapters(registry, (adapter,))
    snapshot = DeviceInspector(registry, probes=registration.inspection_probes).inspect(
        device_id="device-test",
        revision=1,
    )
    assert snapshot.detected_services == ("ollama-local",)
    assert adapter.definition.benchmark_id in snapshot.recommended_benchmark_ids

    store = SQLiteBenchmarkResultStore(tmp_path / "ollama.sqlite3")
    result = BenchmarkRunner(registry, store).run(
        _request(
            adapter,
            run_id="run-ollama",
            target_id=target.service_id,
            dependency_inputs=adapter.dependency_inputs(target.service_id),
            prerequisite="ollama-model-available",
        )
    )
    store.close()

    assert result.state == BenchmarkState.PASSED
    assert result.metrics["generated_tokens"] == 1
    serialized = json.dumps(result.to_dict())
    assert "127.0.0.1" not in serialized
    assert "11434" not in serialized
    assert calls[-1][4] == 2048


def test_ollama_failure_and_skip_are_safe(tmp_path: Any) -> None:
    def failure(*_: Any) -> dict[str, Any]:
        raise RuntimeError("token=secret http://127.0.0.1:11434/private")

    target = OllamaProbeTarget(
        service_id="ollama-local",
        display_label="Local Ollama",
        base_url="http://localhost:11434",
        model="tiny-model",
    )
    adapter = OllamaBenchmarkAdapter((target,), requester=failure)
    runner, store = _runner(tmp_path, adapter)
    skipped = runner.run(
        BenchmarkRunRequest(
            run_id="run-skip",
            device_id="device-test",
            benchmark_id=adapter.definition.benchmark_id,
            target_service_id=target.service_id,
            dependency_inputs=adapter.dependency_inputs(target.service_id),
        )
    )
    failed = runner.run(
        _request(
            adapter,
            run_id="run-failure",
            target_id=target.service_id,
            dependency_inputs=adapter.dependency_inputs(target.service_id),
            prerequisite="ollama-model-available",
        )
    )
    store.close()
    assert skipped.state == BenchmarkState.SKIPPED
    assert failed.state == BenchmarkState.FAILED
    serialized = json.dumps(failed.to_dict())
    assert "secret" not in serialized
    assert "127.0.0.1" not in serialized


def _mtconnect_scan() -> dict[str, Any]:
    return {
        "schema": "msh.mtconnect.network_scan.v1",
        "state": "complete",
        "finished_at": "2026-08-02T20:00:00Z",
        "results": [
            {
                "source_id": "mtconnect-source-a1",
                "base_url": "http://192.168.1.50:5000",
                "probe_url": "http://192.168.1.50:5000/probe",
                "probe_sha256": "abc123",
                "machine_count": 1,
                "machines": [
                    {
                        "identity_kind": "uuid",
                        "identity_value": "private-machine-serial",
                    }
                ],
            }
        ],
    }


def test_mtconnect_adapter_uses_snapshot_only_and_hides_private_source_data(
    tmp_path: Any,
) -> None:
    scans = 0

    def supplier() -> dict[str, Any]:
        nonlocal scans
        scans += 1
        return _mtconnect_scan()

    adapter = MtconnectSourceAdapter(supplier)
    registry = BenchmarkRegistry()
    registration = register_concrete_adapters(registry, (adapter,))
    snapshot = DeviceInspector(registry, probes=registration.inspection_probes).inspect(
        device_id="device-test",
        revision=1,
    )
    assert snapshot.detected_data_sources == ("mtconnect-source-a1",)
    assert "192.168.1.50" not in json.dumps(snapshot.to_dict())

    store = SQLiteBenchmarkResultStore(tmp_path / "mtconnect.sqlite3")
    result = BenchmarkRunner(registry, store).run(
        _request(
            adapter,
            run_id="run-mtconnect",
            target_id="mtconnect-source-a1",
            dependency_inputs=adapter.dependency_inputs("mtconnect-source-a1"),
            prerequisite="mtconnect-scan-current",
        )
    )
    store.close()
    assert result.state == BenchmarkState.PASSED
    assert result.metrics["stable_identity_count"] == 1
    assert "private-machine-serial" not in json.dumps(result.to_dict())
    assert scans >= 2


@dataclass
class _Descriptor:
    handler_id: str = "safe-handler"
    descriptor_fingerprint: str = "sha256:" + "a" * 64


@dataclass
class _Binding:
    descriptor: _Descriptor
    revision: int = 3


class _Inventory:
    def __init__(self) -> None:
        self.executed = False
        self.binding = _Binding(_Descriptor())

    def list_bindings(self) -> tuple[_Binding, ...]:
        return (self.binding,)

    def execute(self) -> None:
        self.executed = True
        raise AssertionError("compute execution is outside CF2-B")


def test_compute_adapter_inspects_registered_inventory_without_execution(tmp_path: Any) -> None:
    inventory = _Inventory()
    adapter = RegisteredComputeHandlerAdapter(inventory)
    runner, store = _runner(tmp_path, adapter)
    result = runner.run(
        _request(
            adapter,
            run_id="run-compute",
            target_id="safe-handler",
            dependency_inputs=adapter.dependency_inputs("safe-handler"),
            prerequisite="registered-compute-handler",
        )
    )
    store.close()
    assert result.state == BenchmarkState.PASSED
    assert result.metrics["registered_only"] is True
    assert inventory.executed is False


def test_storage_candidate_roundtrip_always_cleans_up_and_grants_no_authority(
    tmp_path: Any,
) -> None:
    values: dict[str, bytes] = {}
    authority_calls: list[str] = []

    def write(probe_id: str, payload: bytes, context: Any) -> None:
        context.raise_if_cancelled()
        values[probe_id] = payload

    def read(probe_id: str, context: Any) -> bytes:
        context.raise_if_cancelled()
        return values[probe_id]

    def cleanup(probe_id: str, context: Any) -> bool:
        values.pop(probe_id, None)
        return True

    target = StorageCandidateTarget(
        candidate_id="storage-candidate-a",
        display_label="Storage candidate A",
        candidate_fingerprint="sha256:" + "b" * 64,
        write_probe=write,
        read_probe=read,
        cleanup_probe=cleanup,
        payload_bytes=128,
    )
    adapter = StorageCandidateAdapter((target,))
    runner, store = _runner(tmp_path, adapter)
    result = runner.run(
        _request(
            adapter,
            run_id="run-storage",
            target_id=target.candidate_id,
            dependency_inputs=adapter.dependency_inputs(target.candidate_id),
            prerequisite="storage-probe-cleanup",
        )
    )
    store.close()
    assert result.state == BenchmarkState.PASSED
    assert result.metrics["cleanup_completed"] is True
    assert values == {}
    assert authority_calls == []
    assert "candidate-only" in " ".join(result.diagnostics)


def test_storage_cleanup_failure_fails_closed(tmp_path: Any) -> None:
    values: dict[str, bytes] = {}
    target = StorageCandidateTarget(
        candidate_id="storage-candidate-b",
        display_label="Storage candidate B",
        candidate_fingerprint="sha256:" + "c" * 64,
        write_probe=lambda key, payload, context: values.__setitem__(key, payload),
        read_probe=lambda key, context: values[key],
        cleanup_probe=lambda key, context: False,
    )
    adapter = StorageCandidateAdapter((target,))
    runner, store = _runner(tmp_path, adapter)
    result = runner.run(
        _request(
            adapter,
            run_id="run-storage-failed",
            target_id=target.candidate_id,
            dependency_inputs=adapter.dependency_inputs(target.candidate_id),
            prerequisite="storage-probe-cleanup",
        )
    )
    store.close()
    assert result.state == BenchmarkState.FAILED
    assert result.metrics["cleanup_completed"] is False


def test_authenticated_network_adapter_rejects_unauthenticated_samples(tmp_path: Any) -> None:
    private_route = "relay://token@10.0.0.2:9000"

    def sample(payload: bytes, context: Any) -> AuthenticatedPathSample:
        context.raise_if_cancelled()
        assert len(payload) <= 512
        assert private_route
        return AuthenticatedPathSample(False, 12.5, len(payload))

    target = AuthenticatedPathTarget(
        path_id="relay-path-a",
        route_kind="relay",
        path_fingerprint="sha256:" + "d" * 64,
        probe=sample,
        sample_count=2,
    )
    adapter = AuthenticatedNetworkPathAdapter((target,))
    runner, store = _runner(tmp_path, adapter)
    result = runner.run(
        _request(
            adapter,
            run_id="run-network",
            target_id=target.path_id,
            dependency_inputs=adapter.dependency_inputs(target.path_id),
            prerequisite="authenticated-network-path",
        )
    )
    store.close()
    assert result.state == BenchmarkState.FAILED
    assert result.metrics["authenticated"] is False
    assert "10.0.0.2" not in json.dumps(result.to_dict())
    assert "token" not in json.dumps(result.to_dict())


def test_existing_store_replays_and_validity_detects_expiry_and_invalidation(
    tmp_path: Any,
) -> None:
    target = AuthenticatedPathTarget(
        path_id="secure-local-a",
        route_kind="local-secure",
        path_fingerprint="sha256:" + "e" * 64,
        probe=lambda payload, context: AuthenticatedPathSample(
            True,
            1.5,
            len(payload),
        ),
    )
    adapter = AuthenticatedNetworkPathAdapter((target,))
    registry = BenchmarkRegistry()
    register_concrete_adapters(registry, (adapter,))
    path = tmp_path / "restart.sqlite3"
    request = _request(
        adapter,
        run_id="run-restart",
        target_id=target.path_id,
        dependency_inputs=adapter.dependency_inputs(target.path_id),
        prerequisite="authenticated-network-path",
    )
    first_store = SQLiteBenchmarkResultStore(path)
    first = BenchmarkRunner(registry, first_store).run(request)
    first_store.close()
    second_store = SQLiteBenchmarkResultStore(path)
    replay = BenchmarkRunner(registry, second_store).run(request)
    second_store.close()
    assert replay == first
    assert evaluate_result(
        first,
        adapter.definition,
        adapter.dependency_inputs(target.path_id),
        now=first.expires_at - timedelta(seconds=1),
    ).current
    assert not evaluate_result(
        first,
        adapter.definition,
        {"path_fingerprint": "sha256:" + "f" * 64},
        now=first.finished_at,
    ).current
    assert not evaluate_result(
        first,
        adapter.definition,
        adapter.dependency_inputs(target.path_id),
        now=first.expires_at,
    ).current


def test_storage_probe_cooperatively_cancels_and_times_out(tmp_path: Any) -> None:
    cleaned: list[str] = []

    def blocking_write(probe_id: str, payload: bytes, context: Any) -> None:
        while True:
            context.raise_if_cancelled()
            time.sleep(0.002)

    target = StorageCandidateTarget(
        candidate_id="storage-candidate-c",
        display_label="Storage candidate C",
        candidate_fingerprint="sha256:" + "1" * 64,
        write_probe=blocking_write,
        read_probe=lambda key, context: b"",
        cleanup_probe=lambda key, context: cleaned.append(key) is None or True,
    )
    adapter = StorageCandidateAdapter((target,))
    runner, store = _runner(tmp_path, adapter)
    timed_out = runner.run(
        _request(
            adapter,
            run_id="run-timeout",
            target_id=target.candidate_id,
            dependency_inputs=adapter.dependency_inputs(target.candidate_id),
            prerequisite="storage-probe-cleanup",
            timeout_seconds=0.03,
        )
    )
    token = CancellationToken()
    timer = threading.Timer(0.02, token.cancel)
    timer.start()
    cancelled = runner.run(
        _request(
            adapter,
            run_id="run-cancelled",
            target_id=target.candidate_id,
            dependency_inputs=adapter.dependency_inputs(target.candidate_id),
            prerequisite="storage-probe-cleanup",
            timeout_seconds=1.0,
            cancellation_token=token,
        )
    )
    timer.join()
    store.close()
    assert timed_out.state == BenchmarkState.FAILED
    assert cancelled.state == BenchmarkState.CANCELLED
    assert cleaned
