"""Trusted local Ollama inspection and bounded inference evidence."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from ipaddress import ip_address
from typing import Any
from urllib import error, request
from urllib.parse import urlsplit

from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkState,
)

from ..inspection import InspectionContext, InspectionFinding
from ..runner import BenchmarkExecutionContext, BenchmarkObservation
from .common import (
    MAX_PROBE_PAYLOAD_BYTES,
    MAX_PROBE_RESPONSE_BYTES,
    bounded_positive_int,
    bounded_seconds,
    dependency_matches,
    elapsed_ms,
    safe_identifier,
    stable_fingerprint,
)

OLLAMA_BENCHMARK_ID = "benchmark.ai.ollama-inference.v1"
OLLAMA_PREREQUISITE = "ollama-model-available"
MAX_OLLAMA_TARGETS = 8

JsonRequester = Callable[
    [str, str, Mapping[str, Any] | None, float, int], Mapping[str, Any]
]
HostPredicate = Callable[[str], bool]


class OllamaProbeError(RuntimeError):
    """Safe failure from the adapter-private bounded Ollama client."""


def _loopback_host(host: str) -> bool:
    if host.casefold() == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def _bounded_json_request(
    method: str,
    url: str,
    payload: Mapping[str, Any] | None,
    timeout_seconds: float,
    max_response_bytes: int,
) -> Mapping[str, Any]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(body) > MAX_PROBE_PAYLOAD_BYTES:
            raise OllamaProbeError("Ollama probe payload exceeded its size bound")
        headers["Content-Type"] = "application/json"
    probe_request = request.Request(url, data=body, headers=headers, method=method)
    try:
        with request.urlopen(probe_request, timeout=timeout_seconds) as response:
            raw = response.read(max_response_bytes + 1)
    except error.HTTPError as exc:
        status = int(getattr(exc, "code", 0) or 0)
        message = (
            f"Ollama rejected the bounded request (HTTP {status})"
            if 400 <= status <= 599
            else "Ollama rejected the bounded request"
        )
        raise OllamaProbeError(message) from exc
    except TimeoutError as exc:
        raise OllamaProbeError("Ollama did not respond within the benchmark limit") from exc
    except error.URLError as exc:
        if isinstance(getattr(exc, "reason", None), TimeoutError):
            raise OllamaProbeError(
                "Ollama did not respond within the benchmark limit"
            ) from exc
        raise OllamaProbeError("Ollama service could not be reached") from exc
    except OSError as exc:
        raise OllamaProbeError("Ollama service could not be reached") from exc
    if len(raw) > max_response_bytes:
        raise OllamaProbeError("Ollama probe response exceeded its size bound")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OllamaProbeError("Ollama returned an invalid bounded response") from exc
    if not isinstance(value, Mapping):
        raise OllamaProbeError("Ollama returned an invalid bounded response")
    return value


@dataclass(frozen=True)
class OllamaProbeTarget:
    """Explicit trusted local Ollama target; the private URL is never evidence."""

    service_id: str
    display_label: str
    base_url: str = field(repr=False)
    model: str
    service_version: str = "unknown"
    inspection_timeout_seconds: float = 1.0
    request_timeout_seconds: float = 120.0
    max_response_bytes: int = MAX_PROBE_RESPONSE_BYTES

    def __post_init__(self) -> None:
        object.__setattr__(self, "service_id", safe_identifier(self.service_id, "service_id"))
        object.__setattr__(self, "display_label", safe_identifier(self.display_label, "display_label"))
        object.__setattr__(self, "model", safe_identifier(self.model, "model"))
        object.__setattr__(
            self,
            "service_version",
            safe_identifier(self.service_version, "service_version"),
        )
        parsed = urlsplit(self.base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("base_url must be a credential-free HTTP service root")
        object.__setattr__(
            self,
            "inspection_timeout_seconds",
            bounded_seconds(
                self.inspection_timeout_seconds,
                "inspection_timeout_seconds",
                maximum=2.0,
            ),
        )
        object.__setattr__(
            self,
            "request_timeout_seconds",
            bounded_seconds(
                self.request_timeout_seconds,
                "request_timeout_seconds",
                maximum=120.0,
            ),
        )
        object.__setattr__(
            self,
            "max_response_bytes",
            bounded_positive_int(
                self.max_response_bytes,
                "max_response_bytes",
                maximum=MAX_PROBE_RESPONSE_BYTES,
            ),
        )

    @property
    def service_fingerprint(self) -> str:
        return stable_fingerprint(
            {
                "kind": "ollama",
                "service_id": self.service_id,
                "service_version": self.service_version,
                "private_connection": self.base_url.rstrip("/"),
            }
        )

    @property
    def model_fingerprint(self) -> str:
        return stable_fingerprint({"model": self.model})


class OllamaBenchmarkAdapter:
    """Inspect explicit local Ollama targets and run one bounded generation."""

    definition = BenchmarkDefinition(
        benchmark_id=OLLAMA_BENCHMARK_ID,
        capability_type="language-model",
        capability_protocol="msh-language-model",
        implementation_version="1.1.0",
        max_duration_seconds=120,
        max_parallelism=1,
        prerequisites=(OLLAMA_PREREQUISITE,),
        metric_names=(
            "model_available",
            "completion_present",
            "response_latency_ms",
            "response_bytes",
            "generated_tokens",
            "tokens_per_second",
        ),
        invalidation_inputs=("service_fingerprint", "model_fingerprint"),
        privacy_classification="public-summary",
    )
    # Evidence is explicitly refreshed by the operator. Keep a finite ten-year
    # safety horizon so normal restarts and idle time do not force reruns.
    result_ttl_seconds = 315_360_000

    def __init__(
        self,
        targets: Sequence[OllamaProbeTarget],
        *,
        requester: JsonRequester | None = None,
        trusted_host_predicate: HostPredicate | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not targets or len(targets) > MAX_OLLAMA_TARGETS:
            raise ValueError("Ollama targets must be explicitly bounded")
        predicate = trusted_host_predicate or _loopback_host
        by_id: dict[str, OllamaProbeTarget] = {}
        for target in targets:
            if not isinstance(target, OllamaProbeTarget):
                raise TypeError("targets must contain OllamaProbeTarget values")
            host = urlsplit(target.base_url).hostname or ""
            if not predicate(host):
                raise ValueError("Ollama target is outside the trusted local host policy")
            if target.service_id in by_id:
                raise ValueError("duplicate Ollama service_id")
            by_id[target.service_id] = target
        self._targets = by_id
        self._requester = requester or _bounded_json_request
        self._monotonic = monotonic

    def dependency_inputs(self, service_id: str) -> dict[str, str]:
        target = self._targets[service_id]
        return {
            "service_fingerprint": target.service_fingerprint,
            "model_fingerprint": target.model_fingerprint,
        }

    def __call__(self, context: InspectionContext) -> InspectionFinding:
        detected: list[str] = []
        available = False
        warnings: list[str] = []
        for target in self._targets.values():
            try:
                tags = self._requester(
                    "GET",
                    f"{target.base_url.rstrip('/')}/api/tags",
                    None,
                    target.inspection_timeout_seconds,
                    target.max_response_bytes,
                )
                models = tags.get("models", ())
                names = (
                    {
                        item.get("name") or item.get("model")
                        for item in models
                        if isinstance(item, Mapping)
                    }
                    if isinstance(models, Sequence)
                    else set()
                )
                detected.append(target.service_id)
                if target.model in names:
                    available = True
                else:
                    warnings.append("A configured Ollama model is not currently available")
            except Exception:  # noqa: BLE001 - probe failures become safe evidence
                warnings.append("A trusted local Ollama target could not be inspected")
        prerequisites = (OLLAMA_PREREQUISITE,) if available else ()
        # Keep a configured target visible even while it is unavailable. The
        # resulting blocked check remains evidence only and grants no authority.
        recommended = (self.definition.benchmark_id,)
        return InspectionFinding(
            resource_observations={
                "ollama": {
                    "configured_target_count": len(self._targets),
                    "reachable_target_count": len(detected),
                }
            },
            detected_services=tuple(sorted(detected)),
            available_prerequisites=prerequisites,
            recommended_benchmark_ids=recommended,
            warnings=tuple(warnings),
        )

    def benchmark(self, context: BenchmarkExecutionContext) -> BenchmarkObservation:
        target = self._targets.get(context.target_service_id)
        if target is None:
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.UNSUPPORTED,
                diagnostics=("Ollama target is not in the trusted local inventory",),
            )
        if not dependency_matches(context.dependency_inputs, self.dependency_inputs(target.service_id)):
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.RERUN_REQUIRED,
                diagnostics=("Ollama target changed before benchmark execution",),
            )
        context.raise_if_cancelled()
        payload = {
            "model": target.model,
            "stream": False,
            "messages": [
                {"role": "system", "content": "Reply with exactly OK."},
                {"role": "user", "content": "OK"},
            ],
            "options": {"num_predict": 8, "temperature": 0},
        }
        timeout = min(target.request_timeout_seconds, max(0.001, context.remaining_seconds))
        started = self._monotonic()
        try:
            response = self._requester(
                "POST",
                f"{target.base_url.rstrip('/')}/api/chat",
                payload,
                timeout,
                target.max_response_bytes,
            )
        except OllamaProbeError as exc:
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                diagnostics=(str(exc),),
            )
        except Exception:  # noqa: BLE001 - inference failures become safe evidence
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                diagnostics=("bounded Ollama inference failed",),
            )
        finished = self._monotonic()
        context.raise_if_cancelled()
        message = response.get("message")
        content = message.get("content") if isinstance(message, Mapping) else None
        if not isinstance(content, str) or not content.strip():
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                diagnostics=("Ollama returned a malformed inference response",),
            )
        response_bytes = len(content.encode("utf-8"))
        generated_tokens = response.get("eval_count", 0)
        if isinstance(generated_tokens, bool) or not isinstance(generated_tokens, int):
            generated_tokens = 0
        eval_duration = response.get("eval_duration", 0)
        tokens_per_second = 0.0
        if isinstance(eval_duration, int) and eval_duration > 0 and generated_tokens > 0:
            tokens_per_second = round(generated_tokens / (eval_duration / 1_000_000_000), 3)
        latency = elapsed_ms(started, finished)
        if latency <= 5_000:
            recommendation = BenchmarkRecommendation.RECOMMENDED
        elif latency <= 30_000:
            recommendation = BenchmarkRecommendation.USABLE
        else:
            recommendation = BenchmarkRecommendation.NOT_RECOMMENDED
        return BenchmarkObservation(
            metrics={
                "model_available": True,
                "completion_present": True,
                "response_latency_ms": latency,
                "response_bytes": response_bytes,
                "generated_tokens": max(0, min(generated_tokens, 8)),
                "tokens_per_second": tokens_per_second,
            },
            recommendation=recommendation,
            diagnostics=("AI benchmark evidence grants no compute or storage authority",),
        )
