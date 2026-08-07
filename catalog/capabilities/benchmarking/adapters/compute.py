"""Inspection evidence for explicitly registered trusted local handlers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkState,
)

from ..inspection import InspectionContext, InspectionFinding
from ..policy import DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS
from ..runner import BenchmarkExecutionContext, BenchmarkObservation
from .common import dependency_matches, safe_identifier

COMPUTE_BENCHMARK_ID = "benchmark.compute.registered-handler.v1"
COMPUTE_PREREQUISITE = "registered-compute-handler"


class RegisteredComputeHandlerAdapter:
    """Inspect descriptors only; never execute or dispatch a compute handler."""

    definition = BenchmarkDefinition(
        benchmark_id=COMPUTE_BENCHMARK_ID,
        capability_type="compute",
        capability_protocol="msh-compute-handler",
        implementation_version="1.0.0",
        max_duration_seconds=2,
        max_parallelism=4,
        prerequisites=(COMPUTE_PREREQUISITE,),
        metric_names=(
            "handler_available",
            "descriptor_current",
            "binding_revision",
            "registered_only",
        ),
        invalidation_inputs=("descriptor_fingerprint", "binding_revision"),
        privacy_classification="public-summary",
    )
    result_ttl_seconds = DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS

    def __init__(self, inventory: object) -> None:
        if not callable(getattr(inventory, "list_bindings", None)):
            raise TypeError("inventory must expose list_bindings")
        self._inventory = inventory

    def _bindings(self) -> dict[str, object]:
        values = self._inventory.list_bindings()
        if not isinstance(values, Sequence):
            values = tuple(values)
        result: dict[str, object] = {}
        for binding in list(values)[:128]:
            descriptor = getattr(binding, "descriptor", None)
            try:
                handler_id = safe_identifier(
                    getattr(descriptor, "handler_id", None),
                    "handler_id",
                )
            except ValueError:
                continue
            result[handler_id] = binding
        return result

    def dependency_inputs(self, handler_id: str) -> dict[str, Any]:
        binding = self._bindings()[handler_id]
        descriptor = binding.descriptor
        return {
            "descriptor_fingerprint": safe_identifier(
                getattr(descriptor, "descriptor_fingerprint", None),
                "descriptor_fingerprint",
            ),
            "binding_revision": int(binding.revision),
        }

    def __call__(self, context: InspectionContext) -> InspectionFinding:
        bindings = self._bindings()
        return InspectionFinding(
            resource_observations={
                "compute": {"registered_handler_count": len(bindings)}
            },
            detected_services=("trusted-local-compute-inventory",) if bindings else (),
            registered_handlers=tuple(sorted(bindings)),
            available_prerequisites=(COMPUTE_PREREQUISITE,) if bindings else (),
            recommended_benchmark_ids=(self.definition.benchmark_id,) if bindings else (),
        )

    def benchmark(self, context: BenchmarkExecutionContext) -> BenchmarkObservation:
        context.raise_if_cancelled()
        binding = self._bindings().get(context.target_service_id)
        if binding is None:
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.UNSUPPORTED,
                diagnostics=("Compute handler is not in the registered local inventory",),
            )
        expected = self.dependency_inputs(context.target_service_id)
        current = dependency_matches(context.dependency_inputs, expected)
        metrics = {
            "handler_available": True,
            "descriptor_current": current,
            "binding_revision": expected["binding_revision"],
            "registered_only": True,
        }
        if not current:
            return BenchmarkObservation(
                metrics=metrics,
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.RERUN_REQUIRED,
                diagnostics=("Compute handler descriptor changed before evaluation",),
            )
        return BenchmarkObservation(
            metrics=metrics,
            recommendation=BenchmarkRecommendation.USABLE,
            diagnostics=(
                "Descriptor evidence only; no compute handler was invoked or dispatched",
            ),
        )
