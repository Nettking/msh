"""Host-resource admission for federated analysis workspace materialization."""

from __future__ import annotations

from typing import Any

from catalog.capabilities.dispatch import ExecutionResult
from catalog.capabilities.jobs import JobContract
from catalog.federation.errors import FederationValidationError
from catalog.federation.host_resources import HostResourceRefused, ProcessResourceAdmission
from catalog.federation.object_transfer import MAX_TRANSFER_CHUNKS

from .contracts import ANALYSIS_DATA_SLICE_SCHEMA, ANALYSIS_PLAN_SCHEMA, MAX_PLAN_BYTES
from .packaging import MAX_SLICE_ENTRIES
from .worker import FederatedAnalysisHandler as _FederatedAnalysisHandler

# One process-wide controller is shared by every supported analysis handler so
# concurrent attempts cannot independently spend the same filesystem headroom.
_ANALYSIS_WORKSPACE_ADMISSION = ProcessResourceAdmission()

# Fixed workspace entries cover the ownership marker, plan/slice publication
# files, staging/publication directories and a small margin for atomic temp files.
# Journal completion capacity remains protected by the shared CRITICAL reserve.
_ANALYSIS_WORKSPACE_FIXED_INODES = 16


def analysis_workspace_resource_requirement(
    slice_bytes: int,
    *,
    plan_bytes: int = MAX_PLAN_BYTES,
) -> tuple[int, int]:
    """Return conservative bytes/inodes needed by one materialization attempt."""

    bounded_slice = max(int(slice_bytes), 0)
    bounded_plan = max(int(plan_bytes), 0)

    # Plan retrieval can peak at staged plan chunks + its publication temporary.
    # Once the plan is published, slice retrieval/publication can peak at
    # plan + staged slice chunks + a complete slice publication temporary.
    # Extraction has the same byte ceiling: plan + archive + unpacked slice.
    bytes_required = max(
        2 * bounded_plan,
        bounded_plan + (2 * bounded_slice),
    )

    # ObjectTransferManifest permits chunk_size down to one byte, bounded by the
    # protocol-wide MAX_TRANSFER_CHUNKS ceiling. Reserve the larger legal transfer
    # chunk-file population plus every legal extracted archive entry and fixed
    # workspace identities.
    transfer_inodes = max(
        min(bounded_plan, MAX_TRANSFER_CHUNKS),
        min(bounded_slice, MAX_TRANSFER_CHUNKS),
    )
    inodes_required = transfer_inodes + MAX_SLICE_ENTRIES + _ANALYSIS_WORKSPACE_FIXED_INODES
    return bytes_required, inodes_required


class FederatedAnalysisHandler(_FederatedAnalysisHandler):
    """Analysis handler with shared admission around its owned workspace writes."""

    def __init__(
        self,
        *args: Any,
        resource_admission: ProcessResourceAdmission | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.resource_admission = resource_admission or _ANALYSIS_WORKSPACE_ADMISSION

    async def execute(self, job: JobContract) -> ExecutionResult:
        try:
            return await super().execute(job)
        except HostResourceRefused:
            return ExecutionResult(
                False,
                {"job_id": job.job_id},
                "analysis-resource-pressure",
            )

    def _resource_requirement(self, job: JobContract) -> tuple[int, int]:
        """Run existing pure validation before measuring or reserving host storage."""

        self._validate_contract(job)
        self._active_attempt(job)

        plan = self._input(job, ANALYSIS_PLAN_SCHEMA)
        if plan.size_bytes > MAX_PLAN_BYTES:
            raise FederationValidationError(
                "analysis-plan-too-large",
                "plan",
                "declared plan exceeds the bound",
            )

        data_slice = self._input(job, ANALYSIS_DATA_SLICE_SCHEMA)
        if data_slice.size_bytes > self.max_slice_bytes:
            raise FederationValidationError(
                "analysis-slice-too-large",
                "inputs",
                "declared slice exceeds the bound",
            )

        return analysis_workspace_resource_requirement(
            data_slice.size_bytes,
            plan_bytes=plan.size_bytes,
        )

    async def _execute(self, job: JobContract) -> ExecutionResult:
        # Preserve the worker's existing pure validation precedence before host
        # capacity is measured. The base implementation intentionally repeats the
        # same checks after admission before any workspace write occurs.
        bytes_required, inodes_required = self._resource_requirement(job)
        with self.resource_admission.reserve(
            self.workspace_root,
            bytes_required=bytes_required,
            inodes_required=inodes_required,
        ):
            return await super()._execute(job)


__all__ = [
    "FederatedAnalysisHandler",
    "analysis_workspace_resource_requirement",
]
