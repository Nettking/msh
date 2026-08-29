"""Host-resource admission for federated analysis materialization and publication."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from catalog.capabilities.dispatch import ExecutionResult
from catalog.capabilities.jobs import JobContract
from catalog.federation.errors import FederationOperationError, FederationValidationError
from catalog.federation.host_resources import HostResourceRefused, ProcessResourceAdmission
from catalog.federation.object_transfer import MAX_TRANSFER_CHUNKS

from .contracts import (
    ANALYSIS_DATA_SLICE_SCHEMA,
    ANALYSIS_PLAN_SCHEMA,
    MAX_PLAN_BYTES,
    AnalysisWorkSlice,
)
from .packaging import MAX_SLICE_ENTRIES
from .scheduler import FederatedAnalysisScheduler as _FederatedAnalysisScheduler
from .scheduler import SubmissionOutcome
from .worker import FederatedAnalysisHandler as _FederatedAnalysisHandler

# Separate process-wide controllers cover the two host-owned boundaries. Paths
# that resolve to one filesystem still coalesce inside each boundary, while a
# worker reservation never accidentally accounts for data-owner publication in
# another process-local lifecycle.
_ANALYSIS_WORKSPACE_ADMISSION = ProcessResourceAdmission()
_ANALYSIS_PUBLICATION_ADMISSION = ProcessResourceAdmission()

# Fixed workspace entries cover the ownership marker, plan/slice publication
# files, staging/publication directories and a small margin for atomic temp files.
# Journal completion capacity remains protected by the shared CRITICAL reserve.
_ANALYSIS_WORKSPACE_FIXED_INODES = 16

# Data-owner publication creates one plan artifact and one deterministic slice
# archive. Each uses an atomic partial/final identity, with a small fixed margin
# for publication directories and filesystem bookkeeping. Archive members are
# inputs to one tar.gz and therefore do not consume one destination inode each.
_ANALYSIS_PUBLICATION_FIXED_INODES = 8


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


def analysis_publication_resource_requirement(
    plan_bytes: int,
    *,
    max_slice_bytes: int,
) -> tuple[int, int]:
    """Return the bounded data-owner input-publication reservation.

    Admission happens before either artifact writer starts. At the publication
    peak the plan body may already be durable while the deterministic slice
    archive is being built in its same-directory atomic partial, so reserve the
    full plan plus the maximum legal slice body. The archive itself is one file;
    source member count does not multiply destination inode consumption.
    """

    bounded_plan = max(int(plan_bytes), 0)
    bounded_slice = max(int(max_slice_bytes), 0)
    return bounded_plan + bounded_slice, _ANALYSIS_PUBLICATION_FIXED_INODES


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


class FederatedAnalysisScheduler(_FederatedAnalysisScheduler):
    """Scheduler with shared admission around data-owner input publication."""

    def __init__(
        self,
        *args: Any,
        resource_admission: ProcessResourceAdmission | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.resource_admission = resource_admission or _ANALYSIS_PUBLICATION_ADMISSION

    def submit(
        self,
        work: AnalysisWorkSlice,
        *,
        slice_files: Sequence[Path],
        slice_root: Path,
    ) -> SubmissionOutcome:
        # Build/validate the bounded plan before measuring host capacity. This
        # preserves validation precedence and avoids reporting malformed work as
        # resource pressure. The base implementation intentionally rebuilds the
        # same deterministic bytes inside the admitted transaction.
        plan_bytes = work.plan_bytes()
        bytes_required, inodes_required = analysis_publication_resource_requirement(
            len(plan_bytes),
            max_slice_bytes=self.gateway.content_store.max_bytes,
        )
        try:
            with self.resource_admission.reserve(
                self.gateway.content_store.root,
                bytes_required=bytes_required,
                inodes_required=inodes_required,
            ):
                return super().submit(
                    work,
                    slice_files=slice_files,
                    slice_root=slice_root,
                )
        except HostResourceRefused as exc:
            raise FederationOperationError(
                "analysis-resource-pressure",
                "host resource pressure refused analysis input publication",
            ) from exc


__all__ = [
    "FederatedAnalysisHandler",
    "FederatedAnalysisScheduler",
    "analysis_publication_resource_requirement",
    "analysis_workspace_resource_requirement",
]
