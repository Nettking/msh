"""Host-resource admission for federated analysis materialization and publication."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any

from catalog.capabilities.dispatch import ExecutionResult
from catalog.capabilities.jobs import JobContract
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.host_resources import (
    HostResourceRefused,
    ProcessResourceAdmission,
)
from catalog.federation.object_transfer import MAX_TRANSFER_CHUNKS
from catalog.federation.process_resource_admission import PROCESS_RESOURCE_ADMISSION

from .contracts import (
    ANALYSIS_DATA_SLICE_SCHEMA,
    ANALYSIS_PLAN_SCHEMA,
    MAX_ANALYSIS_RESULT_BYTES,
    MAX_PLAN_BYTES,
    AnalysisWorkSlice,
)
from .packaging import MAX_SLICE_ENTRIES
from .scheduler import FederatedAnalysisScheduler as _FederatedAnalysisScheduler
from .scheduler import SubmissionOutcome
from .worker import FederatedAnalysisHandler as _FederatedAnalysisHandler
from .workspace_reconciliation import reconcile_stale_workspaces

# Fixed workspace entries cover the ownership marker, plan/slice publication
# files, staging/publication directories and a small margin for atomic temp files.
# Journal completion capacity remains protected by the shared CRITICAL reserve.
_ANALYSIS_WORKSPACE_FIXED_INODES = 16

# Data-owner publication creates one plan artifact and one deterministic slice
# archive. Each uses an atomic partial/final identity, with a small fixed margin
# for publication directories and filesystem bookkeeping. Archive members are
# inputs to one tar.gz and therefore do not consume one destination inode each.
_ANALYSIS_PUBLICATION_FIXED_INODES = 8

# Result JSON is deliberately a bounded summary rather than an arbitrary copy of
# executor state.  The worker enforces the same limit after serialization, so the
# reservation below is a real upper bound rather than a hopeful estimate.
_ANALYSIS_RESULT_INODES = 2  # result partial + result identity/directory margin

# A selected catalog script is allowed to write only inside its owned run tree.
# The subprocess boundary enforces these limits after each materialization step
# and while the child is running.  The values are intentionally independent of
# the host's free space: admission protects the host, while the workspace limit
# protects the process from an unexpectedly prolific script.
MAX_CATALOG_COPY_BYTES = 64 * 1024 * 1024
MAX_CATALOG_COPY_INODES = 4096
MAX_SCRIPT_OUTPUT_BYTES = 256 * 1024 * 1024
MAX_SCRIPT_OUTPUT_INODES = 4096
MAX_ANALYSIS_METADATA_BYTES = 2 * 1024 * 1024
MAX_DATA_INDEX_BYTES = 16 * 1024 * 1024


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

    Atomic replacement may temporarily retain an existing destination while its
    new same-directory partial is written. The plan therefore peaks at two plan
    bodies, while slice publication can peak at the durable plan plus an existing
    slice body plus the replacement partial. Reserve the larger legal phase.
    """

    bounded_plan = max(int(plan_bytes), 0)
    bounded_slice = max(int(max_slice_bytes), 0)
    bytes_required = max(
        2 * bounded_plan,
        bounded_plan + (2 * bounded_slice),
    )
    return bytes_required, _ANALYSIS_PUBLICATION_FIXED_INODES


def analysis_result_resource_requirement() -> tuple[int, int]:
    """Return the bounded result-file reservation for one worker attempt."""

    return MAX_ANALYSIS_RESULT_BYTES, _ANALYSIS_RESULT_INODES


def analysis_script_workspace_resource_requirement(
    max_slice_bytes: int,
    *,
    catalog_bytes: int = MAX_CATALOG_COPY_BYTES,
    catalog_inodes: int = MAX_CATALOG_COPY_INODES,
) -> tuple[int, int]:
    """Return the peak persistent workspace envelope for one date slice.

    A run may retain the filtered JSONL, the derived metrics CSV, a playback
    export, the copied catalog, and script-owned outputs at the same time.  The
    input archive itself is already admitted by the worker; this envelope covers
    the durable workflow tree and the fallback data copy when symlinks are not
    available.  All terms are hard-bounded by the executor before it starts.
    """

    bounded_slice = max(int(max_slice_bytes), 0)
    bounded_catalog_bytes = min(max(int(catalog_bytes), 0), MAX_CATALOG_COPY_BYTES)
    bounded_catalog_inodes = min(max(int(catalog_inodes), 0), MAX_CATALOG_COPY_INODES)
    bytes_required = (
        (3 * bounded_slice)
        + bounded_catalog_bytes
        + MAX_SCRIPT_OUTPUT_BYTES
        + MAX_ANALYSIS_METADATA_BYTES
    )
    inodes_required = (
        (2 * MAX_SLICE_ENTRIES)
        + bounded_catalog_inodes
        + MAX_SCRIPT_OUTPUT_INODES
        + 24
    )
    return bytes_required, inodes_required


@contextmanager
def reserve_analysis_requirements(
    controller: ProcessResourceAdmission,
    requirements: Sequence[tuple[Path | str, int, int]],
) -> Iterator[None]:
    """Reserve a logical analysis transaction atomically when supported.

    The production controller is the serialized process-wide implementation and
    exposes ``reserve_many``.  The sequential fallback keeps older injected test
    doubles import-compatible; production never takes it.
    """

    reserve_many = getattr(controller, "reserve_many", None)
    if callable(reserve_many):
        with reserve_many(requirements):
            yield
        return
    with ExitStack() as stack:
        for path, bytes_required, inodes_required in requirements:
            stack.enter_context(
                controller.reserve(
                    path,
                    bytes_required=bytes_required,
                    inodes_required=inodes_required,
                )
            )
        yield


class FederatedAnalysisHandler(_FederatedAnalysisHandler):
    """Analysis handler with shared admission around its owned workspace writes."""

    def __init__(
        self,
        *args: Any,
        resource_admission: ProcessResourceAdmission | None = None,
        **kwargs: Any,
    ) -> None:
        self.resource_admission = resource_admission or PROCESS_RESOURCE_ADMISSION
        workspace_root = kwargs.get("workspace_root")
        if workspace_root is None:
            raise TypeError("workspace_root is required for admitted analysis workers")
        # Defer marker-safe reconciliation until the first validated execution.
        # Construction must remain side-effect free so malformed jobs are still
        # rejected before host measurement, while the actual root/workspace
        # creation stays inside the execution reservation below.
        kwargs["initialize_workspace"] = False
        super().__init__(*args, **kwargs)

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
        requirements: list[tuple[Path | str, int, int]] = [
            (self.workspace_root, bytes_required, inodes_required)
        ]
        if self.content_store is not None:
            result_bytes, result_inodes = analysis_result_resource_requirement()
            requirements.append(
                (self.content_store.root, result_bytes, result_inodes)
            )
        with reserve_analysis_requirements(self.resource_admission, requirements):
            self.workspace_reconciliation = reconcile_stale_workspaces(
                self.workspace_root,
                now=self.clock(),
            )
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
        self.resource_admission = resource_admission or PROCESS_RESOURCE_ADMISSION

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
        # Submission is one logical publication transaction: the plan and slice
        # bodies are durable content, and the job row/audit/WAL mutation makes
        # them discoverable. Admit both backing resources before any of those
        # writers run, then tell the nested helpers the reservation is already
        # held so completion bookkeeping does not spend it twice.
        job_store_bytes = 8 * 1024 * 1024
        job_store_inodes = 4
        store = getattr(self, "store", None)
        store_database = getattr(store, "database", None)
        try:
            requirements = [
                (
                    self.gateway.content_store.root,
                    bytes_required,
                    inodes_required,
                ),
            ]
            if store_database is not None:
                requirements.append(
                    (store_database, job_store_bytes, job_store_inodes)
                )
            with reserve_analysis_requirements(
                self.resource_admission,
                requirements,
            ):
                return super().submit(
                    work,
                    slice_files=slice_files,
                    slice_root=slice_root,
                    admission_held=True,
                )
        except HostResourceRefused as exc:
            raise FederationOperationError(
                "analysis-resource-pressure",
                "host resource pressure refused analysis input publication",
            ) from exc


__all__ = [
    "MAX_ANALYSIS_METADATA_BYTES",
    "MAX_ANALYSIS_RESULT_BYTES",
    "MAX_CATALOG_COPY_BYTES",
    "MAX_CATALOG_COPY_INODES",
    "MAX_DATA_INDEX_BYTES",
    "MAX_SCRIPT_OUTPUT_BYTES",
    "MAX_SCRIPT_OUTPUT_INODES",
    "FederatedAnalysisHandler",
    "FederatedAnalysisScheduler",
    "analysis_publication_resource_requirement",
    "analysis_result_resource_requirement",
    "analysis_script_workspace_resource_requirement",
    "analysis_workspace_resource_requirement",
    "reserve_analysis_requirements",
]
