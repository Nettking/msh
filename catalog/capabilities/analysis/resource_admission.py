"""Host-resource admission for federated analysis workspace materialization."""

from __future__ import annotations

from typing import Any

from catalog.capabilities.dispatch import ExecutionResult
from catalog.capabilities.jobs import JobContract
from catalog.federation.host_resources import HostResourceRefused, ProcessResourceAdmission
from catalog.federation.object_transfer import MAX_TRANSFER_CHUNKS

from .contracts import MAX_PLAN_BYTES
from .packaging import MAX_SLICE_ENTRIES
from .worker import FederatedAnalysisHandler as _FederatedAnalysisHandler

# One process-wide controller is shared by every supported analysis handler so
# concurrent attempts cannot independently spend the same filesystem headroom.
_ANALYSIS_WORKSPACE_ADMISSION = ProcessResourceAdmission()

# A verified slice is materialized in two bounded phases. During F6 publication,
# staged chunks and the complete publication temporary can coexist (2 * slice).
# After publication, the archive and its extracted data can coexist (also
# 2 * slice). The bounded plan remains present throughout. Metadata and filesystem
# journal completion capacity stay protected by the shared CRITICAL reserve.
_ANALYSIS_WORKSPACE_FIXED_BYTES = MAX_PLAN_BYTES
_ANALYSIS_WORKSPACE_FIXED_INODES = 16


def analysis_workspace_resource_requirement(max_slice_bytes: int) -> tuple[int, int]:
    """Return conservative bytes/inodes needed by one materialization attempt."""

    bounded_slice = max(int(max_slice_bytes), 0)
    bytes_required = (2 * bounded_slice) + _ANALYSIS_WORKSPACE_FIXED_BYTES

    # ObjectTransferManifest permits chunk_size down to one byte, with a protocol
    # ceiling of MAX_TRANSFER_CHUNKS. Reserve the worst legal chunk-file count for
    # this slice bound, plus unpacked archive entries and fixed workspace files.
    transfer_inodes = min(bounded_slice, MAX_TRANSFER_CHUNKS)
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

    async def _execute(self, job: JobContract) -> ExecutionResult:
        # Preserve existing validation precedence: malformed or foreign work is
        # rejected before host capacity is measured or reserved.
        self._validate_contract(job)
        bytes_required, inodes_required = analysis_workspace_resource_requirement(
            self.max_slice_bytes
        )
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
