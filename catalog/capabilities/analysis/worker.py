"""Worker-side execution of a dispatched analysis job.

The handler is deliberately thin. It re-validates the capability contract,
retrieves only the artifacts the coordinator explicitly authorized, materializes
them inside an isolated workspace, and then hands the work to the existing
analysis implementation through :class:`AnalysisSliceExecutor`. It duplicates no
analysis logic and never reads the data owner's filesystem directly.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)

from ..artifact_contracts import ArtifactInputReference
from ..dispatch import ExecutionResult
from ..jobs import ArtifactReference, JobContract, protocol_major
from .content_store import LocalArtifactContentStore
from .contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    ANALYSIS_DATA_SLICE_SCHEMA,
    ANALYSIS_ENDPOINT_ID,
    ANALYSIS_PLAN_SCHEMA,
    ANALYSIS_PROTOCOL,
    ANALYSIS_PROTOCOL_VERSION,
    ANALYSIS_RESULT_SCHEMA,
    DEFAULT_MAX_SLICE_BYTES,
    MAX_PLAN_BYTES,
    AnalysisPlan,
    analysis_grant_id,
)
from .gateway import AnalysisInputTransport
from .packaging import extract_slice_archive

MAX_REPORTED_SCRIPTS = 32
MAX_REPORTED_TEXT = 96


@dataclass(frozen=True)
class AnalysisExecutionReport:
    """Bounded summary the worker is allowed to publish about one run."""

    succeeded: bool
    analysis_session_ids: tuple[str, ...] = ()
    processed_dates: tuple[str, ...] = ()
    failed_scripts: tuple[str, ...] = ()
    script_results: tuple[dict[str, Any], ...] = ()
    reason_code: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


class AnalysisSliceExecutor(Protocol):
    """Run the existing analysis pipeline over an already-materialized slice."""

    def execute(
        self,
        *,
        plan: AnalysisPlan,
        data_dir: Path,
        workspace: Path,
    ) -> AnalysisExecutionReport: ...


def _short(value: Any) -> str:
    text = str(value)
    return text if len(text) <= MAX_REPORTED_TEXT else text[:MAX_REPORTED_TEXT]


class FederatedAnalysisHandler:
    """A :class:`~catalog.capabilities.dispatch.CapabilityHandler` for analysis."""

    def __init__(
        self,
        *,
        session_id: str,
        node_id: str,
        provider_id: str,
        input_transport: AnalysisInputTransport,
        executor: AnalysisSliceExecutor,
        workspace_root: Path,
        content_store: LocalArtifactContentStore,
        clock,
        endpoint_id: str = ANALYSIS_ENDPOINT_ID,
        max_slice_bytes: int = DEFAULT_MAX_SLICE_BYTES,
    ) -> None:
        self.session_id = session_id
        self.node_id = node_id
        self.provider_id = provider_id
        self.input_transport = input_transport
        self.executor = executor
        self.workspace_root = Path(workspace_root)
        self.content_store = content_store
        self.clock = clock
        self.endpoint_id = endpoint_id
        self.max_slice_bytes = int(max_slice_bytes)

    async def execute(self, job: JobContract) -> ExecutionResult:
        return await asyncio.to_thread(self.run, job)

    # ------------------------------------------------------------------

    def run(self, job: JobContract) -> ExecutionResult:
        try:
            return self._run(job)
        except (
            FederationValidationError,
            FederationOperationError,
            ProtocolCompatibilityError,
        ) as exc:
            return ExecutionResult(
                False,
                {"job_id": job.job_id},
                _short(getattr(exc, "code", "analysis-worker-failed")),
            )
        except OSError:
            return ExecutionResult(
                False, {"job_id": job.job_id}, "analysis-workspace-failed"
            )

    def _run(self, job: JobContract) -> ExecutionResult:
        self._validate_contract(job)
        attempt = self._active_attempt(job)
        grant_id = analysis_grant_id(job.job_id, attempt.attempt_id)
        workspace = self.workspace_root / job.job_id / attempt.attempt_id
        if workspace.exists():
            shutil.rmtree(workspace, ignore_errors=True)
        workspace.mkdir(parents=True, exist_ok=True)
        try:
            plan = self._resolve_plan(job, grant_id=grant_id)
            data_dir = self._resolve_slice(job, grant_id=grant_id, workspace=workspace)
            report = self.executor.execute(
                plan=plan, data_dir=data_dir, workspace=workspace
            )
            if not isinstance(report, AnalysisExecutionReport):
                return ExecutionResult(
                    False, {"job_id": job.job_id}, "analysis-executor-contract-invalid"
                )
            return self._result(job, attempt.attempt_id, plan, report)
        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    def _validate_contract(self, job: JobContract) -> None:
        capability = job.capability
        if job.session_id != self.session_id:
            raise FederationValidationError(
                "analysis-session-mismatch", "session_id", "job belongs to another session"
            )
        if capability.capability_type != ANALYSIS_CAPABILITY_TYPE:
            raise FederationValidationError(
                "analysis-capability-mismatch",
                "capability_type",
                "job requires another capability",
            )
        if capability.protocol != ANALYSIS_PROTOCOL:
            raise ProtocolCompatibilityError(
                "analysis-protocol-mismatch", "protocol", "unsupported analysis protocol"
            )
        if protocol_major(capability.protocol_version) != protocol_major(
            ANALYSIS_PROTOCOL_VERSION
        ):
            raise ProtocolCompatibilityError(
                "analysis-protocol-version-mismatch",
                "protocol_version",
                "unsupported analysis protocol major version",
            )

    @staticmethod
    def _active_attempt(job: JobContract):
        active = tuple(item for item in job.attempts if not item.terminal)
        if len(active) != 1:
            raise FederationValidationError(
                "analysis-attempt-ambiguous",
                "attempts",
                "dispatched work must identify exactly one active attempt",
            )
        return active[0]

    def _input(self, job: JobContract, schema_name: str) -> ArtifactReference:
        for reference in job.inputs:
            if reference.schema_name == schema_name:
                if reference.content_hash is None or reference.size_bytes is None:
                    raise FederationValidationError(
                        "analysis-input-identity-missing",
                        "inputs",
                        "analysis inputs must carry verifiable identity",
                    )
                return reference
        raise FederationValidationError(
            "analysis-input-missing", "inputs", f"job has no {schema_name} input"
        )

    def _fetch(self, job: JobContract, reference: ArtifactReference, *, grant_id: str):
        return self.input_transport.fetch(
            grant_id=grant_id,
            reference=ArtifactInputReference(
                artifact_id=reference.reference_id,
                session_id=reference.session_id,
                job_id=job.job_id,
                content_hash=reference.content_hash,
                schema_id=reference.schema_name,
                endpoint_id=self.endpoint_id,
            ),
            authenticated_session_id=self.session_id,
            authenticated_worker_node_id=self.node_id,
            provider_id=self.provider_id,
            now=self.clock(),
        )

    def _resolve_plan(self, job: JobContract, *, grant_id: str) -> AnalysisPlan:
        reference = self._input(job, ANALYSIS_PLAN_SCHEMA)
        if reference.size_bytes > MAX_PLAN_BYTES:
            raise FederationValidationError(
                "analysis-plan-too-large", "plan", "declared plan exceeds the bound"
            )
        payload = b"".join(self._fetch(job, reference, grant_id=grant_id))
        plan = AnalysisPlan.from_bytes(payload)
        if plan.job_id != job.job_id or plan.session_id != job.session_id:
            raise FederationValidationError(
                "analysis-plan-context-mismatch",
                "plan",
                "plan does not describe this job",
            )
        return plan

    def _resolve_slice(
        self, job: JobContract, *, grant_id: str, workspace: Path
    ) -> Path:
        reference = self._input(job, ANALYSIS_DATA_SLICE_SCHEMA)
        if reference.size_bytes > self.max_slice_bytes:
            raise FederationValidationError(
                "analysis-slice-too-large", "inputs", "declared slice exceeds the bound"
            )
        data_dir = workspace / "data"
        extract_slice_archive(
            self._fetch(job, reference, grant_id=grant_id),
            archive_path=workspace / "slice.tar.gz",
            destination=data_dir,
            max_bytes=self.max_slice_bytes,
        )
        (workspace / "slice.tar.gz").unlink(missing_ok=True)
        return data_dir

    def _result(
        self,
        job: JobContract,
        attempt_id: str,
        plan: AnalysisPlan,
        report: AnalysisExecutionReport,
    ) -> ExecutionResult:
        declared = job.outputs[0]
        document = {
            "schema": ANALYSIS_RESULT_SCHEMA,
            "job_id": job.job_id,
            "session_id": job.session_id,
            "attempt_id": attempt_id,
            "provider_id": self.provider_id,
            "worker_node_id": self.node_id,
            "slice_kind": plan.slice_kind,
            "slice_key": plan.slice_key,
            "source_signature": plan.source_signature,
            "target_dates": list(plan.target_dates),
            "succeeded": bool(report.succeeded),
            "analysis_session_ids": [
                _short(item) for item in report.analysis_session_ids[:MAX_REPORTED_SCRIPTS]
            ],
            "processed_dates": list(report.processed_dates[:MAX_REPORTED_SCRIPTS]),
            "failed_scripts": [
                _short(item) for item in report.failed_scripts[:MAX_REPORTED_SCRIPTS]
            ],
            "script_results": [
                {key: _short(value) for key, value in dict(item).items()}
                for item in report.script_results[:MAX_REPORTED_SCRIPTS]
            ],
            "completed_at": _stamp(self.clock()),
        }
        payload = json.dumps(
            document, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
        object_key = f"analysis/results/{job.job_id}/{attempt_id}/result.json"
        identity = self.content_store.write_bytes(object_key, payload)
        result_reference = ArtifactReference(
            reference_id=declared.reference_id,
            session_id=declared.session_id,
            schema_name=declared.schema_name,
            media_type=declared.media_type,
            content_hash=identity.content_hash,
            size_bytes=identity.size_bytes,
        )
        details: dict[str, Any] = {
            "result_reference": result_reference.to_dict(),
            "job_id": job.job_id,
            "attempt_id": attempt_id,
            "provider_id": self.provider_id,
            "worker_node_id": self.node_id,
            "session_id": job.session_id,
            "slice_kind": plan.slice_kind,
            "slice_key": plan.slice_key,
            "source_signature": plan.source_signature,
            "input_artifact_ids": [item.reference_id for item in job.inputs],
            "processed_dates": list(report.processed_dates[:MAX_REPORTED_SCRIPTS]),
            "failed_scripts": [
                _short(item) for item in report.failed_scripts[:MAX_REPORTED_SCRIPTS]
            ],
        }
        for key, value in dict(report.details).items():
            # Executor-supplied context is advisory and must never overwrite the
            # coordinator-critical fields above.
            details.setdefault(_short(key), _short(value))
        if report.succeeded:
            return ExecutionResult(True, details)
        return ExecutionResult(
            False, details, _short(report.reason_code or "analysis-execution-failed")
        )


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = [
    "AnalysisExecutionReport",
    "AnalysisSliceExecutor",
    "FederatedAnalysisHandler",
]
