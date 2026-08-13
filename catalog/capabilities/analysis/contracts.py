"""Deterministic contracts for federation-dispatched JSONL analysis jobs.

This module is pure: it derives stable job identity, builds the versioned
:class:`~catalog.capabilities.jobs.JobContract` for one unit of analysis work,
and validates the bounded work plan that travels as an artifact rather than
inside a control message. It performs no storage, selection, dispatch, or
execution.

Three roles are kept separate throughout the package:

``DATA OWNER``
    the node whose disk holds the JSONL and which registers the input artifacts.
``COORDINATOR``
    the node authorized to submit, own, and dispatch the durable job.
``WORKER``
    the provider node selected to execute the analysis.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, ClassVar, Self

from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)

from ..jobs import (
    ArtifactReference,
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)

#: Capability advertised by providers that can run the runtime analysis contract.
ANALYSIS_CAPABILITY_TYPE = "background-analysis"
ANALYSIS_PROTOCOL = "fcp-background-analysis"
ANALYSIS_PROTOCOL_VERSION = "1.0"

#: Logical analysis contract implemented by a provider, and its version. The
#: version participates in job identity: changing it is a new logical job.
ANALYSIS_CONTRACT_ID = "runtime-playback-ready-analysis"
ANALYSIS_CONTRACT_VERSION = "1"

ANALYSIS_PLAN_SCHEMA = "fcp.analysis-plan.v1"
ANALYSIS_DATA_SLICE_SCHEMA = "fcp.analysis-data-slice.v1"
ANALYSIS_RESULT_SCHEMA = "fcp.analysis-result.v1"

ANALYSIS_PLAN_MEDIA_TYPE = "application/json"
ANALYSIS_DATA_SLICE_MEDIA_TYPE = "application/gzip"
ANALYSIS_RESULT_MEDIA_TYPE = "application/json"

#: Logical (never network) endpoint identifier for analysis artifacts.
ANALYSIS_ENDPOINT_ID = "fcp.analysis-artifacts"

SLICE_KIND_DATE = "date-slice"
SLICE_KIND_UPLOAD_BATCH = "upload-batch"
_SLICE_KINDS = frozenset({SLICE_KIND_DATE, SLICE_KIND_UPLOAD_BATCH})

ORIGIN_AUTOMATIC_DISCOVERY = "automatic-discovery"
ORIGIN_MANUAL_UPLOAD = "manual-upload"
_ORIGINS = frozenset({ORIGIN_AUTOMATIC_DISCOVERY, ORIGIN_MANUAL_UPLOAD})

MAX_PLAN_BYTES = 8_192
MAX_TARGET_DATES = 64
MAX_SCRIPT_KEYS = 32
#: Default ceiling for one packed input slice. Analysis work larger than this
#: fails closed at submission instead of silently degrading to local execution.
DEFAULT_MAX_SLICE_BYTES = 512 * 1024 * 1024

#: One attempt must finish inside one ownership lease, so the run timeout is
#: bounded by ``MAX_OWNERSHIP_LEASE_SECONDS`` rather than by wall-clock hope.
ANALYSIS_LEASE_SECONDS = 55 * 60
ANALYSIS_GRANT_SECONDS = 50 * 60

_SLICE_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_SCRIPT_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_NAMESPACE_RE = re.compile(r"^[A-Za-z0-9_-]{1,48}$")
_SIGNATURE_RE = re.compile(r"^[0-9a-f]{8,128}$")
_SESSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

#: Error codes that justify another attempt on a different provider.
ANALYSIS_RETRYABLE_ERROR_CODES: tuple[str, ...] = (
    "analysis-input-unavailable",
    "capability-route-failed",
    "lease-expired",
    "provider-not-hosted",
    "run-timeout",
    "start-timeout",
    "worker-handler-failed",
    "worker-heartbeat-lost",
    "worker-rejected",
)


def _text(value: Any, field: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise FederationValidationError(
            "invalid-analysis-text", field, f"must match {pattern.pattern}"
        )
    return value


def _iso_date(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-analysis-date", field, "must be an ISO-8601 date string"
        )
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise FederationValidationError(
            "invalid-analysis-date", field, "must be an ISO-8601 date string"
        ) from exc
    return parsed.isoformat()


def _unique_sequence(
    values: Any,
    field: str,
    *,
    maximum: int,
    normalize,
) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)):
        raise FederationValidationError("invalid-array", field, "must be an array")
    if not values:
        raise FederationValidationError("empty-array", field, "must not be empty")
    if len(values) > maximum:
        raise FederationValidationError(
            "array-too-large", field, f"must not exceed {maximum} entries"
        )
    normalized = tuple(
        normalize(item, f"{field}[{index}]") for index, item in enumerate(values)
    )
    if len(set(normalized)) != len(normalized):
        raise FederationValidationError(
            "duplicate-array-value", field, "must not contain duplicates"
        )
    return normalized


@dataclass(frozen=True)
class AnalysisWorkSlice:
    """One logical unit of analysis work described by its data owner.

    The same value type is produced by automatic JSONL discovery and by the
    manual upload workflow, so both converge on one federated job path.
    """

    session_id: str
    slice_kind: str
    slice_key: str
    target_dates: tuple[str, ...]
    script_keys: tuple[str, ...]
    runtime_namespace: str
    source_signature: str
    origin: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "session_id", _text(self.session_id, "session_id", _SESSION_RE)
        )
        if self.slice_kind not in _SLICE_KINDS:
            raise FederationValidationError(
                "invalid-slice-kind", "slice_kind", "unsupported analysis slice kind"
            )
        if self.origin not in _ORIGINS:
            raise FederationValidationError(
                "invalid-analysis-origin", "origin", "unsupported analysis origin"
            )
        object.__setattr__(
            self, "slice_key", _text(self.slice_key, "slice_key", _SLICE_KEY_RE)
        )
        object.__setattr__(
            self,
            "target_dates",
            tuple(
                sorted(
                    _unique_sequence(
                        self.target_dates,
                        "target_dates",
                        maximum=MAX_TARGET_DATES,
                        normalize=_iso_date,
                    )
                )
            ),
        )
        object.__setattr__(
            self,
            "script_keys",
            _unique_sequence(
                self.script_keys,
                "script_keys",
                maximum=MAX_SCRIPT_KEYS,
                normalize=lambda item, field: _text(item, field, _SCRIPT_KEY_RE),
            ),
        )
        object.__setattr__(
            self,
            "runtime_namespace",
            _text(self.runtime_namespace, "runtime_namespace", _NAMESPACE_RE),
        )
        object.__setattr__(
            self,
            "source_signature",
            _text(self.source_signature, "source_signature", _SIGNATURE_RE),
        )

    @property
    def identity_digest(self) -> str:
        """Return the stable digest that defines this logical unit of work.

        Job identity is intentionally derived from everything that changes the
        meaning of the result: the federation session, the analysis contract
        version, the slice kind/key, the exact dates, the ordered script set,
        the runtime namespace, and the source data signature. Re-discovering
        unchanged data therefore produces the same identity, while materially
        changed source data produces a new logical job.
        """

        payload = {
            "contract": ANALYSIS_CONTRACT_ID,
            "contract_version": ANALYSIS_CONTRACT_VERSION,
            "session_id": self.session_id,
            "slice_kind": self.slice_kind,
            "slice_key": self.slice_key,
            "target_dates": list(self.target_dates),
            "script_keys": list(self.script_keys),
            "runtime_namespace": self.runtime_namespace,
            "source_signature": self.source_signature,
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @property
    def job_id(self) -> str:
        return f"analysis-{self.identity_digest[:32]}"

    @property
    def request_id(self) -> str:
        return f"analysis-request-{self.identity_digest[:32]}"

    @property
    def idempotency_key(self) -> str:
        return f"analysis:{ANALYSIS_CONTRACT_VERSION}:{self.identity_digest}"

    @property
    def object_key_prefix(self) -> str:
        session_digest = hashlib.sha256(self.session_id.encode("utf-8")).hexdigest()[:16]
        return f"analysis/{session_digest}/{self.identity_digest[:32]}"

    def plan_document(self) -> dict[str, Any]:
        """Return the worker-visible plan.

        ``origin`` is deliberately absent: who asked for the work does not change
        the work. Keeping it out of the plan lets automatic discovery and the
        manual upload workflow produce byte-identical inputs, so the same logical
        slice is one durable job no matter which trigger reached it first.
        """

        return {
            "schema": ANALYSIS_PLAN_SCHEMA,
            "contract": ANALYSIS_CONTRACT_ID,
            "contract_version": ANALYSIS_CONTRACT_VERSION,
            "session_id": self.session_id,
            "job_id": self.job_id,
            "slice_kind": self.slice_kind,
            "slice_key": self.slice_key,
            "target_dates": list(self.target_dates),
            "script_keys": list(self.script_keys),
            "runtime_namespace": self.runtime_namespace,
            "source_signature": self.source_signature,
        }

    def plan_bytes(self) -> bytes:
        encoded = json.dumps(
            self.plan_document(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        if len(encoded) > MAX_PLAN_BYTES:
            raise FederationValidationError(
                "analysis-plan-too-large", "plan", f"must not exceed {MAX_PLAN_BYTES} bytes"
            )
        return encoded


@dataclass(frozen=True)
class AnalysisPlan:
    """The strictly validated work plan a worker is allowed to act upon."""

    SCHEMA: ClassVar[str] = ANALYSIS_PLAN_SCHEMA

    session_id: str
    job_id: str
    slice_kind: str
    slice_key: str
    target_dates: tuple[str, ...]
    script_keys: tuple[str, ...]
    runtime_namespace: str
    source_signature: str

    @classmethod
    def from_bytes(cls, payload: bytes, *, max_bytes: int = MAX_PLAN_BYTES) -> Self:
        if not isinstance(payload, (bytes, bytearray)):
            raise FederationValidationError(
                "invalid-analysis-plan", "plan", "must be UTF-8 JSON bytes"
            )
        if len(payload) > max_bytes:
            raise FederationValidationError(
                "analysis-plan-too-large", "plan", f"must not exceed {max_bytes} bytes"
            )
        try:
            document = json.loads(bytes(payload).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-analysis-plan", "plan", "must be valid UTF-8 JSON"
            ) from exc
        if not isinstance(document, dict):
            raise FederationValidationError(
                "invalid-analysis-plan", "plan", "must be a JSON object"
            )
        if document.get("schema") != ANALYSIS_PLAN_SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-schema", "schema", f"expected {ANALYSIS_PLAN_SCHEMA}"
            )
        if document.get("contract") != ANALYSIS_CONTRACT_ID:
            raise ProtocolCompatibilityError(
                "unsupported-analysis-contract",
                "contract",
                f"expected {ANALYSIS_CONTRACT_ID}",
            )
        if document.get("contract_version") != ANALYSIS_CONTRACT_VERSION:
            raise ProtocolCompatibilityError(
                "unsupported-analysis-contract-version",
                "contract_version",
                f"expected {ANALYSIS_CONTRACT_VERSION}",
            )
        slice_kind = document.get("slice_kind")
        if slice_kind not in _SLICE_KINDS:
            raise FederationValidationError(
                "invalid-slice-kind", "slice_kind", "unsupported analysis slice kind"
            )
        return cls(
            session_id=_text(document.get("session_id"), "session_id", _SESSION_RE),
            job_id=_text(document.get("job_id"), "job_id", _SLICE_KEY_RE),
            slice_kind=slice_kind,
            slice_key=_text(document.get("slice_key"), "slice_key", _SLICE_KEY_RE),
            target_dates=_unique_sequence(
                document.get("target_dates"),
                "target_dates",
                maximum=MAX_TARGET_DATES,
                normalize=_iso_date,
            ),
            script_keys=_unique_sequence(
                document.get("script_keys"),
                "script_keys",
                maximum=MAX_SCRIPT_KEYS,
                normalize=lambda item, field: _text(item, field, _SCRIPT_KEY_RE),
            ),
            runtime_namespace=_text(
                document.get("runtime_namespace"), "runtime_namespace", _NAMESPACE_RE
            ),
            source_signature=_text(
                document.get("source_signature"), "source_signature", _SIGNATURE_RE
            ),
        )


def analysis_capability_requirement() -> CapabilityRequirement:
    """Return the capability every analysis provider must genuinely advertise."""

    return CapabilityRequirement(
        capability_type=ANALYSIS_CAPABILITY_TYPE,
        protocol=ANALYSIS_PROTOCOL,
        protocol_version=ANALYSIS_PROTOCOL_VERSION,
        requirements={
            "analysis_contract": ANALYSIS_CONTRACT_ID,
            "analysis_contract_version": ANALYSIS_CONTRACT_VERSION,
            "input_schemas": [ANALYSIS_DATA_SLICE_SCHEMA, ANALYSIS_PLAN_SCHEMA],
        },
    )


def analysis_provider_attributes(**extra: Any) -> dict[str, Any]:
    """Return the attribute set a real analysis provider publishes."""

    attributes: dict[str, Any] = {
        "analysis_contract": ANALYSIS_CONTRACT_ID,
        "analysis_contract_version": ANALYSIS_CONTRACT_VERSION,
        "input_schemas": [ANALYSIS_DATA_SLICE_SCHEMA, ANALYSIS_PLAN_SCHEMA],
    }
    attributes.update(extra)
    return attributes


def analysis_retry_policy() -> RetryPolicy:
    return RetryPolicy(
        max_attempts=3,
        backoff_seconds=(30, 120),
        retryable_error_codes=ANALYSIS_RETRYABLE_ERROR_CODES,
    )


def analysis_timeout_policy() -> TimeoutPolicy:
    return TimeoutPolicy(
        overall_timeout_seconds=24 * 60 * 60,
        queue_timeout_seconds=6 * 60 * 60,
        start_timeout_seconds=30 * 60,
        run_timeout_seconds=ANALYSIS_LEASE_SECONDS,
        cancellation_grace_seconds=120,
    )


def plan_artifact_id(work: AnalysisWorkSlice) -> str:
    return f"analysis-plan-{work.identity_digest[:32]}"


def slice_artifact_id(work: AnalysisWorkSlice) -> str:
    return f"analysis-slice-{work.identity_digest[:32]}"


def result_artifact_id(work_digest: str) -> str:
    return f"analysis-result-{work_digest[:32]}"


def analysis_grant_id(job_id: str, attempt_id: str) -> str:
    """Return the deterministic grant identity bound to one job attempt.

    The identifier is not a bearer token. Every use is re-authorized against the
    live ownership lease, the authenticated worker node, the provider identity,
    and the explicit artifact allowlist, so knowing it grants nothing.
    """

    digest = hashlib.sha256(f"{job_id}\0{attempt_id}".encode()).hexdigest()
    return f"analysis-grant-{digest[:32]}"


def analysis_dispatch_id(job_id: str, attempt_id: str) -> str:
    digest = hashlib.sha256(f"{job_id}\0{attempt_id}".encode()).hexdigest()
    return f"analysis-dispatch-{digest[:32]}"


def build_analysis_job(
    work: AnalysisWorkSlice,
    *,
    plan_hash: str,
    plan_size: int,
    slice_hash: str,
    slice_size: int,
) -> JobContract:
    """Build the durable contract for one analysis slice.

    Input artifacts are carried by reference with verifiable identity. No JSONL
    ever travels inside the contract or any control message derived from it.
    """

    return JobContract(
        job_id=work.job_id,
        session_id=work.session_id,
        request_id=work.request_id,
        idempotency_key=work.idempotency_key,
        capability=analysis_capability_requirement(),
        inputs=(
            ArtifactReference(
                reference_id=plan_artifact_id(work),
                session_id=work.session_id,
                schema_name=ANALYSIS_PLAN_SCHEMA,
                media_type=ANALYSIS_PLAN_MEDIA_TYPE,
                content_hash=plan_hash,
                size_bytes=plan_size,
            ),
            ArtifactReference(
                reference_id=slice_artifact_id(work),
                session_id=work.session_id,
                schema_name=ANALYSIS_DATA_SLICE_SCHEMA,
                media_type=ANALYSIS_DATA_SLICE_MEDIA_TYPE,
                content_hash=slice_hash,
                size_bytes=slice_size,
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id=result_artifact_id(work.identity_digest),
                session_id=work.session_id,
                schema_name=ANALYSIS_RESULT_SCHEMA,
                media_type=ANALYSIS_RESULT_MEDIA_TYPE,
            ),
        ),
        retry_policy=analysis_retry_policy(),
        timeout_policy=analysis_timeout_policy(),
    )


__all__ = [
    "ANALYSIS_CAPABILITY_TYPE",
    "ANALYSIS_CONTRACT_ID",
    "ANALYSIS_CONTRACT_VERSION",
    "ANALYSIS_DATA_SLICE_MEDIA_TYPE",
    "ANALYSIS_DATA_SLICE_SCHEMA",
    "ANALYSIS_ENDPOINT_ID",
    "ANALYSIS_GRANT_SECONDS",
    "ANALYSIS_LEASE_SECONDS",
    "ANALYSIS_PLAN_MEDIA_TYPE",
    "ANALYSIS_PLAN_SCHEMA",
    "ANALYSIS_PROTOCOL",
    "ANALYSIS_PROTOCOL_VERSION",
    "ANALYSIS_RESULT_MEDIA_TYPE",
    "ANALYSIS_RESULT_SCHEMA",
    "ANALYSIS_RETRYABLE_ERROR_CODES",
    "DEFAULT_MAX_SLICE_BYTES",
    "MAX_PLAN_BYTES",
    "ORIGIN_AUTOMATIC_DISCOVERY",
    "ORIGIN_MANUAL_UPLOAD",
    "SLICE_KIND_DATE",
    "SLICE_KIND_UPLOAD_BATCH",
    "AnalysisPlan",
    "AnalysisWorkSlice",
    "analysis_capability_requirement",
    "analysis_dispatch_id",
    "analysis_grant_id",
    "analysis_provider_attributes",
    "analysis_retry_policy",
    "analysis_timeout_policy",
    "build_analysis_job",
    "plan_artifact_id",
    "result_artifact_id",
    "slice_artifact_id",
]
