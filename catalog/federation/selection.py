"""Deterministic Phase E replica-promotion selection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .manifest import AuthoritativeStorageManifest, DatasetManifest
from .reporting import StorageReplicaAssessment, StorageReplicaReport


@dataclass(frozen=True)
class StoragePromotionCandidate:
    provider_id: str
    assessment: StorageReplicaAssessment
    report: StorageReplicaReport
    complete: bool
    complete_reasons: tuple[str, ...]
    rank: tuple[Any, ...]


@dataclass(frozen=True)
class StoragePromotionSelection:
    decision: str
    selected_provider_id: str | None
    selected_role: str | None
    candidate_provider_ids: tuple[str, ...]
    rejected: dict[str, tuple[str, ...]]
    reason: str
    preferred_current_primary: str | None


def _dataset_reasons(
    manifest_dataset: DatasetManifest,
    report_dataset: DatasetManifest | None,
) -> tuple[bool, tuple[str, ...]]:
    if report_dataset is None:
        return False, ("missing-ranges",)
    if (
        report_dataset.schema_name != manifest_dataset.schema_name
        or report_dataset.schema_version != manifest_dataset.schema_version
        or report_dataset.source_id != manifest_dataset.source_id
    ):
        return False, ("committed-hash-conflict",)
    if report_dataset.last_contiguous_sequence != manifest_dataset.last_contiguous_sequence:
        return False, ("missing-ranges",)
    if report_dataset.missing_ranges:
        return False, ("missing-ranges",)
    return True, ()


def evaluate_storage_promotion_candidate(
    manifest: AuthoritativeStorageManifest,
    assessment: StorageReplicaAssessment,
    *,
    current_primary_provider_id: str | None = None,
) -> StoragePromotionCandidate:
    report = assessment.report
    if report is None:
        return StoragePromotionCandidate(
            provider_id=assessment.provider_id,
            assessment=assessment,
            report=StorageReplicaReport(
                schema="msh.storage_replica_report.v1",
                protocol_version="1.0",
                session_id=assessment.session_id,
                group_id=assessment.group_id,
                provider_id=assessment.provider_id,
                report_revision=assessment.report_revision,
                manifest_revision=0,
                manifest_hash=assessment.report_hash,
                committed_item_hashes=(),
                datasets=(),
                integrity_verified=False,
                synchronization_state="unknown",
                eligible=False,
                eligibility_reasons=("report-missing",),
                reported_at=assessment.assessed_at,
            ),
            complete=False,
            complete_reasons=("report-missing",),
            rank=(1, assessment.provider_id),
        )

    reasons: list[str] = []
    complete = True
    if not assessment.accepted or not assessment.eligibility:
        complete = False
        reasons.append(assessment.eligibility_reason)
    if report.session_id != manifest.session_id:
        complete = False
        reasons.append("session-mismatch")
    if report.group_id != manifest.group_id:
        complete = False
        reasons.append("storage-group-mismatch")
    if report.manifest_revision != manifest.revision:
        complete = False
        reasons.append("manifest-revision-mismatch")
    if report.manifest_hash != manifest.manifest_hash:
        complete = False
        reasons.append("manifest-hash-mismatch")
    if not report.integrity_verified:
        complete = False
        reasons.append("integrity-failure")
    if report.synchronization_state != "synchronized":
        complete = False
        reasons.append("synchronization-in-progress")

    authoritative_datasets = {dataset.dataset_id: dataset for dataset in manifest.datasets}
    report_datasets = {dataset.dataset_id: dataset for dataset in report.datasets}
    for dataset_id, dataset in authoritative_datasets.items():
        ok, dataset_reasons = _dataset_reasons(dataset, report_datasets.get(dataset_id))
        if not ok:
            complete = False
            reasons.extend(dataset_reasons)

    authoritative_hashes = tuple(sorted(item.content_hash for item in manifest.items))
    if tuple(report.committed_item_hashes) != authoritative_hashes:
        complete = False
        reasons.append("committed-hash-conflict")

    if complete:
        reasons = ("eligible",)
    else:
        reasons = tuple(dict.fromkeys(reasons)) or ("candidate-incomplete",)
    watermark = tuple(
        (
            dataset.last_contiguous_sequence
            if dataset.last_contiguous_sequence is not None
            else -1
        )
        for dataset in sorted(manifest.datasets, key=lambda value: value.dataset_id)
    )
    rank = (
        0 if complete else 1,
        tuple(-value for value in watermark),
        0 if assessment.provider_id == current_primary_provider_id else 1,
        assessment.provider_id,
    )
    return StoragePromotionCandidate(
        provider_id=assessment.provider_id,
        assessment=assessment,
        report=report,
        complete=complete,
        complete_reasons=reasons,
        rank=rank,
    )


def select_storage_promotion_candidate(
    manifest: AuthoritativeStorageManifest,
    assessments: tuple[StorageReplicaAssessment, ...],
    *,
    current_primary_provider_id: str | None = None,
) -> StoragePromotionSelection:
    candidates: list[StoragePromotionCandidate] = []
    rejected: dict[str, tuple[str, ...]] = {}
    for assessment in assessments:
        candidate = evaluate_storage_promotion_candidate(
            manifest,
            assessment,
            current_primary_provider_id=current_primary_provider_id,
        )
        if candidate.complete:
            candidates.append(candidate)
        else:
            rejected[candidate.provider_id] = candidate.complete_reasons
    candidates.sort(key=lambda candidate: candidate.rank)
    candidate_ids = tuple(candidate.provider_id for candidate in candidates)
    if not candidates:
        return StoragePromotionSelection(
            decision="storage-degraded",
            selected_provider_id=None,
            selected_role=None,
            candidate_provider_ids=(),
            rejected=rejected,
            reason="required-committed-data-unavailable",
            preferred_current_primary=current_primary_provider_id,
        )
    selected = candidates[0]
    reason = (
        "current-primary-is-healthy-complete-and-not-authoritatively-behind"
        if selected.provider_id == current_primary_provider_id
        else "highest-ranked-complete-candidate"
    )
    decision = "retain-current" if selected.provider_id == current_primary_provider_id else "select-candidate"
    return StoragePromotionSelection(
        decision=decision,
        selected_provider_id=selected.provider_id,
        selected_role="primary",
        candidate_provider_ids=candidate_ids,
        rejected=rejected,
        reason=reason,
        preferred_current_primary=current_primary_provider_id,
    )
