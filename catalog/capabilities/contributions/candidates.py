"""Candidate generation from CF2 inspection snapshots and benchmark evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timedelta, timezone

from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkResult,
    BenchmarkState,
    ContributionCandidate,
    ContributionPolicyState,
    DeviceInspectionSnapshot,
)
from catalog.federation.storage_protocol import STORAGE_PROTOCOL

from .types import CandidateRecommendation, CandidateSource, LocalContributionDescriptor

_LEGACY_STORAGE_CANDIDATE_PROTOCOL = "fcp-storage-candidate"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _identity_protocol(descriptor: LocalContributionDescriptor) -> str:
    """Keep the v1 storage candidate identity stable across protocol correction.

    The first physical v1 builds used the authority-free storage benchmark protocol
    as part of the candidate hash.  The Federation publication later corrected the
    descriptor to the real storage protocol.  Existing enabled intent is keyed by
    the original candidate ID, so changing that hash would orphan a valid storage
    decision during an ordinary upgrade.  Only the supported storage protocol gets
    this compatibility identity; unrelated protocol changes still produce a new ID.
    """

    if (
        descriptor.capability_type == "storage"
        and descriptor.capability_protocol == STORAGE_PROTOCOL
    ):
        return _LEGACY_STORAGE_CANDIDATE_PROTOCOL
    return descriptor.capability_protocol


def _candidate_id(device_id: str, descriptor: LocalContributionDescriptor) -> str:
    identity = json.dumps(
        {
            "device_id": device_id,
            "logical_service_id": descriptor.logical_service_id,
            "capability_type": descriptor.capability_type,
            "capability_protocol": _identity_protocol(descriptor),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return "candidate-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]


class ContributionCandidateGenerator:
    """Generate deterministic candidates without granting any authority."""

    def __init__(
        self,
        *,
        sources: Sequence[CandidateSource],
        benchmark_definitions: Iterable[BenchmarkDefinition] = (),
        candidate_ttl_seconds: int = 900,
        now: Callable[[], datetime] = _utc_now,
        enforce_evidence_expiry: bool = True,
    ) -> None:
        if isinstance(candidate_ttl_seconds, bool) or candidate_ttl_seconds <= 0:
            raise ValueError("candidate_ttl_seconds must be positive")
        if not isinstance(enforce_evidence_expiry, bool):
            raise TypeError("enforce_evidence_expiry must be boolean")
        self._sources = tuple(sources)
        self._definitions = {
            item.benchmark_id: item for item in benchmark_definitions
        }
        self._candidate_ttl_seconds = int(candidate_ttl_seconds)
        self._now = now
        self._enforce_evidence_expiry = enforce_evidence_expiry

    def generate(
        self,
        inspection: DeviceInspectionSnapshot,
        benchmark_results: Iterable[BenchmarkResult],
    ) -> tuple[CandidateRecommendation, ...]:
        now = self._now().astimezone(timezone.utc)
        if self._enforce_evidence_expiry and inspection.expires_at <= now:
            return ()
        results = tuple(
            result
            for result in benchmark_results
            if result.device_id == inspection.device_id
            and (
                not self._enforce_evidence_expiry
                or result.expires_at > now
            )
        )
        descriptors: dict[
            tuple[str, str, str], LocalContributionDescriptor
        ] = {}
        for source in self._sources:
            for descriptor in source.descriptors(inspection):
                key = (
                    descriptor.capability_type,
                    descriptor.capability_protocol,
                    descriptor.logical_service_id,
                )
                previous = descriptors.get(key)
                if previous is not None and previous != descriptor:
                    raise ValueError("candidate sources disagree about one local service")
                descriptors[key] = descriptor

        recommendations: list[CandidateRecommendation] = []
        generated_expires_at = now + timedelta(seconds=self._candidate_ttl_seconds)
        expires_at = (
            min(inspection.expires_at, generated_expires_at)
            if self._enforce_evidence_expiry
            else generated_expires_at
        )
        for descriptor in descriptors.values():
            evidence = self._evidence_for(descriptor, results)
            envelope = dict(descriptor.capacity_envelope)
            passed_metrics = [
                result.metrics
                for result in evidence
                if result.state is BenchmarkState.PASSED
                and result.recommendation
                in {
                    BenchmarkRecommendation.RECOMMENDED,
                    BenchmarkRecommendation.USABLE,
                }
            ]
            if passed_metrics:
                envelope["benchmark_metrics"] = passed_metrics[-1]
            candidate = ContributionCandidate(
                candidate_id=_candidate_id(inspection.device_id, descriptor),
                device_id=inspection.device_id,
                capability_type=descriptor.capability_type,
                capability_protocol=descriptor.capability_protocol,
                display_label=descriptor.display_label,
                inspection_revision=inspection.revision,
                benchmark_run_ids=tuple(result.run_id for result in evidence),
                capacity_envelope=envelope,
                missing_prerequisites=descriptor.missing_prerequisites,
                policy_state=ContributionPolicyState.NOT_EVALUATED,
                created_at=now,
                expires_at=expires_at,
            )
            recommendations.append(
                CandidateRecommendation(
                    candidate=candidate,
                    logical_service_id=descriptor.logical_service_id,
                    benchmark_results=evidence,
                )
            )
        return tuple(
            sorted(
                recommendations,
                key=lambda item: (
                    item.candidate.capability_type,
                    item.candidate.display_label.casefold(),
                    item.candidate.candidate_id,
                ),
            )
        )

    def _evidence_for(
        self,
        descriptor: LocalContributionDescriptor,
        results: tuple[BenchmarkResult, ...],
    ) -> tuple[BenchmarkResult, ...]:
        matching: dict[str, BenchmarkResult] = {}
        for result in results:
            if result.target_service_id != descriptor.logical_service_id:
                continue
            definition = self._definitions.get(result.benchmark_id)
            if definition is not None and (
                definition.capability_type != descriptor.capability_type
                or definition.capability_protocol != descriptor.capability_protocol
            ):
                continue
            current = matching.get(result.benchmark_id)
            if current is None or (
                result.finished_at,
                result.run_id,
            ) > (
                current.finished_at,
                current.run_id,
            ):
                matching[result.benchmark_id] = result
        return tuple(
            sorted(
                matching.values(),
                key=lambda item: (item.finished_at, item.run_id),
            )
        )
