"""Publish capability-first contribution metadata through Federation authority.

This bridge publishes only public-safe capability metadata for an already trusted
Federation member. A capability announcement is metadata only: it does not grant
provider enrollment, storage assignment, compute execution, job, artifact, or
membership authority.  A narrow versioned attestation may be included when current
local evidence proves an enabled, allowed, GREEN contribution: non-storage
contributions carry the F8.1 auto-enrollment attestation, storage carries the
separate storage-authority attestation.  The relay still owns both decisions.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from catalog.capabilities.benchmarking import evaluate_result
from catalog.capabilities.provider_auto_enrollment import (
    AUTO_ENROLLMENT_PROPERTY,
    AUTO_ENROLLMENT_SCHEMA,
)
from catalog.capabilities.storage_authority_enrollment import (
    DEFAULT_STORAGE_GROUP_ID,
    STORAGE_AUTHORITY_PROPERTY,
    STORAGE_AUTHORITY_SCHEMA,
    federation_storage_provider_id,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.onboarding_models import BenchmarkRecommendation, BenchmarkState

_PUBLICATION_KIND = "capability-first-candidate"
_GREEN_RECOMMENDATIONS = frozenset(
    {BenchmarkRecommendation.RECOMMENDED, BenchmarkRecommendation.USABLE}
)


@dataclass(frozen=True)
class ContributionPublication:
    """One idempotent capability metadata update for the authenticated relay."""

    announcement: CapabilityAnnouncement
    request_id: str


def _enum_text(value: object, default: str = "") -> str:
    raw = getattr(value, "value", value)
    return str(raw if raw is not None else default).strip().casefold()


def _status_for_intent(intent: object | None) -> CapabilityStatus:
    if intent is None:
        return CapabilityStatus.DISABLED
    activation = _enum_text(getattr(intent, "activation_state", None))
    return {
        "active": CapabilityStatus.READY,
        "pending": CapabilityStatus.REGISTERING,
        "inactive": CapabilityStatus.DISABLED,
        "suspended": CapabilityStatus.UNAVAILABLE,
        "blocked": CapabilityStatus.UNAVAILABLE,
    }.get(activation, CapabilityStatus.UNAVAILABLE)


def _protocol_version(candidate: object, existing: dict[str, Any] | None) -> str:
    envelope = getattr(candidate, "capacity_envelope", {})
    if isinstance(envelope, dict):
        value = envelope.get("protocol_version")
        if isinstance(value, str) and value.strip():
            return value.strip()
    if existing is not None:
        value = existing.get("protocol_version")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "1.0"


def _properties(
    candidate: object,
    attestation: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    label = getattr(candidate, "display_label", None)
    candidate_id = getattr(candidate, "candidate_id", None)
    payload: dict[str, Any] = {"kind": _PUBLICATION_KIND}
    if isinstance(candidate_id, str) and candidate_id.strip():
        # This is a logical local candidate identity, not an endpoint/path/secret.
        # It lets product adapters correlate a device-scoped Federation ID back
        # to the already-authorized local provider implementation.
        payload["candidate_id"] = candidate_id.strip()
    if isinstance(label, str) and label.strip():
        payload["label"] = label.strip()
    for property_name, block in (attestation or {}).items():
        payload[property_name] = dict(block)
    return payload


def _scoped_capability_id(node_id: str, candidate_id: str) -> str:
    """Create a stable logical ID when another member owns the local candidate ID."""

    digest = hashlib.sha256(f"{node_id}\0{candidate_id}".encode()).hexdigest()
    return f"member-capability-{digest[:32]}"


def published_capability_id(
    *,
    session_id: str,
    node_id: str,
    candidate_id: str,
    authorized_status: dict[str, Any],
) -> str:
    """Resolve the Federation-scoped ID without rewriting stable existing IDs.

    Phase 2 scopes ``capability_id`` to the whole session while capability-first
    candidate IDs are local to one device. Preserve an already accepted local ID
    for compatibility. If another member already owns that ID, derive a stable
    device-scoped ID instead so two devices can contribute the same service type.
    """

    values = authorized_status.get("capabilities", ())
    if not isinstance(values, list):
        return candidate_id
    for value in values:
        if (
            not isinstance(value, dict)
            or value.get("session_id") != session_id
            or value.get("capability_id") != candidate_id
        ):
            continue
        return (
            candidate_id
            if value.get("node_id") == node_id
            else _scoped_capability_id(node_id, candidate_id)
        )
    return candidate_id


def _existing_matches(
    existing: dict[str, Any] | None,
    announcement: CapabilityAnnouncement,
) -> bool:
    if existing is None:
        return False
    return (
        existing.get("session_id") == announcement.session_id
        and existing.get("capability_id") == announcement.capability_id
        and existing.get("node_id") == announcement.node_id
        and existing.get("type") == announcement.type
        and existing.get("protocol") == announcement.protocol
        and existing.get("protocol_version") == announcement.protocol_version
        and _enum_text(existing.get("status")) == announcement.status.value
        and existing.get("properties") == announcement.properties
    )


def _request_id(
    announcement: CapabilityAnnouncement,
    *,
    decision_revision: int,
) -> str:
    semantic = {
        "capability_id": announcement.capability_id,
        "node_id": announcement.node_id,
        "session_id": announcement.session_id,
        "type": announcement.type,
        "protocol": announcement.protocol,
        "protocol_version": announcement.protocol_version,
        "status": announcement.status.value,
        "properties": announcement.properties,
        "decision_revision": decision_revision,
    }
    encoded = json.dumps(
        semantic,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "capability-first-sync-" + hashlib.sha256(encoded).hexdigest()


def _current_green_run_ids(
    *,
    contribution_service: object,
    snapshot: object,
    node_id: str,
    now: datetime,
) -> frozenset[str]:
    benchmark_service = getattr(contribution_service, "benchmark_service", None)
    plan_loader = getattr(benchmark_service, "plan", None)
    results_loader = getattr(benchmark_service, "list_results", None)
    if not callable(plan_loader) or not callable(results_loader):
        return frozenset()
    try:
        plan = tuple(plan_loader(snapshot))
        results = tuple(results_loader())
    except Exception:  # noqa: BLE001 - auto-enrollment evidence fails closed
        return frozenset()

    plan_by_key = {
        (getattr(item, "benchmark_id", None), getattr(item, "target_service_id", None)): item
        for item in plan
        if getattr(item, "runnable", False)
    }
    green: set[str] = set()
    for result in results:
        if getattr(result, "device_id", None) != node_id:
            continue
        run_id = getattr(result, "run_id", None)
        key = (
            getattr(result, "benchmark_id", None),
            getattr(result, "target_service_id", None),
        )
        item = plan_by_key.get(key)
        if not isinstance(run_id, str) or item is None:
            continue
        try:
            validity = evaluate_result(
                result,
                item.definition,
                item.dependency_inputs,
                now=now,
            )
        except Exception:  # noqa: BLE001,S112 - malformed evidence fails closed
            continue
        if (
            validity.current
            and getattr(result, "state", None) is BenchmarkState.PASSED
            and getattr(result, "recommendation", None) in _GREEN_RECOMMENDATIONS
        ):
            green.add(run_id)
    return frozenset(green)


def _auto_enrollment_attestations(
    *,
    contribution_service: object,
    candidates: tuple[object, ...],
    intents: tuple[object, ...],
    node_id: str,
    now: datetime,
) -> dict[str, dict[str, dict[str, Any]]]:
    """Build stable attestations only from current, strict GREEN local evidence.

    Non-storage contributions carry the F8.1 auto-enrollment attestation.  Storage
    carries its own attestation instead, because storage authority is granted by
    the separate storage control plane rather than by provider enrollment.
    """

    snapshot_loader = getattr(contribution_service, "_authorized_snapshot", None)
    if not callable(snapshot_loader):
        return {}
    try:
        snapshot, _benchmark_complete = snapshot_loader(
            require_benchmark_review=False
        )
    except Exception:  # noqa: BLE001 - trust/evidence failures must fail closed
        return {}
    if getattr(snapshot, "device_id", None) != node_id:
        return {}

    green_run_ids = _current_green_run_ids(
        contribution_service=contribution_service,
        snapshot=snapshot,
        node_id=node_id,
        now=now,
    )
    if not green_run_ids:
        return {}

    intent_by_id = {
        str(intent.candidate_id): intent
        for intent in intents
        if isinstance(getattr(intent, "candidate_id", None), str)
        and getattr(intent, "device_id", node_id) == node_id
    }
    evidence: dict[str, dict[str, dict[str, Any]]] = {}
    for candidate in candidates:
        candidate_id = getattr(candidate, "candidate_id", None)
        if not isinstance(candidate_id, str):
            continue
        intent = intent_by_id.get(candidate_id)
        if intent is None:
            continue
        is_storage = _enum_text(getattr(candidate, "capability_type", None)) == "storage"
        if _enum_text(getattr(intent, "desired_state", None)) != "enabled":
            continue
        if _enum_text(getattr(intent, "policy_state", None)) != "allowed":
            continue
        candidate_policy = getattr(candidate, "policy_state", None)
        if candidate_policy is not None and _enum_text(candidate_policy) != "allowed":
            continue
        missing = getattr(candidate, "missing_prerequisites", ())
        if missing:
            continue
        inspection_revision = getattr(candidate, "inspection_revision", None)
        if (
            isinstance(inspection_revision, bool)
            or not isinstance(inspection_revision, int)
            or inspection_revision <= 0
            or inspection_revision != getattr(snapshot, "revision", None)
        ):
            continue
        decision_revision = getattr(intent, "decision_revision", None)
        if (
            isinstance(decision_revision, bool)
            or not isinstance(decision_revision, int)
            or decision_revision <= 0
        ):
            continue
        raw_run_ids = getattr(candidate, "benchmark_run_ids", ())
        if not isinstance(raw_run_ids, tuple) or not raw_run_ids:
            continue
        run_ids = tuple(
            run_id
            for run_id in raw_run_ids
            if isinstance(run_id, str) and run_id
        )
        if len(run_ids) != len(raw_run_ids) or not set(run_ids) <= green_run_ids:
            continue
        attested = {
            "eligible": True,
            "benchmark_state": "green",
            "inspection_revision": inspection_revision,
            "benchmark_run_ids": list(run_ids),
            "decision_revision": decision_revision,
            "desired_state": "enabled",
            "policy_state": "allowed",
        }
        if not is_storage:
            evidence[candidate_id] = {
                AUTO_ENROLLMENT_PROPERTY: {
                    "schema": AUTO_ENROLLMENT_SCHEMA,
                    **attested,
                }
            }
            continue
        envelope = getattr(candidate, "capacity_envelope", None)
        local_provider_id = (
            envelope.get("provider_id") if isinstance(envelope, dict) else None
        )
        if not isinstance(local_provider_id, str) or not local_provider_id.strip():
            continue
        evidence[candidate_id] = {
            STORAGE_AUTHORITY_PROPERTY: {
                "schema": STORAGE_AUTHORITY_SCHEMA,
                # Scope the local candidate to this member so two devices that
                # detect the same storage candidate stay distinct providers.
                "provider_id": federation_storage_provider_id(
                    node_id,
                    local_provider_id.strip(),
                ),
                "group_id": DEFAULT_STORAGE_GROUP_ID,
                "role": "primary",
                **attested,
            }
        }
    return evidence


def plan_contribution_publications(
    *,
    session_id: str,
    node_id: str,
    candidates: tuple[object, ...],
    intents: tuple[object, ...],
    authorized_status: dict[str, Any],
    now: datetime,
    attestation_by_candidate_id: dict[str, dict[str, dict[str, Any]]] | None = None,
) -> tuple[ContributionPublication, ...]:
    """Plan only metadata changes needed to make member views converge.

    All inspected contribution candidates may be advertised because an
    announcement is not provider authority. Their status preserves the local
    operator decision: unreviewed/disabled candidates are announced disabled,
    active contributions are ready, pending contributions are registering, and
    suspended/blocked contributions are unavailable.
    """

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(timezone.utc)
    attestation_by_candidate_id = attestation_by_candidate_id or {}

    intent_by_id = {
        str(intent.candidate_id): intent
        for intent in intents
        if isinstance(getattr(intent, "candidate_id", None), str)
        and getattr(intent, "device_id", node_id) == node_id
    }
    existing_by_id: dict[str, dict[str, Any]] = {}
    values = authorized_status.get("capabilities", ())
    if isinstance(values, list):
        for value in values:
            if (
                isinstance(value, dict)
                and value.get("session_id") == session_id
                and value.get("node_id") == node_id
                and isinstance(value.get("capability_id"), str)
            ):
                existing_by_id[str(value["capability_id"])] = value

    publications: list[ContributionPublication] = []
    current_ids: set[str] = set()
    for candidate in candidates:
        candidate_id = getattr(candidate, "candidate_id", None)
        candidate_node_id = getattr(candidate, "device_id", None)
        if not isinstance(candidate_id, str) or candidate_node_id != node_id:
            continue
        capability_id = published_capability_id(
            session_id=session_id,
            node_id=node_id,
            candidate_id=candidate_id,
            authorized_status=authorized_status,
        )
        current_ids.add(capability_id)
        existing = existing_by_id.get(capability_id)
        intent = intent_by_id.get(candidate_id)
        status = _status_for_intent(intent)
        decided_at = getattr(intent, "decided_at", None)
        announced_at = (
            decided_at.astimezone(timezone.utc)
            if isinstance(decided_at, datetime)
            and decided_at.tzinfo is not None
            and decided_at.utcoffset() is not None
            else now
        )
        announcement = CapabilityAnnouncement(
            capability_id=capability_id,
            node_id=node_id,
            session_id=session_id,
            type=str(getattr(candidate, "capability_type", "unknown-capability")),
            protocol=str(
                getattr(candidate, "capability_protocol", "unknown-protocol")
            ),
            protocol_version=_protocol_version(candidate, existing),
            status=status,
            properties=_properties(
                candidate,
                attestation_by_candidate_id.get(candidate_id),
            ),
            announced_at=announced_at,
        )
        if _existing_matches(existing, announcement):
            continue
        revision = getattr(intent, "decision_revision", 0)
        decision_revision = (
            revision
            if isinstance(revision, int)
            and not isinstance(revision, bool)
            and revision >= 0
            else 0
        )
        publications.append(
            ContributionPublication(
                announcement,
                _request_id(
                    announcement,
                    decision_revision=decision_revision,
                ),
            )
        )

    # A previously published capability-first candidate that can no longer be
    # reconstructed must fail closed. Preserve only its already validated public
    # metadata and mark it unavailable; never infer continued readiness.
    for capability_id, existing in existing_by_id.items():
        if capability_id in current_ids:
            continue
        properties = existing.get("properties")
        if (
            not isinstance(properties, dict)
            or properties.get("kind") != _PUBLICATION_KIND
        ):
            continue
        if _enum_text(existing.get("status")) in {
            CapabilityStatus.UNAVAILABLE.value,
            CapabilityStatus.REVOKED.value,
        }:
            continue
        announcement = CapabilityAnnouncement(
            capability_id=capability_id,
            node_id=node_id,
            session_id=session_id,
            type=str(existing.get("type") or "unknown-capability"),
            protocol=str(existing.get("protocol") or "unknown-protocol"),
            protocol_version=str(existing.get("protocol_version") or "1.0"),
            status=CapabilityStatus.UNAVAILABLE,
            properties=properties,
            announced_at=now,
        )
        publications.append(
            ContributionPublication(
                announcement,
                _request_id(announcement, decision_revision=-1),
            )
        )

    return tuple(publications)


def publish_local_contributions(
    *,
    contribution_service: object,
    runtime: object,
    runtime_state: object,
    session_id: str,
    node_id: str,
    now: datetime,
) -> int:
    """Synchronize public contribution metadata over one authenticated client."""

    intents_loader = getattr(contribution_service, "intents", None)
    recommend = getattr(contribution_service, "recommend", None)
    if not callable(intents_loader) or not callable(recommend):
        raise TypeError("contribution service does not expose the required read methods")
    intents = tuple(intents_loader())
    try:
        candidates = tuple(recommend(require_benchmark_review=False))
    except Exception:  # noqa: BLE001 - missing/stale evidence must fail closed below
        candidates = ()

    status_loader = getattr(runtime, "coordinator_status", None)
    announce = getattr(runtime, "announce_capability", None)
    if not callable(status_loader) or not callable(announce):
        raise TypeError("pairing runtime does not expose authenticated publication")
    status = status_loader()
    if not isinstance(status, dict):
        raise TypeError("coordinator status must be an object")

    attestations = _auto_enrollment_attestations(
        contribution_service=contribution_service,
        candidates=candidates,
        intents=intents,
        node_id=node_id,
        now=now.astimezone(timezone.utc),
    )
    publications = plan_contribution_publications(
        session_id=session_id,
        node_id=node_id,
        candidates=candidates,
        intents=intents,
        authorized_status=status,
        now=now,
        attestation_by_candidate_id=attestations,
    )
    for publication in publications:
        announce(
            runtime_state,
            publication.announcement,
            request_id=publication.request_id,
        )
    return len(publications)


__all__ = [
    "ContributionPublication",
    "plan_contribution_publications",
    "publish_local_contributions",
    "published_capability_id",
]
