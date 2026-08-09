"""Publish capability-first contribution metadata through Federation authority.

This bridge publishes only public-safe capability metadata for an already trusted
Federation member. A capability announcement is metadata only: it does not grant
provider enrollment, storage assignment, compute execution, job, artifact, or
membership authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus

_PUBLICATION_KIND = "capability-first-candidate"


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


def _properties(candidate: object) -> dict[str, Any]:
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


def plan_contribution_publications(
    *,
    session_id: str,
    node_id: str,
    candidates: tuple[object, ...],
    intents: tuple[object, ...],
    authorized_status: dict[str, Any],
    now: datetime,
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
            properties=_properties(candidate),
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

    publications = plan_contribution_publications(
        session_id=session_id,
        node_id=node_id,
        candidates=candidates,
        intents=intents,
        authorized_status=status,
        now=now,
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
