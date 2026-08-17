"""Bounded automatic provider enrollment for trusted GREEN contributions.

This policy is deliberately narrower than generic F8.1 enrollment.  A provider
is auto-enrolled only when an authenticated Federation member has already
published a capability-first announcement carrying the versioned attestation
produced from current local inspection and benchmark evidence.

The policy does not grant membership, storage authority, health, execution,
artifact, job, or resource-binding authority.  REGISTERING capability-first
providers may be enrolled to break the contribution/provider bootstrap cycle,
but F8.1 remains fail-closed because resource binding still requires the current
authoritative announcement to be READY.  Storage is always excluded and keeps
its separate control-plane authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus

from .provider_enrollment import (
    ProviderEnrollmentRecord,
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
    _announcement_fingerprint,
)

AUTO_ENROLLMENT_SCHEMA = "fcp.provider-auto-enrollment.v1"
AUTO_ENROLLMENT_PROPERTY = "auto_enrollment"
CAPABILITY_FIRST_KIND = "capability-first-candidate"
MAX_AUTO_BENCHMARK_RUNS = 64
MAX_AUTO_TEXT_BYTES = 512


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _bounded_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if (
        not normalized
        or normalized != value
        or len(normalized.encode("utf-8")) > MAX_AUTO_TEXT_BYTES
        or any(ord(character) < 32 for character in normalized)
    ):
        return None
    return normalized


def _positive_integer(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


@dataclass(frozen=True)
class AutoEnrollmentEvidence:
    inspection_revision: int
    decision_revision: int
    benchmark_run_ids: tuple[str, ...]


def parse_auto_enrollment_evidence(
    announcement: CapabilityAnnouncement,
) -> AutoEnrollmentEvidence | None:
    """Return a strict attestation or ``None`` for all non-auto-enrollable data."""

    if announcement.status not in {
        CapabilityStatus.REGISTERING,
        CapabilityStatus.READY,
    }:
        return None
    if announcement.type.strip().casefold() == "storage":
        return None
    properties = announcement.properties
    if not isinstance(properties, dict):
        return None
    if properties.get("kind") != CAPABILITY_FIRST_KIND:
        return None
    candidate_id = _bounded_text(properties.get("candidate_id"))
    if candidate_id is None:
        return None
    raw = properties.get(AUTO_ENROLLMENT_PROPERTY)
    if not isinstance(raw, dict):
        return None
    if raw.get("schema") != AUTO_ENROLLMENT_SCHEMA or raw.get("eligible") is not True:
        return None
    if raw.get("benchmark_state") != "green":
        return None
    if raw.get("desired_state") != "enabled" or raw.get("policy_state") != "allowed":
        return None
    inspection_revision = _positive_integer(raw.get("inspection_revision"))
    decision_revision = _positive_integer(raw.get("decision_revision"))
    if inspection_revision is None or decision_revision is None:
        return None
    values = raw.get("benchmark_run_ids")
    if (
        not isinstance(values, list)
        or not values
        or len(values) > MAX_AUTO_BENCHMARK_RUNS
    ):
        return None
    run_ids: list[str] = []
    for value in values:
        run_id = _bounded_text(value)
        if run_id is None or run_id in run_ids:
            return None
        run_ids.append(run_id)
    return AutoEnrollmentEvidence(
        inspection_revision=inspection_revision,
        decision_revision=decision_revision,
        benchmark_run_ids=tuple(run_ids),
    )


def _command_id(prefix: str, announcement: CapabilityAnnouncement, *, term: int) -> str:
    semantic = {
        "prefix": prefix,
        "term": term,
        "announcement_fingerprint": _announcement_fingerprint(announcement),
    }
    encoded = json.dumps(
        semantic,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return f"provider-auto-{prefix}-" + hashlib.sha256(encoded).hexdigest()


class TrustedGreenProviderAutoEnrollment:
    """Reconcile one accepted trusted contribution into F8.1 enrollment."""

    def __init__(
        self,
        coordinator: SessionCoordinator,
        store: SQLiteProviderEnrollmentStore,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.coordinator = coordinator
        self.store = store
        self._clock = clock

    def _current_authoritative(
        self,
        announcement: CapabilityAnnouncement,
    ) -> CapabilityAnnouncement | None:
        self.coordinator.store.require_membership(
            session_id=announcement.session_id,
            node_id=announcement.node_id,
        )
        for current in self.coordinator.store.list_capabilities(
            session_id=announcement.session_id,
        ):
            if current.capability_id != announcement.capability_id:
                continue
            if (
                current.node_id != announcement.node_id
                or _announcement_fingerprint(current)
                != _announcement_fingerprint(announcement)
            ):
                return None
            return current
        return None

    def _leadership(self, announcement: CapabilityAnnouncement):
        leadership = self.coordinator.session_leadership(announcement.session_id)
        if not leadership.leader_connected:
            return None
        self.coordinator.require_session_leader(
            session_id=announcement.session_id,
            actor_node_id=leadership.leader_node_id,
        )
        return leadership

    def _approve_registering(
        self,
        announcement: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
        now: datetime,
    ) -> ProviderEnrollmentRecord:
        """Approve only the policy-validated capability-first REGISTERING state."""

        payload = {
            "session_id": announcement.session_id,
            "capability_id": announcement.capability_id,
            "announcement": announcement.to_dict(),
            "expected_revision": expected_revision,
            "target_state": ProviderEnrollmentState.APPROVED.value,
            "reason_code": "trusted-green-auto-enrollment",
        }
        fingerprint = self.store._command_fingerprint("approve", payload)
        with self.store.transaction() as database:
            replay = self.store._replay_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation="approve",
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay
            current = self.store._current(
                database,
                session_id=announcement.session_id,
                capability_id=announcement.capability_id,
            )
            if current is None:
                raise RuntimeError("provider auto-enrollment request disappeared")
            self.store._assert_expected_revision(current, expected_revision)
            if current.state is ProviderEnrollmentState.REVOKED:
                return current
            changes = self.store._announcement_changes(current, announcement)
            changes.update(
                {
                    "state": ProviderEnrollmentState.APPROVED,
                    "approved_by_node_id": actor_node_id,
                    "reason_code": "trusted-green-auto-enrollment",
                    "revision": current.revision + 1,
                    "updated_at": now,
                }
            )
            record = replace(current, **changes)
            self.store._write_record(database, record)
            self.store._record_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation="approve",
                fingerprint=fingerprint,
                record=record,
                now=now,
            )
            self.store._audit(
                database,
                operation="approve",
                outcome="accepted",
                reason_code="trusted-green-auto-enrollment",
                actor_node_id=actor_node_id,
                session_id=record.session_id,
                capability_id=record.capability_id,
                record=record,
                occurred_at=now,
            )
            return record

    def reconcile(
        self,
        announcement: CapabilityAnnouncement,
    ) -> ProviderEnrollmentRecord | None:
        """Auto-enroll one exact, current, trusted, GREEN contribution announcement."""

        evidence = parse_auto_enrollment_evidence(announcement)
        if evidence is None:
            return None
        current = self._current_authoritative(announcement)
        if current is None:
            return None
        leadership = self._leadership(current)
        if leadership is None:
            return None
        now = self._clock().astimezone(timezone.utc)
        requested = self.store.request(
            current,
            actor_node_id=current.node_id,
            command_id=_command_id("request", current, term=leadership.term),
            now=now,
        )
        if requested.state is ProviderEnrollmentState.REVOKED:
            return requested
        if requested.state is ProviderEnrollmentState.APPROVED:
            return requested

        # Fence the decision against a leadership change between request and approval.
        latest = self._leadership(current)
        if latest is None or latest.term != leadership.term:
            return requested
        command_id = _command_id("approve", current, term=latest.term)
        if current.status is CapabilityStatus.READY:
            return self.store._decide(
                "approve",
                current,
                actor_node_id=latest.leader_node_id,
                command_id=command_id,
                expected_revision=requested.revision,
                target_state=ProviderEnrollmentState.APPROVED,
                reason_code="trusted-green-auto-enrollment",
                now=now,
            )
        return self._approve_registering(
            current,
            actor_node_id=latest.leader_node_id,
            command_id=command_id,
            expected_revision=requested.revision,
            now=now,
        )


__all__ = [
    "AUTO_ENROLLMENT_PROPERTY",
    "AUTO_ENROLLMENT_SCHEMA",
    "AutoEnrollmentEvidence",
    "TrustedGreenProviderAutoEnrollment",
    "parse_auto_enrollment_evidence",
]
