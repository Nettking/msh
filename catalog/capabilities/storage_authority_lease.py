"""Ongoing lease maintenance for trusted GREEN storage authority.

Storage enrollment proves that a provider is a current, trusted, GREEN candidate
and creates the initial coordinator-issued write lease. A lease is deliberately
short-lived, so a healthy primary also needs a bounded renewal path. Renewal
must not bypass enrollment: every pass first re-runs
:class:`TrustedGreenStorageAuthority` against the coordinator's current
capability announcement, then rotates the same provider's grant only when the
existing lease is close to expiry.

This keeps the relay-side coordinator as the authority for terms, fencing tokens
and grants. The storage runtime never self-mints authority merely because it is
reachable.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.phase_d_control import PhaseDControlPlane

from .storage_authority_enrollment import (
    DEFAULT_LEASE_SECONDS,
    StorageAuthorityDecision,
    TrustedGreenStorageAuthority,
    parse_storage_authority_target,
)

DEFAULT_RENEW_BEFORE_SECONDS = 60


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _renewal_grant_id(
    *,
    session_id: str,
    group_id: str,
    provider_id: str,
    term: int,
) -> str:
    value = json.dumps(
        {
            "kind": "storage-auto-renewal",
            "session_id": session_id,
            "group_id": group_id,
            "provider_id": provider_id,
            "term": term,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "storage-auto-renew-" + hashlib.sha256(value).hexdigest()


class RenewingTrustedGreenStorageAuthority:
    """Compose strict storage enrollment with bounded same-primary renewal."""

    def __init__(
        self,
        coordinator: SessionCoordinator,
        control_plane: PhaseDControlPlane,
        *,
        clock: Callable[[], datetime] = _utc_now,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        renew_before_seconds: int = DEFAULT_RENEW_BEFORE_SECONDS,
    ) -> None:
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be a positive integer")
        if (
            isinstance(renew_before_seconds, bool)
            or not isinstance(renew_before_seconds, int)
            or not 0 < renew_before_seconds < lease_seconds
        ):
            raise ValueError(
                "renew_before_seconds must be positive and smaller than lease_seconds"
            )
        self.coordinator = coordinator
        self.control_plane = control_plane
        self._clock = clock
        self._lease_seconds = lease_seconds
        self._renew_before_seconds = renew_before_seconds
        self.enrollment = TrustedGreenStorageAuthority(
            coordinator,
            control_plane,
            clock=clock,
            lease_seconds=lease_seconds,
        )

    def reconcile(
        self,
        announcement,
    ) -> StorageAuthorityDecision:
        """Revalidate GREEN evidence, then renew its primary lease if due."""

        decision = self.enrollment.reconcile(announcement)
        if (
            not decision.active
            or decision.role != "primary"
            or decision.provider_id is None
            or decision.group_id is None
        ):
            return decision

        renewed = self._renew_if_due(
            session_id=announcement.session_id,
            group_id=decision.group_id,
            provider_id=decision.provider_id,
        )
        if renewed and not decision.changed:
            return replace(decision, changed=True)
        return decision

    def reconcile_current_node(
        self,
        *,
        session_id: str,
        node_id: str,
    ) -> tuple[StorageAuthorityDecision, ...]:
        """Revalidate current storage candidates for one authenticated member.

        The product relay calls this after any accepted capability announcement.
        The creator's supervised logical-storage authority announces on its normal
        scan cadence, providing a bounded renewal trigger without introducing a
        second grant authority or trusting transport liveness as storage evidence.
        """

        current = tuple(
            announcement
            for announcement in self.coordinator.store.list_capabilities(
                session_id=session_id,
            )
            if announcement.node_id == node_id
            and parse_storage_authority_target(announcement) is not None
        )
        return tuple(self.reconcile(announcement) for announcement in current)

    def _renew_if_due(
        self,
        *,
        session_id: str,
        group_id: str,
        provider_id: str,
    ) -> bool:
        now = self._clock().astimezone(timezone.utc)
        snapshot = self.control_plane.snapshot(session_id)
        assignment = snapshot.groups.get(group_id)
        grant = snapshot.leader_grants.get(group_id)
        if (
            assignment is None
            or assignment.primary_provider_id != provider_id
            or grant is None
            or grant.get("provider_id") != provider_id
        ):
            return False

        expiry = _utc(grant.get("lease_expires_at"))
        renew_at = now + timedelta(seconds=self._renew_before_seconds)
        if expiry is not None and expiry > renew_at:
            return False

        # Enrollment above has already revalidated exact current GREEN evidence.
        # Re-check connected session leadership immediately before mutation so a
        # leadership change cannot be bridged by the renewal step.
        leadership = self.coordinator.session_leadership(session_id)
        if not leadership.leader_connected:
            return False
        self.coordinator.require_session_leader(
            session_id=session_id,
            actor_node_id=leadership.leader_node_id,
        )

        latest = self.control_plane.snapshot(session_id)
        latest_assignment = latest.groups.get(group_id)
        latest_grant = latest.leader_grants.get(group_id)
        if (
            latest_assignment is None
            or latest_assignment.primary_provider_id != provider_id
            or latest_grant is None
            or latest_grant.get("provider_id") != provider_id
        ):
            return False

        latest_expiry = _utc(latest_grant.get("lease_expires_at"))
        if latest_expiry is not None and latest_expiry > renew_at:
            return False

        term, fencing_token = self.control_plane.next_leader_fencing(
            session_id,
            group_id,
        )
        self.control_plane.grant_leader(
            session_id,
            leadership.leader_node_id,
            group_id,
            provider_id,
            _renewal_grant_id(
                session_id=session_id,
                group_id=group_id,
                provider_id=provider_id,
                term=term,
            ),
            term,
            fencing_token,
            lease_expires_at=now + timedelta(seconds=self._lease_seconds),
            occurred_at=now,
        )
        return True


__all__ = [
    "DEFAULT_RENEW_BEFORE_SECONDS",
    "RenewingTrustedGreenStorageAuthority",
]
