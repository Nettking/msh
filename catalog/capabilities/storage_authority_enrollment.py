"""Automatic storage-authority enrollment for trusted GREEN storage capabilities.

Storage keeps its own control-plane authority and is deliberately excluded from
generic F8.1 provider enrollment (see :mod:`provider_auto_enrollment`).  Before
this policy existed the only automatic storage assignment was the Federation
creator bootstrapping its own first local provider; every other trusted device
stayed ``PENDING`` until a human made an explicit coordinator assignment.

This policy closes that gap without weakening the trust boundary.  A storage
provider is registered, assigned, and granted leadership only when all of the
following hold for the *current authoritative* announcement:

* the announcing node is still a member of the Federation session (trust),
* the announcement is byte-identical to the one the coordinator holds (currency),
* it carries the versioned storage attestation built from current local
  inspection and GREEN benchmark evidence (technical readiness),
* it speaks the supported storage protocol major version (role compatibility),
* a connected session leader exists to author the control-plane events.

Reachability, LAN presence, or Tailscale membership grant nothing here; only
Federation membership does.  When any condition stops holding, the same entry
point deactivates the provider and, where another eligible provider exists,
deterministically fails authority over to it.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_control_plane import (
    StorageControlPlaneSnapshot,
    StorageProviderRegistration,
)
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_MAJOR,
    STORAGE_PROTOCOL_VERSION,
)

from .provider_enrollment import _announcement_fingerprint

STORAGE_AUTHORITY_SCHEMA = "fcp.storage-authority-enrollment.v1"
STORAGE_AUTHORITY_PROPERTY = "storage_authority"
STORAGE_CAPABILITY_TYPE = "storage"
DEFAULT_STORAGE_GROUP_ID = "fcp-local-storage"
MAX_AUTO_BENCHMARK_RUNS = 64
MAX_AUTO_TEXT_BYTES = 512
DEFAULT_LEASE_SECONDS = 300

REGISTERED_STATUS = "ready"
DEACTIVATED_STATUS = "unavailable"
STORAGE_ROLES = ("primary", "replica")


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


def _attestation(announcement: CapabilityAnnouncement) -> dict[str, object] | None:
    if announcement.type.strip().casefold() != STORAGE_CAPABILITY_TYPE:
        return None
    properties = announcement.properties
    if not isinstance(properties, dict):
        return None
    raw = properties.get(STORAGE_AUTHORITY_PROPERTY)
    if not isinstance(raw, dict):
        return None
    if raw.get("schema") != STORAGE_AUTHORITY_SCHEMA:
        return None
    return raw


@dataclass(frozen=True)
class StorageAuthorityTarget:
    """The control-plane coordinates an announcement refers to.

    Extracted leniently so that an announcement which has *stopped* being
    eligible can still be deactivated at the right provider and group.
    """

    provider_id: str
    group_id: str


@dataclass(frozen=True)
class StorageAuthorityEvidence:
    """A strict attestation of current, trusted, GREEN storage readiness."""

    provider_id: str
    group_id: str
    role: str
    inspection_revision: int
    decision_revision: int
    benchmark_run_ids: tuple[str, ...]


@dataclass(frozen=True)
class StorageAuthorityDecision:
    """What one reconciliation did, and why."""

    outcome: str
    reason_code: str
    provider_id: str | None = None
    group_id: str | None = None
    role: str | None = None
    changed: bool = False

    @property
    def active(self) -> bool:
        return self.outcome == "active"


def parse_storage_authority_target(
    announcement: CapabilityAnnouncement,
) -> StorageAuthorityTarget | None:
    """Return the provider/group an announcement addresses, eligibility aside."""

    raw = _attestation(announcement)
    if raw is None:
        return None
    provider_id = _bounded_text(raw.get("provider_id"))
    group_id = _bounded_text(raw.get("group_id"))
    if provider_id is None or group_id is None:
        return None
    return StorageAuthorityTarget(provider_id=provider_id, group_id=group_id)


def parse_storage_authority_evidence(
    announcement: CapabilityAnnouncement,
) -> StorageAuthorityEvidence | None:
    """Return a strict attestation, or ``None`` for anything not auto-eligible.

    Storage is stricter than generic provider enrollment: a storage provider must
    already be READY, because assignment immediately makes it eligible to hold
    committed data.  REGISTERING is never enough.
    """

    raw = _attestation(announcement)
    if raw is None:
        return None
    if announcement.status is not CapabilityStatus.READY:
        return None
    if announcement.protocol != STORAGE_PROTOCOL:
        return None
    major = announcement.protocol_version.split(".", 1)[0]
    if major != str(STORAGE_PROTOCOL_MAJOR):
        return None
    if raw.get("eligible") is not True or raw.get("benchmark_state") != "green":
        return None
    if raw.get("desired_state") != "enabled" or raw.get("policy_state") != "allowed":
        return None
    target = parse_storage_authority_target(announcement)
    if target is None:
        return None
    role = _bounded_text(raw.get("role"))
    if role not in STORAGE_ROLES:
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
    return StorageAuthorityEvidence(
        provider_id=target.provider_id,
        group_id=target.group_id,
        role=role,
        inspection_revision=inspection_revision,
        decision_revision=decision_revision,
        benchmark_run_ids=tuple(run_ids),
    )


def federation_storage_provider_id(node_id: str, local_provider_id: str) -> str:
    """Scope a device-local storage provider id to one Federation member.

    Every device detects the same local storage candidate id, so publishing that
    id directly would make two members collide on a single control-plane
    provider.  The control plane needs one identity per contributing device.
    """

    digest = hashlib.sha256(f"{node_id}\0{local_provider_id}".encode()).hexdigest()
    return f"storage-provider-{digest[:32]}"


def _grant_id(*, session_id: str, group_id: str, provider_id: str, term: int) -> str:
    semantic = {
        "session_id": session_id,
        "group_id": group_id,
        "provider_id": provider_id,
        "term": term,
    }
    encoded = json.dumps(
        semantic,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "storage-auto-grant-" + hashlib.sha256(encoded).hexdigest()


class TrustedGreenStorageAuthority:
    """Reconcile one storage announcement into the storage control plane."""

    def __init__(
        self,
        coordinator: SessionCoordinator,
        control_plane: PhaseDControlPlane,
        *,
        clock: Callable[[], datetime] = _utc_now,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
    ) -> None:
        self.coordinator = coordinator
        self.control_plane = control_plane
        self._clock = clock
        self._lease_seconds = lease_seconds

    # ---- trust, currency, and leadership --------------------------------

    def _is_member(self, announcement: CapabilityAnnouncement) -> bool:
        try:
            self.coordinator.store.require_membership(
                session_id=announcement.session_id,
                node_id=announcement.node_id,
            )
        except (FederationOperationError, FederationValidationError):
            return False
        return True

    def _current_authoritative(
        self,
        announcement: CapabilityAnnouncement,
    ) -> CapabilityAnnouncement | None:
        """Return the coordinator's announcement only when it is exactly this one.

        A late or replayed announcement for a superseded revision fails this
        check, so it can never reactivate state the newer revision has left.
        """

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

    def _leadership(self, session_id: str):
        leadership = self.coordinator.session_leadership(session_id)
        if not leadership.leader_connected:
            return None
        self.coordinator.require_session_leader(
            session_id=session_id,
            actor_node_id=leadership.leader_node_id,
        )
        return leadership

    # ---- eligibility of already-registered providers ---------------------

    @staticmethod
    def _assignable(
        snapshot: StorageControlPlaneSnapshot,
        provider_id: str | None,
    ) -> bool:
        if provider_id is None:
            return False
        provider = snapshot.providers.get(provider_id)
        return provider is not None and provider.assignable

    # ---- reconciliation --------------------------------------------------

    def reconcile(
        self,
        announcement: CapabilityAnnouncement,
    ) -> StorageAuthorityDecision:
        """Drive one announcement to its correct storage-authority state."""

        target = parse_storage_authority_target(announcement)
        if target is None:
            return StorageAuthorityDecision(
                outcome="ignored",
                reason_code="not-a-storage-authority-candidate",
            )

        leadership = self._leadership(announcement.session_id)
        if leadership is None:
            return StorageAuthorityDecision(
                outcome="ignored",
                reason_code="no-connected-session-leader",
                provider_id=target.provider_id,
                group_id=target.group_id,
            )

        # Trust boundary first: a revoked member must lose authority even though
        # its last announcement is no longer readable as authoritative.
        if not self._is_member(announcement):
            return self._deactivate(
                session_id=announcement.session_id,
                target=target,
                actor_node_id=leadership.leader_node_id,
                reason_code="federation-membership-revoked",
            )

        current = self._current_authoritative(announcement)
        if current is None:
            return StorageAuthorityDecision(
                outcome="ignored",
                reason_code="superseded-announcement",
                provider_id=target.provider_id,
                group_id=target.group_id,
            )

        evidence = parse_storage_authority_evidence(current)
        if evidence is None or evidence.provider_id != target.provider_id:
            return self._deactivate(
                session_id=current.session_id,
                target=target,
                actor_node_id=leadership.leader_node_id,
                reason_code="storage-evidence-not-current-green",
            )

        # Fence the decision against a leadership change during the reads above.
        latest = self._leadership(current.session_id)
        if latest is None or latest.term != leadership.term:
            return StorageAuthorityDecision(
                outcome="ignored",
                reason_code="session-leadership-changed",
                provider_id=target.provider_id,
                group_id=target.group_id,
            )

        return self._activate(
            current,
            evidence,
            actor_node_id=latest.leader_node_id,
        )

    def _activate(
        self,
        announcement: CapabilityAnnouncement,
        evidence: StorageAuthorityEvidence,
        *,
        actor_node_id: str,
    ) -> StorageAuthorityDecision:
        session_id = announcement.session_id
        group_id = evidence.group_id
        provider_id = evidence.provider_id
        changed = False

        snapshot = self.control_plane.snapshot(session_id)
        if group_id not in snapshot.groups:
            self.control_plane.create_group(session_id, actor_node_id, group_id)
            snapshot = self.control_plane.snapshot(session_id)
            changed = True

        desired = StorageProviderRegistration(
            session_id=session_id,
            provider_id=provider_id,
            node_id=announcement.node_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status=REGISTERED_STATUS,
        )
        existing = snapshot.providers.get(provider_id)
        if existing is not None and existing.node_id != announcement.node_id:
            # Another device already owns this provider id; never silently take it.
            return StorageAuthorityDecision(
                outcome="ignored",
                reason_code="storage-provider-owned-by-another-device",
                provider_id=provider_id,
                group_id=group_id,
            )
        if existing != desired:
            self.control_plane.register_provider(session_id, actor_node_id, desired)
            snapshot = self.control_plane.snapshot(session_id)
            changed = True

        assignment = snapshot.groups[group_id]
        primary = assignment.primary_provider_id
        replicas = tuple(
            item for item in assignment.replica_provider_ids if item != provider_id
        )

        if primary == provider_id:
            desired_primary = provider_id
            desired_replicas = replicas
        elif self._assignable(snapshot, primary):
            # A healthy primary is never displaced; join as a replica instead.
            desired_primary = primary
            desired_replicas = tuple(sorted({*replicas, provider_id}))
        else:
            # No usable primary.  Promote the lowest-sorted eligible candidate so
            # every node's reconciliation reaches the same conclusion.
            candidates = sorted(
                {provider_id}
                | {item for item in replicas if self._assignable(snapshot, item)}
            )
            desired_primary = candidates[0]
            desired_replicas = tuple(
                item for item in sorted({*replicas, provider_id}) if item != desired_primary
            )

        if (
            desired_primary != assignment.primary_provider_id
            or desired_replicas != assignment.replica_provider_ids
        ):
            self.control_plane.change_assignment(
                session_id,
                actor_node_id,
                group_id,
                desired_primary,
                desired_replicas,
            )
            snapshot = self.control_plane.snapshot(session_id)
            changed = True

        role = "primary" if desired_primary == provider_id else "replica"
        if role == "primary":
            changed = (
                self._grant_leadership(
                    session_id=session_id,
                    group_id=group_id,
                    provider_id=provider_id,
                    actor_node_id=actor_node_id,
                    snapshot=snapshot,
                )
                or changed
            )

        return StorageAuthorityDecision(
            outcome="active",
            reason_code="trusted-green-storage-authority",
            provider_id=provider_id,
            group_id=group_id,
            role=role,
            changed=changed,
        )

    def _grant_leadership(
        self,
        *,
        session_id: str,
        group_id: str,
        provider_id: str,
        actor_node_id: str,
        snapshot: StorageControlPlaneSnapshot,
    ) -> bool:
        active = snapshot.leader_grants.get(group_id)
        if active is not None and active.get("provider_id") == provider_id:
            return False
        term, fencing_token = self.control_plane.next_leader_fencing(
            session_id,
            group_id,
        )
        issued_at = self._clock().astimezone(timezone.utc)
        self.control_plane.grant_leader(
            session_id,
            actor_node_id,
            group_id,
            provider_id,
            _grant_id(
                session_id=session_id,
                group_id=group_id,
                provider_id=provider_id,
                term=term,
            ),
            term,
            fencing_token,
            lease_expires_at=issued_at + timedelta(seconds=self._lease_seconds),
            occurred_at=issued_at,
        )
        return True

    def _deactivate(
        self,
        *,
        session_id: str,
        target: StorageAuthorityTarget,
        actor_node_id: str,
        reason_code: str,
    ) -> StorageAuthorityDecision:
        provider_id = target.provider_id
        group_id = target.group_id
        changed = False

        snapshot = self.control_plane.snapshot(session_id)
        assignment = snapshot.groups.get(group_id)
        if assignment is not None and (
            assignment.primary_provider_id == provider_id
            or provider_id in assignment.replica_provider_ids
        ):
            replicas = tuple(
                item for item in assignment.replica_provider_ids if item != provider_id
            )
            if assignment.primary_provider_id == provider_id:
                # Fail over to the lowest-sorted still-eligible replica, if any.
                candidates = sorted(
                    item for item in replicas if self._assignable(snapshot, item)
                )
                desired_primary = candidates[0] if candidates else None
                replicas = tuple(
                    item for item in replicas if item != desired_primary
                )
            else:
                desired_primary = assignment.primary_provider_id
            self.control_plane.change_assignment(
                session_id,
                actor_node_id,
                group_id,
                desired_primary,
                replicas,
            )
            snapshot = self.control_plane.snapshot(session_id)
            changed = True

        existing = snapshot.providers.get(provider_id)
        if existing is not None and existing.assignable:
            # Keep the registration for audit, but make it unassignable so no
            # later assignment can pick it up without fresh GREEN evidence.
            self.control_plane.register_provider(
                session_id,
                actor_node_id,
                StorageProviderRegistration(
                    session_id=session_id,
                    provider_id=provider_id,
                    node_id=existing.node_id,
                    protocol=existing.protocol,
                    protocol_version=existing.protocol_version,
                    authorized=False,
                    status=DEACTIVATED_STATUS,
                ),
            )
            changed = True

        return StorageAuthorityDecision(
            outcome="inactive",
            reason_code=reason_code,
            provider_id=provider_id,
            group_id=group_id,
            changed=changed,
        )


__all__ = [
    "DEFAULT_STORAGE_GROUP_ID",
    "STORAGE_AUTHORITY_PROPERTY",
    "STORAGE_AUTHORITY_SCHEMA",
    "StorageAuthorityDecision",
    "StorageAuthorityEvidence",
    "StorageAuthorityTarget",
    "TrustedGreenStorageAuthority",
    "federation_storage_provider_id",
    "parse_storage_authority_evidence",
    "parse_storage_authority_target",
]
