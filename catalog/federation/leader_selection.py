"""Deterministic storage leadership and write-authority policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from .models import LeadershipGrant, StorageManifest, StorageNodeState, StorageRole, StorageWriteRequest


@dataclass(frozen=True)
class StorageGroupPolicy:
    lease_duration: timedelta = timedelta(seconds=30)
    controlled_handover: bool = False
    authoritative_fencing_token: int | None = None

    def __post_init__(self) -> None:
        if self.lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        if (
            self.authoritative_fencing_token is not None
            and (
                isinstance(self.authoritative_fencing_token, bool)
                or not isinstance(self.authoritative_fencing_token, int)
                or self.authoritative_fencing_token < 0
            )
        ):
            raise ValueError("authoritative_fencing_token must be a non-negative integer")


@dataclass(frozen=True)
class LeaderSelectionResult:
    decision: str
    selected_node_id: str | None
    selected_role: StorageRole | None
    eligible_node_ids: tuple[str, ...]
    rejected: dict[str, tuple[str, ...]]
    reason: str
    requires_new_grant: bool
    next_term: int | None = None
    next_fencing_token: int | None = None
    lease_expires_at: datetime | None = None


def _rejection_reasons(manifest: StorageManifest, node: StorageNodeState) -> list[str]:
    reasons = []
    if node.session_id != manifest.session_id:
        reasons.append("wrong-session")
    if node.group_id != manifest.group_id:
        reasons.append("wrong-storage-group")
    if not node.online:
        reasons.append("offline")
    if not node.integrity_verified:
        reasons.append("integrity-not-verified")
    if not node.schema_compatible:
        reasons.append("schema-incompatible")
    if not node.authorized:
        reasons.append("unauthorized")
    if not node.leader_eligible:
        reasons.append("eligibility-not-claimed")
    if node.authoritative_revision < manifest.revision:
        reasons.append("authoritatively-behind")
    elif node.authoritative_revision > manifest.revision:
        reasons.append("authoritatively-ahead")
    if node.term < manifest.term:
        reasons.append("stale-term")
    elif node.term > manifest.term:
        reasons.append("future-term")
    for dataset_id, required in manifest.datasets.items():
        if not required.required:
            continue
        actual = node.datasets.get(dataset_id)
        if actual is None:
            reasons.append("missing-required-dataset")
        elif (
            actual.schema_name != required.schema_name
            or actual.schema_version != required.schema_version
        ):
            reasons.append("required-schema-incompatible")
        elif actual.committed_revision < required.committed_revision:
            reasons.append("required-dataset-behind")
        elif actual.committed_revision > required.committed_revision:
            reasons.append("required-dataset-authoritatively-ahead")
        elif actual.manifest_hash != required.manifest_hash:
            reasons.append("required-dataset-hash-mismatch")
        elif actual.last_contiguous_sequence < required.last_contiguous_sequence:
            reasons.append("missing-required-dataset-range")
        elif actual.last_contiguous_sequence > required.last_contiguous_sequence:
            reasons.append("required-dataset-authoritatively-ahead")
        elif actual.missing_ranges:
            reasons.append("missing-required-dataset-range")
    return list(dict.fromkeys(reasons))


def _rank(node: StorageNodeState, manifest: StorageManifest) -> tuple[object, ...]:
    required_dataset_ids = tuple(
        sorted(dataset_id for dataset_id, dataset in manifest.datasets.items() if dataset.required)
    )
    # Eligibility normally guarantees every required dataset is present. The
    # sentinel keeps the rank shape deterministic even for independently used
    # or extended policies and can never reward an absent dataset.
    watermarks = tuple(
        node.datasets[dataset_id].last_contiguous_sequence
        if dataset_id in node.datasets
        else -1
        for dataset_id in required_dataset_ids
    )
    missing = sum(
        len(node.datasets[dataset_id].missing_ranges)
        if dataset_id in node.datasets
        else 1
        for dataset_id in required_dataset_ids
    )
    return (
        -node.authoritative_revision,
        tuple(-value for value in watermarks),
        missing,
        node.replication_lag,
        -node.operational_stability,
        -node.available_bytes,
        node.node_id,
    )


def select_storage_leader(
    manifest: StorageManifest,
    current_primary_id: str | None,
    current_grant: LeadershipGrant | None,
    candidates: list[StorageNodeState] | tuple[StorageNodeState, ...],
    now: datetime,
    policy: StorageGroupPolicy = StorageGroupPolicy(),
) -> LeaderSelectionResult:
    """Select a candidate without granting authority or reading external state."""
    rejected: dict[str, tuple[str, ...]] = {}
    eligible = []
    for candidate in candidates:
        reasons = _rejection_reasons(manifest, candidate)
        if reasons:
            rejected[candidate.node_id] = tuple(reasons)
        else:
            eligible.append(candidate)
    eligible.sort(key=lambda node: _rank(node, manifest))
    eligible_ids = tuple(node.node_id for node in eligible)
    current = next((node for node in eligible if node.node_id == current_primary_id), None)
    valid_grant = bool(
        current
        and current_grant
        and current_grant.node_id == current.node_id
        and current_grant.session_id == manifest.session_id
        and current_grant.group_id == manifest.group_id
        and current_grant.role is StorageRole.PRIMARY
        and current_grant.term == manifest.term
        and current.term == current_grant.term
        and current.last_fencing_token == current_grant.fencing_token
        and (
            policy.authoritative_fencing_token is None
            or current_grant.fencing_token == policy.authoritative_fencing_token
        )
        and current_grant.issued_at <= now
        and now < current_grant.lease_expires_at
    )
    if valid_grant and not policy.controlled_handover:
        return LeaderSelectionResult(
            "retain-current", current.node_id, StorageRole.PRIMARY, eligible_ids, rejected,
            "current-primary-is-healthy-complete-and-not-authoritatively-behind", False,
        )
    if policy.controlled_handover and current is not None:
        replacements = [candidate for candidate in eligible if candidate.node_id != current.node_id]
        if not replacements:
            return LeaderSelectionResult(
                "no-qualified-candidate",
                None,
                None,
                eligible_ids,
                rejected,
                "controlled-handover-has-no-qualified-replacement",
                False,
            )
        eligible = replacements
    if eligible:
        selected = eligible[0]
        scoped_grant = bool(
            current_grant
            and current_grant.session_id == manifest.session_id
            and current_grant.group_id == manifest.group_id
        )
        if policy.authoritative_fencing_token is None and not scoped_grant:
            return LeaderSelectionResult(
                "authoritative-state-required",
                None,
                None,
                eligible_ids,
                rejected,
                "new-grant-requires-authoritative-fencing-state",
                False,
            )
        previous_term = max(manifest.term, current_grant.term if scoped_grant else 0)
        previous_token = max(
            policy.authoritative_fencing_token or 0,
            current_grant.fencing_token if scoped_grant else 0,
        )
        return LeaderSelectionResult(
            "select-candidate", selected.node_id, StorageRole.PRIMARY, eligible_ids, rejected,
            "highest-ranked-qualified-candidate", True, previous_term + 1,
            previous_token + 1, now + policy.lease_duration,
        )
    repair_possible = any(
        node.session_id == manifest.session_id
        and node.group_id == manifest.group_id
        and node.online
        and node.authorized
        and node.schema_compatible
        for node in candidates
    )
    decision = "synchronization-required" if repair_possible else "storage-degraded"
    reason = "candidate-repair-required" if repair_possible else "required-committed-data-unavailable"
    return LeaderSelectionResult(decision, None, None, (), rejected, reason, False)


def validate_primary_write(
    request: StorageWriteRequest,
    grant: LeadershipGrant,
    active_grant: LeadershipGrant | None,
    now: datetime,
    *,
    authorized: bool = True,
    schema_valid: bool = True,
) -> tuple[str, ...]:
    """Return stable rejection reasons, or an empty tuple when a write is valid."""
    reasons = []
    if request.session_id != grant.session_id:
        reasons.append("wrong-session")
    if request.group_id != grant.group_id:
        reasons.append("wrong-storage-group")
    if request.actor_node_id != grant.node_id:
        reasons.append("wrong-node")
    if grant.role is not StorageRole.PRIMARY:
        reasons.append("not-primary")
    if request.term != grant.term:
        reasons.append("stale-term")
    if request.fencing_token != grant.fencing_token:
        reasons.append("stale-fencing-token")
    if now >= grant.lease_expires_at:
        reasons.append("lease-expired")
    if grant.issued_at > now:
        reasons.append("grant-not-yet-valid")
    if active_grant is None or active_grant.grant_id != request.grant_id or active_grant != grant:
        reasons.append("unknown-grant")
    if not authorized:
        reasons.append("unauthorized")
    if not schema_valid:
        reasons.append("invalid-schema")
    return tuple(reasons)
