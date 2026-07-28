"""Configurable storage acknowledgement policies."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .errors import FederationValidationError


class AcknowledgementMode(str, Enum):
    PRIMARY = "primary"
    ONE_REPLICA = "one-replica"
    QUORUM = "quorum"
    ALL = "all"


@dataclass(frozen=True)
class AcknowledgementPolicy:
    mode: AcknowledgementMode = AcknowledgementMode.PRIMARY

    def required_replica_acks(self, replica_count: int) -> int:
        if isinstance(replica_count, bool) or not isinstance(replica_count, int) or replica_count < 0:
            raise FederationValidationError(
                "invalid-replica-count", "replica_count", "must be a non-negative integer"
            )
        if self.mode is AcknowledgementMode.PRIMARY:
            return 0
        if self.mode is AcknowledgementMode.ONE_REPLICA:
            if replica_count < 1:
                raise FederationValidationError(
                    "ack-policy-unavailable", "replica_count", "one replica acknowledgement is required"
                )
            return 1
        if self.mode is AcknowledgementMode.ALL:
            return replica_count
        total_voters = replica_count + 1
        return max(0, total_voters // 2 + 1 - 1)

    def satisfied(self, *, replica_count: int, acknowledged_replicas: int) -> bool:
        if (
            isinstance(acknowledged_replicas, bool)
            or not isinstance(acknowledged_replicas, int)
            or acknowledged_replicas < 0
            or acknowledged_replicas > replica_count
        ):
            raise FederationValidationError(
                "invalid-ack-count", "acknowledged_replicas", "must be between zero and replica_count"
            )
        return acknowledged_replicas >= self.required_replica_acks(replica_count)


@dataclass(frozen=True)
class AcknowledgementProgress:
    policy: AcknowledgementPolicy
    replica_count: int
    acknowledged_provider_ids: frozenset[str] = frozenset()

    def acknowledge(self, provider_id: str) -> "AcknowledgementProgress":
        if not isinstance(provider_id, str) or not provider_id:
            raise FederationValidationError("invalid-id", "provider_id", "must be non-empty text")
        if len(self.acknowledged_provider_ids) >= self.replica_count and provider_id not in self.acknowledged_provider_ids:
            raise FederationValidationError(
                "unexpected-replica-ack", "provider_id", "more replicas acknowledged than assigned"
            )
        return AcknowledgementProgress(
            self.policy,
            self.replica_count,
            self.acknowledged_provider_ids | {provider_id},
        )

    @property
    def complete(self) -> bool:
        return self.policy.satisfied(
            replica_count=self.replica_count,
            acknowledged_replicas=len(self.acknowledged_provider_ids),
        )
