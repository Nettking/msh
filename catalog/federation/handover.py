"""Controlled, fenced storage leadership handover."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .errors import FederationValidationError
from .storage_control_plane import StorageControlPlaneSnapshot


class HandoverState(str, Enum):
    PLANNED = "planned"
    DRAINING = "draining"
    READY = "ready"
    COMMITTED = "committed"
    ABORTED = "aborted"


@dataclass
class ControlledHandover:
    session_id: str
    group_id: str
    source_provider_id: str
    target_provider_id: str
    source_term: int
    target_term: int
    target_fencing_token: int
    state: HandoverState = HandoverState.PLANNED
    pending_outbox_ids: set[int] = field(default_factory=set)
    target_caught_up: bool = False

    @classmethod
    def plan(
        cls,
        snapshot: StorageControlPlaneSnapshot,
        *,
        group_id: str,
        target_provider_id: str,
        target_term: int,
        target_fencing_token: int,
        pending_outbox_ids: set[int] | None = None,
    ) -> "ControlledHandover":
        assignment = snapshot.groups.get(group_id)
        if assignment is None or assignment.primary_provider_id is None:
            raise FederationValidationError(
                "handover-no-primary", "group_id", "storage group has no assigned primary"
            )
        if target_provider_id not in assignment.replica_provider_ids:
            raise FederationValidationError(
                "handover-target-not-replica",
                "target_provider_id",
                "target must be an assigned replica",
            )
        target = snapshot.providers.get(target_provider_id)
        if target is None or not target.assignable:
            raise FederationValidationError(
                "handover-target-unavailable",
                "target_provider_id",
                "target provider is not assignable",
            )
        active = snapshot.leader_grants.get(group_id)
        source_term = int(active["term"]) if active is not None else 0
        source_fence = int(active["fencing_token"]) if active is not None else 0
        if target_term <= source_term:
            raise FederationValidationError("stale-term", "target_term", "must exceed source term")
        if target_fencing_token <= source_fence:
            raise FederationValidationError(
                "stale-fencing-token",
                "target_fencing_token",
                "must exceed the active fencing token",
            )
        return cls(
            session_id=snapshot.session_id,
            group_id=group_id,
            source_provider_id=assignment.primary_provider_id,
            target_provider_id=target_provider_id,
            source_term=source_term,
            target_term=target_term,
            target_fencing_token=target_fencing_token,
            pending_outbox_ids=set(pending_outbox_ids or set()),
        )

    def begin_draining(self) -> None:
        self._require(HandoverState.PLANNED)
        self.state = HandoverState.DRAINING

    def acknowledge_outbox(self, outbox_id: int) -> None:
        self._require(HandoverState.DRAINING)
        self.pending_outbox_ids.discard(outbox_id)
        self._refresh_ready()

    def mark_target_caught_up(self) -> None:
        self._require(HandoverState.DRAINING)
        self.target_caught_up = True
        self._refresh_ready()

    def commit(self) -> tuple[str, str, int, int]:
        self._require(HandoverState.READY)
        self.state = HandoverState.COMMITTED
        return (
            self.source_provider_id,
            self.target_provider_id,
            self.target_term,
            self.target_fencing_token,
        )

    def abort(self) -> None:
        if self.state in (HandoverState.COMMITTED, HandoverState.ABORTED):
            raise FederationValidationError(
                "invalid-handover-transition", "state", f"cannot abort from {self.state.value}"
            )
        self.state = HandoverState.ABORTED

    def _refresh_ready(self) -> None:
        if self.target_caught_up and not self.pending_outbox_ids:
            self.state = HandoverState.READY

    def _require(self, expected: HandoverState) -> None:
        if self.state is not expected:
            raise FederationValidationError(
                "invalid-handover-transition",
                "state",
                f"expected {expected.value}, got {self.state.value}",
            )
