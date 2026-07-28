"""Operational controlled handover backed by durable Phase D state."""

from __future__ import annotations

from datetime import datetime

from .commit_tracking import DurableAcknowledgementStore
from .errors import FederationValidationError
from .handover import ControlledHandover, HandoverState
from .outbox import OutboxState, SQLiteOutbox
from .phase_d_control import HandoverCommit, PhaseDControlPlane
from .replication import REPLICATION_SCHEMA


class PhaseDHandoverCoordinator:
    """Bind the D8 state machine to durable replication and control-plane commits."""

    def __init__(
        self,
        *,
        control_plane: PhaseDControlPlane,
        outbox: SQLiteOutbox,
        acknowledgements: DurableAcknowledgementStore,
    ) -> None:
        self.control_plane = control_plane
        self.outbox = outbox
        self.acknowledgements = acknowledgements

    def plan(
        self,
        *,
        session_id: str,
        group_id: str,
        target_provider_id: str,
        target_term: int,
        target_fencing_token: int,
    ) -> ControlledHandover:
        snapshot = self.control_plane.snapshot(session_id)
        pending_ids = {
            entry.outbox_id
            for entry in (*self.outbox.prepared(), *self.outbox.pending())
            if entry.schema_id == REPLICATION_SCHEMA
            and entry.destination_id == target_provider_id
            and isinstance(entry.payload, dict)
            and entry.payload.get("group_id") == group_id
        }
        return ControlledHandover.plan(
            snapshot,
            group_id=group_id,
            target_provider_id=target_provider_id,
            target_term=target_term,
            target_fencing_token=target_fencing_token,
            pending_outbox_ids=pending_ids,
        )

    def refresh(self, handover: ControlledHandover) -> ControlledHandover:
        if handover.state is HandoverState.PLANNED:
            handover.begin_draining()
        if handover.state is not HandoverState.DRAINING:
            return handover
        for outbox_id in tuple(handover.pending_outbox_ids):
            entry = self.outbox.get(outbox_id)
            if entry is not None and entry.state is OutboxState.COMPLETED:
                handover.acknowledge_outbox(outbox_id)
        pending_batches = tuple(
            key
            for key in self.acknowledgements.pending_for_provider(handover.target_provider_id)
            if key[0] == handover.session_id and key[1] == handover.group_id
        )
        if not pending_batches and not handover.pending_outbox_ids:
            handover.mark_target_caught_up()
        return handover

    def commit(
        self,
        handover: ControlledHandover,
        *,
        actor_node_id: str,
        grant_id: str,
        lease_expires_at: datetime,
        occurred_at: datetime | None = None,
    ) -> HandoverCommit:
        self.refresh(handover)
        if handover.state is not HandoverState.READY:
            raise FederationValidationError(
                "handover-not-ready",
                "state",
                "target still has outstanding replication obligations",
            )
        result = self.control_plane.complete_handover(
            session_id=handover.session_id,
            actor_node_id=actor_node_id,
            group_id=handover.group_id,
            target_provider_id=handover.target_provider_id,
            grant_id=grant_id,
            term=handover.target_term,
            fencing_token=handover.target_fencing_token,
            lease_expires_at=lease_expires_at,
            occurred_at=occurred_at,
        )
        handover.commit()
        return result
