from catalog.federation.acknowledgement import (
    AcknowledgementMode,
    AcknowledgementPolicy,
    AcknowledgementProgress,
)
from catalog.federation.handover import ControlledHandover, HandoverState
from catalog.federation.storage_control_plane import (
    StorageControlPlaneSnapshot,
    StorageGroupAssignment,
    StorageProviderRegistration,
)


def _snapshot() -> StorageControlPlaneSnapshot:
    snapshot = StorageControlPlaneSnapshot("session-a")
    snapshot.providers = {
        "primary": StorageProviderRegistration(
            "session-a", "primary", "node-p", "fcp-storage-v1", "1.0", True, "ready"
        ),
        "replica-a": StorageProviderRegistration(
            "session-a", "replica-a", "node-a", "fcp-storage-v1", "1.0", True, "ready"
        ),
        "replica-b": StorageProviderRegistration(
            "session-a", "replica-b", "node-b", "fcp-storage-v1", "1.0", True, "ready"
        ),
    }
    snapshot.groups["reports"] = StorageGroupAssignment(
        "reports", "primary", ("replica-a", "replica-b")
    )
    snapshot.leader_grants["reports"] = {
        "term": 4,
        "fencing_token": 12,
    }
    return snapshot


def test_acknowledgement_policy_thresholds():
    assert AcknowledgementPolicy(AcknowledgementMode.PRIMARY).required_replica_acks(2) == 0
    assert AcknowledgementPolicy(AcknowledgementMode.ONE_REPLICA).required_replica_acks(2) == 1
    assert AcknowledgementPolicy(AcknowledgementMode.QUORUM).required_replica_acks(2) == 1
    assert AcknowledgementPolicy(AcknowledgementMode.ALL).required_replica_acks(2) == 2


def test_acknowledgement_progress_is_idempotent():
    progress = AcknowledgementProgress(
        AcknowledgementPolicy(AcknowledgementMode.ALL), replica_count=2
    )
    progress = progress.acknowledge("replica-a").acknowledge("replica-a")
    assert not progress.complete
    assert progress.acknowledge("replica-b").complete


def test_handover_requires_drain_and_catch_up_before_commit():
    handover = ControlledHandover.plan(
        _snapshot(),
        group_id="reports",
        target_provider_id="replica-a",
        target_term=5,
        target_fencing_token=13,
        pending_outbox_ids={10, 11},
    )
    handover.begin_draining()
    handover.mark_target_caught_up()
    assert handover.state is HandoverState.DRAINING
    handover.acknowledge_outbox(10)
    handover.acknowledge_outbox(11)
    assert handover.state is HandoverState.READY
    assert handover.commit() == ("primary", "replica-a", 5, 13)
    assert handover.state is HandoverState.COMMITTED


def test_handover_can_abort_before_commit():
    handover = ControlledHandover.plan(
        _snapshot(),
        group_id="reports",
        target_provider_id="replica-b",
        target_term=5,
        target_fencing_token=13,
    )
    handover.abort()
    assert handover.state is HandoverState.ABORTED
