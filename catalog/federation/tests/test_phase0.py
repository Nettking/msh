from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from catalog.federation.errors import FederationValidationError, ProtocolCompatibilityError
from catalog.federation.leader_selection import (
    StorageGroupPolicy,
    select_storage_leader,
    validate_primary_write,
)
from catalog.federation.models import (
    CapabilityAnnouncement,
    DatasetCoverage,
    LeadershipGrant,
    NodeIdentity,
    Session,
    SessionEvent,
    StorageBatch,
    StorageManifest,
    StorageNodeState,
    StorageRole,
    StorageWriteRequest,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=timezone.utc)
AUTHORITATIVE_POLICY = StorageGroupPolicy(authoritative_fencing_token=8)


def coverage(dataset_id: str = "telemetry", **changes: object) -> DatasetCoverage:
    values = {
        "dataset_id": dataset_id,
        "schema_name": "msh.test",
        "schema_version": 1,
        "required": True,
        "committed_revision": 10,
        "last_contiguous_sequence": 100,
        "missing_ranges": (),
        "batch_count": 10,
        "object_count": 0,
        "manifest_hash": "sha256:dataset",
    }
    values.update(changes)
    return DatasetCoverage(**values)


def manifest(*datasets: DatasetCoverage, revision: int = 10, term: int = 4) -> StorageManifest:
    datasets = datasets or (coverage(),)
    return StorageManifest(
        "session", "main", revision, term, {item.dataset_id: item for item in datasets},
        "sha256:manifest", NOW,
    )


def node(node_id: str, **changes: object) -> StorageNodeState:
    values = {
        "session_id": "session",
        "group_id": "main",
        "node_id": node_id,
        "role": "replica",
        "online": True,
        "integrity_verified": True,
        "schema_compatible": True,
        "leader_eligible": True,
        "authorized": True,
        "authoritative_revision": 10,
        "replication_lag": 0,
        "datasets": {"telemetry": coverage()},
        "term": 4,
        "last_fencing_token": 8,
        "reported_at": NOW,
    }
    values.update(changes)
    return StorageNodeState(**values)


def grant(node_id: str = "a", **changes: object) -> LeadershipGrant:
    values = {
        "session_id": "session",
        "group_id": "main",
        "node_id": node_id,
        "role": "primary",
        "term": 4,
        "fencing_token": 8,
        "issued_at": NOW,
        "lease_expires_at": NOW + timedelta(seconds=30),
        "grant_id": "grant",
    }
    values.update(changes)
    return LeadershipGrant(**values)


def test_f0_001_first_qualified_storage_node() -> None:
    policy = StorageGroupPolicy(authoritative_fencing_token=8)
    result = select_storage_leader(manifest(), None, None, [node("a")], NOW, policy)
    assert (result.selected_node_id, result.selected_role) == ("a", StorageRole.PRIMARY)
    assert result.next_term > 4 and result.next_fencing_token > 8
    assert result.lease_expires_at > NOW


def test_f0_002_sticky_leadership_for_equivalent_replica() -> None:
    result = select_storage_leader(
        manifest(), "a", grant(), [node("b"), node("a")], NOW
    )
    assert result.decision == "retain-current"
    assert result.selected_node_id == "a"
    assert result.requires_new_grant is False
    assert "b" in result.eligible_node_ids


def test_repeated_normal_evaluations_retain_grant_without_incrementing_counters() -> None:
    candidates = [node("a"), node("b")]
    active = grant()

    first = select_storage_leader(manifest(), "a", active, candidates, NOW)
    second = select_storage_leader(manifest(), "a", active, candidates, NOW)

    assert first.decision == second.decision == "retain-current"
    assert first.requires_new_grant is second.requires_new_grant is False
    assert first.next_term is second.next_term is None
    assert first.next_fencing_token is second.next_fencing_token is None


def test_restart_without_grant_or_fencing_counter_requires_authoritative_state() -> None:
    result = select_storage_leader(manifest(term=15), None, None, [node("a", term=15)], NOW)

    assert result.decision == "authoritative-state-required"
    assert result.selected_node_id is None
    assert result.requires_new_grant is False
    assert result.next_term is None
    assert result.next_fencing_token is None
    assert result.reason == "new-grant-requires-authoritative-fencing-state"


def test_new_grant_uses_explicitly_persisted_fencing_token() -> None:
    policy = StorageGroupPolicy(authoritative_fencing_token=9815)

    result = select_storage_leader(
        manifest(term=15), None, None, [node("a", term=15)], NOW, policy
    )

    assert result.decision == "select-candidate"
    assert result.next_term == 16
    assert result.next_fencing_token == 9816


def test_next_token_exceeds_all_authoritative_fencing_state() -> None:
    policy = StorageGroupPolicy(authoritative_fencing_token=9815)
    previous_grant = grant(
        term=15,
        fencing_token=9816,
        issued_at=NOW - timedelta(minutes=1),
        lease_expires_at=NOW,
    )

    result = select_storage_leader(
        manifest(term=15),
        "a",
        previous_grant,
        [node("a", term=15, last_fencing_token=9816), node("b", term=15)],
        NOW,
        policy,
    )

    assert result.decision == "select-candidate"
    assert result.next_fencing_token == 9817
    assert result.next_fencing_token > policy.authoritative_fencing_token
    assert result.next_fencing_token > previous_grant.fencing_token


def test_f0_003_through_005_authoritative_revision_not_rows_or_local_data() -> None:
    current = grant(issued_at=NOW - timedelta(minutes=1), lease_expires_at=NOW)
    a = node("a", authoritative_revision=10)
    b = node("b", authoritative_revision=11)
    result = select_storage_leader(manifest(revision=11), "a", current, [a, b], NOW)
    assert result.selected_node_id == "b"
    # No row-count or uncommitted-batch properties exist in the authoritative model.
    assert "local_rows" not in b.to_dict() and "uncommitted_batches" not in b.to_dict()


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"datasets": {"telemetry": coverage(missing_ranges=((50, 60),))}}, "missing-required-dataset-range"),
        ({"integrity_verified": False}, "integrity-not-verified"),
        ({"schema_compatible": False}, "schema-incompatible"),
        ({"datasets": {}}, "missing-required-dataset"),
    ],
)
def test_f0_006_through_009_incomplete_candidates_are_rejected(changes: dict, reason: str) -> None:
    result = select_storage_leader(
        manifest(),
        None,
        None,
        [node("bad", **changes), node("good")],
        NOW,
        AUTHORITATIVE_POLICY,
    )
    assert result.selected_node_id == "good"
    assert reason in result.rejected["bad"]


def test_f0_010_complementary_partial_datasets_do_not_win() -> None:
    telemetry, audio = coverage(), coverage("audio")
    value = manifest(telemetry, audio)
    a = node("a", datasets={"telemetry": telemetry})
    b = node("b", datasets={"audio": audio})
    result = select_storage_leader(value, None, None, [a, b], NOW)
    assert result.selected_node_id is None
    assert result.decision == "synchronization-required"


def test_f0_011_stable_tie_breaker_ignores_input_order() -> None:
    assert select_storage_leader(
        manifest(), None, None, [node("b"), node("a")], NOW, AUTHORITATIVE_POLICY
    ).selected_node_id == "a"
    assert select_storage_leader(
        manifest(), None, None, [node("a"), node("b")], NOW, AUTHORITATIVE_POLICY
    ).selected_node_id == "a"


def test_f0_012_current_primary_behind_manifest() -> None:
    result = select_storage_leader(
        manifest(), "a", grant(issued_at=NOW - timedelta(minutes=1), lease_expires_at=NOW),
        [node("a", authoritative_revision=9), node("b")], NOW,
    )
    assert result.selected_node_id == "b"
    assert "authoritatively-behind" in result.rejected["a"]


def test_unexpired_grant_older_than_manifest_term_is_not_retained() -> None:
    value = manifest(term=5)
    stale_grant = grant(term=4)
    current = node("a", term=5, last_fencing_token=9)

    result = select_storage_leader(value, "a", stale_grant, [current, node("b", term=5)], NOW)

    assert result.decision == "select-candidate"
    assert result.requires_new_grant is True


def test_node_fencing_token_mismatch_prevents_grant_retention() -> None:
    value = manifest(term=5)
    active = grant(term=5, fencing_token=9)
    current = node("a", term=5, last_fencing_token=8)

    result = select_storage_leader(value, "a", active, [current, node("b", term=5)], NOW)

    assert result.decision == "select-candidate"
    assert result.requires_new_grant is True


def test_obsolete_primary_loses_to_complete_current_term_replica() -> None:
    value = manifest(term=5)
    stale_grant = grant(term=4)
    obsolete = node("a", role="primary", term=4, last_fencing_token=9)
    replica = node("b", term=5, last_fencing_token=9)

    result = select_storage_leader(value, "a", stale_grant, [obsolete, replica], NOW)

    assert result.selected_node_id == "b"
    assert "stale-term" in result.rejected["a"]


def test_future_issued_grant_is_not_retained() -> None:
    value = manifest(term=5)
    future_grant = grant(
        term=5,
        issued_at=NOW + timedelta(seconds=1),
        lease_expires_at=NOW + timedelta(seconds=31),
    )
    current = node("a", term=5, last_fencing_token=9)

    result = select_storage_leader(value, "a", future_grant, [current], NOW)

    assert result.decision == "select-candidate"
    assert result.requires_new_grant is True


def test_candidate_ahead_of_authoritative_manifest_is_rejected() -> None:
    ahead = node("ahead", authoritative_revision=11)

    result = select_storage_leader(
        manifest(), None, None, [ahead, node("current")], NOW, AUTHORITATIVE_POLICY
    )

    assert result.selected_node_id == "current"
    assert "authoritatively-ahead" in result.rejected["ahead"]


def test_optional_dataset_presence_does_not_change_completeness_rank() -> None:
    optional = coverage("optional", required=False)
    value = manifest(coverage(), optional)
    without_optional = node("a")
    with_optional = node("b", datasets={"telemetry": coverage(), "optional": optional})

    result = select_storage_leader(
        value, None, None, [with_optional, without_optional], NOW, AUTHORITATIVE_POLICY
    )

    assert result.selected_node_id == "a"


@pytest.mark.parametrize(
    "candidate_coverage",
    [
        coverage(committed_revision=11),
        coverage(last_contiguous_sequence=101),
    ],
)
def test_candidate_local_coverage_cannot_increase_authority(
    candidate_coverage: DatasetCoverage,
) -> None:
    exact = node("a")
    locally_ahead = node("b", datasets={"telemetry": candidate_coverage})

    result = select_storage_leader(
        manifest(),
        None,
        None,
        [locally_ahead, exact],
        NOW,
        AUTHORITATIVE_POLICY,
    )

    assert result.selected_node_id == "a"
    assert "required-dataset-authoritatively-ahead" in result.rejected["b"]


def test_rejected_candidate_cannot_control_authoritative_counters() -> None:
    faulty = node(
        "faulty",
        authorized=False,
        term=999_999,
        last_fencing_token=999_999,
    )
    policy = StorageGroupPolicy(authoritative_fencing_token=20)

    result = select_storage_leader(manifest(), None, None, [faulty, node("good")], NOW, policy)

    assert result.selected_node_id == "good"
    assert result.next_term == 5
    assert result.next_fencing_token == 21


def test_required_dataset_hash_must_match_authoritative_manifest() -> None:
    mismatched = node(
        "mismatched",
        datasets={"telemetry": coverage(manifest_hash="sha256:different")},
    )

    result = select_storage_leader(
        manifest(), None, None, [mismatched, node("good")], NOW, AUTHORITATIVE_POLICY
    )

    assert result.selected_node_id == "good"
    assert "required-dataset-hash-mismatch" in result.rejected["mismatched"]


def test_unrelated_node_does_not_make_repair_possible() -> None:
    unrelated = node("unrelated", session_id="other-session", datasets={})

    result = select_storage_leader(manifest(), None, None, [unrelated], NOW)

    assert result.decision == "storage-degraded"


def test_future_issued_grant_rejects_primary_write() -> None:
    future_grant = grant(
        term=5,
        fencing_token=9,
        issued_at=NOW + timedelta(seconds=1),
        lease_expires_at=NOW + timedelta(seconds=31),
    )
    request = StorageWriteRequest("session", "main", "a", 5, 9, "grant")

    assert "grant-not-yet-valid" in validate_primary_write(
        request, future_grant, future_grant, NOW
    )


def test_grant_behind_authoritative_fencing_token_is_not_retained() -> None:
    policy = StorageGroupPolicy(authoritative_fencing_token=9)

    result = select_storage_leader(
        manifest(), "a", grant(), [node("a"), node("b")], NOW, policy
    )

    assert result.decision == "select-candidate"
    assert result.requires_new_grant is True
    assert result.next_fencing_token == 10


def test_controlled_handover_selects_equivalent_replica() -> None:
    policy = StorageGroupPolicy(
        controlled_handover=True,
        authoritative_fencing_token=8,
    )

    result = select_storage_leader(
        manifest(), "a", grant(), [node("a"), node("b")], NOW, policy
    )

    assert result.decision == "select-candidate"
    assert result.selected_node_id == "b"
    assert result.requires_new_grant is True


def test_controlled_handover_without_replacement_is_explicit() -> None:
    policy = StorageGroupPolicy(
        controlled_handover=True,
        authoritative_fencing_token=8,
    )
    incomplete = node("b", integrity_verified=False)

    result = select_storage_leader(
        manifest(), "a", grant(), [node("a"), incomplete], NOW, policy
    )

    assert result.decision == "no-qualified-candidate"
    assert result.selected_node_id is None
    assert result.requires_new_grant is False
    assert result.reason == "controlled-handover-has-no-qualified-replacement"


@pytest.mark.parametrize(
    ("request_changes", "grant_changes", "now", "reason"),
    [({}, {}, NOW + timedelta(seconds=30), "lease-expired"),
     ({"term": 4}, {}, NOW, "stale-term"),
     ({"fencing_token": 8}, {}, NOW, "stale-fencing-token"),
     ({"session_id": "other"}, {}, NOW, "wrong-session")],
)
def test_f0_013_through_016_write_fencing(request_changes, grant_changes, now, reason) -> None:
    active = grant(term=5, fencing_token=9, **grant_changes)
    values = dict(session_id="session", group_id="main", actor_node_id="a", term=5, fencing_token=9, grant_id="grant")
    values.update(request_changes)
    assert reason in validate_primary_write(StorageWriteRequest(**values), active, active, now)


def test_valid_primary_write_has_no_rejections() -> None:
    active = grant(term=5, fencing_token=9)
    request = StorageWriteRequest("session", "main", "a", 5, 9, "grant")
    assert validate_primary_write(request, active, active, NOW) == ()


def test_f0_017_and_018_all_protocol_models_round_trip_and_ignore_additive_fields() -> None:
    models = [
        NodeIdentity("node", "Node", "public", NOW, 1),
        Session("session", "Session", "active", 10, NOW, "node", "coordinator"),
        CapabilityAnnouncement("cap", "node", "session", "storage", "msh-storage-v1", "1.0", "ready", {}, NOW),
        SessionEvent("session", 10, "event", "session.active", NOW, "node", {}),
        manifest(),
        StorageBatch("session", "main", "telemetry", "batch", "node", "key", "sha256:data", 1, 1, 1, NOW),
        node("node"),
        grant(),
    ]
    for value in models:
        encoded = value.to_dict()
        json.dumps(encoded)
        encoded["future_optional_field"] = True
        assert type(value).from_dict(encoded) == value


@pytest.mark.parametrize(
    "mutator",
    [lambda value: value.pop("node_id"), lambda value: value.update(node_id=""),
     lambda value: value.update(identity_version=-1), lambda value: value.update(created_at="yesterday")],
)
def test_f0_017_invalid_values_are_structured(mutator) -> None:
    value = NodeIdentity("node", "Node", "public", NOW, 1).to_dict()
    mutator(value)
    with pytest.raises(FederationValidationError) as raised:
        NodeIdentity.from_dict(value)
    assert raised.value.field
    assert raised.value.code


def test_f0_019_protocol_major_mismatch_is_rejected() -> None:
    value = NodeIdentity("node", "Node", "public", NOW, 1).to_dict()
    value["schema"] = "msh.node.v2"
    with pytest.raises(ProtocolCompatibilityError, match="unsupported-schema"):
        NodeIdentity.from_dict(value)


def test_f0_020_report_timestamp_does_not_affect_authority() -> None:
    old_authoritative = node("old", authoritative_revision=11, reported_at=NOW - timedelta(days=1))
    recent_behind = node("recent", authoritative_revision=10, reported_at=NOW + timedelta(days=1))
    result = select_storage_leader(
        manifest(revision=11),
        None,
        None,
        [recent_behind, old_authoritative],
        NOW,
        AUTHORITATIVE_POLICY,
    )
    assert result.selected_node_id == "old"


def test_missing_ranges_must_be_normalized() -> None:
    with pytest.raises(FederationValidationError, match="sorted, non-overlapping"):
        replace(coverage(), missing_ranges=((10, 20), (20, 30)))
