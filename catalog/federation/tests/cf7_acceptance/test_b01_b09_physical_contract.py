"""Adversarial tests for the supplemental B01-B09 physical contract."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from scripts.acceptance import b01_b09_physical_contract as contract

CANDIDATE = "1" * 40
OTHER = "2" * 40
NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)


def verification() -> dict[str, object]:
    return {
        "candidate_sha": CANDIDATE,
        "host_id": "nitro",
        "verified_at": NOW.isoformat().replace("+00:00", "Z"),
        "evidence_sha256": "a" * 64,
        "redacted": True,
        "source": "physical-observation",
    }


def packet(case_id: str = "B01", **updates: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": contract.SCHEMA,
        "case_id": case_id,
        "candidate_sha": CANDIDATE,
        "host_id": "nitro",
        "observed_at": NOW.isoformat().replace("+00:00", "Z"),
        "status": "pass",
        "verification": verification(),
    }
    if case_id == "B01":
        value.update({"recorder_ids": ["recorder-a", "recorder-b"], "admission_observed": True})
    if case_id == "B03":
        value.update(
            {
                "target_node_id": "recorder-a",
                "request_target": "recorder-a",
                "execution_node_id": "recorder-a",
                "report_actor": "recorder-a",
                "expected_report_actor": "recorder-a",
                "report_target_node_id": "recorder-a",
                "corresponding_report_count": 1,
                "nonpublic_payload": False,
                "nonpublic_property": False,
                "private_address_leakage": False,
            }
        )
    if case_id == "B02":
        value.update({"recorder_ids": ["recorder-a", "recorder-b"], "identities_unique": True})
    if case_id in {"B04", "B05"}:
        value.update({"restart_observed": True, "rejoined": True})
    if case_id == "B06":
        value.update({"restart_observed": True, "replay_converged": True})
    if case_id == "B07":
        value.update({"legacy_state_present": True, "converged": True})
    if case_id == "B08":
        value.update({"stale_state_present": True, "converged": True})
    if case_id == "B09":
        value.update(
            {
                "old_candidate_sha": OTHER,
                "isolation_confirmed": True,
                "contact_observed": False,
            }
        )
    value.update(updates)
    return value


def test_contract_catalog_maps_every_case_to_p_and_cf7_lanes() -> None:
    assert tuple(contract.CASE_CONTRACT) == contract.CASE_IDS
    for row in contract.CASE_CONTRACT.values():
        assert row["p_ids"]
        assert row["cf7_ids"]
        assert row["required"]


def test_wrong_candidate_is_rejected() -> None:
    with pytest.raises(contract.ContractError, match="different candidate"):
        contract.validate_packet(packet(candidate_sha=OTHER), expected_candidate=CANDIDATE, now=NOW)


def test_wrong_host_is_rejected() -> None:
    with pytest.raises(contract.ContractError, match="outside"):
        contract.validate_packet(packet(host_id="beast"), expected_candidate=CANDIDATE, expected_hosts={"nitro"}, now=NOW)


def test_verification_host_must_match_packet_host() -> None:
    with pytest.raises(contract.ContractError, match="verification host"):
        contract.validate_packet(
            packet("B03", verification={**verification(), "host_id": "beast"}),
            expected_candidate=CANDIDATE,
            now=NOW,
        )


def test_b03_requires_one_target_and_rejects_cross_target_report() -> None:
    bad = packet("B03", report_target_node_id="recorder-b")
    with pytest.raises(contract.ContractError, match="different node"):
        contract.validate_packet(bad, expected_candidate=CANDIDATE, now=NOW)
    missing = packet("B03")
    del missing["target_node_id"]
    with pytest.raises(contract.ContractError, match="missing target_node_id"):
        contract.validate_packet(missing, expected_candidate=CANDIDATE, now=NOW)


def test_b03_failure_can_preserve_privacy_violation_diagnostic() -> None:
    diagnostic = packet("B03", status="fail", nonpublic_payload=True)
    result = contract.validate_packet(diagnostic, expected_candidate=CANDIDATE, now=NOW)
    assert result["status"] == "fail"


def test_duplicate_packets_are_rejected_even_when_each_packet_is_valid() -> None:
    with pytest.raises(contract.ContractError, match="duplicate"):
        contract.validate_campaign(
            [packet(), packet()],
            expected_candidate=CANDIDATE,
            expected_hosts={"nitro"},
            now=NOW,
        )


def test_stale_packet_is_not_revalidated_as_current() -> None:
    stale = packet(observed_at=(NOW - timedelta(days=2)).isoformat().replace("+00:00", "Z"))
    with pytest.raises(contract.ContractError, match="observation window"):
        contract.validate_packet(stale, expected_candidate=CANDIDATE, now=NOW)


def test_fabricated_pass_without_verification_provenance_is_rejected() -> None:
    fabricated = packet()
    del fabricated["verification"]
    with pytest.raises(contract.ContractError, match="verification provenance"):
        contract.validate_packet(fabricated, expected_candidate=CANDIDATE, now=NOW)


def test_private_paths_endpoints_and_addresses_are_rejected() -> None:
    unsafe = packet(operator_note="http://192.168.1.20:8000 from C:\\msh\\secret")
    with pytest.raises(contract.ContractError, match="private"):
        contract.validate_packet(unsafe, expected_candidate=CANDIDATE, now=NOW)


@pytest.mark.parametrize("value", ["/home/martin/private/state", "/tmp/fcp-evidence", "/var/lib/fcp/state"])
def test_posix_absolute_paths_are_rejected(value: str) -> None:
    with pytest.raises(contract.ContractError, match="private"):
        contract.validate_packet(packet(operator_note=value), expected_candidate=CANDIDATE, now=NOW)


def test_benign_identifiers_remain_allowed() -> None:
    result = contract.validate_packet(packet("B03", operator_note="node/one"), expected_candidate=CANDIDATE, now=NOW)
    assert result["status"] == "pass"


@pytest.mark.parametrize(
    ("case_id", "updates", "message"),
    [
        ("B01", {"recorder_ids": [], "admission_observed": True}, "recorder_ids"),
        ("B01", {"recorder_ids": ["recorder-a", "recorder-b"], "admission_observed": False}, "admission"),
        ("B02", {"recorder_ids": ["recorder-a", "recorder-a"], "identities_unique": True}, "unique"),
        ("B04", {"restart_observed": False, "rejoined": True}, "restart_observed"),
        ("B05", {"restart_observed": True, "rejoined": False}, "rejoined"),
        ("B06", {"restart_observed": True, "replay_converged": False}, "replay_converged"),
        ("B07", {"legacy_state_present": False, "converged": True}, "legacy_state_present"),
        ("B08", {"stale_state_present": True, "converged": False}, "converged"),
    ],
)
def test_case_specific_success_semantics_are_strict(case_id: str, updates: dict[str, object], message: str) -> None:
    with pytest.raises(contract.ContractError, match=message):
        contract.validate_packet(packet(case_id, **updates), expected_candidate=CANDIDATE, now=NOW)


def test_b09_rejects_accidental_contact_with_old_candidate() -> None:
    unsafe = packet("B09", contact_observed=True)
    with pytest.raises(contract.ContractError, match="contact"):
        contract.validate_packet(unsafe, expected_candidate=CANDIDATE, now=NOW)


def test_mixed_candidate_campaign_is_rejected_before_completion() -> None:
    mixed = packet("B09", candidate_sha=OTHER)
    with pytest.raises(contract.ContractError, match="different candidate"):
        contract.validate_campaign(
            [packet(), mixed],
            expected_candidate=CANDIDATE,
            expected_hosts={"nitro"},
            now=NOW,
        )


def test_valid_b03_is_accepted_but_campaign_remains_incomplete_without_all_cases() -> None:
    result = contract.validate_packet(packet("B03"), expected_candidate=CANDIDATE, now=NOW)
    assert result["status"] == "pass"
    campaign = contract.validate_campaign(
        [packet("B03")], expected_candidate=CANDIDATE, expected_hosts={"nitro"}, now=NOW
    )
    assert campaign["complete"] is False
    assert "B03" in campaign["cases_seen"]


def test_campaign_with_all_required_cases_but_one_blocked_is_incomplete() -> None:
    packets = [packet(case_id) for case_id in contract.CASE_IDS]
    packets[-1]["status"] = "blocked"
    result = contract.validate_campaign(
        packets,
        expected_candidate=CANDIDATE,
        expected_hosts={"nitro"},
        now=NOW,
    )
    assert result["complete"] is False
