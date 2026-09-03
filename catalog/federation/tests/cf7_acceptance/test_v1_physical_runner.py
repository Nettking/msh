"""Contract tests for the P01-P12 physical automation runner.

These cover the ordinary shape of the runner: the classification stays aligned
with the evidence contract, automated probes record real verdicts through the
existing campaign API, and the report exposes every state an operator needs.
Adversarial abuse cases live in ``test_v1_physical_runner_adversarial``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acceptance import v1_physical_campaign as campaign
from scripts.acceptance import v1_physical_probes as probes
from scripts.acceptance import v1_physical_runner as runner
from scripts.acceptance.v1_physical_automation import (
    AUTOMATED,
    FAULT_INJECTION,
    HUMAN,
    PLANS,
    classification_summary,
    lane_counts,
)
from scripts.acceptance.v1_physical_campaign_contract import SCENARIOS

COMMIT = "a" * 40


def ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    system: str = "Linux",
    node: str = "private-host",
    host: str = "nitro",
    profile: str = "school-control",
) -> tuple[Path, Path]:
    checkout = tmp_path / "checkout"
    checkout.mkdir(exist_ok=True)
    root = checkout / "evidence" / "v1-physical"
    monkeypatch.setattr(
        campaign,
        "verify_checkout",
        lambda *_args, **_kwargs: {"commit_sha": COMMIT},
    )
    monkeypatch.setattr(campaign.platform, "system", lambda: system)
    monkeypatch.setattr(campaign.platform, "node", lambda: node)
    campaign.initialize(checkout, root, commit=COMMIT, operator="Martin")
    campaign.register_host(
        checkout,
        root,
        commit=COMMIT,
        host=host,
        role="physical-test-host",
        profile=profile,
    )
    return checkout, root


def _replace_runs(monkeypatch: pytest.MonkeyPatch, status: str) -> list[str]:
    executed: list[str] = []
    patched = {}
    for probe_id, spec in probes.PROBES.items():

        def _run(_context, _probe_id=probe_id):
            executed.append(_probe_id)
            return probes.ProbeOutcome(_probe_id, status, "stubbed", {})

        patched[probe_id] = probes.ProbeSpec(
            spec.probe_id,
            spec.title,
            _run,
            os_category=spec.os_category,
            profiles=spec.profiles,
            options=spec.options,
            reads_evidence=spec.reads_evidence,
        )
    monkeypatch.setattr(probes, "PROBES", patched)
    return executed


def test_every_contract_assertion_has_exactly_one_automation_lane() -> None:
    assert set(PLANS) == set(SCENARIOS)
    for scenario, spec in SCENARIOS.items():
        assert set(PLANS[scenario]) == set(spec.assertions)
    counts = lane_counts()
    assert set(counts) == {AUTOMATED, FAULT_INJECTION, HUMAN}
    assert sum(counts.values()) == sum(
        len(spec.assertions) for spec in SCENARIOS.values()
    )
    assert counts[AUTOMATED] > 0
    assert counts[FAULT_INJECTION] > 0
    assert counts[HUMAN] > 0


def test_every_fault_injection_lane_declares_prepare_action_and_verify() -> None:
    for scenario, plans in PLANS.items():
        for assertion, plan in plans.items():
            if plan.classification != FAULT_INJECTION:
                continue
            assert plan.prepare, f"{scenario}/{assertion} has no prepare probe"
            assert plan.verify, f"{scenario}/{assertion} has no verify probe"
            assert plan.operator_action.strip(), (
                f"{scenario}/{assertion} has no explicit operator action"
            )


def test_human_lane_assertions_declare_why_they_cannot_be_automated() -> None:
    human = {
        f"{scenario}/{assertion}": plan
        for scenario, plans in PLANS.items()
        for assertion, plan in plans.items()
        if plan.classification == HUMAN
    }
    assert human
    for name, plan in human.items():
        assert plan.rationale.strip(), f"{name} is human-only without a rationale"
        assert not plan.all_probe_ids()


def test_classification_summary_covers_the_whole_contract() -> None:
    summary = classification_summary()
    assert set(summary) == set(SCENARIOS)
    for scenario, spec in SCENARIOS.items():
        rows = summary[scenario]["assertions"]
        assert set(rows) == set(spec.assertions)
        for assertion, row in rows.items():
            assert row["assertion_text"] == spec.assertions[assertion]
            assert row["allow_not_applicable"] == (assertion in spec.allow_na)


def test_automated_probe_records_pass_through_the_campaign_api(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.PASS)
    result = runner.probe_assertion(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="malformed-timestamp-path",
        run_id=None,
        overrides={},
    )
    assert result["verdict"] == probes.PASS
    assert result["recorded"] is True
    status = campaign.scenario_status(root, "P05", expected_commit=COMMIT)
    assert "malformed-timestamp-path" not in status["missing_assertions"]
    assert "malformed-timestamp-path" not in status["failing_assertions"]


def test_probe_failure_is_recorded_as_a_failed_assertion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.FAIL)
    result = runner.probe_assertion(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="malformed-timestamp-path",
        run_id=None,
        overrides={},
    )
    assert result["verdict"] == probes.FAIL
    status = campaign.scenario_status(root, "P05", expected_commit=COMMIT)
    assert "malformed-timestamp-path" in status["failing_assertions"]


def test_full_prepare_action_verify_cycle_closes_one_assertion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.PASS)
    prepared = runner.prepare_assertion(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="relay-crash",
        run_id=None,
        overrides={},
    )
    assert prepared["harness_performs_this_action"] is False
    assert prepared["operator_action"]
    status = campaign.scenario_status(root, "P05", expected_commit=COMMIT)
    assert "relay-crash" in status["missing_assertions"]

    runner.record_action(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="relay-crash",
        prepare_id=str(prepared["prepare_id"]),
        note="killed the relay process and watched supervision recover it",
    )
    verified = runner.verify_assertion(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="relay-crash",
        prepare_id=str(prepared["prepare_id"]),
        run_id=None,
        overrides={},
    )
    assert verified["verdict"] == probes.PASS
    status = campaign.scenario_status(root, "P05", expected_commit=COMMIT)
    assert "relay-crash" not in status["missing_assertions"]
    assert "relay-crash" not in status["failing_assertions"]


def test_report_exposes_every_operator_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.PASS)
    runner.probe_assertion(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="malformed-timestamp-path",
        run_id=None,
        overrides={},
    )
    runner.prepare_assertion(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="relay-crash",
        run_id=None,
        overrides={},
    )
    campaign.observe(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P02",
        assertion="inode-pressure",
        status="not-applicable",
        note="this filesystem does not expose inode accounting",
    )
    document = runner.report(checkout, root, commit=COMMIT, host="nitro")
    rows = {
        row["assertion"]: row
        for item in document["scenarios"]
        for row in item["assertions"]
        if item["scenario"] in {"P02", "P05"}
    }
    assert rows["malformed-timestamp-path"]["state"] == runner.STATE_PASS
    assert rows["relay-crash"]["state"] == runner.STATE_READY
    assert rows["inode-pressure"]["state"] == runner.STATE_NOT_APPLICABLE
    assert rows["flask-crash"]["state"] == runner.STATE_MISSING
    assert document["complete"] is False
    assert set(document["state_totals"]) == set(runner.REPORT_STATES)


def test_report_next_step_is_the_exact_command_to_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.PASS)
    prepared = runner.prepare_assertion(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="relay-crash",
        run_id=None,
        overrides={},
    )
    document = runner.report(checkout, root, commit=COMMIT, host="nitro")
    rows = {
        row["assertion"]: row
        for item in document["scenarios"]
        if item["scenario"] == "P05"
        for row in item["assertions"]
    }
    assert "runner action" in rows["relay-crash"]["next_step"]
    assert str(prepared["prepare_id"]) in rows["relay-crash"]["next_step"]
    assert "runner prepare" in rows["flask-crash"]["next_step"]
    human = {
        row["assertion"]: row
        for item in document["scenarios"]
        if item["scenario"] == "P06"
        for row in item["assertions"]
    }
    assert "v1_physical_campaign observe" in human["service-manager-boundary"]["next_step"]


def test_scenario_command_runs_only_assertions_this_host_may_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.PASS)
    result = runner.run_scenario(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P01",
        run_id=None,
        overrides={},
    )
    executed = {item["assertion"] for item in result["executed"]}
    skipped = {item["assertion"] for item in result["not_run_here"]}
    assert "posix-resource-baseline" in executed
    assert "windows-resource-baseline" in skipped
    assert "windows-runtime-state" in skipped


def test_sample_attaches_the_p12_soak_series(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    run_id, _path = campaign.begin_session(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P12",
    )
    result = runner.sample(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P12",
        label="soak-sample",
        run_id=run_id,
    )
    assert set(result["series"]) >= {
        "recorder",
        "publication",
        "history",
        "orphan",
        "cpu_ram",
    }
    packets = campaign.read_packets(root, "P12", expected_commit=COMMIT)
    samples = [packet for packet in packets if packet.get("kind") == "sample"]
    assert samples and "extras" in samples[0]


def test_probe_detail_is_redacted_before_it_reaches_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    patched = dict(probes.PROBES)
    spec = patched["recorder-path-containment"]

    def _leaky(_context: probes.ProbeContext) -> probes.ProbeOutcome:
        return probes.ProbeOutcome(
            "recorder-path-containment",
            probes.PASS,
            "endpoint http://192.168.1.50:5000 token=private-token",
            {"endpoint": "http://192.168.1.50:5000", "pairing": "FCP1-privateCode123"},
        )

    patched["recorder-path-containment"] = probes.ProbeSpec(
        spec.probe_id,
        spec.title,
        _leaky,
        os_category=spec.os_category,
        profiles=spec.profiles,
        options=spec.options,
    )
    monkeypatch.setattr(probes, "PROBES", patched)
    runner.probe_assertion(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="malformed-timestamp-path",
        run_id=None,
        overrides={},
    )
    serialized = json.dumps(
        campaign.read_packets(root, "P05", expected_commit=COMMIT)
    )
    assert "192.168.1.50" not in serialized
    assert "private-token" not in serialized
    assert campaign.privacy_check(checkout, root, commit=COMMIT)["passed"] is True
