"""Adversarial tests for the P01-P12 physical automation runner.

Every test here is one way an operator, a stale evidence tree, or a copied
command line could try to manufacture a pass. The runner must fail closed on all
of them.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acceptance import v1_physical_campaign as campaign
from scripts.acceptance import v1_physical_campaign_strict as strict
from scripts.acceptance import v1_physical_probes as probes
from scripts.acceptance import v1_physical_runner as runner

from .test_v1_physical_runner import COMMIT, _replace_runs, ready

OTHER_COMMIT = "b" * 40


def _fail_on_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every probe body a tripwire."""

    patched = {}
    for probe_id, spec in probes.PROBES.items():

        def _run(_context, _probe_id=probe_id):
            pytest.fail(f"probe {_probe_id} must not execute here")

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


# ---------------------------------------------------------------------------
# Candidate mixing
# ---------------------------------------------------------------------------


def test_runner_refuses_a_different_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _fail_on_execution(monkeypatch)
    with pytest.raises(campaign.CampaignError):
        runner.probe_assertion(
            checkout,
            root,
            commit=OTHER_COMMIT,
            host="nitro",
            scenario="P05",
            assertion="malformed-timestamp-path",
            run_id=None,
            overrides={},
        )


def test_report_refuses_evidence_from_another_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    host = campaign.load_host(root, "nitro", commit=COMMIT)
    path = root / "observations" / "P05" / "foreign-candidate.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema": campaign.PACKET_SCHEMA,
                "kind": "assertion",
                "candidate_sha": OTHER_COMMIT,
                "scenario": "P05",
                "host_id": "nitro",
                "host_fingerprint": host["host_fingerprint"],
                "os_category": "posix",
                "recorded_at": "2026-09-02T12:00:00Z",
                "assertion": "flask-crash",
                "status": "pass",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(campaign.CampaignError, match="different candidate"):
        runner.report(checkout, root, commit=COMMIT, host="nitro")


# ---------------------------------------------------------------------------
# Host mixing
# ---------------------------------------------------------------------------


def test_runner_refuses_an_alias_registered_to_another_machine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path, node="nitro-machine")
    _fail_on_execution(monkeypatch)
    monkeypatch.setattr(campaign.platform, "node", lambda: "beast-machine")
    with pytest.raises(runner.RunnerError, match="another machine"):
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


def test_verify_refuses_a_preparation_staged_on_another_host(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path, node="nitro-machine")
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
    runner.record_action(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="relay-crash",
        prepare_id=str(prepared["prepare_id"]),
        note="crashed the relay on nitro",
    )
    monkeypatch.setattr(campaign.platform, "node", lambda: "beast-machine")
    campaign.register_host(
        checkout,
        root,
        commit=COMMIT,
        host="beast",
        role="physical-test-host",
        profile="local-ai",
    )
    with pytest.raises(campaign.CampaignError, match="matching preparation"):
        runner.verify_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="beast",
            scenario="P05",
            assertion="relay-crash",
            prepare_id=str(prepared["prepare_id"]),
            run_id=None,
            overrides={},
        )


def test_runner_refuses_a_host_without_a_declared_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path, host="undeclared", profile="unspecified")
    _fail_on_execution(monkeypatch)
    with pytest.raises(runner.RunnerError, match="no declared profile"):
        runner.probe_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="undeclared",
            scenario="P05",
            assertion="malformed-timestamp-path",
            run_id=None,
            overrides={},
        )


def test_host_alias_cannot_change_profile_after_registration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    with pytest.raises(campaign.CampaignError, match="already registered as profile"):
        campaign.register_host(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            role="physical-test-host",
            profile="local-ai",
        )


# ---------------------------------------------------------------------------
# Timed-run mixing
# ---------------------------------------------------------------------------


def _timed_host(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> tuple[Path, Path, str]:
    checkout, root = ready(monkeypatch, tmp_path)
    run_id, _path = campaign.begin_session(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P07",
    )
    return checkout, root, run_id


def test_timed_assertion_requires_the_active_run_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root, _run_id = _timed_host(monkeypatch, tmp_path)
    _fail_on_execution(monkeypatch)
    with pytest.raises(runner.RunnerError, match="timed --run-id"):
        runner.probe_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P07",
            assertion="aged-corpus",
            run_id=None,
            overrides={},
        )


def test_timed_assertion_refuses_a_run_id_from_another_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root, first = _timed_host(monkeypatch, tmp_path)
    campaign.finish_session(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P07",
        run_id=first,
    )
    _fail_on_execution(monkeypatch)
    with pytest.raises(campaign.CampaignError, match="already been finished"):
        runner.probe_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P07",
            assertion="aged-corpus",
            run_id=first,
            overrides={},
        )


def test_timed_report_never_unions_two_runs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.PASS)
    spec = campaign.SCENARIOS["P07"]

    first, _path = campaign.begin_session(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P07",
    )
    for assertion in list(spec.assertions)[:4]:
        strict.timed_observe(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P07",
            run_id=first,
            assertion=assertion,
            status="pass",
            note="first outage",
        )
    campaign.finish_session(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P07",
        run_id=first,
    )

    second, _path = campaign.begin_session(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P07",
    )
    for assertion in list(spec.assertions)[4:]:
        strict.timed_observe(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P07",
            run_id=second,
            assertion=assertion,
            status="pass",
            note="second outage",
        )
    campaign.finish_session(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P07",
        run_id=second,
    )

    document = runner.report(checkout, root, commit=COMMIT, host="nitro")
    rows = {
        row["assertion"]: row["state"]
        for item in document["scenarios"]
        if item["scenario"] == "P07"
        for row in item["assertions"]
    }
    passed = [name for name, state in rows.items() if state == runner.STATE_PASS]
    assert len(passed) < len(spec.assertions)
    assert any(state == runner.STATE_MISSING for state in rows.values())
    scenario_row = next(
        item for item in document["scenarios"] if item["scenario"] == "P07"
    )
    assert scenario_row["complete"] is False


def test_non_timed_scenario_refuses_a_run_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _fail_on_execution(monkeypatch)
    with pytest.raises(runner.RunnerError, match="not a timed scenario"):
        runner.probe_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P05",
            assertion="malformed-timestamp-path",
            run_id="0" * 32,
            overrides={},
        )


# ---------------------------------------------------------------------------
# Preparation output cannot become a pass
# ---------------------------------------------------------------------------


def test_preparation_alone_never_satisfies_its_assertion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.PASS)
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
    status = campaign.scenario_status(root, "P05", expected_commit=COMMIT)
    assert "relay-crash" in status["missing_assertions"]
    packets = campaign.read_packets(root, "P05", expected_commit=COMMIT)
    prepared = [item for item in packets if item.get("kind") == "prepare"]
    assert prepared
    for packet in prepared:
        assert "assertion" not in packet
        assert "status" not in packet


def test_a_prepare_packet_carrying_a_verdict_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _checkout, root = ready(monkeypatch, tmp_path)
    packet = campaign.base_packet(
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        kind="prepare",
    )
    packet.update(
        {
            "prepare_for": "relay-crash",
            "prepare_id": "0" * 32,
            "assertion": "relay-crash",
            "status": "pass",
        }
    )
    with pytest.raises(campaign.CampaignError, match="must not carry an assertion"):
        campaign.write_packet(root, packet)


def test_an_imported_prepare_packet_with_a_verdict_is_rejected_on_read(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _checkout, root = ready(monkeypatch, tmp_path)
    host = campaign.load_host(root, "nitro", commit=COMMIT)
    path = root / "observations" / "P05" / "smuggled-prepare.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema": campaign.PACKET_SCHEMA,
                "kind": "prepare",
                "candidate_sha": COMMIT,
                "scenario": "P05",
                "host_id": "nitro",
                "host_fingerprint": host["host_fingerprint"],
                "os_category": "posix",
                "recorded_at": "2026-09-02T12:00:00Z",
                "prepare_for": "relay-crash",
                "prepare_id": "0" * 32,
                "assertion": "relay-crash",
                "status": "pass",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(campaign.CampaignError, match="must not carry an assertion"):
        campaign.scenario_status(root, "P05", expected_commit=COMMIT)


def test_verify_refuses_without_a_recorded_operator_action(
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
    with pytest.raises(campaign.CampaignError, match="operator fault action"):
        runner.verify_assertion(
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


def test_verify_refuses_a_prepare_id_from_another_assertion(
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
    runner.record_action(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="relay-crash",
        prepare_id=str(prepared["prepare_id"]),
        note="crashed the relay",
    )
    with pytest.raises(campaign.CampaignError, match="matching preparation"):
        runner.verify_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P05",
            assertion="flask-crash",
            prepare_id=str(prepared["prepare_id"]),
            run_id=None,
            overrides={},
        )


def test_operator_action_requires_an_existing_preparation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    with pytest.raises(campaign.CampaignError, match="matching preparation"):
        runner.record_action(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P05",
            assertion="relay-crash",
            prepare_id="0" * 32,
            note="never prepared",
        )


def test_fault_injection_assertion_cannot_be_closed_by_the_probe_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _fail_on_execution(monkeypatch)
    with pytest.raises(runner.RunnerError, match="prepare/action/verify"):
        runner.probe_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P05",
            assertion="relay-crash",
            run_id=None,
            overrides={},
        )


def test_human_observation_assertion_has_no_runner_shortcut(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(
        monkeypatch,
        tmp_path,
        system="Windows",
        node="beast-machine",
        host="beast",
        profile="local-ai",
    )
    _fail_on_execution(monkeypatch)
    with pytest.raises(runner.RunnerError, match="human-observation"):
        runner.probe_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="beast",
            scenario="P06",
            assertion="service-manager-boundary",
            run_id=None,
            overrides={},
        )
    with pytest.raises(runner.RunnerError, match="human-observation"):
        runner.prepare_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="beast",
            scenario="P06",
            assertion="service-manager-boundary",
            run_id=None,
            overrides={},
        )


# ---------------------------------------------------------------------------
# Wrong OS or profile
# ---------------------------------------------------------------------------


def test_wrong_os_assertion_is_refused_before_any_probe_runs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path, system="Linux")
    _fail_on_execution(monkeypatch)
    with pytest.raises(campaign.CampaignError, match="windows"):
        runner.probe_assertion(
            checkout,
            root,
            commit=COMMIT,
            host="nitro",
            scenario="P03",
            assertion="update-cmd-disposition",
            run_id=None,
            overrides={},
        )


def test_wrong_os_probe_is_refused_before_it_executes() -> None:
    spec = probes.ProbeSpec(
        "checkout-identity",
        "windows only",
        lambda _context: pytest.fail("wrong-OS probe must not execute"),
        os_category="windows",
    )
    context = probes.ProbeContext(
        checkout=Path("."),
        evidence_root=Path("."),
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="school-control",
        scenario="P01",
        assertion="posix-runtime-state",
    )
    with pytest.raises(probes.ProbeError, match="windows"):
        probes.execute(spec, context)


def test_wrong_profile_probe_is_refused_before_it_executes() -> None:
    spec = probes.ProbeSpec(
        "checkout-identity",
        "language-model hosts only",
        lambda _context: pytest.fail("wrong-profile probe must not execute"),
        profiles=frozenset({"local-ai"}),
    )
    context = probes.ProbeContext(
        checkout=Path("."),
        evidence_root=Path("."),
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="school-control",
        scenario="P01",
        assertion="posix-runtime-state",
    )
    with pytest.raises(probes.ProbeError, match="profiles"):
        probes.execute(spec, context)


def test_undeclared_probe_options_are_refused() -> None:
    spec = probes.probe_spec("checkout-identity")
    context = probes.ProbeContext(
        checkout=Path("."),
        evidence_root=Path("."),
        commit=COMMIT,
        host_id="nitro",
        os_category="posix",
        profile="school-control",
        scenario="P01",
        assertion="posix-runtime-state",
        options={"destination": "/tmp"},
    )
    with pytest.raises(probes.ProbeError, match="does not accept options"):
        probes.execute(spec, context)


def test_probe_options_reject_shell_and_unknown_keys() -> None:
    with pytest.raises(probes.ProbeError, match="option name"):
        probes.parse_options(["Bad Key=1"])
    with pytest.raises(probes.ProbeError, match="option value"):
        probes.parse_options(["destination=$(rm -rf /)"])
    with pytest.raises(probes.ProbeError, match="key=value"):
        probes.parse_options(["destination"])
    assert probes.parse_options(["destination=/mnt/backup"]) == {
        "destination": "/mnt/backup"
    }


# ---------------------------------------------------------------------------
# Skipped and destructive operations are never successful
# ---------------------------------------------------------------------------


def test_unavailable_probe_leaves_the_assertion_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.UNAVAILABLE)
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
    assert result["verdict"] == probes.UNAVAILABLE
    assert result["recorded"] is False
    status = campaign.scenario_status(root, "P05", expected_commit=COMMIT)
    assert "malformed-timestamp-path" in status["missing_assertions"]
    document = runner.report(checkout, root, commit=COMMIT, host="nitro")
    rows = {
        row["assertion"]: row["state"]
        for item in document["scenarios"]
        if item["scenario"] == "P05"
        for row in item["assertions"]
    }
    assert rows["malformed-timestamp-path"] == runner.STATE_MISSING


def test_one_unavailable_probe_blocks_a_verify_that_otherwise_passes(
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
    runner.record_action(
        checkout,
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        assertion="relay-crash",
        prepare_id=str(prepared["prepare_id"]),
        note="crashed the relay",
    )
    patched = dict(probes.PROBES)
    spec = patched["core-availability"]
    patched["core-availability"] = probes.ProbeSpec(
        spec.probe_id,
        spec.title,
        lambda _context: probes.ProbeOutcome(
            "core-availability",
            probes.UNAVAILABLE,
            "nothing observable",
            {},
        ),
        os_category=spec.os_category,
        profiles=spec.profiles,
        options=spec.options,
    )
    monkeypatch.setattr(probes, "PROBES", patched)
    result = runner.verify_assertion(
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
    assert result["verdict"] == probes.UNAVAILABLE
    assert result["recorded"] is False
    status = campaign.scenario_status(root, "P05", expected_commit=COMMIT)
    assert "relay-crash" in status["missing_assertions"]


def test_the_runner_never_executes_the_operator_fault_action(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    _replace_runs(monkeypatch, probes.PASS)

    def _forbidden(*_args, **_kwargs):
        pytest.fail("the runner must not spawn a destructive command")

    monkeypatch.setattr(probes.subprocess, "run", _forbidden)
    monkeypatch.setattr(campaign.subprocess, "run", _forbidden)
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
    packets = campaign.read_packets(root, "P05", expected_commit=COMMIT)
    prepare_packets = [item for item in packets if item.get("kind") == "prepare"]
    assert prepare_packets
    assert prepare_packets[0]["detail"]["harness_performed_fault"] is False


def test_reviewed_helpers_are_named_but_never_invoked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(
        monkeypatch,
        tmp_path,
        system="Windows",
        node="beast-machine",
        host="beast",
        profile="local-ai",
    )
    _replace_runs(monkeypatch, probes.PASS)

    def _forbidden(*_args, **_kwargs):
        pytest.fail("a reviewed helper must stay an operator decision")

    monkeypatch.setattr(probes.subprocess, "run", _forbidden)
    prepared = runner.prepare_assertion(
        checkout,
        root,
        commit=COMMIT,
        host="beast",
        scenario="P11",
        assertion="successful-backup",
        run_id=None,
        overrides={},
    )
    assert prepared["reviewed_helper"]
    assert prepared["harness_performs_this_action"] is False


def test_report_is_incomplete_while_any_assertion_is_unproven(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    checkout, root = ready(monkeypatch, tmp_path)
    document = runner.report(checkout, root, commit=COMMIT, host="nitro")
    assert document["complete"] is False
    assert document["state_totals"][runner.STATE_MISSING] > 0
    assert document["state_totals"][runner.STATE_PASS] == 0


def test_probe_outcome_rejects_an_invented_status() -> None:
    with pytest.raises(probes.ProbeError, match="invalid probe status"):
        probes.ProbeOutcome("checkout-identity", "skipped", "not a verdict", {})


def test_a_verdict_cannot_be_recorded_without_any_probe() -> None:
    with pytest.raises(runner.RunnerError, match="no probe was bound"):
        runner._verdict([])


def test_an_invented_assertion_status_is_refused_on_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _checkout, root = ready(monkeypatch, tmp_path)
    packet = campaign.base_packet(
        root,
        commit=COMMIT,
        host="nitro",
        scenario="P05",
        kind="assertion",
    )
    packet.update({"assertion": "relay-crash", "status": "skipped"})
    with pytest.raises(campaign.CampaignError, match="unsupported observation status"):
        campaign.write_packet(root, packet)
