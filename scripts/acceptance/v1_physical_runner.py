"""Deterministic runner for the Federation v1 P01-P12 physical campaign.

The runner is the operator-facing layer above the existing evidence harness. It
does not replace it: candidate identity, host provenance, redaction, timed-run
binding, privacy sealing and the release decision all still belong to
``v1_physical_campaign`` and ``v1_physical_campaign_strict``.

What this layer adds:

* it refuses to do anything unless the checkout is exactly the candidate SHA,
  clean, and the registered host record actually belongs to *this* machine;
* it runs only the checked-in probes the target host's OS and profile allow;
* it records every verdict through the existing campaign API, so timed P07/P12
  evidence stays bound to one run;
* it never marks an assertion as passing on its own. A probe that cannot observe
  its surface leaves the assertion missing rather than passing; and
* it separates PREPARE, the explicit OPERATOR ACTION, and VERIFY, and never
  performs a destructive action itself.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final

from scripts.acceptance import v1_physical_campaign as campaign
from scripts.acceptance import v1_physical_campaign_strict as strict
from scripts.acceptance import v1_physical_probes as probes
from scripts.acceptance import v1_physical_runtime_binding as runtime_binding
from scripts.acceptance.v1_physical_automation import (
    AUTOMATED,
    FAULT_INJECTION,
    HUMAN,
    PLANS,
    AssertionPlan,
    ProbeBinding,
    classification_summary,
    lane_counts,
    plan_for,
)

REPORT_SCHEMA: Final = "fcp.v1.physical-campaign-report.v1"

STATE_PASS: Final = "pass"
STATE_FAIL: Final = "fail"
STATE_READY: Final = "ready-for-operator-action"
STATE_MISSING: Final = "missing"
STATE_NOT_APPLICABLE: Final = "not-applicable"
REPORT_STATES: Final = (
    STATE_PASS,
    STATE_FAIL,
    STATE_READY,
    STATE_MISSING,
    STATE_NOT_APPLICABLE,
)


class RunnerError(RuntimeError):
    """The runner cannot safely continue."""


def _print(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


# --------------------------------------------------------------------------
# Host and candidate gating
# --------------------------------------------------------------------------


def resolve_host(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
) -> dict[str, object]:
    """Load the registered host and prove it is the machine running right now.

    ``load_host`` already binds a host record to one candidate. This adds the
    check the runner needs before it executes anything: the alias must resolve to
    *this* machine's fingerprint, so evidence collected on Beast can never be
    filed under Nitro by passing another alias on the command line.
    """

    record = campaign.load_host(root, host, commit=commit)
    if record.get("host_fingerprint") != campaign.host_fingerprint():
        raise RunnerError(
            f"host {host} is registered to another machine; register this host first"
        )
    if record.get("os_category") != campaign.os_category():
        raise RunnerError(
            f"host {host} is registered as {record.get('os_category')} evidence"
        )
    return record


def _gate(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    runtime_binding_file: Path | None = None,
) -> tuple[str, dict[str, object]]:
    """Verify candidate, checkout cleanliness and host identity, in that order."""

    expected = campaign.require_commit(commit)
    binding = None
    if runtime_binding_file is not None:
        binding = runtime_binding.load(
            runtime_binding_file,
            host_id=host,
            target_candidate_sha=expected,
        )
    campaign.load_campaign(
        checkout,
        root,
        expected,
        verify_checkout_identity=binding is None,
    )
    if binding is not None:
        campaign.bind_harness(
            root,
            commit=expected,
            harness_sha=binding.acceptance_harness_sha,
        )
    record = resolve_host(checkout, root, commit=expected, host=host)
    profile = campaign.host_profile_of(record)
    if profile == "unspecified":
        raise RunnerError(
            f"host {host} has no declared profile; re-register it with --profile "
            "so probe applicability can be decided"
        )
    if binding is not None:
        record = dict(record)
        record["__runtime_binding"] = binding
    return expected, record


def _require_active_run(
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    run_id: str | None,
) -> str | None:
    spec = campaign.SCENARIOS[scenario]
    if not spec.minimum_elapsed_seconds:
        if run_id:
            raise RunnerError(f"{scenario} is not a timed scenario; drop --run-id")
        return None
    if not run_id:
        raise RunnerError(
            f"{scenario} evidence must be bound to the active timed --run-id"
        )
    strict._require_active_run(
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        run_id=run_id,
    )
    return run_id


# --------------------------------------------------------------------------
# Probe execution
# --------------------------------------------------------------------------


def _context(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    record: Mapping[str, object],
    scenario: str,
    assertion: str,
    run_id: str | None,
    binding: ProbeBinding,
    overrides: Mapping[str, str],
) -> probes.ProbeContext:
    options = dict(binding.option_map())
    spec = probes.probe_spec(binding.probe_id)
    for key, value in overrides.items():
        if key in spec.options and key not in binding.option_map():
            options[key] = value
    return probes.ProbeContext(
        checkout=checkout,
        evidence_root=root,
        commit=commit,
        host_id=str(record["host_id"]),
        os_category=str(record["os_category"]),
        profile=campaign.host_profile_of(record),
        scenario=scenario,
        assertion=assertion,
        run_id=run_id,
        options=options,
        runtime_binding=(
            record.get("__runtime_binding")
            if isinstance(record.get("__runtime_binding"), runtime_binding.RuntimeBinding)
            else None
        ),
        harness_sha=(
            str(record.get("__runtime_binding").acceptance_harness_sha)
            if isinstance(record.get("__runtime_binding"), runtime_binding.RuntimeBinding)
            else None
        ),
    )


def require_operator_options(
    bindings: Sequence[ProbeBinding],
    overrides: Mapping[str, str],
    *,
    scenario: str,
    assertion: str,
) -> None:
    """Refuse a probe whose subject only the operator can name.

    A restored destination or a completed activation count cannot be inferred.
    Without them a probe would silently answer about the wrong thing, so the
    assertion is refused until they are supplied.
    """

    missing = sorted(
        {
            name
            for binding in bindings
            for name in binding.required
            if not overrides.get(name)
        }
    )
    if missing:
        raise RunnerError(
            f"{scenario}/{assertion} needs operator-supplied probe options: "
            + ", ".join(f"--option {name}=<value>" for name in missing)
        )


def run_bindings(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    record: Mapping[str, object],
    scenario: str,
    assertion: str,
    run_id: str | None,
    bindings: Sequence[ProbeBinding],
    overrides: Mapping[str, str],
) -> list[probes.ProbeOutcome]:
    external_harness = isinstance(
        record.get("__runtime_binding"), runtime_binding.RuntimeBinding
    )
    selected_bindings = tuple(
        binding
        for binding in bindings
        if not (external_harness and binding.probe_id == "checkout-identity")
    )
    require_operator_options(
        selected_bindings,
        overrides,
        scenario=scenario,
        assertion=assertion,
    )
    outcomes: list[probes.ProbeOutcome] = []
    for binding in selected_bindings:
        spec = probes.probe_spec(binding.probe_id)
        context = _context(
            checkout,
            root,
            commit=commit,
            record=record,
            scenario=scenario,
            assertion=assertion,
            run_id=run_id,
            binding=binding,
            overrides=overrides,
        )
        outcomes.append(probes.execute(spec, context))
    return outcomes


def _verdict(outcomes: Sequence[probes.ProbeOutcome]) -> str:
    """Collapse probe results into an assertion verdict, failing closed.

    ``unavailable`` is deliberately not a verdict. A probe that saw nothing can
    neither pass nor fail an assertion, so the assertion stays unrecorded.
    """

    if not outcomes:
        raise RunnerError("no probe was bound to this assertion")
    if any(outcome.status == probes.FAIL for outcome in outcomes):
        return probes.FAIL
    if any(outcome.status == probes.UNAVAILABLE for outcome in outcomes):
        return probes.UNAVAILABLE
    return probes.PASS


def _detail(
    outcomes: Sequence[probes.ProbeOutcome],
    *,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    detail: dict[str, object] = {
        "probes": [
            {
                "probe_id": outcome.probe_id,
                "status": outcome.status,
                "summary": outcome.summary,
                "detail": outcome.detail,
            }
            for outcome in outcomes
        ]
    }
    if extra:
        detail.update(dict(extra))
    return detail


def _binding_detail(record: Mapping[str, object]) -> dict[str, object]:
    bound = record.get("__runtime_binding")
    if not isinstance(bound, runtime_binding.RuntimeBinding):
        return {"runtime_binding": {"explicit": False}}
    return {"runtime_binding": runtime_binding.public_summary(bound)}


def _record_verdict(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    run_id: str | None,
    status: str,
    note: str,
    detail: Mapping[str, object],
    source: str,
    allow_external_harness: bool = False,
) -> Path:
    if run_id:
        return strict.timed_observe(
            checkout,
            root,
            commit=commit,
            host=host,
            scenario=scenario,
            run_id=run_id,
            assertion=assertion,
            status=status,
            note=note,
            detail=detail,
            source=source,
            allow_external_harness=allow_external_harness,
        )
    return campaign.observe(
        checkout,
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        assertion=assertion,
        status=status,
        note=note,
        detail=detail,
        source=source,
        allow_external_harness=allow_external_harness,
    )


def _summary_note(outcomes: Sequence[probes.ProbeOutcome]) -> str:
    return "; ".join(
        f"{outcome.probe_id}={outcome.status}: {outcome.summary}"
        for outcome in outcomes
    )


def probe_assertion(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    run_id: str | None,
    overrides: Mapping[str, str],
    runtime_binding_file: Path | None = None,
) -> dict[str, object]:
    expected, record = _gate(
        checkout, root, commit=commit, host=host,
        runtime_binding_file=runtime_binding_file,
    )
    scenario_id, _spec = campaign._assertion_contract(
        root,
        commit=expected,
        host=host,
        scenario=scenario,
        assertion=assertion,
    )
    plan = plan_for(scenario_id, assertion)
    if plan.classification != AUTOMATED:
        raise RunnerError(
            f"{scenario_id}/{assertion} is {plan.classification}; "
            "use prepare/action/verify or record the human observation"
        )
    active = _require_active_run(
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        run_id=run_id,
    )
    outcomes = run_bindings(
        checkout,
        root,
        commit=expected,
        record=record,
        scenario=scenario_id,
        assertion=assertion,
        run_id=active,
        bindings=plan.probes,
        overrides=overrides,
    )
    verdict = _verdict(outcomes)
    result: dict[str, object] = {
        "scenario": scenario_id,
        "assertion": assertion,
        "classification": plan.classification,
        "verdict": verdict,
        "probes": [
            {
                "probe_id": outcome.probe_id,
                "status": outcome.status,
                "summary": outcome.summary,
            }
            for outcome in outcomes
        ],
    }
    if verdict == probes.UNAVAILABLE:
        result["recorded"] = False
        result["reason"] = (
            "a bound probe could not observe its surface; the assertion stays "
            "missing rather than passing"
        )
        return result
    path = _record_verdict(
        checkout,
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        assertion=assertion,
        run_id=active,
        status=verdict,
        note=_summary_note(outcomes),
        detail=_detail(outcomes, extra=_binding_detail(record)),
        source="automated-probe",
        allow_external_harness=runtime_binding_file is not None,
    )
    result["recorded"] = True
    result["evidence"] = campaign._display_path(path, checkout, root)
    return result


def prepare_assertion(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    run_id: str | None,
    overrides: Mapping[str, str],
    runtime_binding_file: Path | None = None,
) -> dict[str, object]:
    expected, record = _gate(
        checkout, root, commit=commit, host=host,
        runtime_binding_file=runtime_binding_file,
    )
    scenario_id, _spec = campaign._assertion_contract(
        root,
        commit=expected,
        host=host,
        scenario=scenario,
        assertion=assertion,
    )
    plan = plan_for(scenario_id, assertion)
    if plan.classification != FAULT_INJECTION:
        raise RunnerError(
            f"{scenario_id}/{assertion} is {plan.classification} and has no "
            "prepare/operator-action/verify cycle"
        )
    active = _require_active_run(
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        run_id=run_id,
    )
    outcomes = run_bindings(
        checkout,
        root,
        commit=expected,
        record=record,
        scenario=scenario_id,
        assertion=assertion,
        run_id=active,
        bindings=plan.prepare,
        overrides=overrides,
    )
    detail = _detail(
        outcomes,
        extra={
            **_binding_detail(record),
            "prepare_verdict": _verdict(outcomes),
            "harness_performed_fault": False,
        },
    )
    prepare_id, path = campaign.record_preparation(
        checkout,
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        assertion=assertion,
        operator_action=plan.operator_action,
        detail=detail,
        allow_external_harness=runtime_binding_file is not None,
    )
    return {
        "scenario": scenario_id,
        "assertion": assertion,
        "prepare_id": prepare_id,
        "evidence": campaign._display_path(path, checkout, root),
        "prepare_probes": [
            {
                "probe_id": outcome.probe_id,
                "status": outcome.status,
                "summary": outcome.summary,
            }
            for outcome in outcomes
        ],
        "operator_action": plan.operator_action,
        "reviewed_helper": plan.reviewed_helper,
        "harness_performs_this_action": False,
        "next_command": (
            "python -m scripts.acceptance.v1_physical_runner action"
            f" --commit {expected} --host {host} --scenario {scenario_id}"
            f" --assertion {assertion} --prepare-id {prepare_id}"
            ' --note "<what you actually did>"'
        ),
    }


def record_action(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    prepare_id: str,
    note: str,
    runtime_binding_file: Path | None = None,
) -> dict[str, object]:
    expected, _record = _gate(
        checkout, root, commit=commit, host=host,
        runtime_binding_file=runtime_binding_file,
    )
    scenario_id = campaign.require_scenario(scenario)
    path = campaign.record_operator_action(
        checkout,
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        assertion=assertion,
        prepare_id=prepare_id,
        note=note,
        allow_external_harness=runtime_binding_file is not None,
    )
    return {
        "scenario": scenario_id,
        "assertion": assertion,
        "prepare_id": campaign.require_prepare_id(prepare_id),
        "evidence": campaign._display_path(path, checkout, root),
        "next_command": (
            "python -m scripts.acceptance.v1_physical_runner verify"
            f" --commit {expected} --host {host} --scenario {scenario_id}"
            f" --assertion {assertion} --prepare-id {prepare_id}"
        ),
    }


def verify_assertion(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    prepare_id: str,
    run_id: str | None,
    overrides: Mapping[str, str],
    runtime_binding_file: Path | None = None,
) -> dict[str, object]:
    expected, record = _gate(
        checkout, root, commit=commit, host=host,
        runtime_binding_file=runtime_binding_file,
    )
    scenario_id, _spec = campaign._assertion_contract(
        root,
        commit=expected,
        host=host,
        scenario=scenario,
        assertion=assertion,
    )
    plan = plan_for(scenario_id, assertion)
    if plan.classification != FAULT_INJECTION:
        raise RunnerError(
            f"{scenario_id}/{assertion} is {plan.classification} and has no "
            "prepare/operator-action/verify cycle"
        )
    identifier = campaign.require_prepare_id(prepare_id)
    preparation = campaign.find_preparation(
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        assertion=assertion,
        prepare_id=identifier,
    )
    action = campaign.find_operator_action(
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        assertion=assertion,
        prepare_id=identifier,
    )
    if campaign.parse_time(str(action["recorded_at"])) < campaign.parse_time(
        str(preparation["recorded_at"])
    ):
        raise RunnerError(
            "the recorded operator action predates its preparation"
        )
    active = _require_active_run(
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        run_id=run_id,
    )
    outcomes = run_bindings(
        checkout,
        root,
        commit=expected,
        record=record,
        scenario=scenario_id,
        assertion=assertion,
        run_id=active,
        bindings=plan.verify,
        overrides=overrides,
    )
    verdict = _verdict(outcomes)
    result: dict[str, object] = {
        "scenario": scenario_id,
        "assertion": assertion,
        "classification": plan.classification,
        "prepare_id": identifier,
        "verdict": verdict,
        "probes": [
            {
                "probe_id": outcome.probe_id,
                "status": outcome.status,
                "summary": outcome.summary,
            }
            for outcome in outcomes
        ],
    }
    if verdict == probes.UNAVAILABLE:
        result["recorded"] = False
        result["reason"] = (
            "a verify probe could not observe its surface; the fault-injection "
            "case stays open rather than passing"
        )
        return result
    path = _record_verdict(
        checkout,
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        assertion=assertion,
        run_id=active,
        status=verdict,
        note=_summary_note(outcomes),
        detail=_detail(
            outcomes,
            extra={
                **_binding_detail(record),
                "prepare_id": identifier,
                "operator_action_recorded_at": str(action["recorded_at"]),
                "harness_performed_fault": False,
            },
        ),
        source="operator-fault-injection-verify",
        allow_external_harness=runtime_binding_file is not None,
    )
    result["recorded"] = True
    result["evidence"] = campaign._display_path(path, checkout, root)
    return result


def run_scenario(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    run_id: str | None,
    overrides: Mapping[str, str],
    runtime_binding_file: Path | None = None,
) -> dict[str, object]:
    """Run every automated assertion this host is allowed to close."""

    expected, record = _gate(
        checkout, root, commit=commit, host=host,
        runtime_binding_file=runtime_binding_file,
    )
    scenario_id = campaign.require_scenario(scenario)
    spec = campaign.SCENARIOS[scenario_id]
    host_os = str(record["os_category"])
    executed: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    for assertion, plan in PLANS[scenario_id].items():
        required_os = spec.assertion_os.get(assertion)
        if required_os is not None and required_os != host_os:
            skipped.append(
                {
                    "assertion": assertion,
                    "reason": f"assertion belongs to {required_os} evidence",
                }
            )
            continue
        if plan.classification != AUTOMATED:
            skipped.append(
                {
                    "assertion": assertion,
                    "reason": f"{plan.classification} lane",
                }
            )
            continue
        missing = sorted(plan.required_options() - set(overrides))
        if missing:
            skipped.append(
                {
                    "assertion": assertion,
                    "reason": (
                        "needs operator-supplied probe options: "
                        + ", ".join(f"--option {name}=<value>" for name in missing)
                    ),
                }
            )
            continue
        executed.append(
            probe_assertion(
                checkout,
                root,
                commit=expected,
                host=host,
                scenario=scenario_id,
                assertion=assertion,
                run_id=run_id,
                overrides=overrides,
                runtime_binding_file=runtime_binding_file,
            )
        )
    return {
        "scenario": scenario_id,
        "host_id": str(record["host_id"]),
        "os_category": host_os,
        "profile": campaign.host_profile_of(record),
        "executed": executed,
        "not_run_here": skipped,
    }


def sample(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    label: str,
    run_id: str | None,
    runtime_binding_file: Path | None = None,
) -> dict[str, object]:
    """Record one resource sample enriched with the soak series P12 requires."""

    expected, record = _gate(
        checkout, root, commit=commit, host=host,
        runtime_binding_file=runtime_binding_file,
    )
    scenario_id = campaign.require_scenario(scenario)
    binding = record.get("__runtime_binding")
    bound = binding if isinstance(binding, runtime_binding.RuntimeBinding) else None
    if scenario_id == "P12" and (
        bound is None or bound.data_root is None or bound.results_root is None
    ):
        raise RunnerError(
            "P12 resource samples require --runtime-binding with both "
            "runtime.data_root and runtime.results_root"
        )
    extras = probes.collect_sample_extras(checkout, bound)
    path = campaign.sample_resources(
        checkout,
        root,
        commit=expected,
        host=host,
        scenario=scenario_id,
        label=label,
        run_id=run_id,
        extras=extras,
        allow_external_harness=runtime_binding_file is not None,
        # The bound runtime surfaces, not this harness checkout, are what the
        # soak series has to measure.
        data_root=bound.data_root if bound is not None else None,
        results_root=bound.results_root if bound is not None else None,
    )
    return {
        "scenario": scenario_id,
        "evidence": campaign._display_path(path, checkout, root),
        "series": sorted(extras),
    }


# --------------------------------------------------------------------------
# Machine-readable progress report
# --------------------------------------------------------------------------


def _selected_packets(
    root: Path,
    scenario: str,
    *,
    commit: str,
) -> tuple[list[dict[str, object]], dict[str, object] | None]:
    """Return the packets a report may reason about for one scenario.

    Timed scenarios are reduced to the single strict session that would be used
    for the release decision, so a report can never show a PASS that strict
    validation would reject.
    """

    packets = campaign.read_packets(root, scenario, expected_commit=commit)
    spec = campaign.SCENARIOS[scenario]
    if not spec.minimum_elapsed_seconds:
        return packets, None
    sessions = strict._session_statuses(packets, spec)
    if not sessions:
        return [], None
    passing = [item for item in sessions if bool(item["passed"])]
    pool = passing or sessions
    best = max(
        pool,
        key=lambda item: (float(item["elapsed_seconds"]), int(item["sample_count"])),
    )
    run_id = str(best["run_id"])
    begin = next(
        packet
        for packet in packets
        if packet.get("kind") == "begin" and packet.get("run_id") == run_id
    )
    finish = next(
        packet
        for packet in packets
        if packet.get("kind") == "finish" and packet.get("run_id") == run_id
    )
    started = campaign.parse_time(str(begin["recorded_at"]))
    ended = campaign.parse_time(str(finish["recorded_at"]))
    bound = [
        packet
        for packet in packets
        if packet.get("run_id") == run_id
        and packet.get("host_fingerprint") == begin.get("host_fingerprint")
        and started <= campaign.parse_time(str(packet["recorded_at"])) <= ended
    ]
    return bound, best


def _latest(
    packets: Sequence[Mapping[str, object]],
    value_key: str,
) -> dict[str, dict[str, object]]:
    latest: dict[str, dict[str, object]] = {}
    for packet in packets:
        name = packet.get(value_key)
        if not isinstance(name, str):
            continue
        current = latest.get(name)
        if current is None or str(packet.get("recorded_at", "")) > str(
            current.get("recorded_at", "")
        ):
            latest[name] = dict(packet)
    return latest


def _next_step(
    plan: AssertionPlan,
    *,
    state: str,
    commit: str,
    host: str | None,
    scenario: str,
    assertion: str,
    prepare_id: str | None,
    action_recorded: bool,
) -> str:
    target = host or "<host>"
    base = "python -m scripts.acceptance.v1_physical_runner"
    common = (
        f" --commit {commit} --host {target} --scenario {scenario}"
        f" --assertion {assertion}"
    )
    if state in {STATE_PASS, STATE_NOT_APPLICABLE}:
        return ""
    required = "".join(
        f" --option {name}=<value>" for name in sorted(plan.required_options())
    )
    if plan.classification == AUTOMATED:
        return f"{base} probe{common}{required}"
    if plan.classification == HUMAN:
        return (
            "python -m scripts.acceptance.v1_physical_campaign observe"
            f"{common} --status <pass|fail|not-applicable> --note \"<observation>\""
        )
    if prepare_id is None:
        return f"{base} prepare{common}{required}"
    if not action_recorded:
        return (
            f"{base} action{common} --prepare-id {prepare_id}"
            ' --note "<what you actually did>"'
        )
    return f"{base} verify{common} --prepare-id {prepare_id}{required}"


def report(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str | None = None,
    runtime_binding_file: Path | None = None,
) -> dict[str, object]:
    expected = campaign.require_commit(commit)
    bound = None
    if runtime_binding_file is not None:
        if host is None:
            raise RunnerError("--runtime-binding requires --host for report")
        bound = runtime_binding.load(
            runtime_binding_file,
            host_id=host,
            target_candidate_sha=expected,
        )
        campaign.bind_harness(
            root,
            commit=expected,
            harness_sha=bound.acceptance_harness_sha,
        )
    campaign.load_campaign(
        checkout,
        root,
        expected,
        verify_checkout_identity=bound is None,
    )
    hosts = [
        campaign._load_json(path)
        for path in sorted((root / "hosts").glob("*.json"))
    ]
    totals = dict.fromkeys(REPORT_STATES, 0)
    lanes: dict[str, dict[str, int]] = {
        lane: dict.fromkeys(REPORT_STATES, 0)
        for lane in (AUTOMATED, FAULT_INJECTION, HUMAN)
    }
    scenarios: list[dict[str, object]] = []
    for scenario_id, spec in campaign.SCENARIOS.items():
        packets, session = _selected_packets(root, scenario_id, commit=expected)
        assertions = _latest(packets, "assertion")
        prepared = _latest(
            [item for item in packets if item.get("kind") == "prepare"],
            "prepare_for",
        )
        actioned = {
            str(item.get("prepare_id"))
            for item in packets
            if item.get("kind") == "operator-action"
        }
        rows: list[dict[str, object]] = []
        for assertion, plan in PLANS[scenario_id].items():
            packet = assertions.get(assertion)
            preparation = prepared.get(assertion)
            prepare_id = (
                str(preparation["prepare_id"]) if preparation is not None else None
            )
            action_recorded = prepare_id is not None and prepare_id in actioned
            recorded_status = packet.get("status") if packet else None
            if recorded_status == "pass":
                state = STATE_PASS
            elif recorded_status == "not-applicable" and assertion in spec.allow_na:
                state = STATE_NOT_APPLICABLE
            elif recorded_status in {"fail", "not-applicable"}:
                state = STATE_FAIL
            elif preparation is not None:
                state = STATE_READY
            else:
                state = STATE_MISSING
            totals[state] += 1
            lanes[plan.classification][state] += 1
            rows.append(
                {
                    "assertion": assertion,
                    "assertion_text": spec.assertions[assertion],
                    "classification": plan.classification,
                    "state": state,
                    "required_os": spec.assertion_os.get(assertion),
                    "allow_not_applicable": assertion in spec.allow_na,
                    "recorded_status": recorded_status,
                    "recorded_at": packet.get("recorded_at") if packet else None,
                    "recorded_by": packet.get("source") if packet else None,
                    "host_id": packet.get("host_id") if packet else None,
                    "prepare_id": prepare_id,
                    "operator_action_recorded": action_recorded,
                    "operator_action": plan.operator_action,
                    "required_options": sorted(plan.required_options()),
                    "rationale": plan.rationale,
                    "next_step": _next_step(
                        plan,
                        state=state,
                        commit=expected,
                        host=host,
                        scenario=scenario_id,
                        assertion=assertion,
                        prepare_id=prepare_id,
                        action_recorded=action_recorded,
                    ),
                }
            )
        scenario_row: dict[str, object] = {
            "scenario": scenario_id,
            "title": spec.title,
            "timed": bool(spec.minimum_elapsed_seconds),
            "required_elapsed_seconds": spec.minimum_elapsed_seconds,
            "required_samples": spec.minimum_samples,
            "assertions": rows,
            "complete": all(
                row["state"] in {STATE_PASS, STATE_NOT_APPLICABLE} for row in rows
            ),
        }
        if spec.minimum_elapsed_seconds:
            scenario_row["strict_session"] = session
            scenario_row["complete"] = bool(
                scenario_row["complete"] and session and bool(session["passed"])
            )
        scenarios.append(scenario_row)
    return {
        "schema": REPORT_SCHEMA,
        "candidate_sha": expected,
        "generated_at": campaign.utc_now(),
        "hosts": [
            {
                "host_id": item.get("host_id"),
                "os_category": item.get("os_category"),
                "profile": campaign.host_profile_of(item),
                "role": item.get("role"),
            }
            for item in hosts
        ],
        "lane_totals": lane_counts(),
        "state_totals": totals,
        "state_totals_by_lane": lanes,
        "scenarios": scenarios,
        "complete": all(bool(item["complete"]) for item in scenarios),
    }


# --------------------------------------------------------------------------
# Command line
# --------------------------------------------------------------------------


def _add_target(parser: argparse.ArgumentParser, *, assertion: bool = True) -> None:
    parser.add_argument("--commit", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--scenario", required=True)
    if assertion:
        parser.add_argument("--assertion", required=True)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="override a declared probe option (checked-in keys only)",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the checked-in Federation v1 P01-P12 physical probes and record "
            "their verdicts through the existing campaign evidence API."
        )
    )
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument(
        "--evidence-root",
        type=Path,
        default=Path("evidence/v1-physical"),
    )
    parser.add_argument(
        "--runtime-binding",
        type=Path,
        help=(
            "ignored local manifest naming the deployed Compose/native runtime; "
            "never copied into portable evidence"
        ),
    )
    sub = parser.add_subparsers(dest="command_name", required=True)

    classify = sub.add_parser(
        "classify",
        help="print the checked-in automation lane for every P01-P12 assertion",
    )
    classify.add_argument("--scenario")

    sub.add_parser("probes", help="list the checked-in probes")

    _add_target(sub.add_parser("probe", help="run one automated assertion"))
    _add_target(
        sub.add_parser("scenario", help="run every automated assertion for a scenario"),
        assertion=False,
    )
    _add_target(sub.add_parser("prepare", help="stage one fault-injection case"))

    action = sub.add_parser("action", help="attest the explicit operator fault action")
    action.add_argument("--commit", required=True)
    action.add_argument("--host", required=True)
    action.add_argument("--scenario", required=True)
    action.add_argument("--assertion", required=True)
    action.add_argument("--prepare-id", required=True)
    action.add_argument("--note", required=True)

    verify = sub.add_parser("verify", help="verify one fault-injection consequence")
    _add_target(verify)
    verify.add_argument("--prepare-id", required=True)

    sample_parser = sub.add_parser(
        "sample",
        help="record a resource sample with the P12 soak series attached",
    )
    _add_target(sample_parser, assertion=False)
    sample_parser.add_argument("--label", default="resource-sample")

    report_parser = sub.add_parser("report", help="machine-readable campaign progress")
    report_parser.add_argument("--commit", required=True)
    report_parser.add_argument("--host")

    args = parser.parse_args(argv)
    checkout = args.checkout.resolve()
    root = args.evidence_root
    if not root.is_absolute():
        root = checkout / root
    binding_file = args.runtime_binding
    if binding_file is not None and not binding_file.is_absolute():
        binding_file = checkout / binding_file

    try:
        overrides = probes.parse_options(getattr(args, "option", []) or [])
        if args.command_name == "classify":
            summary = classification_summary()
            if args.scenario:
                scenario_id = campaign.require_scenario(args.scenario)
                result: object = {scenario_id: summary[scenario_id]}
            else:
                result = {"lane_totals": lane_counts(), "scenarios": summary}
        elif args.command_name == "probes":
            result = {
                probe_id: {
                    "title": spec.title,
                    "os_category": spec.os_category,
                    "profiles": sorted(spec.profiles),
                    "options": sorted(spec.options),
                    "reads_campaign_evidence": spec.reads_evidence,
                }
                for probe_id, spec in sorted(probes.PROBES.items())
            }
        elif args.command_name == "probe":
            result = probe_assertion(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                assertion=args.assertion,
                run_id=args.run_id,
                overrides=overrides,
                runtime_binding_file=binding_file,
            )
        elif args.command_name == "scenario":
            result = run_scenario(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                run_id=args.run_id,
                overrides=overrides,
                runtime_binding_file=binding_file,
            )
        elif args.command_name == "prepare":
            result = prepare_assertion(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                assertion=args.assertion,
                run_id=args.run_id,
                overrides=overrides,
                runtime_binding_file=binding_file,
            )
        elif args.command_name == "action":
            result = record_action(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                assertion=args.assertion,
                prepare_id=args.prepare_id,
                note=args.note,
                runtime_binding_file=binding_file,
            )
        elif args.command_name == "verify":
            result = verify_assertion(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                assertion=args.assertion,
                prepare_id=args.prepare_id,
                run_id=args.run_id,
                overrides=overrides,
                runtime_binding_file=binding_file,
            )
        elif args.command_name == "sample":
            result = sample(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                label=args.label,
                run_id=args.run_id,
                runtime_binding_file=binding_file,
            )
        else:
            result = report(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                runtime_binding_file=binding_file,
            )
    except (
        RunnerError,
        probes.ProbeError,
        campaign.CampaignError,
        OSError,
        ValueError,
    ) as exc:
        _print(
            {
                "error": campaign.redact_text(str(exc), cwd=checkout),
                "recorded": False,
            }
        )
        return 2

    _print(result)
    if args.command_name in {"probe", "verify"}:
        return 0 if result.get("verdict") == probes.PASS else 1
    if args.command_name == "scenario":
        executed = list(result.get("executed", []))
        return 0 if all(item.get("verdict") == probes.PASS for item in executed) else 1
    if args.command_name == "report":
        return 0 if bool(result.get("complete")) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
