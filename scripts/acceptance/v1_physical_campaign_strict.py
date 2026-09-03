"""Strict timed-evidence layer for the Federation v1 physical campaign.

The base campaign harness owns candidate/host provenance, redaction, resource
sampling, privacy sealing, and the P01-P12 assertion contract.  This layer closes
one additional evidence-integrity requirement for P07 and P12: every assertion
used to pass a timed scenario must belong to the same host/run and fall inside
that run's begin/finish interval.  Evidence from different timed sessions is
never combined to manufacture a pass.

Use ``timed-observe`` or ``timed-run`` for P07/P12 assertions, then use this
module's ``status`` and ``validate`` commands for the release decision.  Other
campaign commands remain provided by ``v1_physical_campaign``.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path

from scripts.acceptance import v1_physical_campaign as campaign

TIMED_SCENARIOS = frozenset(
    scenario
    for scenario, spec in campaign.SCENARIOS.items()
    if spec.minimum_elapsed_seconds > 0
)


def _timed_spec(scenario: str) -> tuple[str, campaign.ScenarioSpec]:
    scenario_id = campaign.require_scenario(scenario)
    spec = campaign.SCENARIOS[scenario_id]
    if scenario_id not in TIMED_SCENARIOS:
        raise campaign.CampaignError(
            f"{scenario_id} is not a timed scenario; use the base campaign command"
        )
    return scenario_id, spec


def _require_active_run(
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    run_id: str,
) -> dict[str, object]:
    if not run_id:
        raise campaign.CampaignError("timed evidence requires --run-id")
    return campaign._active_session(
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        run_id=run_id,
    )


def timed_observe(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    run_id: str,
    assertion: str,
    status: str,
    note: str,
    detail: Mapping[str, object] | None = None,
    source: str = "operator",
) -> Path:
    campaign.load_campaign(checkout, root, commit)
    scenario_id, spec = _timed_spec(scenario)
    campaign._assertion_contract(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        assertion=assertion,
    )
    _require_active_run(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        run_id=run_id,
    )
    if status not in {"pass", "fail", "not-applicable"}:
        raise campaign.CampaignError(
            "observation status must be pass, fail, or not-applicable"
        )
    if status == "not-applicable" and assertion not in spec.allow_na:
        raise campaign.CampaignError(
            f"{scenario_id}/{assertion} cannot be marked not-applicable"
        )
    packet = campaign.base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="assertion",
    )
    packet.update(
        {
            "run_id": run_id,
            "assertion": assertion,
            "assertion_text": spec.assertions[assertion],
            "status": status,
            "note": campaign.redact_text(note, cwd=checkout),
            "source": campaign.redact_text(source, cwd=checkout) or "operator",
        }
    )
    redacted = campaign.sanitize_detail(detail, cwd=checkout)
    if redacted is not None:
        packet["detail"] = redacted
    return campaign.write_packet(root, packet)


def timed_run(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    run_id: str,
    assertion: str,
    label: str,
    command: list[str],
    expected_exit: int,
    timeout: float,
) -> Path:
    campaign.load_campaign(checkout, root, commit)
    scenario_id, spec = _timed_spec(scenario)
    campaign._assertion_contract(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        assertion=assertion,
    )
    _require_active_run(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        run_id=run_id,
    )
    if command and command[0] == "--":
        command = command[1:]
    returncode, duration, output = campaign._run(
        command,
        checkout=checkout,
        timeout=timeout,
    )
    passed = returncode == expected_exit
    packet = campaign.base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="command",
    )
    packet.update(
        {
            "run_id": run_id,
            "label": campaign.redact_text(label, cwd=checkout),
            "command": campaign.redact_text(" ".join(command), cwd=checkout),
            "expected_exit": expected_exit,
            "returncode": returncode,
            "duration_seconds": duration,
            "output_tail": campaign.redact_text(output, cwd=checkout),
            "passed": passed,
            "assertion": assertion,
            "assertion_text": spec.assertions[assertion],
            "status": "pass" if passed else "fail",
        }
    )
    return campaign.write_packet(root, packet)


def _session_statuses(
    packets: list[dict[str, object]],
    spec: campaign.ScenarioSpec,
) -> list[dict[str, object]]:
    begins: dict[str, dict[str, object]] = {}
    for packet in packets:
        if packet.get("kind") != "begin" or not packet.get("run_id"):
            continue
        run_id = str(packet["run_id"])
        if run_id in begins:
            raise campaign.CampaignError("timed run has more than one begin packet")
        begins[run_id] = packet

    sessions: list[dict[str, object]] = []
    seen_finishes: set[str] = set()
    for finish in packets:
        if finish.get("kind") != "finish" or not finish.get("run_id"):
            continue
        run_id = str(finish["run_id"])
        if run_id in seen_finishes:
            raise campaign.CampaignError("timed run has more than one finish packet")
        seen_finishes.add(run_id)
        begin = begins.get(run_id)
        if begin is None:
            raise campaign.CampaignError("timed finish has no matching begin")
        if finish.get("host_fingerprint") != begin.get("host_fingerprint"):
            raise campaign.CampaignError("timed begin/finish host provenance differs")
        started = campaign.parse_time(str(begin["recorded_at"]))
        ended = campaign.parse_time(str(finish["recorded_at"]))
        elapsed = (ended - started).total_seconds()
        if elapsed < 0:
            raise campaign.CampaignError("timed session finishes before it starts")

        run_packets = [
            packet
            for packet in packets
            if packet.get("run_id") == run_id
            and packet.get("host_fingerprint") == begin.get("host_fingerprint")
            and started
            <= campaign.parse_time(str(packet["recorded_at"]))
            <= ended
        ]
        samples = [packet for packet in run_packets if packet.get("kind") == "sample"]
        latest: dict[str, dict[str, object]] = {}
        for packet in run_packets:
            assertion = packet.get("assertion")
            if not isinstance(assertion, str) or assertion not in spec.assertions:
                continue
            current = latest.get(assertion)
            if current is None or str(packet["recorded_at"]) > str(current["recorded_at"]):
                latest[assertion] = packet

        missing: list[str] = []
        failing: list[str] = []
        for assertion in spec.assertions:
            packet = latest.get(assertion)
            if packet is None:
                missing.append(assertion)
                continue
            status = packet.get("status")
            if status == "pass":
                continue
            if status == "not-applicable" and assertion in spec.allow_na:
                continue
            failing.append(assertion)

        duration_ok = elapsed >= spec.minimum_elapsed_seconds
        samples_ok = len(samples) >= spec.minimum_samples
        sessions.append(
            {
                "run_id": run_id,
                "host_id": begin.get("host_id"),
                "elapsed_seconds": round(elapsed, 3),
                "sample_count": len(samples),
                "missing_assertions": missing,
                "failing_assertions": failing,
                "duration_ok": duration_ok,
                "samples_ok": samples_ok,
                "passed": not missing and not failing and duration_ok and samples_ok,
            }
        )
    return sessions


def strict_scenario_status(
    root: Path,
    scenario: str,
    *,
    expected_commit: str,
) -> dict[str, object]:
    scenario_id = campaign.require_scenario(scenario)
    spec = campaign.SCENARIOS[scenario_id]
    if scenario_id not in TIMED_SCENARIOS:
        return campaign.scenario_status(
            root,
            scenario_id,
            expected_commit=expected_commit,
        )
    packets = campaign.read_packets(
        root,
        scenario_id,
        expected_commit=expected_commit,
    )
    sessions = _session_statuses(packets, spec)
    passing = [session for session in sessions if bool(session["passed"])]
    if passing:
        best = max(
            passing,
            key=lambda item: (
                float(item["elapsed_seconds"]),
                int(item["sample_count"]),
            ),
        )
    elif sessions:
        best = max(
            sessions,
            key=lambda item: (
                float(item["elapsed_seconds"]),
                int(item["sample_count"]),
            ),
        )
    else:
        best = {
            "elapsed_seconds": 0.0,
            "sample_count": 0,
            "missing_assertions": list(spec.assertions),
            "failing_assertions": [],
            "passed": False,
        }
    return {
        "scenario": scenario_id,
        "title": spec.title,
        "passed": bool(best["passed"]),
        "missing_assertions": list(best["missing_assertions"]),
        "failing_assertions": list(best["failing_assertions"]),
        "elapsed_seconds": float(best["elapsed_seconds"]),
        "required_elapsed_seconds": spec.minimum_elapsed_seconds,
        "sample_count": int(best["sample_count"]),
        "required_samples": spec.minimum_samples,
        "timed_sessions": sessions,
        "strict_run_binding": True,
    }


def strict_validate(
    checkout: Path,
    root: Path,
    *,
    commit: str,
) -> dict[str, object]:
    base = campaign.validate_campaign(checkout, root, commit=commit)
    expected = campaign.require_commit(commit)
    statuses = [
        strict_scenario_status(root, scenario, expected_commit=expected)
        for scenario in campaign.SCENARIOS
    ]
    accepted = (
        bool(base["host_coverage_ok"])
        and bool(base["privacy_ok"])
        and all(bool(item["passed"]) for item in statuses)
    )
    return {
        **base,
        "accepted": accepted,
        "scenarios": statuses,
        "strict_timed_assertion_binding": True,
    }


def _print(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Strict P07/P12 run-bound evidence commands for Federation v1."
    )
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument(
        "--evidence-root",
        type=Path,
        default=Path("evidence/v1-physical"),
    )
    sub = parser.add_subparsers(dest="command_name", required=True)

    observation = sub.add_parser("timed-observe")
    observation.add_argument("--commit", required=True)
    observation.add_argument("--host", required=True)
    observation.add_argument("--scenario", choices=sorted(TIMED_SCENARIOS), required=True)
    observation.add_argument("--run-id", required=True)
    observation.add_argument("--assertion", required=True)
    observation.add_argument(
        "--status",
        choices=("pass", "fail", "not-applicable"),
        required=True,
    )
    observation.add_argument("--note", default="")

    run = sub.add_parser("timed-run")
    run.add_argument("--commit", required=True)
    run.add_argument("--host", required=True)
    run.add_argument("--scenario", choices=sorted(TIMED_SCENARIOS), required=True)
    run.add_argument("--run-id", required=True)
    run.add_argument("--assertion", required=True)
    run.add_argument("--label", required=True)
    run.add_argument("--expect-exit", type=int, default=0)
    run.add_argument("--timeout", type=float, default=300.0)
    run.add_argument("cmd", nargs=argparse.REMAINDER)

    status = sub.add_parser("status")
    status.add_argument("--commit", required=True)
    status.add_argument("--scenario")

    validate = sub.add_parser("validate")
    validate.add_argument("--commit", required=True)

    args = parser.parse_args(argv)
    checkout = args.checkout.resolve()
    root = args.evidence_root
    if not root.is_absolute():
        root = checkout / root

    try:
        if args.command_name == "timed-observe":
            result: object = str(
                timed_observe(
                    checkout,
                    root,
                    commit=args.commit,
                    host=args.host,
                    scenario=args.scenario,
                    run_id=args.run_id,
                    assertion=args.assertion,
                    status=args.status,
                    note=args.note,
                )
            )
        elif args.command_name == "timed-run":
            result = str(
                timed_run(
                    checkout,
                    root,
                    commit=args.commit,
                    host=args.host,
                    scenario=args.scenario,
                    run_id=args.run_id,
                    assertion=args.assertion,
                    label=args.label,
                    command=args.cmd,
                    expected_exit=args.expect_exit,
                    timeout=args.timeout,
                )
            )
        elif args.command_name == "status":
            campaign.load_campaign(checkout, root, args.commit)
            if args.scenario:
                result = strict_scenario_status(
                    root,
                    args.scenario,
                    expected_commit=args.commit,
                )
            else:
                result = {
                    scenario: strict_scenario_status(
                        root,
                        scenario,
                        expected_commit=args.commit,
                    )
                    for scenario in campaign.SCENARIOS
                }
        elif args.command_name == "validate":
            result = strict_validate(checkout, root, commit=args.commit)
        else:  # pragma: no cover
            raise campaign.CampaignError("unsupported strict command")
    except (campaign.CampaignError, OSError, ValueError) as exc:
        print(f"physical campaign strict check failed: {exc}", file=__import__("sys").stderr)
        return 2

    _print(result)
    if args.command_name == "validate" and not bool(result["accepted"]):
        return 1
    if args.command_name == "status":
        if args.scenario:
            return 0 if bool(result["passed"]) else 1
        return 0 if all(bool(item["passed"]) for item in result.values()) else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
