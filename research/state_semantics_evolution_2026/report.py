"""Render a research report solely from approved, measured JSON summaries.

Run with --measurements MEASUREMENTS.json --manifest DATASET_MANIFEST.json
--tests TEST_RESULTS.json --output EXPERIMENT_RESULTS.md. No telemetry is read.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _n(value: Any, decimals: int = 3) -> str:
    return "n/a" if value is None else f"{value:.{decimals}f}"


def _percent(numerator: float, denominator: float) -> str:
    return "n/a" if not denominator else f"{100 * numerator / denominator:.3f}%"


def _table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    return ["| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
            *("| " + " | ".join(str(cell).replace("|", "\\|") for cell in row) + " |"
              for row in rows), ""]


def _counts(counter: Counter) -> str:
    return "; ".join(f"{key}={value}" for key, value in sorted(counter.items())) or "none"


def render_report(measurements: dict, manifest: dict, tests: dict,
                  b0_sensitivity: dict | None = None, ci: dict | None = None) -> str:
    measured_windows = measurements["windows"]
    source_windows = manifest["windows"]
    if len(measured_windows) != len(source_windows):
        raise ValueError("Manifest and measurements window counts differ")
    pairs = []
    indices = set()
    for position, measured in enumerate(measured_windows):
        index = measured.get("window_index", position)
        if index in indices or not 0 <= index < len(source_windows):
            raise ValueError("Duplicate or invalid measured window index")
        indices.add(index)
        source = source_windows[index]
        if measured["records"] != source["features"]["records"]:
            raise ValueError("Manifest and measured record counts differ")
        pairs.append((source, measured))

    lines = ["# Executed operational-state evolution results", "",
             f"Measurement timestamp (UTC): `{measurements.get('created_at_utc', 'not recorded')}`. "
             "This report is generated from the measured JSON summaries; it makes no claim of physical ground truth.", "",
             "## Executed scope", "",
             f"Evaluated **{len(pairs)} source windows**, **{sum(w['records'] for _, w in pairs):,} original rows**, "
             f"and **{len({s['machine_hash'] for s, _ in pairs})} pseudonymous machines**. "
             f"Source archive SHA256: `{manifest['archive_sha256']}` "
             f"({manifest['archive_size_bytes']:,} compressed bytes).", "",
             f"Sampling policy: {manifest['selection']['strategy']}. "
             f"Target: {manifest['selection']['windows_per_machine']} windows per machine and "
             f"{manifest['selection']['target_records']} records per block, retaining whole timestamp groups. "
             "A final positive-time block can be shorter than the record target.", "",
             f"Selection limitation: {manifest['selection']['bias']}. "
             "The same original source windows feed every scenario and mutant. Windows have strict timestamp separation "
             "within each machine; machines can overlap in clock time. Window counts are not independent statistical replications.", "",
             "The production classifier assesses **active, non-active and unclassified** operation. "
             "There are no independent physical labels, so these results cannot establish stable/unstable machining, "
             "fault-detection accuracy or actual production downtime.", "",
             "## Dataset and baseline coverage", "",
             f"JSONL members inventoried: **{len(manifest.get('members', [])):,}**. "
             f"Reader inventory: `{json.dumps(manifest.get('quality', {}), sort_keys=True)}`. "
             f"Schema inventory: `{json.dumps(manifest.get('schema_inventory', {}), sort_keys=True)}`.", ""]
    record_counts = [measured["records"] for _, measured in pairs]
    sufficient = sum(measured["baseline"].get("service_sufficient_data", False) for _, measured in pairs)
    zero_usable = sum(measured["baseline"]["usable_seconds"] == 0 for _, measured in pairs)
    short_tails = [(source["window_id"], measured["records"]) for source, measured in pairs
                   if measured["records"] < manifest["selection"]["target_records"]]
    lines += [f"Selected blocks contain **{min(record_counts) if record_counts else 0}–"
              f"{max(record_counts) if record_counts else 0} records**. "
              f"**{sufficient}/{len(pairs)}** windows satisfy the production sufficient-data criterion; "
              f"**{zero_usable}** windows have zero usable time. Zero usable time gives undefined class coverage, "
              "not evidence of downtime or perfect coverage. "
              + ("Short final blocks retained: " + "; ".join(f"`{window_id}` = {count} records" for window_id, count in short_tails) + "."
                 if short_tails else "No selected block is shorter than the target."), ""]
    groups = defaultdict(list)
    for source, measured in pairs:
        groups[source["machine_hash"]].append((source, measured))
    machine_rows = []
    for machine, group in sorted(groups.items()):
        baselines = [m["baseline"] for _, m in group]
        durations = {state: sum(b["durations_seconds"][state] for b in baselines)
                     for state in ("active", "non_active", "unclassified")}
        observed = sum(b["observed_seconds"] for b in baselines)
        usable = sum(b["usable_seconds"] for b in baselines)
        span = sum(b["positive_span_seconds"] for b in baselines)
        machine_rows.append([machine, len(group), sum(m["records"] for _, m in group),
                             *(_n(durations[k]) for k in ("active", "non_active", "unclassified")),
                             _percent(observed, usable), _percent(observed, span),
                             _n(sum(b["excluded"]["long_gap_seconds"] for b in baselines)),
                             _n(sum(b["excluded"]["subminimum_seconds"] for b in baselines)),
                             f"{sum(b.get('service_sufficient_data', False) for b in baselines)}/{len(group)}"])
    lines += _table(["Machine hash", "Windows", "Rows", "Active s", "Non-active s", "Unclassified s",
                     "Class coverage", "Wall-span coverage", "Excluded long-gap s", "Excluded short s",
                     "Sufficient-data windows"], machine_rows)
    lines += ["Class coverage = classified usable time / usable time. Wall-span coverage = classified time / "
              "positive elapsed span. These differ from production `continuity`, which divides usable by positive interval counts. "
              "Intervals outside [0.1,30] seconds are excluded; zero-length duplicate intervals have no duration. "
              "Unclassified usable time is kept distinct from excluded gap time. Final observations are not extrapolated.", ""]
    feature_totals = Counter()
    for source, _ in pairs:
        feature_totals.update(source["features"])
    condition_counts = Counter({
        key: feature_totals[key] for key in (
            "missing_execution", "unavailable_execution", "fallback_rows", "fallback_classified_rows",
            "threshold_sensitive_rows", "active_rows", "non_active_rows", "unclassified_rows",
            "state_transitions", "long_gaps", "duplicate_timestamp_records", "conflicting_state_timestamp_groups")
    })
    condition_counts["row_label_adjacency_changes"] = condition_counts.pop("state_transitions", 0)
    observable_boundaries = sum(len(measured["baseline"]["transition_boundaries"]) for _, measured in pairs)
    lines += ["Selected-window condition inventory (counts can overlap): " + _counts(condition_counts) + ".", "",
              f"The inventory's **{condition_counts['row_label_adjacency_changes']} row-label adjacency changes** "
              "include neighboring raw rows and timestamp ties. They are a sampling feature, not the temporal outcome metric. "
              f"The baseline measurement contains **{observable_boundaries} observable transition boundaries** under the "
              "declared final-state-per-timestamp and usable-interval convention.", ""]
    required = ("missing_execution", "unavailable_execution", "fallback_classified_rows", "threshold_sensitive_rows",
                "active_rows", "non_active_rows", "state_transitions", "long_gaps", "duplicate_timestamp_records")
    missing = [key for key in required if not feature_totals[key]]
    lines += [("Uncovered requested conditions: " + ", ".join(missing) + ". These are limitations of this historical selection.")
              if missing else "All requested condition categories have at least one selected historical witness; this does not establish representative sampling.", "",
              "## Controlled scenario comparisons", "",
              "Agreement below is pooled over compared original source rows. Duplicated records do not inflate the denominator. "
              "Duration deltas are signed sums across windows, so opposite changes can cancel; full per-window measurements "
              "and row traces remain in `MEASUREMENTS.json`. Transition counts report removed/added observable boundaries "
              "under the declared timestamp-tie and usable-interval convention.", ""]
    experiment_names = sorted({name for _, m in pairs for name in m["experiments"]})
    scenario_rows = []
    valid_verdicts = Counter()
    ambiguous_trials = 0
    for name in experiment_names:
        trials = [m["experiments"][name] for _, m in pairs if name in m["experiments"]]
        compared = sum(t.get("compared_rows", 0) for t in trials)
        changed = sum(t.get("changed_rows", 0) for t in trials)
        deltas = {s: sum(t.get("duration_deltas_seconds", {}).get(s, 0) for t in trials)
                  for s in ("active", "non_active", "unclassified")}
        removed = sum(t.get("transition_boundary_changes", {}).get("removed_count", 0) for t in trials)
        added = sum(t.get("transition_boundary_changes", {}).get("added_count", 0) for t in trials)
        duration_windows = sum(any(abs(v) > 1e-8 for v in t.get("duration_deltas_seconds", {}).values()) for t in trials)
        transition_windows = sum(any(t.get("transition_boundary_changes", {}).get(k, 0)
                                     for k in ("removed_count", "added_count")) for t in trials)
        verdicts = Counter(t["verdict"] for t in trials)
        scenario_rows.append([name, compared, changed, _percent(compared - changed, compared),
                              *(_n(deltas[s]) for s in ("active", "non_active", "unclassified")),
                              f"{removed}/{added}", f"{duration_windows}/{transition_windows}", _counts(verdicts)])
        for trial in trials:
            verdict = trial["verdict"]
            if verdict == "ambiguous_ties":
                ambiguous_trials += 1
            elif verdict != "not_activated":
                valid_verdicts[verdict] += 1
    lines += _table(["Scenario", "Compared rows", "Changed rows", "Pooled agreement", "Δ active s",
                     "Δ non-active s", "Δ unclassified s", "Transitions removed/added",
                     "Windows with duration/transition differences", "Verdicts"], scenario_rows)
    lines += ["E1–E3 test declared preservation. E4 permits changes only in masked execution rows. E5a preserves tie order "
              "and inserts an adjacent identical duplicate. E5b intentionally removes the usable interval crossed by the "
              "injected gap and moves later timestamps. E6 intentionally changes the fallback speed threshold from >100 "
              "to >150 rpm; expected changes are checked against the predefined mask. These expected changes are not defects.", "",
              f"The arbitrary timestamp-tie probe has **{ambiguous_trials} ambiguous trials**. Ambiguity does not imply an "
              "observed change in every such trial; the duration/transition window counts above distinguish actual sensitivity. "
              "Such trials have no unconditional "
              "preservation contract and are excluded from valid-control false-positive denominators. Their numerical differences "
              "remain reported in the scenario table and JSON.", ""]
    for name in ("E1", "E2", "E3", "E6"):
        trials = [m["experiments"][name] for _, m in pairs if name in m["experiments"]]
        changed = sum(t.get("changed_rows", 0) for t in trials)
        failures = sum(t.get("verdict") == "unexpected_difference" for t in trials)
        lines.append(f"- **{name}:** {changed:,} changed source-row classifications; {failures} unexpected-outcome window trials.")
    lines += ["", "## Injected defects and error accounting", "",
              "B1 has the same source rows, declared invariants and expected-change masks as all evaluated variants. "
              "Activation means the mutant caused an original-row classification difference. Detection means that B1 rejected "
              "its preservation contract using rows, durations, transitions or interval counts. A non-activated mutant trial "
              "is not an activated miss.", ""]
    defect_names = sorted({name for _, m in pairs for name in m["defects"]})
    defect_rows = []
    totals = Counter()
    type_totals = Counter()
    for name in defect_names:
        trials = [m["defects"][name] for _, m in pairs if name in m["defects"]]
        activated = sum(bool(t["activated"]) for t in trials)
        detected = sum(bool(t["detected"]) for t in trials)
        missed = sum(bool(t["activated"]) and not t["detected"] for t in trials)
        changed = sum(t.get("changed_rows", 0) for t in trials)
        defect_rows.append([name, len(trials), activated, detected, missed, len(trials) - activated, changed])
        totals.update(trials=len(trials), activated=activated, detected=detected, missed=missed,
                      not_activated=len(trials) - activated)
        type_totals.update(types=1, activated_types=int(activated > 0), detected_types=int(detected > 0),
                           completely_missed_activated_types=int(activated > 0 and detected == 0))
    lines += _table(["Defect type", "Window trials", "Activated", "Detected", "Activated misses",
                     "Not activated", "Changed source rows"], defect_rows)
    lines += [f"Defect-trial totals: **{totals['trials']} trials**, **{totals['activated']} activated**, "
              f"**{totals['detected']} detected**, **{totals['missed']} activated misses**, "
              f"**{totals['not_activated']} not activated**. Type totals: {type_totals['types']} injected types, "
              f"{type_totals['activated_types']} activated types, {type_totals['detected_types']} detected types, "
              f"{type_totals['completely_missed_activated_types']} completely missed activated types. "
              "These are selected sensitivity witnesses, not an estimate of defect detection in the population of real changes.", ""]
    valid_trials = sum(valid_verdicts.values())
    false_positives = valid_verdicts["unexpected_difference"]
    lines += [f"Valid-control rejection count: **{false_positives}/{valid_trials}** "
              f"({_percent(false_positives, valid_trials)}), excluding {ambiguous_trials} ambiguous tie trials "
              "and non-activated scenarios. Every rejection must be investigated as an unexpected valid-control outcome; "
              "the label does not establish that the harness, rather than the implementation, is correct. "
              f"Measured unexpected experiment outcomes: **{measurements.get('unexpected_experiment_outcomes', 'not recorded')}**.", "",
              "B0 consists of the unchanged production regression tests. The three research adapter mutants are outside their "
              "execution scope and are reported as unexercised. The shared unknown-as-non-active policy can be exercised by "
              "B0 using an isolated in-memory classifier mutation; its separate execution evidence appears below when supplied. "
              "B1 is conventional regression/differential/metamorphic "
              "testing. **B2 was not implemented** because no equal-information B1 limitation justified an additional procedure. "
              "Traceability and explicit expectations alone are not a novel testing method.", "",
              "## Per-window baseline and selection audit", ""]
    window_rows = []
    for source, measured in pairs:
        baseline = measured["baseline"]
        features = source["features"]
        strata = ";".join(f"{key}={features.get(key, 0)}" for key in (
            "missing_execution", "unavailable_execution", "fallback_classified_rows", "threshold_sensitive_rows"))
        window_rows.append([source["window_id"], measured["records"],
                            f"{source['start_utc']} / {source['end_utc']}",
                            _n(baseline["usable_seconds"]), _n(baseline["durations_seconds"]["unclassified"]),
                            _percent(baseline["observed_seconds"], baseline["usable_seconds"]),
                            baseline["excluded"]["long_gaps"], features["duplicate_timestamp_records"],
                            baseline["conflicting_state_timestamps"], baseline.get("service_sufficient_data", "not recorded"), strata])
    lines += _table(["Window ID", "Rows", "UTC start / end", "Usable s", "Unclassified s", "Class coverage",
                     "Long gaps", "Duplicate timestamp rows", "Conflicting timestamp groups", "Sufficient data", "Selection strata"], window_rows)
    lines += ["Each `MEASUREMENTS.json` window index resolves to the corresponding manifest window. A changed-row reference "
              "is a zero-based ordinal in that canonical, source-ordered window. The manifest provides source member-content "
              "and member-name hashes plus one-based inclusive source-line ranges, making the same input rows reconstructible "
              "on Nitro without publishing records or machine names. Per-difference before/after labels, duration deltas, "
              "added/removed boundaries and scenario labels are retained in the measured JSON.", "",
              "## Software, resources and test execution", ""]
    software = measurements.get("software", {})
    lines += [f"Experiment source commit: `{software.get('commit', 'not recorded')}`. "
              f"Tracked working-tree changes at execution: `{software.get('tracked_dirty', 'not recorded')}`. "
              f"Test-validation commit: `{tests.get('commit', 'not recorded')}`. "
              "Exact source SHA256 values are preserved in the measurement and dataset JSON; a later report-only commit "
              "does not retroactively change the executed source revision.", "",
              f"Python: `{software.get('python', 'not recorded').replace(chr(10), ' ')}`. "
              f"Platform: `{software.get('platform', 'not recorded')}`. "
              f"Dependencies: `{json.dumps(software.get('dependencies', {}), sort_keys=True)}`.", ""]
    resources = measurements.get("resources", {})
    lines += [f"Measured process costs: wall **{_n(resources.get('wall_seconds'))} s**, CPU "
              f"**{_n(resources.get('cpu_seconds'))} s**, peak RSS **{resources.get('peak_rss_kib_linux', 'not recorded')} KiB** "
              "(Linux `ru_maxrss`, including dataset selection and evaluation). One-host costs are descriptive; "
              "they establish neither capacity qualification nor scalability.", ""]
    for name, suite in tests.get("suites", {}).items():
        lines += [f"### Executed {name} tests", "",
                  f"Exit code: **{suite['exit_code']}**. Wall seconds: **{_n(suite.get('wall_seconds'))}**.", "",
                  "Exact argument vector:", "", "```json", json.dumps(suite["command"]), "```", "",
                  "Captured stdout:", "", "```text", suite.get("stdout", "").rstrip() or "(empty)", "```", "",
                  "Captured stderr:", "", "```text", suite.get("stderr", "").rstrip() or "(empty)", "```", ""]
    if b0_sensitivity is not None:
        evidence = b0_sensitivity.get("pytest_evidence", {})
        errors = sum(len(evidence.get(key, [])) for key in
                     ("collection_errors", "setup_teardown_errors", "other_call_failures"))
        failures = len(evidence.get("assertion_failures", []))
        lines += ["### Executed B0 shared-policy sensitivity probe", "",
                  f"Mutant: `{b0_sensitivity.get('mutant', 'not recorded')}`. Source commit: "
                  f"`{b0_sensitivity.get('source_commit', 'not recorded')}`. "
                  f"Exit code: **{b0_sensitivity.get('exit_code', 'not recorded')}**; "
                  f"captured assertion failures: **{failures}**; collection/setup/teardown/other call errors: **{errors}**. "
                  f"The probe reports assertion-based detection: **{b0_sensitivity.get('assertion_failure_detection', 'not recorded')}**. "
                  "A nonzero exit caused by environmental errors alone would not count as defect detection.", "",
                  "Research adapters outside B0 execution scope: "
                  + ", ".join(f"`{name}`" for name in b0_sensitivity.get("unexercised_research_adapters", [])) + ". "
                  "These are not activated misses and do not establish an advantage over conventional testing.", "",
                  "Exact probe argument vector:", "", "```json",
                  json.dumps(b0_sensitivity.get("command_argv", [])), "```", "",
                  "Captured probe stdout:", "", "```text", b0_sensitivity.get("stdout", "").rstrip() or "(empty)", "```", "",
                  "Captured probe stderr:", "", "```text", b0_sensitivity.get("stderr", "").rstrip() or "(empty)", "```", ""]
    if ci is not None:
        lines += ["### Self-hosted CI evidence", "",
                  "The supplied CI evidence records runner identity, source revision, test output, final conclusion and cleanup. "
                  "CI executes synthetic/regression tests and does not receive historical telemetry. A successful CI test suite "
                  "is separate from the Nitro historical replay measurements.", "", "```json",
                  json.dumps(ci, indent=2, ensure_ascii=False), "```", ""]
    lines += ["### Failure history and operational limits", "",
              "Initial workstation pytest execution encountered permission errors in the default Windows pytest temporary "
              "directory; explicit job-local temporary directories were used for verification. The initial remote minimal "
              "environment produced 10 passing research tests and six production setup errors because Flask was missing. "
              "Installing the full pinned dependency set under Nitro's default Python 3.14 failed on the pinned psycopg "
              "dependency. The existing Python 3.12.13 interpreter supported a separate research venv and full requirements; "
              "the complete relevant suite then executed with the final outcomes captured above. A local validation-wrapper "
              "attempt also produced 28 setup errors because its explicit temporary directory's parent did not exist; creating "
              "that parent resolved the wrapper error. These were dependency/environment failures, not evidence of semantic "
              "regressions or successful mutant detection. No expected assertion outcome was changed to make tests pass. "
              "The final JSON test capture "
              "above is the executed evidence for the reported suites; additional initial/final validation artifacts document the "
              "broader audit history where supplied. No unavailable test is counted as passed.", "",
              "Historical evaluation runs in the isolated Nitro research checkout and virtual environment. Source archive and "
              "telemetry are read-only. No raw telemetry is transferred to the workstation or GitHub. Production files, services "
              "and recorder configuration are outside the experiment changes. See `SCIENTIFIC_ASSESSMENT.md` for the bounded "
              "scientific interpretation and literature comparison.", ""]
    return "\n".join(line.rstrip() for line in "\n".join(lines).split("\n"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurements", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--tests", type=Path, required=True)
    parser.add_argument("--b0-sensitivity", type=Path)
    parser.add_argument("--ci", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    measurements = json.loads(args.measurements.read_text(encoding="utf-8"))
    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    tests = json.loads(args.tests.read_text(encoding="utf-8"))
    b0_sensitivity = json.loads(args.b0_sensitivity.read_text(encoding="utf-8")) if args.b0_sensitivity else None
    ci = json.loads(args.ci.read_text(encoding="utf-8")) if args.ci else None
    expected = measurements.get("manifest_sha256")
    if expected and hashlib.sha256(manifest_bytes).hexdigest() != expected:
        parser.error("Manifest bytes do not match the manifest frozen for these measurements")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_report(measurements, manifest, tests, b0_sensitivity, ci), encoding="utf-8")
    print(f"report_written={args.output} windows={len(measurements['windows'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
