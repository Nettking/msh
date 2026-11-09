"""Conventional, equal-information metamorphic measurements for bounded replay.

All classifiers originate from the production function. The intentional E6
variant is a research-local AST clone with only ``rpm.gt(100)`` changed to
``rpm.gt(150)``; production source and module globals remain untouched.
Output contains aggregate measurements and ordinal traces, never input signals.
"""
from __future__ import annotations

import ast
import copy
import inspect
import math
import random
import time
from collections import Counter
from typing import Callable

import pandas as pd

from catalog.flask_app.services import digital_twin_service as production
from research.state_semantics_evolution_2026 import replay


def threshold_classifier(threshold: float) -> Callable:
    """Clone the actual rule, changing its sole spindle-speed comparison."""
    tree = ast.parse(inspect.getsource(production._classify_rows))
    changes = 0
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "rpm" and node.func.attr == "gt"
                and len(node.args) == 1 and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == 100):
            node.args[0] = ast.Constant(value=threshold)
            changes += 1
    if changes != 1:
        raise RuntimeError("Production rpm rule changed; review the E6 mutation")
    namespace = dict(vars(production))
    exec(compile(ast.fix_missing_locations(tree), "<research-threshold-variant>", "exec"), namespace)
    return namespace["_classify_rows"]


def _frame(records: list[dict], classifier: Callable = production._classify_rows) -> pd.DataFrame:
    columns = ["machine_id", "timestamp", "execution", "Srpm", "Sload", "_row_id"]
    frame = pd.DataFrame([{**r, "_row_id": r.get("_row_id", i)}
                          for i, r in enumerate(records)], columns=columns)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], format="mixed", errors="coerce", utc=True)
    return classifier(frame)


def summarize(records: list[dict], classifier: Callable = production._classify_rows) -> dict:
    """Measure production labels and its exact left-endpoint interval policy."""
    frame = _frame(records, classifier)
    states = {int(row._row_id): str(state) for (_, row), state in
              zip(frame.iterrows(), frame["state_class"].fillna("unclassified"))}
    valid = frame.dropna(subset=["timestamp", "machine_id"])
    durations = dict(active=0.0, non_active=0.0, unclassified=0.0)
    excluded = dict(long_gap_seconds=0.0, subminimum_seconds=0.0,
                    long_gaps=0, subminimum_intervals=0, duplicate_intervals=0)
    positive = usable = classified = 0
    transitions = []
    conflict_timestamps = 0
    for _, group in valid.groupby("machine_id", sort=False):
        ordered = group.sort_values("timestamp", kind="mergesort")
        effective_states = {}
        for _, tied in ordered.groupby("timestamp", sort=False):
            if tied["state_class"].fillna("unclassified").nunique() > 1:
                conflict_timestamps += 1
            last = tied.iloc[-1]
            effective_states[last["timestamp"]] = (
                "unclassified" if pd.isna(last["state_class"]) else str(last["state_class"]))
        rows = list(ordered.to_dict("records"))
        for first, second in zip(rows, rows[1:]):
            delta = (second["timestamp"] - first["timestamp"]).value / 1_000_000_000
            a = "unclassified" if pd.isna(first["state_class"]) else str(first["state_class"])
            b = effective_states[second["timestamp"]]
            if delta == 0:
                excluded["duplicate_intervals"] += 1
                continue
            positive += 1
            if delta > production.MAX_INTERVAL_SECONDS:
                excluded["long_gap_seconds"] += delta
                excluded["long_gaps"] += 1
                continue
            if delta < production.MIN_INTERVAL_SECONDS:
                excluded["subminimum_seconds"] += delta
                excluded["subminimum_intervals"] += 1
                continue
            usable += 1
            durations[a] += delta
            classified += a != "unclassified"
            if a != b:
                transitions.append((second["timestamp"].isoformat(), a, b))
    # Independently cross-check the custom accounting against the production
    # daily aggregator (including its UTC midnight splitting behavior).
    quality, daily = production._daily_aggregates(valid)
    for state in ("active", "non_active"):
        actual = float(daily.loc[daily["state_class"] == state, "seconds"].sum())
        if not math.isclose(actual, durations[state], rel_tol=1e-9, abs_tol=1e-8):
            raise AssertionError(f"Production daily duration mismatch: {state}")
    observed = durations["active"] + durations["non_active"]
    usable_seconds = sum(durations.values())
    positive_span = usable_seconds + excluded["long_gap_seconds"] + excluded["subminimum_seconds"]
    return {"states": states, "durations_seconds": durations,
            "observed_seconds": observed, "usable_seconds": usable_seconds,
            "coverage_fraction": observed / usable_seconds if usable_seconds else None,
            "positive_span_seconds": positive_span,
            "wall_span_coverage_fraction": observed / positive_span if positive_span else None,
            "continuity": usable / positive if positive else None,
            "invalid_timestamp_rows": int(frame["timestamp"].isna().sum()),
            "positive_intervals": positive, "usable_intervals": usable,
            "classified_intervals": classified, "excluded": excluded,
            "transition_boundaries": transitions,
            "transition_convention": "final source-ordered state per timestamp; boundary at next distinct timestamp across a usable preceding interval only",
            "conflicting_state_timestamps": conflict_timestamps}


def _comparison(baseline: dict, changed: dict, count: int) -> dict:
    refs = [i for i in range(count) if baseline["states"][i] != changed["states"][i]]
    before, after = Counter(baseline["transition_boundaries"]), Counter(changed["transition_boundaries"])
    removed, added = list((before - after).elements()), list((after - before).elements())
    deltas = {k: changed["durations_seconds"][k] - baseline["durations_seconds"][k]
              for k in ("active", "non_active", "unclassified")}
    return {"compared_rows": count, "agreement": (count-len(refs))/count if count else None,
            "changed_rows": len(refs), "changed_row_refs": refs,
            "changed_row_trace": [{"row_ref":i,"before":baseline["states"][i],"after":changed["states"][i]} for i in refs],
            "duration_deltas_seconds": deltas,
            "transition_boundary_changes": {"removed_count": len(removed), "added_count": len(added),
                                             "removed": removed, "added": added},
            "changed": _public(changed)}


def _public(summary: dict) -> dict:
    return {k: v for k, v in summary.items() if k != "states"}


def _preserved(comparison: dict, baseline: dict) -> bool:
    changed = comparison["changed"]
    return (comparison["changed_rows"] == 0
            and all(math.isclose(changed["durations_seconds"][k], baseline["durations_seconds"][k], rel_tol=1e-9, abs_tol=1e-8)
                    for k in ("active", "non_active", "unclassified"))
            and comparison["transition_boundary_changes"]["removed_count"] == 0
            and comparison["transition_boundary_changes"]["added_count"] == 0
            and all(changed[k] == baseline[k] for k in
                    ("positive_intervals", "usable_intervals", "classified_intervals"))
            and changed["excluded"]["long_gaps"] == baseline["excluded"]["long_gaps"])


def _numeric(value):
    return pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]


def evaluate_window(records: list[dict]) -> dict:
    """Execute E1--E6 and four explicit mutants against identical source rows."""
    started = time.perf_counter()
    if not records:
        raise ValueError("A nonempty source window is required")
    rows = [{**r, "_row_id": i} for i, r in enumerate(records)]
    baseline = summarize(rows)
    service = replay._analyze(rows)
    for key, actual in (("active_seconds",baseline["durations_seconds"]["active"]),
                        ("non_active_seconds",baseline["durations_seconds"]["non_active"]),
                        ("observed_seconds",baseline["observed_seconds"]),
                        ("positive_intervals",baseline["positive_intervals"]),
                        ("usable_intervals",baseline["usable_intervals"]),
                        ("classified_intervals",baseline["classified_intervals"]),
                        ("long_gaps",baseline["excluded"]["long_gaps"])):
        if not math.isclose(float(service[key]), actual, rel_tol=1e-9, abs_tol=1e-8):
            raise AssertionError(f"Production service mismatch: {key}")
    baseline["service_crosscheck_passed"] = True
    baseline["service_sufficient_data"] = bool(service["sufficient_data"])
    n = len(rows)
    experiments = {}
    for label, transform in (("E1", "rename_execution"), ("E2", "rpm_unit"), ("E3", "provider")):
        transformed = replay._transform(rows, transform)
        # Provider simulation intentionally restores only canonical fields.
        for i, row in enumerate(transformed):
            row["_row_id"] = i
        comparison = _comparison(baseline, summarize(transformed), n)
        comparison.update(expected="preservation", verdict="pass" if _preserved(comparison, baseline) else "unexpected_difference")
        experiments[label] = comparison

    degraded = replay._transform(rows, "missing_execution")
    comparison = _comparison(baseline, summarize(degraded), n)
    selected = set(range(0, n, 10))
    unexpected = sorted(set(comparison["changed_row_refs"]) - selected)
    comparison.update(expected="degradation_local_to_masked_rows", masked_rows=len(selected),
                      unexpected_row_refs=unexpected, verdict="pass" if not unexpected else "unexpected_difference")
    experiments["E4"] = comparison

    # A permutation may reorder timestamps while retaining the original total
    # order inside a tie. Arbitrary permutation of conflicting ties is a
    # different, semantically underspecified transformation, reported below.
    duplicate = copy.deepcopy(rows[n // 3])
    groups = {}
    canonical_times = pd.to_datetime([r["timestamp"] for r in rows], format="mixed", errors="coerce", utc=True)
    for i, (row, stamp) in enumerate(zip(rows, canonical_times)):
        key = ("invalid", i) if pd.isna(stamp) else stamp.value
        group = groups.setdefault(key, [])
        group.append(row)
        if row["_row_id"] == duplicate["_row_id"]:
            group.append(duplicate)
    shuffled_groups = list(groups.values())
    random.Random(2026).shuffle(shuffled_groups)
    preserved_ties = [r for group in shuffled_groups for r in group]
    # Duplicate is adjacent to its original: appending it at the end of a
    # conflicting tie would change which state owns the next interval.
    comparison = _comparison(baseline, summarize(preserved_ties), n)
    comparison.update(expected="preservation_with_tie_order_retained", verdict="pass" if _preserved(comparison, baseline) else "unexpected_difference")
    experiments["E5a"] = comparison
    arbitrary = copy.deepcopy(rows + [duplicate])
    random.Random(2026).shuffle(arbitrary)
    comparison = _comparison(baseline, summarize(arbitrary), n)
    ambiguous = baseline["conflicting_state_timestamps"] > 0
    comparison.update(expected="conditional_on_no_conflicting_timestamps",
                      verdict="ambiguous_ties" if ambiguous else ("pass" if _preserved(comparison, baseline) else "unexpected_difference"))
    experiments["E5_conflict"] = comparison

    # Move a chronological suffix, so the injected gap cannot be hidden by an
    # unrelated source-order permutation. Keep all equal timestamps together.
    gapped = copy.deepcopy(rows)
    times = pd.to_datetime([r["timestamp"] for r in rows], format="mixed", errors="coerce", utc=True)
    valid_times = sorted(set(t for t in times if not pd.isna(t)))
    cut = valid_times[len(valid_times)//2] if len(valid_times) >= 2 else None
    if cut is not None:
        for row, stamp in zip(gapped, times):
            if not pd.isna(stamp) and stamp >= cut:
                row["timestamp"] = (stamp + pd.Timedelta(seconds=60)).isoformat()
        ordered_source = _frame(rows).dropna(subset=["timestamp"]).sort_values("timestamp", kind="mergesort")
        previous = ordered_source[ordered_source["timestamp"] < cut].iloc[-1]
        seconds = (cut-previous["timestamp"]).value / 1_000_000_000
        state = "unclassified" if pd.isna(previous["state_class"]) else str(previous["state_class"])
        expected_deltas = dict(active=0., non_active=0., unclassified=0.)
        if production.MIN_INTERVAL_SECONDS <= seconds <= production.MAX_INTERVAL_SECONDS:
            expected_deltas[state] = -seconds
        comparison = _comparison(baseline, summarize(gapped), n)
        correct = all(math.isclose(comparison["duration_deltas_seconds"][k], v, rel_tol=1e-9, abs_tol=1e-8)
                      for k,v in expected_deltas.items())
        correct = correct and comparison["changed"]["excluded"]["long_gaps"] >= baseline["excluded"]["long_gaps"]
        comparison.update(expected="new_gap_excluded_never_downtime", expected_duration_deltas_seconds=expected_deltas,
                          verdict="pass" if correct else "unexpected_difference")
    else:
        comparison = {"expected": "new_gap_excluded_never_downtime", "verdict": "not_activated"}
    experiments["E5b"] = comparison

    changed_threshold = summarize(rows, threshold_classifier(150))
    comparison = _comparison(baseline, changed_threshold, n)
    expected_refs = []
    for i, row in enumerate(rows):
        execution = replay._execution_token(row.get("execution"))
        rpm, load = _numeric(row.get("Srpm")), _numeric(row.get("Sload"))
        if (execution in production.UNAVAILABLE_EXECUTION_STATES
                and pd.notna(rpm) and 100 < rpm <= 150 and (pd.isna(load) or load <= 1)):
            expected_refs.append(i)
    comparison.update(expected="intentional_threshold_change", expected_changed_row_refs=expected_refs,
                      verdict="pass" if comparison["changed_row_refs"] == expected_refs else "unexpected_difference")
    experiments["E6"] = comparison

    mutations = {}
    bad_units = copy.deepcopy(rows)
    for row in bad_units:
        rpm = _numeric(row.get("Srpm"))
        row["Srpm"] = None if pd.isna(rpm) else float(rpm)*2*math.pi/60
    missing_rename = [{**r, "execution": None} for r in rows]
    missing_load = [{**r, "Sload": None} for r in rows]
    def unknown_as_idle(frame):
        result = production._classify_rows(frame)
        unknown = result["state_class"].isna()
        result.loc[unknown, "state_class"] = "non_active"
        result.loc[unknown, "state_label"] = "DEFECT: unknown as non-active"
        return result
    for label, mutant_rows, classifier in (
            ("wrong_unit_adapter", bad_units, production._classify_rows),
            ("omitted_rename_adapter", missing_rename, production._classify_rows),
            ("omitted_provider_load", missing_load, production._classify_rows),
            ("unknown_as_non_active", rows, unknown_as_idle)):
        comparison = _comparison(baseline, summarize(mutant_rows, classifier), n)
        activated = comparison["changed_rows"] > 0
        # Each mutant breaks a preservation contract. B1 observes rows, time,
        # and boundaries with exactly the same inputs as all correct variants.
        detected = not _preserved(comparison, baseline)
        comparison.update(expected="preservation", activated=activated, detected=detected,
                          verdict="detected" if detected else "not_activated")
        mutations[label] = comparison
    failures = sum(v["verdict"] == "unexpected_difference" for v in experiments.values())
    return {"records": n, "baseline": _public(baseline), "experiments": experiments,
            "defects": mutations, "unexpected_experiment_outcomes": failures,
            "elapsed_seconds": time.perf_counter()-started}
