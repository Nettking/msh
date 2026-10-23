"""Deterministic presentation of existing owner timelines and episodes."""

from __future__ import annotations

import json
from bisect import bisect_left, bisect_right
from dataclasses import asdict
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal

from catalog.mtconnect_recorder.canonical.observation import CanonicalObservation
from catalog.mtconnect_recorder.operational.policy import device_key
from catalog.mtconnect_recorder.operational.timeline import (
    build_device_timeline_reports,
)

from .contracts import VIEWS, digest, encode, identifier, micros, revision, timestamp

METRICS = (
    "covered_duration_us",
    "uncovered_duration_us",
    "active_us",
    "program_stopped_us",
    "feed_hold_us",
    "other_execution_us",
    "unknown_state_us",
    "active_share",
)
OPERATOR_FIELDS = (
    "observation",
    "trigger",
    "context",
    "hypothesis",
    "goal",
    "decision",
    "rationale",
    "expected_outcome",
    "outcome",
    "notes",
    "review_status",
    "worked",
    "action_type",
)


def service(state, reasons=(), counts=True):
    return {
        "state": state,
        "reason_codes": list(dict.fromkeys(reasons)),
        "counts_available": counts,
    }


def metrics(spans, a, b, *, unresolved=False, check=None):
    result = dict(requested_duration_us=b - a, **dict.fromkeys(METRICS))
    if unresolved:
        return result
    durations = dict.fromkeys(METRICS[2:7], 0)
    ranges = []
    for span in spans:
        if check is not None:
            check()
        s, e = max(a, span["start_observed_at_us"]), min(b, span["end_observed_at_us"])
        if s >= e:
            continue
        ranges.append((s, e))
        state = span["state"]
        name = {
            "ACTIVE": "active_us",
            "PROGRAM_STOPPED": "program_stopped_us",
            "FEED_HOLD": "feed_hold_us",
        }.get(state)
        if name is None:
            name = (
                "unknown_state_us"
                if state in ("UNKNOWN", "UNAVAILABLE", "")
                else "other_execution_us"
            )
        durations[name] += e - s
    covered = 0
    previous = a
    for s, e in sorted(ranges):
        if check is not None:
            check()
        covered += max(0, e - max(s, previous))
        previous = max(previous, e)
    result.update(covered_duration_us=covered, uncovered_duration_us=b - a - covered)
    if covered:
        # Overlapping intervals after a clock regression have no defensible sum.
        if sum(durations.values()) != covered:
            return dict(requested_duration_us=b - a, **dict.fromkeys(METRICS))
        result.update(durations, active_share=durations["active_us"] / covered)
    return result


def _time(value):
    if not isinstance(value, str) or not value.endswith(("Z", "+00:00")):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return None


def build(dataset, definition, window, budget):
    a, b = (micros(window[k]) for k in ("start_at", "end_at"))
    namespace = dataset["input_binding"]
    views = {view: [] for view in VIEWS}
    evidence, records, links = {}, {}, []
    included_segments, segment_revisions = {}, {}
    source_rows = dataset["content"]
    unresolved = dataset["time_policy"] == "UNRESOLVED"
    reasons = ["SOURCE_TIME_UNRESOLVED"] if unresolved else []
    first = last = None

    def evidence_for(value, claim="SYSTEM_INFERRED"):
        binding = (
            dataset["operator_binding"] if claim == "HUMAN_REPORTED" else namespace
        )

        # Never expose owner filesystem paths or source URLs in the locator.
        def public_content(item):
            if isinstance(item, dict):
                return {
                    k: public_content(v)
                    for k, v in item.items()
                    if k
                    not in (
                        "raw_path",
                        "manifest_path",
                        "source_name",
                        "_source_revision",
                    )
                }
            if isinstance(item, (list, tuple)):
                return [public_content(v) for v in item]
            return item

        clean = public_content(value)
        content = encode(clean)
        eid = identifier("ev", [binding, clean])
        evidence[eid] = {
            "state": "AVAILABLE",
            "evidence_id": eid,
            "source_revision": value.get("_source_revision") or revision(value),
            "locator": {
                "input_id": binding["input_id"],
                "object_ref": identifier("row", value),
            },
            "claim_class": claim,
            "content": content,
            "content_digest": digest(content),
            "reason_codes": [],
        }
        return eid

    def row(
        kind,
        value,
        s=None,
        e=None,
        classification=None,
        context=None,
        reason_codes=(),
        row_metrics=None,
        claim="SYSTEM_INFERRED",
    ):
        eid = evidence_for(value, claim)
        return {
            "row_id": identifier("row", [namespace, kind, value]),
            "row_kind": kind,
            "source_ref": {
                "input_id": (
                    dataset["operator_binding"]
                    if claim == "HUMAN_REPORTED"
                    else namespace
                )["input_id"],
                "evidence_ref": eid,
            },
            "claim_class": claim,
            "original_start_at": timestamp(s) if s is not None else None,
            "original_end_at": timestamp(e) if e is not None else None,
            "visible_start_at": timestamp(max(a, s)) if s is not None else None,
            "visible_end_at": timestamp(min(b, e)) if e is not None else None,
            "clipped_start": s is not None and s < a,
            "clipped_end": e is not None and e > b,
            "classification": classification,
            "context": context or {"values": [], "pending_roles": []},
            "reason_codes": list(reason_codes),
            "evidence_refs": [eid],
            "metrics": row_metrics,
        }

    canonical = dataset["kind"] == "CANONICAL_DEVICE"

    def canonical_history():
        nonlocal first, last
        raw_observations = source_rows["observations"]
        observations = (
            CanonicalObservation(**value) for value in source_rows["observations"]
        )
        reports = build_device_timeline_reports(observations, check=budget.checkpoint)
        report = next(
            r
            for r in reports
            if r.device_key == dataset["machine_binding"]["device_key"]
        )
        spans = [asdict(s) for s in budget.iterate(report.execution_spans)]
        span_starts = [s["start_observed_at_us"] for s in spans]
        span_ends = [s["end_observed_at_us"] for s in spans]

        def interval_metrics(start, end):
            if unresolved or report.timestamp_discontinuities:
                return metrics([], start, end, unresolved=True)
            selected = spans[
                bisect_right(span_ends, start) : bisect_left(span_starts, end)
            ]
            return metrics(selected, start, end, check=budget.checkpoint)

        if report.timestamp_discontinuities:
            reasons.extend(("TIMESTAMP_REGRESSION", "DURATION_UNRESOLVED"))
        if report.agent_sequence_discontinuities:
            reasons.append("SEQUENCE_GAP")
        if not report.execution_is_usable:
            reasons.append("SEMANTIC_ROLE_UNRESOLVED")
        if not unresolved:
            # Device membership is determined by the owner's device_key policy,
            # never by a machine's display label or another Device's sequence.
            times = [
                value["observed_at_us"]
                for value in budget.iterate(raw_observations)
                if device_key(CanonicalObservation(**value)) == report.device_key
                and a <= value["observed_at_us"] < b
            ]
            first, last = (
                (timestamp(min(times)), timestamp(max(times)))
                if times
                else (None, None)
            )
            context_sequences = [snap.sequence for snap in report.context_snapshots]
            by_seq = {o["sequence"]: o for o in budget.iterate(raw_observations)}

            def context_at(sequence):
                index = bisect_right(context_sequences, sequence) - 1
                if index < 0:
                    return {"values": [], "pending_roles": []}
                value = asdict(report.context_snapshots[index])
                return {key: value[key] for key in ("values", "pending_roles")}

            for span in spans:
                budget.check(force=False)
                s, e = span["start_observed_at_us"], span["end_observed_at_us"]
                if (s == e and a <= s < b) or max(a, s) < min(b, e):
                    views["execution"].append(
                        row(
                            "EXECUTION_SPAN",
                            dict(
                                span,
                                boundary_observations=[
                                    by_seq[seq]
                                    for seq in (
                                        span["start_sequence"],
                                        span["end_sequence"],
                                    )
                                    if seq in by_seq
                                    and device_key(CanonicalObservation(**by_seq[seq]))
                                    == report.device_key
                                ],
                            ),
                            s,
                            e,
                            span["state"],
                            context_at(span["start_sequence"]),
                            row_metrics=metrics(
                                [span],
                                max(a, s),
                                min(b, e),
                                unresolved="DURATION_UNRESOLVED" in reasons,
                            ),
                        )
                    )
            views["execution"].sort(
                key=lambda r: (
                    json.loads(evidence[r["evidence_refs"][0]]["content"])[
                        "start_sequence"
                    ],
                    json.loads(evidence[r["evidence_refs"][0]]["content"])[
                        "end_sequence"
                    ],
                    r["row_id"],
                )
            )
            for episode in sorted(
                source_rows["episodes"],
                key=lambda v: (v["start_sequence"], v["fingerprint"]),
            ):
                budget.check(force=False)
                s, e = episode["start_at_us"], episode["end_at_us"]
                if not ((s == e and a <= s < b) or max(a, s) < min(b, e)):
                    continue
                context = context_at(episode["first_sequence"])
                item = row(
                    "EPISODE",
                    episode,
                    s,
                    e,
                    None,
                    context,
                    row_metrics=interval_metrics(max(a, s), min(b, e)),
                )
                item.update(
                    run_ref=identifier("row", [namespace, episode["machine_run_id"]]),
                    cycle_ref=identifier(
                        "row", [namespace, episode["production_cycle_id"]]
                    ),
                    boundary_reasons={
                        "start": episode["start_reason"],
                        "end": episode["end_reason"],
                    },
                    partial_start=bool(episode["partial_start"]),
                    partial_end=bool(episode["partial_end"]),
                    episode_entry_context=json.loads(episode["entry_context"])
                    if isinstance(episode["entry_context"], str)
                    else episode["entry_context"],
                    link_refs=[],
                )
                included_segments[episode["segment_id"]] = item["row_id"]
                segment_revisions[episode["segment_id"]] = episode["fingerprint"]
                if episode["partial_start"] or episode["partial_end"]:
                    reasons.append("PARTIAL_SOURCE_BOUNDARY")
                if episode["tool_context_partial"]:
                    reasons.append("CONTEXT_UNRESOLVED")
                views["episodes"].append(item)
            gaps = list(report.agent_sequence_discontinuities) + list(
                report.timestamp_discontinuities
            )
            for gap in sorted(
                gaps,
                key=lambda g: (g.next_sequence, getattr(g, "reason", "SEQUENCE_GAP")),
            ):
                value = asdict(gap)
                point = by_seq.get(gap.next_sequence)
                if point and a <= point["observed_at_us"] < b:
                    views["gaps"].append(
                        row(
                            "DISCONTINUITY",
                            value,
                            point["observed_at_us"],
                            point["observed_at_us"],
                            reason_codes=[getattr(gap, "reason", "SEQUENCE_GAP")],
                        )
                    )
        summary = metrics(
            spans,
            a,
            b,
            unresolved=unresolved or "DURATION_UNRESOLVED" in reasons,
            check=budget.checkpoint,
        )
        if summary["uncovered_duration_us"]:
            reasons.append("PARTIAL_COVERAGE")
        if not unresolved and not any(
            views[v] for v in ("episodes", "execution", "gaps")
        ):
            reasons.append("EMPTY_WINDOW")
        if unresolved or "SEMANTIC_ROLE_UNRESOLVED" in reasons:
            history = service("UNRESOLVED_CONTEXT", reasons, False)
        elif "EMPTY_WINDOW" in reasons:
            history = service("INSUFFICIENT_DATA", ["EMPTY_WINDOW"], True)
        else:
            history = service(
                "PARTIAL" if reasons else "AVAILABLE", sorted(set(reasons))
            )
        unplaced = (
            sum(
                1
                for value in raw_observations
                if device_key(CanonicalObservation(**value)) == report.device_key
            )
            if unresolved
            else 0
        )
        return summary, history, unplaced

    if canonical:
        summary, history, primary_unplaced = canonical_history()
    else:
        summary = metrics([], a, b, unresolved=unresolved)
        if not unresolved:
            machine = dataset["machine_binding"].get("machine_id")
            bounds = [
                bounds
                for day, bounds in source_rows.get("observed_daily_bounds", {})
                .get(machine, {})
                .items()
                if window["start_at"][:10] <= day < window["end_at"][:10]
            ]
            if bounds:
                first = min(pair[0] for pair in bounds)
                last = max(pair[1] for pair in bounds)
            daily = {}
            for value in source_rows["daily_states"]:
                day = value["date"]
                if (
                    value["machine_id"] == machine
                    and window["start_at"][:10] <= day < window["end_at"][:10]
                ):
                    daily.setdefault(day, []).append(value)

            def us(value):
                return int(
                    (Decimal(str(value)) * 1_000_000).quantize(
                        Decimal(1), rounding=ROUND_HALF_EVEN
                    )
                )

            active = covered = 0
            for day, values in sorted(daily.items()):
                active_day = us(
                    sum(v["seconds"] for v in values if v["state_class"] == "active")
                )
                covered_day = us(sum(v["seconds"] for v in values))
                active += active_day
                covered += covered_day
                daily_metrics = dict(
                    requested_duration_us=86_400_000_000, **dict.fromkeys(METRICS)
                )
                daily_metrics.update(
                    covered_duration_us=covered_day,
                    uncovered_duration_us=86_400_000_000 - covered_day,
                    active_us=active_day if covered_day else None,
                    active_share=active_day / covered_day if covered_day else None,
                )
                views["legacy_daily"].append(
                    row(
                        "LEGACY_DAY",
                        {"date": day, "states": values},
                        micros(day + "T00:00:00.000000Z"),
                        micros(day + "T00:00:00.000000Z") + 86_400_000_000,
                        reason_codes=["METRIC_NOT_PROVIDED"],
                        row_metrics=daily_metrics,
                    )
                )
            summary.update(
                covered_duration_us=covered,
                uncovered_duration_us=b - a - covered,
                active_us=active if covered else None,
                active_share=active / covered if covered else None,
            )
            reasons.append("METRIC_NOT_PROVIDED")
            if dataset["time_policy"] == "ASSUME_UTC":
                reasons.append("UTC_ASSUMED")
            if not daily:
                reasons.append("EMPTY_WINDOW")
        history = service(
            "UNRESOLVED_CONTEXT"
            if unresolved
            else "PARTIAL"
            if views["legacy_daily"]
            else "INSUFFICIENT_DATA",
            reasons,
            not unresolved,
        )

    human = source_rows.get("operator")
    unplaced = 0
    for original in human["records"] if human else []:
        budget.check(force=False)
        source_id = original["id"]
        source_revision = original["_source_revision"]
        rid = identifier("rec", [dataset["operator_binding"], source_id])
        parsed_time = _time(
            original.get("occurred_at") or original.get("decision_time")
        )
        source_time_status = original.get("time_status")
        time_status = (
            "KNOWN"
            if human.get("utc_verified")
            and source_time_status in (None, "KNOWN")
            and parsed_time
            else "ASSUMED"
            if source_time_status == "ASSUMED" and parsed_time
            else "UNRESOLVED"
        )
        occurred = parsed_time if time_status != "UNRESOLVED" else None
        recorded = _time(original.get("recorded_at") or original.get("captured_at"))
        related = [
            v
            for v in human["links"]
            if v.get("record_id") == source_id
            and v.get("segment_id") in included_segments
            and v.get("input_id") == namespace["input_id"]
        ]
        verified_binding = human.get("record_bindings", {}).get(source_id)
        same_machine = verified_binding == dataset["machine_binding"]
        if not related and not (
            same_machine
            and time_status == "KNOWN"
            and occurred
            and a <= micros(occurred) < b
        ):
            unplaced += 1
            continue
        data = {
            "record_id": rid,
            "source_revision": source_revision,
            "occurred_at": occurred,
            "recorded_at": recorded,
            "time_status": time_status,
            "author_ref": original.get("author_ref"),
            "source_status": original.get("review_status"),
            "fields": {k: original.get(k) for k in OPERATOR_FIELDS},
            "field_states": {
                k: "MISSING"
                if k not in original
                else "SOURCE_EMPTY"
                if original[k] == ""
                else "PRESENT"
                for k in OPERATOR_FIELDS
            },
            "link_status": "UNLINKED",
            "link_refs": [],
            "evidence_refs": [evidence_for(original, "HUMAN_REPORTED")],
        }
        for source_link in related:
            status = source_link.get("status")
            target_revision = source_link.get("segment_revision")
            if status not in (
                "PROPOSED",
                "CONFIRMED",
                "REJECTED",
            ) or target_revision not in (
                segment_revisions[source_link["segment_id"]],
                "rev_" + segment_revisions[source_link["segment_id"]],
            ):
                status = "UNRESOLVED"
            link = {
                "link_id": identifier("row", [namespace, source_link]),
                "source_object_ref": rid,
                "target_object_ref": included_segments[source_link["segment_id"]],
                "source_revision": source_revision,
                "target_revision": (
                    "rev_" + segment_revisions[source_link["segment_id"]]
                    if status != "UNRESOLVED"
                    else target_revision
                ),
                "relation_kind": source_link.get("relation_kind"),
                "status": status,
                "origin": "EXISTING_SOURCE",
                "evidence_refs": [evidence_for(source_link, "HUMAN_REPORTED")],
            }
            links.append(link)
            data["link_refs"].append(link["link_id"])
        if related:
            statuses = [v["status"] for v in links if v["source_object_ref"] == rid]
            data["link_status"] = next(
                v
                for v in ("UNRESOLVED", "PROPOSED", "CONFIRMED", "REJECTED")
                if v in statuses
            )
        records[rid] = data
        point = micros(occurred) if occurred and time_status == "KNOWN" else None
        item = row("OPERATOR_RECORD", data, point, point, claim="HUMAN_REPORTED")
        item["reason_codes"] = (
            ["UNLINKED_RECORDS"] if data["link_status"] != "CONFIRMED" else []
        ) + (["RECORD_TIME_UNRESOLVED"] if data["time_status"] != "KNOWN" else [])
        if point is not None and not a <= point < b:
            item.update(
                visible_start_at=None,
                visible_end_at=None,
                clipped_start=False,
                clipped_end=False,
            )
        # Linked, untimed records remain visible without inventing a position.
        item["record_id"] = rid
        views["operator"].append(item)
    views["operator"].sort(
        key=lambda r: (
            records[r["record_id"]]["occurred_at"]
            or records[r["record_id"]]["recorded_at"]
            or "~",
            r["record_id"],
        )
    )
    for episode in views["episodes"]:
        episode["link_refs"] = [
            link["link_id"]
            for link in links
            if link["target_object_ref"] == episode["row_id"]
        ]
    operator_reasons = []
    if not human:
        operator_state = service(
            "INSUFFICIENT_DATA", ["OPERATOR_SOURCE_NOT_SELECTED"], False
        )
    elif not records:
        operator_state = service("INSUFFICIENT_DATA", ["NO_VISIBLE_OPERATOR_RECORDS"])
    else:
        if any(r["link_status"] != "CONFIRMED" for r in records.values()):
            operator_reasons.append("UNLINKED_RECORDS")
        if any(r["time_status"] != "KNOWN" for r in records.values()):
            operator_reasons.append("RECORD_TIME_UNRESOLVED")
        operator_state = service(
            "PARTIAL" if operator_reasons else "AVAILABLE", operator_reasons
        )
    counts = {v: len(views[v]) for v in VIEWS}
    unplaced_primary = (
        primary_unplaced
        if canonical
        else source_rows.get("record_count", 0)
        if unresolved
        else 0
    )
    counts.update(
        unplaced_primary_records=unplaced_primary,
        unplaced_operator_records=unplaced if human else None,
    )
    available = (
        ["episodes", "execution", "gaps", "operator"]
        if canonical
        else ["legacy_daily", "operator"]
    )
    for v in VIEWS:
        if v not in available or (v == "operator" and not human):
            counts[v] = None
    if unresolved:
        for v in available:
            if v != "operator":
                counts[v] = None
    services = {
        "history": history,
        "operator_knowledge": operator_state,
        "evidence": service("AVAILABLE")
        if evidence
        else service("INSUFFICIENT_DATA", ["NO_EVIDENCE_IN_SCOPE"]),
    }
    services.update(
        {
            v: service("UNAVAILABLE", ["NOT_IN_RELEASE"], False)
            for v in ("feedback", "comparison", "simulation")
        }
    )
    logical = {
        "contract_version": 1,
        "source_bindings": [dataset["input_binding"], dataset["operator_binding"]],
        "source_content_digests": [dataset["content_digest"]],
        "definition_content": definition["definition"],
        "window_us": [a, b],
        "time_policy": dataset["time_policy"],
        "transformation_versions": [
            "process-twin-history.v1",
            "owner-segmentation.v1" if canonical else "owner-legacy-daily.v1",
        ],
        "rows_by_view": views,
        "links": links,
        "operator_source_revisions": [r["source_revision"] for r in records.values()],
        "services": services,
        "summary_integer_values": {
            k: v for k, v in summary.items() if k != "active_share"
        },
    }
    return {
        "views": views,
        "records": records,
        "links": links,
        "evidence": evidence,
        "summary": summary,
        "services": services,
        "counts": counts,
        "available_views": available,
        "first_observed_at": first,
        "last_observed_at": last,
        "content_revision": revision(logical),
    }
