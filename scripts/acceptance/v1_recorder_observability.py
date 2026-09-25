"""Read the Recorder's product-owned acceptance observations without probing a host.

Completed operation spans are measured inside the owning product functions.
Heartbeat ages, enqueue delays and container uptime are not substitute durations.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

PRODUCT_SCHEMA = "fcp.recorder.acceptance-observability.v1"
EVIDENCE_SCHEMA = "fcp.recorder.acceptance-evidence.v1"
OPERATIONS = ("recorder-recovery", "publication-reconcile")
MANAGED_WORKERS = ("managed_companion", "federation_control", "recorder_publication")
MAX_STATUS_BYTES = 1024 * 1024


def _utc(value: object) -> dt.datetime:
    if not isinstance(value, str):
        raise TypeError("missing UTC timestamp")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        raise ValueError("UTC timestamp required")
    return parsed


def _integer(value: object, *, minimum: int = 0) -> bool:
    return type(value) is int and value >= minimum


def _uuid(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value) is not None


def _provenance(value: object, candidate: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError("missing runtime provenance")
    if value.get("candidate_sha") != candidate or not _uuid(value.get("runtime_generation")):
        raise ValueError("candidate/runtime generation mismatch")
    if not _integer(value.get("pid"), minimum=1):
        raise ValueError("missing product process identity")
    return value


def _same_runtime(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return all(left.get(key) == right.get(key) for key in (
        "candidate_sha", "runtime_generation", "supervisor_generation", "pid"
    ))


def evidence(
    status: object,
    *,
    expected_candidate: str,
    expected_session: str | None = None,
    expected_runtime_generation: str | None = None,
    not_before_utc: str | None = None,
) -> dict[str, Any]:
    """Fail closed on absent, stale, partial, dropped or contradictory evidence.

    The caller supplies the current campaign begin as ``not_before_utc`` when
    seeking timed credit. This function neither creates history nor declares
    an entire P07/P12 assertion passed.
    """
    result: dict[str, Any] = {
        "schema": EVIDENCE_SCHEMA, "candidate_sha": expected_candidate,
        "status": "unavailable", "durations": {}, "worker_health": {},
        "physical_assertion_pass": False,
    }
    try:
        if not re.fullmatch(r"[0-9a-f]{40}", expected_candidate):
            raise ValueError("exact candidate required")
        if not isinstance(status, dict):
            raise TypeError("Recorder heartbeat unavailable")
        raw = status.get("acceptance_observability")
        result["source"] = {"acceptance_observability": raw, "workers": status.get("workers")}
        if not isinstance(raw, dict) or raw.get("schema") != PRODUCT_SCHEMA or raw.get("available") is not True:
            raise ValueError("product observations unavailable")
        provenance = _provenance(raw.get("provenance"), expected_candidate)
        if expected_runtime_generation is not None and provenance["runtime_generation"] != expected_runtime_generation:
            raise ValueError("stale runtime generation")
        observed = _utc(raw.get("observed_at_utc"))
        lower = _utc(not_before_utc) if not_before_utc is not None else None
        if lower is not None and observed < lower:
            raise ValueError("observation predates this campaign")
        observed_ns = raw.get("observed_at_monotonic_ns")
        if not _integer(observed_ns) or not _integer(raw.get("dropped_updates")):
            raise ValueError("observation clock or dropped-update evidence incomplete")
        operations = raw.get("operations")
        if (not isinstance(operations, list) or len(operations) > 16
                or any(not isinstance(op, dict) or op.get("operation") not in OPERATIONS for op in operations)):
            raise ValueError("operation evidence bound/schema mismatch")
        if (raw.get("retention_truncated") is not False
                or not _integer(raw.get("evicted_operations")) or raw["evicted_operations"] != 0):
            raise ValueError("operation retention incomplete")
        result["provenance"] = provenance
        result["observed_at_utc"] = raw["observed_at_utc"]
        result["observed_at_monotonic_ns"] = observed_ns
        result["dropped_updates"] = raw["dropped_updates"]
        for kind in OPERATIONS:
            selected = [operation for operation in operations
                        if isinstance(operation, dict) and operation.get("operation") == kind]
            checked = []
            reasons = []
            seen: set[str] = set()
            for operation in selected:
                try:
                    op_provenance = _provenance(operation.get("provenance"), expected_candidate)
                    if not _same_runtime(provenance, op_provenance):
                        raise ValueError("operation belongs to another runtime")
                    identifier = operation.get("operation_id")
                    if not _uuid(identifier) or identifier in seen:
                        raise ValueError("operation correlation missing or duplicated")
                    seen.add(identifier)
                    if not _integer(operation.get("operation_sequence"), minimum=1):
                        raise ValueError("operation sequence missing")
                    if (not _integer(operation.get("observation_loss_count"))
                            or operation["observation_loss_count"] != raw["dropped_updates"]):
                        raise ValueError("operation predates an observation loss")
                    if operation.get("outcome") != "completed":
                        raise ValueError("operation is incomplete, failed or interrupted")
                    if operation.get("error_type") is not None:
                        raise ValueError("completed operation contains an error")
                    started, ended = _utc(operation.get("started_at_utc")), _utc(operation.get("ended_at_utc"))
                    if ended < started or ended > observed or (lower is not None and started < lower):
                        raise ValueError("operation lies outside this observation/campaign")
                    start_ns, end_ns, duration = (operation.get(key) for key in (
                        "started_at_monotonic_ns", "ended_at_monotonic_ns", "duration_ns"))
                    if not all(_integer(value) for value in (start_ns, end_ns, duration)):
                        raise ValueError("explicit product monotonic duration missing")
                    if not start_ns <= end_ns <= observed_ns or duration != end_ns - start_ns:
                        raise ValueError("operation duration/boundaries disagree")
                    context = operation.get("context")
                    progress = operation.get("progress")
                    if not isinstance(context, dict) or not isinstance(progress, dict) or not progress:
                        raise ValueError("operation context/progress missing")
                    worker_rows = status.get("workers")
                    anchor = worker_rows.get("managed_companion", {}) if isinstance(worker_rows, dict) else {}
                    if (not isinstance(anchor, dict) or not _same_runtime(provenance, anchor)
                            or not all(isinstance(context.get(key), str) and context[key]
                                       and context[key] == anchor.get(key) for key in ("session_id", "node_id"))
                            or (expected_session is not None and context["session_id"] != expected_session)):
                        raise ValueError("operation does not belong to the current managed session/node")
                    if kind == "recorder-recovery":
                        if not isinstance(context.get("source_alias"), str) or not re.fullmatch(r"[0-9a-f]{64}", context["source_alias"]):
                            raise ValueError("recovery source correlation missing")
                        if (not _integer(context.get("agent_instance_id"))
                                or not _integer(context.get("next_sequence_before")) or not _integer(progress.get("next_sequence_after"))):
                            raise ValueError("recovery checkpoint correlation missing")
                        if (not _integer(progress.get("advanced_sequences"))
                                or progress["next_sequence_after"] - context["next_sequence_before"] != progress["advanced_sequences"]):
                            raise ValueError("recovery checkpoint progress disagrees")
                    else:
                        if not all(isinstance(context.get(key), str) and context[key]
                                   for key in ("session_id", "node_id", "storage_group")):
                            raise ValueError("reconciliation authority correlation missing")
                        if expected_session is not None and context["session_id"] != expected_session:
                            raise ValueError("reconciliation session mismatch")
                        if not all(_integer(progress.get(key)) for key in (
                            "scanned_batches", "eligible_batches", "publication_chunks", "enqueued", "already_enqueued", "quarantined"
                        )):
                            raise ValueError("reconciliation outcome counters missing")
                    checked.append(operation)
                except (KeyError, TypeError, ValueError) as exc:
                    reasons.append(str(exc))
            result["durations"][kind] = {
                "status": "pass" if checked and not reasons else "unavailable",
                "completed_operations": checked, "reasons": reasons or ([] if checked else ["no observed operation"]),
            }
        rows = status.get("workers")
        if not isinstance(rows, dict) or not all(name in rows for name in MANAGED_WORKERS):
            raise ValueError("required managed-worker observations unavailable")
        checked_workers: dict[str, Any] = {}
        for name in MANAGED_WORKERS:
            row = rows[name]
            if not isinstance(row, dict) or not _same_runtime(provenance, _provenance(row, expected_candidate)):
                raise ValueError("managed worker belongs to another runtime")
            if not _uuid(row.get("worker_generation")) or row.get("generation_current") is not True or row.get("generation_overlap") is not False:
                raise ValueError("managed worker generation unavailable/stale/overlapping")
            if row.get("supervisor") != "compose" or not isinstance(row.get("session_id"), str) or not row.get("node_id"):
                raise ValueError("managed worker owner/session correlation missing")
            if expected_session is not None and row["session_id"] != expected_session:
                raise ValueError("managed worker session mismatch")
            if any(type(row.get(key)) is not bool for key in ("alive", "healthy", "started", "stop_requested")):
                raise ValueError("managed worker state missing")
            if row.get("state") not in {"starting", "alive", "recovering", "failed", "stopping", "stopped", "stale-generation"}:
                raise ValueError("unknown managed worker state")
            if row["healthy"] and (not row["alive"] or row["stop_requested"] or row["state"] != "alive"
                                   or row.get("consecutive_failures") != 0 or row.get("last_cycle_outcome") != "completed"):
                raise ValueError("contradictory healthy worker claim")
            if name == "recorder_publication":
                owner_fields = ("future_pending", "event_loop_thread_alive", "event_loop_running")
                if (any(type(row.get(key)) is not bool for key in owner_fields)
                        or row["alive"] != all(row[key] for key in owner_fields)):
                    raise ValueError("publication liveness disagrees with its actual future/event-loop owner")
            when = _utc(row.get("observed_at_utc"))
            if when > observed or (lower is not None and when < lower):
                raise ValueError("worker observation lies outside this observation/campaign")
            worker_ns = row.get("observed_at_monotonic_ns")
            if not _integer(worker_ns) or worker_ns > observed_ns:
                raise ValueError("worker observation clock missing or in the future")
            checked_workers[name] = row
        alive = all(row["alive"] and row["started"] and not row["stop_requested"] for row in checked_workers.values())
        result["worker_health"] = {
            "status": "pass" if alive else "fail", "workers": checked_workers,
            "all_expected_alive": alive,
            "all_expected_healthy": all(row["healthy"] for row in checked_workers.values()),
            "interpretation": "Recovering can remain alive while explicitly not healthy; no host resource inference.",
        }
        result["status"] = "pass" if alive and all(value["status"] == "pass" for value in result["durations"].values()) else "unavailable"
    except (KeyError, TypeError, ValueError) as exc:
        result["reason"] = str(exc)
    return result


def evidence_series(
    observations: list[dict[str, Any]], *, expected_candidate: str,
    expected_session: str | None = None,
    not_before_utc: str | None = None,
) -> dict[str, Any]:
    """Revalidate raw observations; a baseline span is never timed-run credit.

    Each later sample must contain a new completed operation since the preceding
    product snapshot. A retained old completion cannot satisfy a whole campaign.
    Worker liveness is evaluated separately so intentional managed recovery is
    not misreported as a dead worker, or as a successfully completed operation.
    """
    result: dict[str, Any] = {
        "status": "unavailable", "sample_count": len(observations),
        "durations": {kind: {"status": "unavailable"} for kind in OPERATIONS},
        "worker_health": {"status": "unavailable"}, "physical_assertion_pass": False,
        "runtime_generation_changes": [],
    }
    if len(observations) < 2:
        result["reason"] = "baseline and subsequent observation required"
        return result
    checked, duration_rows = [], {kind: [] for kind in OPERATIONS}
    seen: set[tuple[str, str]] = set()
    previous_utc = None
    previous_ns = None
    previous_generation = None
    try:
        for observation in observations:
            if not isinstance(observation, dict):
                raise TypeError("missing raw observation")
            current = evidence(observation.get("source"), expected_candidate=expected_candidate,
                               expected_session=expected_session)
            now = _utc(current.get("observed_at_utc"))
            generation = current.get("provenance", {}).get("runtime_generation")
            if previous_utc is not None:
                if now <= _utc(previous_utc):
                    raise ValueError("product snapshots are stale or out of order")
                if generation == previous_generation and current["observed_at_monotonic_ns"] <= previous_ns:
                    raise ValueError("product monotonic snapshots are stale or out of order")
                if generation != previous_generation:
                    result["runtime_generation_changes"].append({
                        "previous": previous_generation, "current": generation,
                        "observed_at_utc": current["observed_at_utc"],
                    })
                lower = previous_utc
                if not_before_utc is not None and _utc(not_before_utc) > _utc(lower):
                    lower = not_before_utc
                timed = evidence(observation.get("source"), expected_candidate=expected_candidate,
                                 expected_session=expected_session, not_before_utc=lower)
                for kind in OPERATIONS:
                    row = timed.get("durations", {}).get(kind, {"status": "unavailable"})
                    for op in row.get("completed_operations", []):
                        key = (generation, op["operation_id"])
                        if key in seen:
                            raise ValueError("operation completion reused between samples")
                        seen.add(key)
                    duration_rows[kind].append(row)
            else:
                for row in current.get("durations", {}).values():
                    for op in row.get("completed_operations", []):
                        seen.add((generation, op["operation_id"]))
            checked.append(current)
            previous_utc = current["observed_at_utc"]
            previous_ns = current["observed_at_monotonic_ns"]
            previous_generation = generation
        for kind, rows in duration_rows.items():
            result["durations"][kind] = {
                "status": "pass" if rows and all(row.get("status") == "pass" for row in rows) else "unavailable",
                "samples": rows,
            }
        workers = [row.get("worker_health", {}) for row in checked]
        result["worker_health"] = {
            "status": "fail" if any(row.get("status") == "fail" for row in workers) else
                      "pass" if all(row.get("status") == "pass" for row in workers) else "unavailable",
            "samples": workers,
        }
        result["status"] = "pass" if (result["worker_health"]["status"] == "pass"
            and all(row["status"] == "pass" for row in result["durations"].values())) else "unavailable"
    except (KeyError, TypeError, ValueError) as exc:
        result["reason"] = str(exc)
    return result


def read_evidence(path: Path, **kwargs: Any) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_STATUS_BYTES + 1)
        if len(raw) > MAX_STATUS_BYTES:
            raise ValueError("Recorder heartbeat exceeds evidence bound")
        return evidence(json.loads(raw), **kwargs)
    except (OSError, TypeError, ValueError) as exc:
        return {"schema": EVIDENCE_SCHEMA, "status": "unavailable", "reason": str(exc),
                "physical_assertion_pass": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status-file", type=Path, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--session")
    parser.add_argument("--runtime-generation")
    parser.add_argument("--not-before-utc")
    args = parser.parse_args()
    result = read_evidence(args.status_file, expected_candidate=args.candidate, expected_session=args.session,
                           expected_runtime_generation=args.runtime_generation, not_before_utc=args.not_before_utc)
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
