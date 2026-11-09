"""Read-only, deterministic selection from a closed MTConnect tar archive.

No archive members are extracted. Exported metadata contains hashes, UTC
boundaries and line ranges, never source paths, machine names or telemetry.
Tolerant JSON parsing and timestamp/signal normalization follow the service.
"""
from __future__ import annotations

import hashlib
import json
import math
import tarfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from catalog.flask_app.services.digital_twin_service import (
    MAX_INTERVAL_SECONDS,
    MIN_INTERVAL_SECONDS,
    UNAVAILABLE_EXECUTION_STATES,
    _classify_rows,
    _machine_identifier,
)


def digest(value: Any) -> str:
    """SHA256 of sorted-key ASCII JSON, matching the existing replay digest."""
    return hashlib.sha256(json.dumps(value, ensure_ascii=True, sort_keys=True,
                                     default=str).encode("utf-8")).hexdigest()


def _sha_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return math.nan


def _features(rows: list[dict]) -> dict:
    classified = _classify_rows(pd.DataFrame.from_records(rows))
    states = classified["state_class"].fillna("unclassified").tolist()
    ts = pd.to_datetime(classified["timestamp"], format="mixed", utc=True)
    ns = ts.to_numpy(dtype="datetime64[ns]").view("int64")
    deltas = [(int(b) - int(a)) / 1e9 for a, b in zip(ns, ns[1:])]
    execution = [str(row.get("execution") or "").strip().upper() for row in rows]
    fallback = [e in UNAVAILABLE_EXECUTION_STATES for e in execution]
    unique_groups: dict[int, set[str]] = defaultdict(set)
    for stamp, state in zip(ns, states):
        unique_groups[int(stamp)].add(state)
    return {
        "records": len(rows),
        "unique_timestamps": len(unique_groups),
        "duplicate_timestamp_records": len(rows) - len(unique_groups),
        "conflicting_state_timestamp_groups": sum(len(v) > 1 for v in unique_groups.values()),
        "missing_execution": sum(e == "" for e in execution),
        "unavailable_execution": sum(e == "UNAVAILABLE" for e in execution),
        "fallback_rows": sum(fallback),
        "fallback_classified_rows": sum(f and s != "unclassified" for f, s in zip(fallback, states)),
        "threshold_sensitive_rows": sum(
            f and 100 < _number(r.get("Srpm")) <= 150
            and not _number(r.get("Sload")) > 1
            for f, r in zip(fallback, rows)
        ),
        "active_rows": states.count("active"),
        "non_active_rows": states.count("non_active"),
        "unclassified_rows": states.count("unclassified"),
        "state_transitions": sum(a != b for a, b in zip(states, states[1:])),
        "long_gaps": sum(d > MAX_INTERVAL_SECONDS for d in deltas),
        "usable_intervals": sum(MIN_INTERVAL_SECONDS <= d <= MAX_INTERVAL_SECONDS for d in deltas),
        "classified_intervals": sum(
            MIN_INTERVAL_SECONDS <= d <= MAX_INTERVAL_SECONDS and s != "unclassified"
            for d, s in zip(deltas, states)
        ),
    }


def _line_ranges(numbers: list[int]) -> list[list[int]]:
    ranges: list[list[int]] = []
    for number in sorted(set(numbers)):
        if ranges and number == ranges[-1][1] + 1:
            ranges[-1][1] = number
        else:
            ranges.append([number, number])
    return ranges


def select_archive(archive_path: Path, windows_per_machine: int = 6,
                   target_records: int = 200) -> tuple[list[list[dict]], dict]:
    """Select disjoint timestamp-group blocks, balanced across machines.

    Global stable ordering is (UTC timestamp, member name, one-based line).
    A timestamp group is never split between windows. The first blocks with
    rare feature strata are selected, then temporal quantiles fill the quota.
    The fixed archive is read in full; no claim of population sampling follows.
    """
    if windows_per_machine < 1 or target_records < 2:
        raise ValueError("Positive machine quota and target_records >= 2 required")
    archive_path = Path(archive_path)
    before = archive_path.stat()
    source_sha = _sha_file(archive_path)
    machines: dict[str, list[tuple[str, int, str, dict]]] = defaultdict(list)
    members = []
    quality: Counter = Counter()
    schema: Counter = Counter()
    key_presence: Counter = Counter()
    with tarfile.open(archive_path, "r|gz") as archive:
        for member in archive:
            if not member.isfile() or not member.name.startswith("jsonl/") or not member.name.endswith(".jsonl"):
                continue
            stream = archive.extractfile(member)
            assert stream is not None
            content = stream.read()
            member_sha = hashlib.sha256(content).hexdigest()
            member_name_sha = hashlib.sha256(member.name.encode("utf-8")).hexdigest()
            members.append({"member_sha256": member_sha,
                            "member_name_sha256": member_name_sha,
                            "size_bytes": len(content)})
            for line_number, raw in enumerate(content.decode("utf-8").splitlines(), start=1):
                quality["lines"] += 1
                if not raw.strip():
                    quality["blank_lines"] += 1
                    continue
                try:
                    item = json.loads(raw)
                except json.JSONDecodeError:
                    quality["malformed_json_lines"] += 1
                    continue
                if not isinstance(item, dict):
                    quality["non_dictionary_lines"] += 1
                    continue
                quality["dictionary_records"] += 1
                machine = _machine_identifier(item)
                if machine is None:
                    quality["missing_machine_records"] += 1
                    continue
                schema[str(item.get("schema", "absent"))] += 1
                for field in ("execution", "Srpm", "Sload", "timestamp", "sequence", "received_at"):
                    key_presence[field] += field in item
                canonical = {"timestamp": item.get("timestamp"), "machine_id": machine,
                             "execution": item.get("execution"), "Srpm": item.get("Srpm"),
                             "Sload": item.get("Sload")}
                machines[machine].append((member.name, line_number, member_sha, canonical))
    windows = []
    selected_metadata = []
    inventory = []
    feature_order = ("threshold_sensitive_rows", "missing_execution", "unavailable_execution",
                     "fallback_classified_rows", "state_transitions", "long_gaps",
                     "active_rows", "non_active_rows")
    for machine in sorted(machines, key=lambda m: hashlib.sha256(m.encode()).hexdigest()):
        source_rows = machines[machine]
        parsed = pd.to_datetime([entry[3]["timestamp"] for entry in source_rows],
                                format="mixed", errors="coerce", utc=True)
        ordered = [(int(stamp.value), *entry) for stamp, entry in zip(parsed, source_rows)
                   if pd.notna(stamp)]
        quality["invalid_timestamp_records"] += len(source_rows) - len(ordered)
        ordered.sort(key=lambda entry: (entry[0], entry[1], entry[2]))
        anon = hashlib.sha256(machine.encode("utf-8")).hexdigest()[:12]
        candidates = []
        start = 0
        while start < len(ordered):
            end = min(start + target_records, len(ordered))
            while end < len(ordered) and ordered[end][0] == ordered[end - 1][0]:
                end += 1
            block = ordered[start:end]
            rows = [entry[4] for entry in block]
            features = _features(rows)
            if features["unique_timestamps"] >= 2:
                candidates.append((block, features))
            else:
                quality["excluded_single_timestamp_records"] += len(rows)
            start = end
        chosen = []
        covered = set()
        # Maximize coverage of still-unseen strata; earliest block breaks ties.
        while len(chosen) < min(windows_per_machine, len(candidates)):
            choices = [(sum(f[k] > 0 for k in feature_order if k not in covered), -i, i)
                       for i, (_, f) in enumerate(candidates) if i not in chosen]
            score, _, index = max(choices)
            if score == 0:
                break
            chosen.append(index)
            covered.update(k for k in feature_order if candidates[index][1][k] > 0)
        quantiles = [round(i * (len(candidates) - 1) / max(windows_per_machine - 1, 1))
                     for i in range(windows_per_machine)] if candidates else []
        for index in quantiles + list(range(len(candidates))):
            if len(chosen) >= windows_per_machine:
                break
            if index not in chosen:
                chosen.append(index)
        for index in sorted(chosen):
            block, features = candidates[index]
            rows = [entry[4] for entry in block]
            source_lines: dict[tuple[str, str], list[int]] = defaultdict(list)
            for _, name, number, sha, _ in block:
                source_lines[(sha, hashlib.sha256(name.encode("utf-8")).hexdigest())].append(number)
            windows.append(rows)
            selected_metadata.append({
                "window_id": f"{anon}-block-{index:04d}", "machine_hash": anon,
                "candidate_block_index": index, "window_sha256": digest(rows),
                "start_utc": pd.Timestamp(block[0][0], unit="ns", tz="UTC").isoformat(),
                "end_utc": pd.Timestamp(block[-1][0], unit="ns", tz="UTC").isoformat(),
                "features": features,
                "source_refs": [{"member_sha256": sha, "member_name_sha256": name_sha,
                                 "line_ranges_inclusive": _line_ranges(numbers)}
                                for (sha, name_sha), numbers in sorted(source_lines.items())],
            })
        inventory.append({"machine_hash": anon, "records": len(source_rows),
                          "valid_timestamp_records": len(ordered),
                          "unique_timestamps": len({entry[0] for entry in ordered}),
                          "candidate_windows": len(candidates), "selected_windows": len(chosen),
                          "inventory_features": _features([entry[4] for entry in ordered]) if ordered else {}})
    after = archive_path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError("Archive changed during selection; no reproducible manifest available")
    return windows, {
        "manifest_version": 1, "archive_sha256": source_sha, "archive_size_bytes": before.st_size,
        "source_revision": "immutable archive bytes identified by SHA256",
        "selection": {"strategy": "complete timestamp-group blocks; greedy feature coverage then temporal quantiles",
                      "windows_per_machine": windows_per_machine, "target_records": target_records,
                      "ordering": "UTC nanoseconds, member name lexicographically, one-based source line",
                      "normalization": "canonical fields only; absent -> null; no forward fill; invalid timestamps excluded",
                      "bias": "purposive balanced sample of first1000-batch verified subset; not representative of complete corpus",
                      "windows_selected": len(windows), "nonoverlap": "strict timestamp separation within each machine"},
        "quality": dict(quality), "schema_inventory": dict(schema), "field_presence": dict(key_presence),
        "members": members, "machine_inventory": inventory, "windows": selected_metadata,
    }
