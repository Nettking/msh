"""Profile a directory of recorder raw MTConnect batches.

The FCP recorder stores every fetched MTConnect ``<MTConnectStreams>`` response
as ``seq-<first>-<last>-next-<next>-<sha256>.xml.gz`` beside a
``.manifest.json`` sidecar. This script reads such a directory and reports what
a digital-twin model can actually be built from: integrity, sequence
continuity, observed components and data items, sampling cadence, and the
controller state timeline.

It is read-only and does not require a running FCP installation.

    python scripts/profile_mtconnect_raw_batches.py <directory> [--json]
"""

from __future__ import annotations

import argparse
import datetime as _dt
import gzip
import json
import statistics
import xml.etree.ElementTree as ET
import zlib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from hashlib import sha256
from itertools import pairwise
from pathlib import Path

MANIFEST_SUFFIX = ".manifest.json"
# Kept deliberately in step with catalog/mtconnect_recorder/schema_compat.py,
# which is the product-side authority. This script stays importable without the
# catalog package so it can profile a capture on a machine that has no FCP
# installation, so the enumeration is repeated here rather than imported.
_RETIRED_PRODUCT = bytes((109, 115, 104)).decode("ascii")
ACCEPTED_MANIFEST_SCHEMAS = frozenset(
    {
        "fcp.mtconnect.raw_batch_manifest.v1",
        # Batches recorded before the product rename carry the retired prefix.
        f"{_RETIRED_PRODUCT}.mtconnect.raw_batch_manifest.v1",
    }
)
OBSERVATION_CONTAINERS = frozenset({"Samples", "Events", "Condition"})
STATE_CHANNELS = {
    "exec": "Execution",
    "mode": "ControllerMode",
    "pgm": "Program",
    "avail": "Availability",
    "estop": "EmergencyStop",
}


@dataclass
class Observation:
    sequence: int
    timestamp: str
    data_item_id: str
    name: str | None
    observation_type: str
    component: str
    value: str
    #: Which Agent buffer this observation came from. Sequence numbers restart
    #: per agent instance, so anything ordered or differenced must stay inside
    #: one stream. Defaults keep hand-built observations usable in tests.
    source_name: str = ""
    agent_instance_id: str = ""

    @property
    def channel(self) -> tuple[str, str, str]:
        """Identify the one physical channel this observation belongs to."""

        return (self.source_name, self.agent_instance_id, self.component)


@dataclass
class Profile:
    batch_count: int = 0
    manifest_count: int = 0
    digest_mismatches: list[str] = field(default_factory=list)
    unreadable: list[str] = field(default_factory=list)
    schemas: Counter = field(default_factory=Counter)
    sources: Counter = field(default_factory=Counter)
    instances: Counter = field(default_factory=Counter)
    observations: list[Observation] = field(default_factory=list)
    sequence_gaps: list[tuple[int, int]] = field(default_factory=list)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_timestamp(value: str) -> _dt.datetime | None:
    try:
        return _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None


def _iter_observations(
    xml_text: str,
    *,
    source_name: str = "",
    agent_instance_id: str = "",
) -> list[Observation]:
    root = ET.fromstring(xml_text)
    found: list[Observation] = []
    for device_stream in root.iter():
        if _local_name(device_stream.tag) != "ComponentStream":
            continue
        component = (
            f"{device_stream.get('component')}/{device_stream.get('name')}"
            if device_stream.get("name")
            else str(device_stream.get("component"))
        )
        for container in device_stream:
            if _local_name(container.tag) not in OBSERVATION_CONTAINERS:
                continue
            for element in container:
                sequence = element.get("sequence")
                if sequence is None:
                    continue
                found.append(
                    Observation(
                        sequence=int(sequence),
                        timestamp=str(element.get("timestamp") or ""),
                        data_item_id=str(element.get("dataItemId") or ""),
                        name=element.get("name"),
                        observation_type=_local_name(element.tag),
                        component=component,
                        value=(element.text or "").strip(),
                        source_name=source_name,
                        agent_instance_id=agent_instance_id,
                    )
                )
    return found


def profile_directory(directory: Path) -> Profile:
    profile = Profile()
    manifests = sorted(directory.rglob(f"*{MANIFEST_SUFFIX}"))
    spans_by_stream: defaultdict[tuple[str, str], list[tuple[int, int, int]]] = (
        defaultdict(list)
    )

    for manifest_path in manifests:
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            profile.unreadable.append(str(manifest_path))
            continue

        profile.manifest_count += 1
        profile.schemas[str(payload.get("schema"))] += 1
        if str(payload.get("schema")) not in ACCEPTED_MANIFEST_SCHEMAS:
            continue
        profile.sources[str(payload.get("source_name"))] += 1
        profile.instances[str(payload.get("agent_instance_id"))] += 1

        raw_path = manifest_path.with_name(
            manifest_path.name.removesuffix(MANIFEST_SUFFIX) + ".gz"
        )
        if not raw_path.exists():
            profile.unreadable.append(str(raw_path))
            continue

        try:
            raw_bytes = gzip.decompress(raw_path.read_bytes())
        except (OSError, ValueError, EOFError, zlib.error):
            # Truncation raises EOFError and a bad header raises BadGzipFile,
            # but corruption inside the deflate stream surfaces as a bare
            # zlib.error, which is the most common single-byte damage. Profiling
            # a directory must survive one damaged payload.
            profile.unreadable.append(str(raw_path))
            continue

        # The manifest digest covers the uncompressed XML, not the .gz payload.
        if sha256(raw_bytes).hexdigest() != str(payload.get("raw_sha256")):
            profile.digest_mismatches.append(str(raw_path))

        try:
            profile.observations.extend(
                _iter_observations(
                    raw_bytes.decode("utf-8", errors="replace"),
                    source_name=str(payload.get("source_name")),
                    agent_instance_id=str(payload.get("agent_instance_id")),
                )
            )
        except (ET.ParseError, ValueError):
            profile.unreadable.append(str(raw_path))
            continue

        profile.batch_count += 1
        try:
            spans_by_stream[
                (
                    str(payload.get("source_name")),
                    str(payload.get("agent_instance_id")),
                )
            ].append(
                (
                    int(payload["first_observation_sequence"]),
                    int(payload["last_observation_sequence"]),
                    int(payload["next_sequence"]),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue

    # Sequence numbers belong to one Agent buffer, so continuity is only
    # meaningful within a single (source, instance). Chaining every batch in the
    # directory through one cursor invents reverse gaps when two instances both
    # start at 1, and can hide a real gap when sources interleave.
    for _stream, stream_spans in sorted(spans_by_stream.items()):
        stream_spans.sort()
        expected_next: int | None = None
        for first, _last, next_sequence in stream_spans:
            if expected_next is not None and first != expected_next:
                profile.sequence_gaps.append((expected_next, first))
            expected_next = next_sequence

    profile.observations.sort(key=lambda observation: observation.sequence)
    return profile


def _cadence(observations: list[Observation]) -> dict[str, float]:
    stamps = [
        parsed
        for parsed in (_parse_timestamp(item.timestamp) for item in observations)
        if parsed is not None
    ]
    deltas = sorted(
        (later - earlier).total_seconds() for earlier, later in pairwise(stamps)
    )
    if not deltas:
        return {}
    return {
        "median_seconds": round(statistics.median(deltas), 3),
        "p95_seconds": round(deltas[int(len(deltas) * 0.95)], 3),
        "max_seconds": round(deltas[-1], 3),
    }


def _by_channel(observations: list[Observation]) -> list[list[Observation]]:
    """Split one observation type into its independent physical channels.

    Grouping is by (source, agent instance, component) because all three can
    repeat inside one capture directory: two machines, one machine restarted so
    its sequence numbers begin again, and one machine exposing the same type on
    several Path or Controller components.
    """

    grouped: dict[tuple[str, str, str], list[Observation]] = defaultdict(list)
    for observation in observations:
        grouped[observation.channel].append(observation)
    return [grouped[key] for key in sorted(grouped)]


def _accumulate_dwell(dwell: Counter, series: list[Observation]) -> None:
    """Add each state's duration within one channel to the running total."""

    for current, following in pairwise(series):
        started = _parse_timestamp(current.timestamp)
        ended = _parse_timestamp(following.timestamp)
        if started and ended:
            dwell[current.value] += round((ended - started).total_seconds(), 3)


def summarize(profile: Profile) -> dict:
    by_data_item: dict[str, list[Observation]] = defaultdict(list)
    by_observation_type: dict[str, list[Observation]] = defaultdict(list)
    for observation in profile.observations:
        by_data_item[observation.data_item_id].append(observation)
        by_observation_type[observation.observation_type].append(observation)

    timestamps = [
        item.timestamp for item in profile.observations if item.timestamp
    ]
    state_timeline: dict[str, list[dict[str, str]]] = {}
    for output_name, observation_type in STATE_CHANNELS.items():
        series = by_observation_type.get(observation_type, [])
        # A capture directory can hold several machines, several agent
        # instances of one machine, and several Path/Controller components of
        # one machine. They all publish the same observation type, so name the
        # channel on every entry rather than presenting one merged series as if
        # it described a single thing.
        state_timeline[output_name] = [
            {
                "timestamp": item.timestamp,
                "value": item.value,
                "component": item.component,
            }
            for item in series
        ]

    dwell: Counter = Counter()
    # Dwell is a difference between consecutive states, so it is only defined
    # inside one channel. Differencing the merged series attributes the gap
    # between two machines to whichever state happened to come first, which
    # silently produces plausible but wrong durations.
    for channel_series in _by_channel(by_observation_type.get("Execution", [])):
        _accumulate_dwell(dwell, channel_series)

    return {
        "batches": profile.batch_count,
        "manifests": profile.manifest_count,
        "manifest_schemas": dict(profile.schemas),
        "sources": dict(profile.sources),
        "agent_instances": dict(profile.instances),
        "observations": len(profile.observations),
        "unavailable_observations": sum(
            1 for item in profile.observations if item.value == "UNAVAILABLE"
        ),
        "digest_mismatches": profile.digest_mismatches,
        "unreadable": profile.unreadable,
        "sequence_gaps": profile.sequence_gaps,
        "time_span": {
            "first": min(timestamps) if timestamps else None,
            "last": max(timestamps) if timestamps else None,
        },
        "components": dict(
            Counter(item.component for item in profile.observations).most_common()
        ),
        "observation_types": dict(
            Counter(
                item.observation_type for item in profile.observations
            ).most_common()
        ),
        "data_items": {
            data_item_id: {
                "count": len(series),
                "name": series[0].name,
                "type": series[0].observation_type,
                "component": series[0].component,
                "cadence": _cadence(series),
            }
            for data_item_id, series in sorted(
                by_data_item.items(), key=lambda pair: -len(pair[1])
            )
        },
        "execution_dwell_seconds": dict(dwell.most_common()),
        "state_timeline": state_timeline,
    }


def _print_report(summary: dict) -> None:
    print(f"batches:        {summary['batches']}")
    print(f"observations:   {summary['observations']}")
    print(f"time span:      {summary['time_span']['first']} -> {summary['time_span']['last']}")
    print(f"sources:        {summary['sources']}")
    print(f"agent instance: {summary['agent_instances']}")
    print(f"schemas:        {summary['manifest_schemas']}")
    print(f"digest errors:  {len(summary['digest_mismatches'])}")
    print(f"unreadable:     {len(summary['unreadable'])}")
    print(f"sequence gaps:  {len(summary['sequence_gaps'])} {summary['sequence_gaps'][:5]}")
    print(f"UNAVAILABLE:    {summary['unavailable_observations']}")

    print("\ncomponents:")
    for component, count in summary["components"].items():
        print(f"  {component:<24} {count}")

    print("\ntop data items:")
    for data_item_id, detail in list(summary["data_items"].items())[:25]:
        cadence = detail["cadence"].get("median_seconds")
        cadence_text = f"median {cadence}s" if cadence is not None else "event"
        print(
            f"  {data_item_id:<14} {detail['count']:>7}  "
            f"{detail['type']:<20} {detail['component']:<20} {cadence_text}"
        )

    print("\nexecution dwell (seconds):")
    for value, seconds in summary["execution_dwell_seconds"].items():
        print(f"  {value:<18} {seconds}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument(
        "--json", action="store_true", help="Emit the full summary as JSON."
    )
    arguments = parser.parse_args()

    if not arguments.directory.is_dir():
        parser.error(f"{arguments.directory} is not a directory")

    summary = summarize(profile_directory(arguments.directory))
    if arguments.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_report(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
