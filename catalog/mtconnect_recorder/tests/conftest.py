"""Recorder-shaped MTConnect fixtures for canonical projection tests.

The documents built here mirror the *shape* of the real Mazak capture that
motivated this work — per-axis loads, a path feedrate, execution/program/tool
events, and controller conditions at sub-second cadence — with every identifying
value replaced. No machine name, program number, tool number, or controller
message from a production capture appears anywhere in this repository.

Batches are written through the recorder's own ``store_raw_batch`` so fixtures
exercise the real write path. A pre-rename capture is produced by rewriting only
the manifest's ``schema`` string afterwards: the compressed payload and its
``raw_sha256`` are byte-identical to what the current recorder writes, which is
exactly how a capture recorded before the rename differs from a current one.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape, quoteattr

import pytest

from catalog.mtconnect_recorder.parsing import parse_streams
from catalog.mtconnect_recorder.storage import DurableRecorderStore

MTCONNECT_NAMESPACE = "urn:mtconnect.org:MTConnectStreams:1.3"

LEGACY_RAW_BATCH_MANIFEST_SCHEMA = (
    bytes((109, 115, 104)).decode("ascii") + ".mtconnect.raw_batch_manifest.v1"
)

BASE_TIME = datetime(2026, 8, 7, 9, 0, 33, tzinfo=timezone.utc)


def stamp(offset_seconds: float, *, base: datetime = BASE_TIME) -> str:
    moment = base + timedelta(seconds=offset_seconds)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class Observation:
    """One observation to render into a ``ComponentStream``."""

    sequence: int
    component: str
    element: str
    data_item_id: str
    value: str
    category: str = "Samples"
    timestamp: str | None = None
    name: str | None = None
    component_id: str | None = None
    component_name: str | None = None
    attributes: dict[str, str] = field(default_factory=dict)


def _render_observation(item: Observation) -> str:
    attributes = {
        "dataItemId": item.data_item_id,
        "timestamp": item.timestamp or stamp(item.sequence * 0.5),
        "sequence": str(item.sequence),
    }
    if item.name:
        attributes["name"] = item.name
    attributes.update(item.attributes)
    rendered = " ".join(
        f"{key}={quoteattr(value)}" for key, value in sorted(attributes.items())
    )
    return f"<{item.element} {rendered}>{escape(item.value)}</{item.element}>"


def streams_document(
    *,
    instance_id: int,
    observations: list[Observation],
    device_name: str = "MachineAlpha",
    device_uuid: str = "MACHINE-ALPHA-0001",
    first_sequence: int | None = None,
    last_sequence: int | None = None,
    next_sequence: int | None = None,
    sender: str = "agent-alpha",
    buffer_size: int = 131072,
    creation_time: str = "2026-08-07T09:00:33Z",
) -> str:
    """Render a valid ``MTConnectStreams`` document for one device."""

    sequences = [item.sequence for item in observations]
    first = first_sequence if first_sequence is not None else min(sequences)
    last = last_sequence if last_sequence is not None else max(sequences)
    following = next_sequence if next_sequence is not None else last + 1

    grouped: dict[tuple[str, str | None, str | None], list[Observation]] = {}
    for item in observations:
        key = (item.component, item.component_id, item.component_name)
        grouped.setdefault(key, []).append(item)

    component_blocks: list[str] = []
    for (component, component_id, component_name), items in grouped.items():
        attributes = {"component": component}
        if component_id:
            attributes["componentId"] = component_id
        if component_name:
            attributes["name"] = component_name
        rendered_attributes = " ".join(
            f"{key}={quoteattr(value)}" for key, value in sorted(attributes.items())
        )
        sections: list[str] = []
        for section in ("Samples", "Events", "Condition"):
            in_section = [item for item in items if item.category == section]
            if not in_section:
                continue
            body = "".join(_render_observation(item) for item in in_section)
            sections.append(f"<{section}>{body}</{section}>")
        component_blocks.append(
            f"<ComponentStream {rendered_attributes}>{''.join(sections)}</ComponentStream>"
        )

    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<MTConnectStreams xmlns="{MTCONNECT_NAMESPACE}">'
        f'<Header creationTime="{creation_time}" sender="{sender}" '
        f'instanceId="{instance_id}" bufferSize="{buffer_size}" '
        f'firstSequence="{first}" lastSequence="{last}" nextSequence="{following}"/>'
        "<Streams>"
        f"<DeviceStream name={quoteattr(device_name)} uuid={quoteattr(device_uuid)}>"
        f"{''.join(component_blocks)}"
        "</DeviceStream>"
        "</Streams>"
        "</MTConnectStreams>"
    )


#: One cycle of the channels the reference capture actually carries. Spanning
#: all three MTConnect categories and every value kind — integer, decimal, the
#: ``UNAVAILABLE`` sentinel, text and empty — means a single fixture run covers
#: sample, event, condition, numeric, text, unavailable and empty handling.
_TEMPLATES: tuple[dict[str, Any], ...] = (
    {
        "component": "Linear",
        "component_id": "x1",
        "component_name": "X",
        "element": "Load",
        "data_item_id": "xl",
        "name": "Xload",
        "value": "3",
        "attributes": {"subType": "ACTUAL"},
    },
    {
        "component": "Linear",
        "component_id": "y1",
        "component_name": "Y",
        "element": "Load",
        "data_item_id": "yl",
        "name": "Yload",
        "value": "39.5",
    },
    {
        "component": "Path",
        "component_id": "p1",
        "component_name": "path",
        "element": "PathFeedrate",
        "data_item_id": "pf",
        "name": "Fovr",
        "value": "UNAVAILABLE",
    },
    {
        "component": "Controller",
        "component_id": "c1",
        "component_name": "controller",
        "category": "Events",
        "element": "Execution",
        "data_item_id": "exec",
        "name": "execution",
        "value": "ACTIVE",
    },
    {
        "component": "Path",
        "component_id": "p1",
        "component_name": "path",
        "category": "Events",
        "element": "Program",
        "data_item_id": "pgm",
        "name": "program",
        "value": "ANONYMISED-PROGRAM",
    },
    {
        "component": "Controller",
        "component_id": "c1",
        "component_name": "controller",
        "category": "Condition",
        "element": "Warning",
        "data_item_id": "cond_sys",
        "name": "system",
        "value": "ANONYMISED CONTROLLER MESSAGE",
        "attributes": {
            "nativeCode": "1234",
            "nativeSeverity": "2",
            "qualifier": "HIGH",
        },
    },
)


def observation_run(
    *,
    start_sequence: int,
    count: int,
    base: datetime = BASE_TIME,
) -> list[Observation]:
    """Return observations addressed by sequence number.

    The observation at a given sequence is always identical regardless of which
    batch it is sliced into. That is what makes an overlapping replay a genuine
    repeat of the same observation rather than a different one that happens to
    share an identity — the property the projection's overlap check relies on.
    """

    run: list[Observation] = []
    for sequence in range(start_sequence, start_sequence + count):
        template = dict(_TEMPLATES[(sequence - 1) % len(_TEMPLATES)])
        attributes = dict(template.pop("attributes", {}))
        run.append(
            Observation(
                sequence=sequence,
                timestamp=stamp((sequence - 1) * 0.5, base=base),
                attributes=attributes,
                **template,
            )
        )
    return run


def machine_batch(
    *,
    instance_id: int,
    start_sequence: int,
    count: int = 6,
    device_name: str = "MachineAlpha",
    device_uuid: str = "MACHINE-ALPHA-0001",
    base: datetime = BASE_TIME,
) -> tuple[str, list[Observation]]:
    """Return one batch document plus the observations it contains."""

    observations = observation_run(
        start_sequence=start_sequence, count=count, base=base
    )
    document = streams_document(
        instance_id=instance_id,
        observations=observations,
        device_name=device_name,
        device_uuid=device_uuid,
    )
    return document, observations


class RecorderArchive:
    """Write recorder-shaped raw batches into a temporary data directory."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.store = DurableRecorderStore(self.data_dir)

    def write_batch(
        self,
        *,
        source_name: str,
        xml_text: str,
        requested_from: int | None = None,
        legacy_manifest: bool = False,
        manifest_schema: str | None = None,
    ) -> Path:
        """Store one batch and return the manifest path.

        ``legacy_manifest`` rewrites only the manifest ``schema`` field, leaving
        the compressed payload and its digest untouched.
        """

        batch = parse_streams(xml_text, source_name=source_name, probe=None)
        ref = self.store.store_raw_batch(
            source_name=source_name,
            requested_from=(
                requested_from
                if requested_from is not None
                else int(batch.first_observation_sequence or 1)
            ),
            xml_text=xml_text,
            batch=batch,
        )
        schema = manifest_schema
        if schema is None and legacy_manifest:
            schema = LEGACY_RAW_BATCH_MANIFEST_SCHEMA
        if schema is not None:
            self.rewrite_manifest(ref.manifest_path, schema=schema)
        return ref.manifest_path

    @staticmethod
    def read_manifest(manifest_path: Path) -> dict[str, Any]:
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    @classmethod
    def rewrite_manifest(cls, manifest_path: Path, **changes: Any) -> None:
        payload = cls.read_manifest(manifest_path)
        payload.update(changes)
        manifest_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def raw_payload_path(manifest_path: Path) -> Path:
        return manifest_path.with_name(
            manifest_path.name.removesuffix(".manifest.json") + ".gz"
        )


@pytest.fixture
def recorder_archive(tmp_path: Path) -> RecorderArchive:
    return RecorderArchive(tmp_path / "data")
