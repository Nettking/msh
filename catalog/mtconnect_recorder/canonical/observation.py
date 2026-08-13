"""One MTConnect observation, in a form that does not need the XML again.

The representation is deliberately generic. It carries the MTConnect concepts
that every Agent emits — category, type, sub type, component, data item, value,
units, condition state — and keeps everything else it was given in a residual
attribute map. It does not model any machine's particular data items, so the
same table holds a Mazak, a Haas, and a bare adapter without a schema change.

Identity
--------

The durable natural identity of an observation is::

    (source_key, agent_instance_id, sequence)

``sequence`` is unique per observation *within one Agent instance*: MTConnect
assigns a distinct sequence number to every observation an Agent publishes.
``instanceId`` changes whenever the Agent reinitialises its buffer, which is
exactly when sequence numbers restart, so the pair survives an Agent restart
without collision.

``instanceId`` alone is not enough across machines. It is conventionally the
Agent's start time in seconds, so two Agents that start in the same second
share it. ``source_key`` closes that: it is ``_slug(source_name)``, the same
partition the recorder already writes its immutable raw batches under
(``raw/<source_key>/<instance_id>/``). Using the recorder's own partitioning
rather than a new notion of machine identity means the projection and the raw
evidence layer agree by construction, and recorder source names are already
validated to be unique per installation.

No random identifiers are generated anywhere. Every field below is a pure
function of the raw batch it came from.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..model import _slug

#: MTConnect observation categories, as they appear on the stream document.
CATEGORY_SAMPLE = "SAMPLE"
CATEGORY_EVENT = "EVENT"
CATEGORY_CONDITION = "CONDITION"

#: How a value was interpreted. ``native_value`` always keeps the exact text.
VALUE_NUMBER = "number"
VALUE_TEXT = "text"
VALUE_UNAVAILABLE = "unavailable"
VALUE_EMPTY = "empty"

#: Where ``observed_at`` came from. MTConnect requires a timestamp on every
#: observation; ``batch`` records the rare document that omitted one, in which
#: case the recorder's manifest ``received_at`` is used so the projection stays
#: deterministic instead of reading a wall clock.
TIMESTAMP_FROM_OBSERVATION = "observation"
TIMESTAMP_FROM_BATCH = "batch"

#: The MTConnect sentinel meaning "this data item currently has no value".
UNAVAILABLE = "UNAVAILABLE"

#: Attributes promoted to their own field. Everything else survives in
#: ``attributes`` so no information is lost by canonicalisation.
_PROMOTED_ATTRIBUTES = frozenset(
    {
        "dataItemId",
        "name",
        "nativeCode",
        "nativeSeverity",
        "qualifier",
        "sequence",
        "subType",
        "timestamp",
    }
)


class CanonicalObservationError(ValueError):
    """Raised when a raw observation cannot be canonicalised safely."""


def source_key(source_name: str) -> str:
    """Return the recorder's own storage partition key for a source name."""

    return _slug(source_name)


def _parse_timestamp(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise CanonicalObservationError("observation timestamp is empty")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CanonicalObservationError(
            f"observation timestamp is not ISO-8601: {value!r}"
        ) from exc
    if parsed.tzinfo is None:
        # MTConnect timestamps are UTC. A document that omits the designator is
        # read as UTC rather than as this machine's local time, so the same
        # capture projects identically wherever it is rebuilt.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _epoch_micros(moment: datetime) -> int:
    """Return exact integer microseconds; never a float for range queries."""

    return round(moment.timestamp() * 1_000_000)


def _canonical_stamp(moment: datetime) -> str:
    """Return one fixed-width UTC spelling for every observation.

    ``datetime.isoformat()`` omits the fractional part entirely when it is zero,
    which would give rows of two different widths and make text comparison
    disagree with time comparison. Pinning the precision keeps the canonical
    form a single normal form.
    """

    return moment.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _classify_value(text: str, *, category: str) -> tuple[str, float | None, str | None]:
    stripped = text.strip()
    if stripped == "":
        return VALUE_EMPTY, None, None
    if category != CATEGORY_CONDITION and stripped.upper() == UNAVAILABLE:
        # For a condition the element name already carries Unavailable as the
        # state, and the text is a message. Only non-condition observations use
        # the value itself as the sentinel.
        return VALUE_UNAVAILABLE, None, None
    try:
        return VALUE_NUMBER, float(stripped), None
    except ValueError:
        return VALUE_TEXT, None, stripped


@dataclass(frozen=True)
class CanonicalObservation:
    """One MTConnect observation with its provenance and typed value."""

    # -- identity ---------------------------------------------------------
    source_key: str
    agent_instance_id: int
    sequence: int

    # -- capture provenance ----------------------------------------------
    source_name: str
    raw_batch_sha256: str

    # -- time --------------------------------------------------------------
    observed_at: str
    observed_at_us: int
    timestamp_source: str

    # -- device ------------------------------------------------------------
    machine_id: str
    device_uuid: str | None
    device_name: str | None

    # -- component ---------------------------------------------------------
    component_type: str | None
    component_id: str | None
    component_name: str | None
    component_path: str | None

    # -- data item ---------------------------------------------------------
    data_item_id: str | None
    data_item_name: str | None
    category: str
    observation_type: str
    mtconnect_type: str | None
    sub_type: str | None

    # -- value -------------------------------------------------------------
    value_kind: str
    value_number: float | None
    value_text: str | None
    native_value: str
    units: str | None
    native_units: str | None

    # -- condition ---------------------------------------------------------
    condition_state: str | None
    condition_native_code: str | None
    condition_native_severity: str | None
    condition_qualifier: str | None

    # -- residual ----------------------------------------------------------
    attributes: Mapping[str, str]

    @property
    def identity(self) -> tuple[str, int, int]:
        return (self.source_key, self.agent_instance_id, self.sequence)

    @property
    def value(self) -> float | str | None:
        """Return the typed value, or ``None`` when unavailable or empty."""

        if self.value_kind == VALUE_NUMBER:
            return self.value_number
        if self.value_kind == VALUE_TEXT:
            return self.value_text
        return None

    def attributes_json(self) -> str:
        return json.dumps(dict(self.attributes), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_recorder_record(
        cls,
        record: Mapping[str, Any],
        *,
        source_name: str,
        agent_instance_id: int,
        raw_batch_sha256: str,
        fallback_timestamp: str | None = None,
    ) -> CanonicalObservation:
        """Canonicalise one record produced by :func:`parsing.parse_streams`.

        The recorder's parser is reused rather than reimplemented so the
        projection cannot drift from what the recorder itself understands.
        """

        sequence = record.get("sequence")
        if sequence is None or isinstance(sequence, bool) or not isinstance(sequence, int):
            raise CanonicalObservationError(
                "observation has no integer sequence number and cannot be identified"
            )

        category = str(record.get("category") or "").upper()
        if category not in {CATEGORY_SAMPLE, CATEGORY_EVENT, CATEGORY_CONDITION}:
            raise CanonicalObservationError(
                f"observation has an unknown MTConnect category: {category!r}"
            )

        raw_attributes = record.get("attributes")
        attributes: dict[str, str] = (
            {str(key): str(value) for key, value in raw_attributes.items()}
            if isinstance(raw_attributes, Mapping)
            else {}
        )

        declared_timestamp = attributes.get("timestamp")
        if declared_timestamp and declared_timestamp.strip():
            timestamp_text = declared_timestamp
            timestamp_source = TIMESTAMP_FROM_OBSERVATION
        elif fallback_timestamp and fallback_timestamp.strip():
            timestamp_text = fallback_timestamp
            timestamp_source = TIMESTAMP_FROM_BATCH
        else:
            raise CanonicalObservationError(
                f"observation {sequence} has no timestamp and the batch supplies none"
            )
        moment = _parse_timestamp(timestamp_text)

        observation_type = str(record.get("observation_type") or "")
        if not observation_type:
            raise CanonicalObservationError(
                f"observation {sequence} has no MTConnect element name"
            )

        native_value = str(record.get("native_value") or "")
        value_kind, value_number, value_text = _classify_value(
            native_value, category=category
        )

        component_type = _optional(record.get("stream_component"))
        component_name = _optional(record.get("stream_component_name"))
        component_id = _optional(record.get("stream_component_id"))
        # Derived from the stream document alone so it is present even for a
        # capture with no archived probe.
        leaf = component_name or component_id
        component_path = (
            f"{component_type}/{leaf}" if component_type and leaf else component_type
        )

        condition_state = (
            observation_type.upper() if category == CATEGORY_CONDITION else None
        )

        return cls(
            source_key=source_key(source_name),
            agent_instance_id=int(agent_instance_id),
            sequence=int(sequence),
            source_name=source_name,
            raw_batch_sha256=raw_batch_sha256,
            observed_at=_canonical_stamp(moment),
            observed_at_us=_epoch_micros(moment),
            timestamp_source=timestamp_source,
            machine_id=str(record.get("machine_id") or source_name),
            device_uuid=_optional(record.get("device_uuid")),
            device_name=_optional(record.get("device_name")),
            component_type=component_type,
            component_id=component_id,
            component_name=component_name,
            component_path=component_path,
            data_item_id=_optional(record.get("data_item_id")),
            data_item_name=_optional(record.get("name")),
            category=category,
            observation_type=observation_type,
            mtconnect_type=_optional(record.get("type")),
            # The probe is authoritative for sub type, but an Agent may also
            # declare it on the observation. Falling back to the attribute means
            # a capture with no archived probe keeps the value instead of
            # dropping it as a promoted-but-unfilled field.
            sub_type=_optional(record.get("sub_type"))
            or _optional(attributes.get("subType")),
            value_kind=value_kind,
            value_number=value_number,
            value_text=value_text,
            native_value=native_value,
            units=_optional(record.get("units")),
            native_units=_optional(record.get("native_units")),
            condition_state=condition_state,
            condition_native_code=_optional(attributes.get("nativeCode")),
            condition_native_severity=_optional(attributes.get("nativeSeverity")),
            condition_qualifier=_optional(attributes.get("qualifier")),
            attributes={
                key: value
                for key, value in sorted(attributes.items())
                if key not in _PROMOTED_ATTRIBUTES
            },
        )


def _optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "CATEGORY_CONDITION",
    "CATEGORY_EVENT",
    "CATEGORY_SAMPLE",
    "TIMESTAMP_FROM_BATCH",
    "TIMESTAMP_FROM_OBSERVATION",
    "UNAVAILABLE",
    "VALUE_EMPTY",
    "VALUE_NUMBER",
    "VALUE_TEXT",
    "VALUE_UNAVAILABLE",
    "CanonicalObservation",
    "CanonicalObservationError",
    "source_key",
]
