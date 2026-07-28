"""MTConnect /probe and /sample parsing."""
from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha256
from typing import Any
from xml.etree import ElementTree as ET

from .model import (
    MtconnectProtocolError,
    ParsedBatch,
    ProbeModel,
    SequencePlan,
    SourceCheckpoint,
    StreamHeader,
    _find_first,
    _local_name,
    _try_number,
    _utc_now,
)


def machine_display_name(
    *,
    device_name: str | None,
    serial_number: str | None,
    machine_id: str | None,
    fallback: str,
) -> str:
    """Return a readable name that cannot collapse same-brand machines.

    MTConnect adapters often expose a generic Device name such as ``Mazak``.
    The serial number or UUID from the same MTConnect document is therefore
    included whenever it is available.
    """

    name = str(device_name or "").strip()
    serial = str(serial_number or "").strip()
    identity = str(machine_id or "").strip()
    if serial:
        if name and serial.casefold() not in name.casefold():
            return f"{name} {serial}"
        return name or serial
    if name and identity and name.casefold() != identity.casefold():
        return f"{name} [{identity}]"
    return name or identity or fallback


def parse_stream_header(xml_text: str) -> StreamHeader:
    root = ET.fromstring(xml_text)
    if _local_name(root.tag) == "MTConnectError":
        error = _find_first(root, "Error")
        code = error.attrib.get("errorCode", "UNKNOWN") if error is not None else "UNKNOWN"
        detail = (error.text or "MTConnect Agent returned an error").strip() if error is not None else ""
        raise MtconnectProtocolError(f"{code}: {detail}")
    header = _find_first(root, "Header")
    if header is None:
        raise MtconnectProtocolError("MTConnect document did not contain Header.")
    required = ("instanceId", "firstSequence", "lastSequence", "nextSequence")
    missing = [name for name in required if header.attrib.get(name) is None]
    if missing:
        raise MtconnectProtocolError(
            "MTConnect Header is missing required attributes: " + ", ".join(missing)
        )
    try:
        return StreamHeader(
            instance_id=int(header.attrib["instanceId"]),
            first_sequence=int(header.attrib["firstSequence"]),
            last_sequence=int(header.attrib["lastSequence"]),
            next_sequence=int(header.attrib["nextSequence"]),
            creation_time=header.attrib.get("creationTime"),
            sender=header.attrib.get("sender"),
            buffer_size=(int(header.attrib["bufferSize"]) if header.attrib.get("bufferSize") else None),
        )
    except ValueError as exc:
        raise MtconnectProtocolError(f"Invalid numeric MTConnect Header value: {exc}") from exc


def parse_probe(xml_text: str) -> ProbeModel:
    raw = xml_text.encode("utf-8")
    digest = sha256(raw).hexdigest()
    root = ET.fromstring(xml_text)
    if _local_name(root.tag) == "MTConnectError":
        parse_stream_header(xml_text)

    devices: dict[str, dict[str, Any]] = {}
    data_items: dict[str, dict[str, Any]] = {}

    for device in (element for element in root.iter() if _local_name(element.tag) == "Device"):
        device_id = device.attrib.get("id") or device.attrib.get("uuid") or device.attrib.get("name") or "device"
        description = next(
            (element for element in device if _local_name(element.tag) == "Description"),
            None,
        )
        device_info: dict[str, Any] = {
            "id": device.attrib.get("id"),
            "name": device.attrib.get("name"),
            "uuid": device.attrib.get("uuid"),
            "attributes": dict(device.attrib),
        }
        if description is not None:
            device_info["description"] = (description.text or "").strip() or None
            device_info["description_attributes"] = dict(description.attrib)
            if description.attrib.get("serialNumber"):
                device_info["serial_number"] = description.attrib["serialNumber"]
        devices[str(device_id)] = device_info

        def walk(
            element: ET.Element,
            component_path: list[dict[str, Any]],
            device_metadata: dict[str, Any],
        ) -> None:
            local = _local_name(element.tag)
            current_path = component_path
            if local not in {
                "Device",
                "Components",
                "DataItems",
                "Description",
                "Constraints",
                "Value",
                "Configuration",
            } and local != "DataItem":
                current_path = component_path + [
                    {
                        "type": local,
                        "id": element.attrib.get("id"),
                        "name": element.attrib.get("name"),
                        "native_name": element.attrib.get("nativeName"),
                    }
                ]

            if local == "DataItem":
                data_item_id = element.attrib.get("id")
                if not data_item_id:
                    return
                constraints = [
                    (child.text or "").strip()
                    for child in element.iter()
                    if _local_name(child.tag) == "Value" and (child.text or "").strip()
                ]
                data_items[data_item_id] = {
                    "data_item_id": data_item_id,
                    "name": element.attrib.get("name"),
                    "category": element.attrib.get("category"),
                    "type": element.attrib.get("type"),
                    "sub_type": element.attrib.get("subType"),
                    "units": element.attrib.get("units"),
                    "native_units": element.attrib.get("nativeUnits"),
                    "coordinate_system": element.attrib.get("coordinateSystem"),
                    "representation": element.attrib.get("representation"),
                    "sample_rate": element.attrib.get("sampleRate"),
                    "device_id": device_metadata.get("id"),
                    "device_name": device_metadata.get("name"),
                    "device_uuid": device_metadata.get("uuid"),
                    "serial_number": device_metadata.get("serial_number"),
                    "component_path": current_path,
                    "constraints": constraints,
                    "attributes": dict(element.attrib),
                }
                return

            for child in element:
                walk(child, current_path, device_metadata)

        walk(device, [], device_info)

    return ProbeModel(sha256=digest, devices=devices, data_items=data_items)


def _iter_stream_observations(root: ET.Element) -> Iterable[tuple[dict[str, Any], str, ET.Element]]:
    for device_stream in (
        element for element in root.iter() if _local_name(element.tag) == "DeviceStream"
    ):
        device_context = {
            "stream_device_name": device_stream.attrib.get("name"),
            "stream_device_uuid": device_stream.attrib.get("uuid"),
        }
        for component_stream in (
            element
            for element in device_stream.iter()
            if _local_name(element.tag) == "ComponentStream"
        ):
            context = {
                **device_context,
                "stream_component": component_stream.attrib.get("component"),
                "stream_component_id": component_stream.attrib.get("componentId"),
                "stream_component_name": component_stream.attrib.get("name"),
            }
            for section in component_stream:
                section_name = _local_name(section.tag)
                if section_name not in {"Samples", "Events", "Condition"}:
                    continue
                category = {
                    "Samples": "SAMPLE",
                    "Events": "EVENT",
                    "Condition": "CONDITION",
                }[section_name]
                for observation in section:
                    yield context, category, observation


def parse_streams(
    xml_text: str,
    *,
    source_name: str,
    probe: ProbeModel | None,
    received_at: str | None = None,
) -> ParsedBatch:
    header = parse_stream_header(xml_text)
    root = ET.fromstring(xml_text)
    received = received_at or _utc_now()
    observations: list[dict[str, Any]] = []
    sequences: list[int] = []

    for ordinal, (stream_context, category, element) in enumerate(
        _iter_stream_observations(root)
    ):
        attributes = dict(element.attrib)
        sequence_raw = attributes.get("sequence")
        try:
            sequence = int(sequence_raw) if sequence_raw is not None else None
        except ValueError:
            sequence = None
        if sequence is not None:
            sequences.append(sequence)

        data_item_id = attributes.get("dataItemId")
        metadata = probe.data_items.get(data_item_id, {}) if probe and data_item_id else {}
        native_value = (element.text or "").strip()
        device_uuid = (
            stream_context.get("stream_device_uuid")
            or metadata.get("device_uuid")
        )
        machine_id = str(
            device_uuid
            or metadata.get("serial_number")
            or metadata.get("device_id")
            or source_name
        )
        device_name = (
            stream_context.get("stream_device_name")
            or metadata.get("device_name")
        )
        serial_number = metadata.get("serial_number")
        machine_name = machine_display_name(
            device_name=device_name,
            serial_number=serial_number,
            machine_id=machine_id,
            fallback=source_name,
        )
        observation_type = _local_name(element.tag)
        source_record_id = (
            f"{header.instance_id}:{sequence}:{data_item_id or observation_type}:{ordinal}"
        )

        record: dict[str, Any] = {
            "schema": "msh.mtconnect.observation.v2",
            "source": "mtconnect_recorder",
            "source_name": source_name,
            "source_record_id": source_record_id,
            "machine": machine_name,
            "machine_name": machine_name,
            "machine_id": machine_id,
            "agent_instance_id": header.instance_id,
            "sequence": sequence,
            "timestamp": attributes.get("timestamp") or received,
            "received_at": received,
            "data_item_id": data_item_id,
            "name": attributes.get("name") or metadata.get("name"),
            "category": category,
            "measurement_type": f"mtconnect_{category.lower()}",
            "observation_type": observation_type,
            "type": metadata.get("type") or observation_type,
            "sub_type": metadata.get("sub_type"),
            "value": _try_number(native_value),
            "native_value": native_value,
            "unit": metadata.get("units"),
            "units": metadata.get("units"),
            "native_units": metadata.get("native_units"),
            "coordinate_system": metadata.get("coordinate_system"),
            "component_path": metadata.get("component_path", []),
            "stream_component": stream_context.get("stream_component"),
            "stream_component_id": stream_context.get("stream_component_id"),
            "stream_component_name": stream_context.get("stream_component_name"),
            "device_uuid": stream_context.get("stream_device_uuid") or metadata.get("device_uuid"),
            "device_name": device_name,
            "serial_number": serial_number,
            "condition_level": observation_type if category == "CONDITION" else None,
            "attributes": attributes,
            "data_item_attributes": metadata.get("attributes", {}),
            "constraints": metadata.get("constraints", []),
            "probe_sha256": probe.sha256 if probe else None,
        }
        observations.append(record)

    return ParsedBatch(
        header=header,
        observations=observations,
        first_observation_sequence=min(sequences) if sequences else None,
        last_observation_sequence=max(sequences) if sequences else None,
    )


def plan_sequence(checkpoint: SourceCheckpoint | None, header: StreamHeader) -> SequencePlan:
    """Choose the next sequence and explicitly describe unavoidable loss."""

    if checkpoint is None:
        return SequencePlan(requested_from=header.first_sequence)
    if checkpoint.agent_instance_id != header.instance_id:
        return SequencePlan(
            requested_from=header.first_sequence,
            reason="agent_instance_changed",
        )
    if checkpoint.next_sequence < header.first_sequence:
        return SequencePlan(
            requested_from=header.first_sequence,
            gap_from=checkpoint.next_sequence,
            gap_to=header.first_sequence - 1,
            reason="agent_buffer_overflow",
        )
    if checkpoint.next_sequence > header.last_sequence + 1:
        return SequencePlan(
            requested_from=header.first_sequence,
            reason="sequence_regression",
        )
    return SequencePlan(requested_from=checkpoint.next_sequence)


def validate_batch_continuity(batch: ParsedBatch, expected_from: int) -> None:
    sequences = sorted(
        {int(record["sequence"]) for record in batch.observations if record.get("sequence") is not None}
    )
    if not sequences:
        if batch.header.next_sequence > expected_from:
            raise MtconnectProtocolError(
                "Agent advanced nextSequence without returning observations."
            )
        return
    if sequences[0] != expected_from:
        raise MtconnectProtocolError(
            f"Expected first observation sequence {expected_from}, received {sequences[0]}."
        )
    expected = list(range(sequences[0], sequences[-1] + 1))
    if sequences != expected:
        missing = sorted(set(expected) - set(sequences))
        preview = ", ".join(str(value) for value in missing[:10])
        raise MtconnectProtocolError(
            f"Non-contiguous sequence response; missing {preview or 'unknown sequences'}."
        )
    if batch.header.next_sequence != sequences[-1] + 1:
        raise MtconnectProtocolError(
            "Header.nextSequence does not follow the final returned observation: "
            f"{batch.header.next_sequence} != {sequences[-1] + 1}."
        )
