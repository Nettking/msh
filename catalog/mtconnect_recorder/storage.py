"""Durable raw, detailed observation, compatibility snapshot, probe, gap, and event storage."""
from __future__ import annotations

import gzip
import json
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import Any

from .model import (
    MtconnectProtocolError,
    ParsedBatch,
    ProbeModel,
    RawBatchRef,
    StoredBatch,
    _read_json,
    _slug,
    _utc_now,
    _write_bytes_atomic,
    _write_json_atomic,
    _write_text_atomic,
)
from .parsing import parse_probe


class DurableRecorderStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.root = data_dir / "sources" / "mtconnect_recorder"
        self.raw_root = self.root / "raw"
        self.probe_root = self.root / "probe"
        self.observation_root = self.root / "observations"
        self.normalized_root = self.root / "jsonl"
        self.gap_root = self.root / "gaps"
        self.event_root = self.root / "events"

    def store_probe(
        self,
        *,
        source_name: str,
        instance_id: int,
        xml_text: str,
        probe: ProbeModel,
    ) -> Path:
        path = (
            self.probe_root
            / _slug(source_name)
            / str(instance_id)
            / f"probe-{probe.sha256}.xml.gz"
        )
        _write_bytes_atomic(path, gzip.compress(xml_text.encode("utf-8"), mtime=0))
        manifest = {
            "schema": "msh.mtconnect.probe_manifest.v1",
            "source_name": source_name,
            "agent_instance_id": instance_id,
            "probe_sha256": probe.sha256,
            "stored_at": _utc_now(),
            "raw_file": str(path),
            "device_count": len(probe.devices),
            "data_item_count": len(probe.data_items),
        }
        _write_json_atomic(path.with_suffix(".manifest.json"), manifest)
        return path

    def store_raw_batch(
        self,
        *,
        source_name: str,
        requested_from: int,
        xml_text: str,
        batch: ParsedBatch,
    ) -> RawBatchRef:
        if not batch.observations:
            raise ValueError("Cannot store an empty MTConnect observation batch.")
        first = batch.first_observation_sequence
        last = batch.last_observation_sequence
        if first is None or last is None:
            raise ValueError("Observation batch does not contain sequence numbers.")

        raw_bytes = xml_text.encode("utf-8")
        raw_digest = sha256(raw_bytes).hexdigest()
        day = str(batch.observations[0].get("timestamp") or _utc_now())[:10]
        source_slug = _slug(source_name)
        instance = str(batch.header.instance_id)
        base_name = f"seq-{first}-{last}-next-{batch.header.next_sequence}"
        raw_path = (
            self.raw_root
            / source_slug
            / instance
            / day
            / f"{base_name}-{raw_digest[:12]}.xml.gz"
        )
        _write_bytes_atomic(raw_path, gzip.compress(raw_bytes, mtime=0))

        manifest_path = raw_path.with_suffix(".manifest.json")
        raw_manifest = {
            "schema": "msh.mtconnect.raw_batch_manifest.v1",
            "source_name": source_name,
            "agent_instance_id": batch.header.instance_id,
            "requested_from": requested_from,
            "first_observation_sequence": first,
            "last_observation_sequence": last,
            "next_sequence": batch.header.next_sequence,
            "agent_first_sequence": batch.header.first_sequence,
            "agent_last_sequence": batch.header.last_sequence,
            "observation_count": len(batch.observations),
            "received_at": batch.observations[0].get("received_at"),
            "raw_sha256": raw_digest,
            "raw_file": str(raw_path),
        }
        _write_json_atomic(manifest_path, raw_manifest)
        return RawBatchRef(
            raw_path=raw_path,
            manifest_path=manifest_path,
            raw_sha256=raw_digest,
            requested_from=requested_from,
            first_sequence=first,
            last_sequence=last,
            next_sequence=batch.header.next_sequence,
            observation_count=len(batch.observations),
        )

    def _batch_location(self, *, source_name: str, batch: ParsedBatch) -> tuple[str, str]:
        if not batch.observations:
            raise ValueError("Cannot store an empty MTConnect observation batch.")
        first = batch.first_observation_sequence
        last = batch.last_observation_sequence
        if first is None or last is None:
            raise ValueError("Observation batch does not contain sequence numbers.")
        day = str(batch.observations[0].get("timestamp") or _utc_now())[:10]
        return day, f"seq-{first}-{last}-next-{batch.header.next_sequence}"

    def store_observation_batch(
        self,
        *,
        source_name: str,
        batch: ParsedBatch,
    ) -> Path:
        """Store complete normalized observations outside MSH's wide JSONL scan."""

        day, base_name = self._batch_location(source_name=source_name, batch=batch)
        path = (
            self.observation_root
            / _slug(source_name)
            / str(batch.header.instance_id)
            / day
            / f"{base_name}.ndjson"
        )
        text = "".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
            for record in batch.observations
        )
        _write_text_atomic(path, text)
        return path

    @staticmethod
    def _signal_key(record: Mapping[str, Any]) -> str:
        return str(
            record.get("name")
            or record.get("data_item_id")
            or record.get("observation_type")
            or "unknown"
        )

    @staticmethod
    def _signal_value(record: Mapping[str, Any]) -> Any:
        if record.get("category") == "CONDITION":
            return record.get("condition_level") or record.get("native_value")
        return record.get("value")

    def store_normalized_batch(
        self,
        *,
        source_name: str,
        batch: ParsedBatch,
        initial_values: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> tuple[Path, dict[str, dict[str, Any]]]:
        """Write MSH-compatible wide snapshots while retaining every sequence.

        The detailed observation representation is stored separately as NDJSON.
        This JSONL view carries forward each machine's latest known signal values,
        so existing MSH live, playback, cache, and analysis paths continue to see
        columns such as ``Srpm``, ``execution``, and ``Xabs``.
        """

        day, base_name = self._batch_location(source_name=source_name, batch=batch)
        normalized_path = (
            self.normalized_root
            / _slug(source_name)
            / str(batch.header.instance_id)
            / day
            / f"{base_name}.jsonl"
        )
        states: dict[str, dict[str, Any]] = {
            str(machine): dict(values)
            for machine, values in (initial_values or {}).items()
        }
        snapshots: list[dict[str, Any]] = []
        for record in batch.observations:
            machine_id = str(record.get("machine_id") or source_name)
            state = states.setdefault(machine_id, {})
            state[self._signal_key(record)] = self._signal_value(record)
            snapshots.append(
                {
                    "schema": "msh.mtconnect.snapshot.v2",
                    "source": "mtconnect_recorder",
                    "source_name": source_name,
                    "source_record_id": f"snapshot:{record.get('source_record_id')}",
                    "machine": record.get("machine") or source_name,
                    "machine_name": (
                        record.get("machine_name")
                        or record.get("machine")
                        or source_name
                    ),
                    "machine_id": machine_id,
                    "agent_instance_id": batch.header.instance_id,
                    "sequence": record.get("sequence"),
                    "timestamp": record.get("timestamp"),
                    "received_at": record.get("received_at"),
                    "changed_data_item_id": record.get("data_item_id"),
                    "changed_name": record.get("name"),
                    "changed_category": record.get("category"),
                    **state,
                }
            )
        normalized_text = "".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
            for record in snapshots
        )
        _write_text_atomic(normalized_path, normalized_text)
        return normalized_path, states

    def store_batch(
        self,
        *,
        source_name: str,
        requested_from: int,
        xml_text: str,
        batch: ParsedBatch,
        initial_values: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> StoredBatch:
        # The write order is intentional: immutable raw -> detailed normalized
        # observations -> MSH-compatible snapshots -> caller commits checkpoint.
        raw = self.store_raw_batch(
            source_name=source_name,
            requested_from=requested_from,
            xml_text=xml_text,
            batch=batch,
        )
        observation_path = self.store_observation_batch(
            source_name=source_name,
            batch=batch,
        )
        normalized_path, latest_values = self.store_normalized_batch(
            source_name=source_name,
            batch=batch,
            initial_values=initial_values,
        )
        return StoredBatch(
            raw_path=raw.raw_path,
            observation_path=observation_path,
            normalized_path=normalized_path,
            raw_sha256=raw.raw_sha256,
            observation_count=raw.observation_count,
            latest_values=latest_values,
        )

    def iter_raw_batches(
        self,
        *,
        source_name: str,
        instance_id: int,
    ) -> list[RawBatchRef]:
        root = self.raw_root / _slug(source_name) / str(instance_id)
        refs: list[RawBatchRef] = []
        if not root.exists():
            return refs
        for manifest_path in root.rglob("*.manifest.json"):
            payload = _read_json(manifest_path)
            if payload.get("schema") != "msh.mtconnect.raw_batch_manifest.v1":
                continue
            try:
                raw_path = Path(str(payload["raw_file"]))
                if not raw_path.exists():
                    candidate_name = manifest_path.name.removesuffix(".manifest.json")
                    raw_path = manifest_path.with_name(candidate_name)
                refs.append(
                    RawBatchRef(
                        raw_path=raw_path,
                        manifest_path=manifest_path,
                        raw_sha256=str(payload["raw_sha256"]),
                        requested_from=int(payload["requested_from"]),
                        first_sequence=int(payload["first_observation_sequence"]),
                        last_sequence=int(payload["last_observation_sequence"]),
                        next_sequence=int(payload["next_sequence"]),
                        observation_count=int(payload["observation_count"]),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return sorted(refs, key=lambda ref: (ref.first_sequence, ref.last_sequence))

    def read_raw_batch(self, ref: RawBatchRef) -> str:
        compressed = ref.raw_path.read_bytes()
        xml_bytes = gzip.decompress(compressed)
        digest = sha256(xml_bytes).hexdigest()
        if digest != ref.raw_sha256:
            raise MtconnectProtocolError(
                f"Raw MTConnect batch checksum mismatch: {ref.raw_path}"
            )
        return xml_bytes.decode("utf-8")

    def load_archived_probe(
        self,
        *,
        source_name: str,
        instance_id: int,
        probe_sha256: str | None = None,
    ) -> ProbeModel | None:
        root = self.probe_root / _slug(source_name) / str(instance_id)
        if not root.exists():
            return None
        pattern = f"probe-{probe_sha256}.xml.gz" if probe_sha256 else "probe-*.xml.gz"
        candidates = sorted(root.glob(pattern))
        if not candidates:
            return None
        xml_text = gzip.decompress(candidates[-1].read_bytes()).decode("utf-8")
        probe = parse_probe(xml_text)
        if probe_sha256 and probe.sha256 != probe_sha256:
            raise MtconnectProtocolError(
                f"Archived probe checksum mismatch: {candidates[-1]}"
            )
        return probe

    def record_gap(
        self,
        *,
        source_name: str,
        instance_id: int,
        missing_from: int,
        missing_to: int,
        reason: str,
        agent_first_sequence: int,
        agent_last_sequence: int,
    ) -> Path:
        path = (
            self.gap_root
            / _slug(source_name)
            / str(instance_id)
            / f"gap-{missing_from}-{missing_to}.json"
        )
        payload = {
            "schema": "msh.mtconnect.gap.v1",
            "source": "mtconnect_recorder",
            "source_name": source_name,
            "agent_instance_id": instance_id,
            "missing_from": missing_from,
            "missing_to": missing_to,
            "missing_count": max(0, missing_to - missing_from + 1),
            "reason": reason,
            "agent_first_sequence": agent_first_sequence,
            "agent_last_sequence": agent_last_sequence,
            "detected_at": _utc_now(),
        }
        _write_json_atomic(path, payload)
        return path

    def record_event(
        self,
        *,
        source_name: str,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> Path:
        event_payload = {
            "schema": "msh.mtconnect.recorder_event.v1",
            "source": "mtconnect_recorder",
            "source_name": source_name,
            "event_type": event_type,
            "timestamp": _utc_now(),
            **dict(payload),
        }
        digest = sha256(
            json.dumps(event_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()[:12]
        path = self.event_root / _slug(source_name) / f"{event_type}-{digest}.json"
        _write_json_atomic(path, event_payload)
        return path
