"""Durable raw, detailed observation, compatibility snapshot, probe, gap, and event storage."""
from __future__ import annotations

import gzip
import json
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import Any

from .model import (
    RAW_BATCH_ISSUE_MALFORMED,
    RAW_BATCH_ISSUE_RAW_MISSING,
    RAW_BATCH_ISSUE_UNSUPPORTED_SCHEMA,
    MtconnectProtocolError,
    ParsedBatch,
    ProbeModel,
    RawBatchIssue,
    RawBatchRef,
    RawBatchScan,
    StoredBatch,
    _slug,
    _utc_now,
    _write_bytes_atomic,
    _write_json_atomic,
    _write_text_atomic,
)
from .parsing import parse_probe
from .schema_compat import (
    PROBE_MANIFEST_SCHEMA,
    RAW_BATCH_MANIFEST_SCHEMA,
    SCHEMA_UNSUPPORTED,
    SUPPORTED_RAW_BATCH_MANIFEST_SCHEMAS,
    classify_raw_batch_manifest_schema,
)


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
            "schema": PROBE_MANIFEST_SCHEMA,
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
            / f"{base_name}-{raw_digest}.xml.gz"
        )
        _write_bytes_atomic(raw_path, gzip.compress(raw_bytes, mtime=0))

        manifest_path = raw_path.with_suffix(".manifest.json")
        raw_manifest = {
            "schema": RAW_BATCH_MANIFEST_SCHEMA,
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
        received_at = raw_manifest["received_at"]
        return RawBatchRef(
            raw_path=raw_path,
            manifest_path=manifest_path,
            raw_sha256=raw_digest,
            requested_from=requested_from,
            first_sequence=first,
            last_sequence=last,
            next_sequence=batch.header.next_sequence,
            observation_count=len(batch.observations),
            manifest_schema=RAW_BATCH_MANIFEST_SCHEMA,
            received_at=str(received_at) if isinstance(received_at, str) else None,
        )

    def _batch_location(
        self,
        *,
        source_name: str,
        batch: ParsedBatch,
        raw_sha256: str | None = None,
    ) -> tuple[str, str]:
        if not batch.observations:
            raise ValueError("Cannot store an empty MTConnect observation batch.")
        first = batch.first_observation_sequence
        last = batch.last_observation_sequence
        if first is None or last is None:
            raise ValueError("Observation batch does not contain sequence numbers.")
        day = str(batch.observations[0].get("timestamp") or _utc_now())[:10]
        base_name = f"seq-{first}-{last}-next-{batch.header.next_sequence}"
        if raw_sha256 is not None:
            if len(raw_sha256) != 64 or any(
                character not in "0123456789abcdef" for character in raw_sha256
            ):
                raise ValueError("raw_sha256 must be 64 lowercase hexadecimal characters")
            base_name = f"{base_name}-{raw_sha256}"
        return day, base_name

    def store_observation_batch(
        self,
        *,
        source_name: str,
        batch: ParsedBatch,
        raw_sha256: str | None = None,
    ) -> Path:
        """Store complete normalized observations outside FCP's wide JSONL scan."""

        day, base_name = self._batch_location(
            source_name=source_name,
            batch=batch,
            raw_sha256=raw_sha256,
        )
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
        raw_sha256: str | None = None,
    ) -> tuple[Path, dict[str, dict[str, Any]]]:
        """Write FCP-compatible wide snapshots while retaining every sequence.

        The detailed observation representation is stored separately as NDJSON.
        This JSONL view carries forward each machine's latest known signal values,
        so existing FCP live, playback, cache, and analysis paths continue to see
        columns such as ``Srpm``, ``execution``, and ``Xabs``.
        """

        day, base_name = self._batch_location(
            source_name=source_name,
            batch=batch,
            raw_sha256=raw_sha256,
        )
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
                    "schema": "fcp.mtconnect.snapshot.v2",
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
        # observations -> FCP-compatible snapshots -> caller commits checkpoint.
        raw = self.store_raw_batch(
            source_name=source_name,
            requested_from=requested_from,
            xml_text=xml_text,
            batch=batch,
        )
        observation_path = self.store_observation_batch(
            source_name=source_name,
            batch=batch,
            raw_sha256=raw.raw_sha256,
        )
        normalized_path, latest_values = self.store_normalized_batch(
            source_name=source_name,
            batch=batch,
            initial_values=initial_values,
            raw_sha256=raw.raw_sha256,
        )
        return StoredBatch(
            raw_path=raw.raw_path,
            observation_path=observation_path,
            normalized_path=normalized_path,
            raw_sha256=raw.raw_sha256,
            observation_count=raw.observation_count,
            latest_values=latest_values,
        )

    @staticmethod
    def _local_raw_candidates(manifest_path: Path) -> tuple[Path, ...]:
        """Return the sibling payload names a manifest may describe.

        ``store_raw_batch`` derives the manifest name with
        ``Path.with_suffix('.manifest.json')``, which replaces only the final
        ``.gz``. ``seq-...-<digest>.xml.gz`` therefore has the manifest
        ``seq-...-<digest>.xml.manifest.json``, and stripping the manifest
        suffix yields ``seq-...-<digest>.xml`` — a file that does not exist.

        This matters whenever the recorded absolute ``raw_file`` path does not
        resolve, which is exactly what happens when a capture is copied off the
        machine that recorded it. Offer the compressed sibling first so a
        relocated capture stays readable, and keep the uncompressed name for any
        archive that genuinely stored one.
        """

        stem = manifest_path.name.removesuffix(".manifest.json")
        return (
            manifest_path.with_name(f"{stem}.gz"),
            manifest_path.with_name(stem),
        )

    def _read_manifest_payload(
        self, manifest_path: Path
    ) -> tuple[dict[str, Any] | None, RawBatchIssue | None]:
        """Parse one manifest, separating "cannot read" from "do not support"."""

        try:
            text = manifest_path.read_text(encoding="utf-8")
        except OSError as exc:
            return None, RawBatchIssue(
                manifest_path=manifest_path,
                reason=RAW_BATCH_ISSUE_MALFORMED,
                detail=f"manifest could not be read: {exc.strerror or exc}",
            )
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            return None, RawBatchIssue(
                manifest_path=manifest_path,
                reason=RAW_BATCH_ISSUE_MALFORMED,
                detail=f"manifest is not valid JSON: {exc.msg}",
            )
        if not isinstance(payload, dict):
            return None, RawBatchIssue(
                manifest_path=manifest_path,
                reason=RAW_BATCH_ISSUE_MALFORMED,
                detail="manifest is not a JSON object",
            )

        declared = payload.get("schema")
        if classify_raw_batch_manifest_schema(declared) == SCHEMA_UNSUPPORTED:
            return None, RawBatchIssue(
                manifest_path=manifest_path,
                reason=RAW_BATCH_ISSUE_UNSUPPORTED_SCHEMA,
                detail=(
                    "manifest declares an unsupported schema; supported schemas are "
                    + ", ".join(sorted(SUPPORTED_RAW_BATCH_MANIFEST_SCHEMAS))
                ),
                schema=declared if isinstance(declared, str) else None,
            )
        return payload, None

    def scan_raw_batches(
        self,
        *,
        source_name: str,
        instance_id: int,
    ) -> RawBatchScan:
        """Return every readable raw batch plus a reason for each skipped one.

        Current and pre-rename manifests both resolve here, and both go through
        the same structural validation; only the declared schema string differs.
        Nothing is rewritten — the capture on disk stays exactly as recorded.
        """

        root = self.raw_root / _slug(source_name) / str(instance_id)
        refs: list[RawBatchRef] = []
        issues: list[RawBatchIssue] = []
        if not root.exists():
            return RawBatchScan(refs=(), issues=())
        for manifest_path in sorted(root.rglob("*.manifest.json")):
            payload, issue = self._read_manifest_payload(manifest_path)
            if payload is None:
                if issue is not None:
                    issues.append(issue)
                continue
            try:
                declared_raw_file = str(payload["raw_file"])
                raw_sha256 = str(payload["raw_sha256"])
                requested_from = int(payload["requested_from"])
                first_sequence = int(payload["first_observation_sequence"])
                last_sequence = int(payload["last_observation_sequence"])
                next_sequence = int(payload["next_sequence"])
                observation_count = int(payload["observation_count"])
            except (KeyError, TypeError, ValueError) as exc:
                issues.append(
                    RawBatchIssue(
                        manifest_path=manifest_path,
                        reason=RAW_BATCH_ISSUE_MALFORMED,
                        detail=f"manifest is missing or has an invalid field: {exc}",
                        schema=str(payload.get("schema") or "") or None,
                    )
                )
                continue

            raw_path = Path(declared_raw_file)
            if not raw_path.exists():
                raw_path = next(
                    (
                        candidate
                        for candidate in self._local_raw_candidates(manifest_path)
                        if candidate.exists()
                    ),
                    raw_path,
                )
            if not raw_path.exists():
                issues.append(
                    RawBatchIssue(
                        manifest_path=manifest_path,
                        reason=RAW_BATCH_ISSUE_RAW_MISSING,
                        detail=(
                            "no compressed payload beside the manifest and "
                            f"{declared_raw_file!r} does not exist"
                        ),
                        schema=str(payload.get("schema") or "") or None,
                    )
                )
                continue

            received_at = payload.get("received_at")
            refs.append(
                RawBatchRef(
                    raw_path=raw_path,
                    manifest_path=manifest_path,
                    raw_sha256=raw_sha256,
                    requested_from=requested_from,
                    first_sequence=first_sequence,
                    last_sequence=last_sequence,
                    next_sequence=next_sequence,
                    observation_count=observation_count,
                    manifest_schema=str(payload["schema"]),
                    received_at=(
                        str(received_at) if isinstance(received_at, str) else None
                    ),
                )
            )
        refs.sort(key=lambda ref: (ref.first_sequence, ref.last_sequence, ref.raw_sha256))
        return RawBatchScan(refs=tuple(refs), issues=tuple(issues))

    def iter_raw_batches(
        self,
        *,
        source_name: str,
        instance_id: int,
    ) -> list[RawBatchRef]:
        return list(
            self.scan_raw_batches(
                source_name=source_name,
                instance_id=instance_id,
            ).refs
        )

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
            "schema": "fcp.mtconnect.gap.v1",
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
            "schema": "fcp.mtconnect.recorder_event.v1",
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
