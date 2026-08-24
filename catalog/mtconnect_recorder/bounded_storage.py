"""Finite, streaming derived writers for recorder capture transactions.

The MTConnect HTTP/parser limits bound ingress, but the compatibility JSONL view
can still amplify one bounded input because each observation carries the current
wide machine state. This module keeps that compatibility representation while
streaming it through an atomic, byte-bounded publication boundary.
"""
from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

from .limits import (
    MAX_COMPATIBILITY_BATCH_BYTES,
    MAX_COMPATIBILITY_STATE_BYTES,
    MAX_OBSERVATION_ARCHIVE_BYTES,
)
from .model import MtconnectProtocolError, ParsedBatch, _slug
from .storage import DurableRecorderStore as _BaseDurableRecorderStore
from .storage import _confined_storage_path


def _fsync_directory(path: Path) -> None:
    """Persist a replaced directory entry where CPython exposes that primitive."""

    if os.name == "nt":
        return
    descriptor = os.open(
        str(path),
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_bounded_jsonl_atomic(
    path: Path,
    records: Iterable[Mapping[str, Any]],
    *,
    max_bytes: int,
    label: str,
    before_replace: Callable[[], None] | None = None,
) -> None:
    """Stream JSONL to a same-directory partial and publish only within ``max_bytes``."""

    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    written = 0
    try:
        with partial.open("xb") as handle:
            for record in records:
                encoded = (
                    json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
                ).encode("utf-8")
                if len(encoded) > max_bytes - written:
                    raise MtconnectProtocolError(
                        f"{label} exceeds the finite recorder transaction limit "
                        f"of {max_bytes} bytes."
                    )
                handle.write(encoded)
                written += len(encoded)
            if before_replace is not None:
                before_replace()
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            pass
        raise


class BoundedDurableRecorderStore(_BaseDurableRecorderStore):
    """Recorder store whose derived representations have finite publication work."""

    def store_observation_batch(
        self,
        *,
        source_name: str,
        batch: ParsedBatch,
        raw_sha256: str | None = None,
    ) -> Path:
        """Stream detailed observations without materializing a second batch copy."""

        day, base_name = self._batch_location(
            source_name=source_name,
            batch=batch,
            raw_sha256=raw_sha256,
        )
        path = _confined_storage_path(
            self.observation_root,
            _slug(source_name),
            str(batch.header.instance_id),
            day,
            f"{base_name}.ndjson",
        )
        _write_bounded_jsonl_atomic(
            path,
            batch.observations,
            max_bytes=MAX_OBSERVATION_ARCHIVE_BYTES,
            label="Recorder observation archive",
        )
        return path

    def store_normalized_batch(
        self,
        *,
        source_name: str,
        batch: ParsedBatch,
        initial_values: Mapping[str, Mapping[str, Any]] | None = None,
        raw_sha256: str | None = None,
    ) -> tuple[Path, dict[str, dict[str, Any]]]:
        """Stream wide compatibility rows while bounding output and checkpoint state."""

        day, base_name = self._batch_location(
            source_name=source_name,
            batch=batch,
            raw_sha256=raw_sha256,
        )
        normalized_path = _confined_storage_path(
            self.normalized_root,
            _slug(source_name),
            str(batch.header.instance_id),
            day,
            f"{base_name}.jsonl",
        )
        states: dict[str, dict[str, Any]] = {
            str(machine): dict(values)
            for machine, values in (initial_values or {}).items()
        }

        def _snapshots() -> Iterable[Mapping[str, Any]]:
            for record in batch.observations:
                machine_id = str(record.get("machine_id") or source_name)
                state = states.setdefault(machine_id, {})
                state[self._signal_key(record)] = self._signal_value(record)
                yield {
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

        def _validate_checkpoint_state() -> None:
            encoded = json.dumps(
                states,
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
            if len(encoded) > MAX_COMPATIBILITY_STATE_BYTES:
                raise MtconnectProtocolError(
                    "Recorder compatibility state exceeds the finite checkpoint "
                    f"limit of {MAX_COMPATIBILITY_STATE_BYTES} bytes."
                )

        _write_bounded_jsonl_atomic(
            normalized_path,
            _snapshots(),
            max_bytes=MAX_COMPATIBILITY_BATCH_BYTES,
            label="Recorder compatibility batch",
            before_replace=_validate_checkpoint_state,
        )
        return normalized_path, states


__all__ = ["BoundedDurableRecorderStore"]
