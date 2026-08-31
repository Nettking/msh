"""Recorder-side composition for the incremental publication frontier.

The frontier record is written while the recorder's existing B01 transaction
reservation is still held.  This module does not create a second reservation:
it extends the existing finite data requirement, then wraps the detailed
observation writer (which is already inside that transaction) to publish the
bounded discovery identity after immutable raw evidence exists.
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

from .model import RawBatchRef, _slug
from .publication_frontier import (
    PUBLICATION_FRONTIER_RECORD_INODES,
    PUBLICATION_FRONTIER_RECORD_MAX_BYTES,
    RecorderPublicationFrontier,
)
from .schema_compat import RAW_BATCH_MANIFEST_SCHEMA
from .storage import _confined_storage_path

_INSTALLED = "_PUBLICATION_FRONTIER_RUNTIME_INSTALLED"


def install_publication_frontier_runtime(runtime_module: ModuleType) -> None:
    """Attach frontier discovery to the already resource-aware recorder runtime."""

    if getattr(runtime_module, _INSTALLED, False):
        return

    # Resource admission is installed before this function is called. Patch the
    # concrete guard class that is actually attached to recorder runtimes, not
    # the historical implementation class retained for compatibility.
    from . import resource_pressure as pressure

    guard_class = pressure.RecorderResourceGuard
    original_requirements = guard_class._requirements

    def frontier_requirements(self: Any, *args: Any, **kwargs: Any):
        requirements = original_requirements(self, *args, **kwargs)
        if not requirements:
            return requirements
        data = requirements[0]
        return (
            replace(
                data,
                bytes_required=(
                    int(data.bytes_required) + PUBLICATION_FRONTIER_RECORD_MAX_BYTES
                ),
                inodes_required=(
                    int(data.inodes_required) + PUBLICATION_FRONTIER_RECORD_INODES
                ),
            ),
            *requirements[1:],
        )

    guard_class._requirements = frontier_requirements

    store_class = runtime_module.DurableRecorderStore
    original_store_observation = store_class.store_observation_batch

    def store_observation_with_publication_frontier(
        self: Any,
        *,
        source_name: str,
        batch: Any,
        raw_sha256: str | None = None,
    ) -> Path:
        path = original_store_observation(
            self,
            source_name=source_name,
            batch=batch,
            raw_sha256=raw_sha256,
        )
        if raw_sha256 is None:
            return path
        first = batch.first_observation_sequence
        last = batch.last_observation_sequence
        if first is None or last is None:
            raise ValueError("Publication discovery requires sequence-bounded observations.")
        day, base_name = self._batch_location(
            source_name=source_name,
            batch=batch,
            raw_sha256=raw_sha256,
        )
        raw_path = _confined_storage_path(
            self.raw_root,
            _slug(source_name),
            str(batch.header.instance_id),
            day,
            f"{base_name}.xml.gz",
        )
        # ``_batch_location`` uses the same validated identity as the raw writer.
        # Resolve the manifest name exactly as store_raw_batch does; no archive
        # traversal is needed to publish the discovery pointer.
        manifest_path = raw_path.with_suffix(".manifest.json")
        frontier = RecorderPublicationFrontier(self)
        frontier.mark_pending(
            source_name=source_name,
            archive_source_name=source_name,
            instance_id=int(batch.header.instance_id),
            ref=RawBatchRef(
                raw_path=raw_path,
                manifest_path=manifest_path,
                raw_sha256=raw_sha256,
                requested_from=int(first),
                first_sequence=int(first),
                last_sequence=int(last),
                next_sequence=int(batch.header.next_sequence),
                observation_count=len(batch.observations),
                manifest_schema=RAW_BATCH_MANIFEST_SCHEMA,
                received_at=(
                    str(batch.observations[0].get("received_at"))
                    if batch.observations and batch.observations[0].get("received_at")
                    else None
                ),
                source_name=source_name,
            ),
        )
        return path

    store_class.store_observation_batch = store_observation_with_publication_frontier
    setattr(runtime_module, _INSTALLED, True)


__all__ = ["install_publication_frontier_runtime"]
