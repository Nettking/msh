"""Recorder-side composition for the incremental publication frontier.

The frontier record is written while the recorder's existing B01 transaction
reservation is still held. This module does not create a second reservation:
it extends the existing finite data requirement, then scopes the detailed
observation writer to recorder runtime stores so unrelated direct stores are
not changed by import order.
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


def _mark_pending_after_observation_write(
    store: Any,
    *,
    source_name: str,
    batch: Any,
    raw_sha256: str,
    archive_source_name: str | None = None,
    raw_path: Path | None = None,
    manifest_path: Path | None = None,
    requested_from: int | None = None,
    received_at: str | None = None,
) -> None:
    first = batch.first_observation_sequence
    last = batch.last_observation_sequence
    if first is None or last is None:
        raise ValueError("Publication discovery requires sequence-bounded observations.")
    day, base_name = store._batch_location(
        source_name=source_name,
        batch=batch,
        raw_sha256=raw_sha256,
    )
    archive_source = archive_source_name or source_name
    raw_path = raw_path or _confined_storage_path(
        store.raw_root,
        _slug(archive_source),
        str(batch.header.instance_id),
        day,
        f"{base_name}.xml.gz",
    )
    # ``_batch_location`` uses the same validated identity as the raw writer.
    # Resolve the manifest name exactly as store_raw_batch does; no archive
    # traversal is needed to publish the discovery pointer.
    manifest_path = manifest_path or raw_path.with_suffix(".manifest.json")
    RecorderPublicationFrontier(store).mark_pending(
        source_name=source_name,
        archive_source_name=archive_source,
        instance_id=int(batch.header.instance_id),
        ref=RawBatchRef(
            raw_path=raw_path,
            manifest_path=manifest_path,
            raw_sha256=raw_sha256,
            requested_from=(
                int(first) if requested_from is None else int(requested_from)
            ),
            first_sequence=int(first),
            last_sequence=int(last),
            next_sequence=int(batch.header.next_sequence),
            observation_count=len(batch.observations),
            manifest_schema=RAW_BATCH_MANIFEST_SCHEMA,
            received_at=(
                received_at
                if received_at is not None
                else (
                    str(batch.observations[0].get("received_at"))
                    if batch.observations
                    and batch.observations[0].get("received_at")
                    else None
                )
            ),
            source_name=archive_source,
        ),
    )


def install_publication_frontier_runtime(runtime_module: ModuleType) -> None:
    """Attach frontier discovery to the already resource-aware recorder runtime."""

    if getattr(runtime_module, _INSTALLED, False):
        return

    # Resource admission is installed before this function is called. Extend
    # the concrete runtime guard so the bounded frontier record remains inside
    # the same recorder transaction reservation rather than nesting admission.
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

    # Do not mutate the shared DurableRecorderStore class. Doing so makes
    # unrelated direct-store behavior depend on whether recorder runtime startup
    # happened earlier in the process. A runtime-local subclass preserves the
    # normal storage API while confining producer-side publication bookkeeping
    # to stores actually constructed by RecorderRuntime.
    store_class = runtime_module.DurableRecorderStore

    class PublicationFrontierDurableRecorderStore(store_class):
        _publication_frontier_runtime_store = True

        def store_observation_batch(
            self,
            *,
            source_name: str,
            batch: Any,
            raw_sha256: str | None = None,
            archive_source_name: str | None = None,
            raw_path: Path | None = None,
            manifest_path: Path | None = None,
            requested_from: int | None = None,
            received_at: str | None = None,
        ) -> Path:
            path = super().store_observation_batch(
                source_name=source_name,
                batch=batch,
                raw_sha256=raw_sha256,
            )
            if raw_sha256 is not None:
                _mark_pending_after_observation_write(
                    self,
                    source_name=source_name,
                    batch=batch,
                    raw_sha256=raw_sha256,
                    archive_source_name=archive_source_name,
                    raw_path=raw_path,
                    manifest_path=manifest_path,
                    requested_from=requested_from,
                    received_at=received_at,
                )
            return path

    PublicationFrontierDurableRecorderStore.__name__ = store_class.__name__
    PublicationFrontierDurableRecorderStore.__qualname__ = store_class.__qualname__
    runtime_module.DurableRecorderStore = PublicationFrontierDurableRecorderStore
    setattr(runtime_module, _INSTALLED, True)


__all__ = ["install_publication_frontier_runtime"]
