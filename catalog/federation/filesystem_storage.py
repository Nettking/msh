"""Recorder filesystem adapter and deterministic outbox reconciliation.

The filesystem and SQLite are not one ACID resource.  A SQLite prepared record
persists immutable routing first; recorder recovery and reconciliation use it
with the raw manifest to finish activation without runtime routing guesses.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from catalog.mtconnect_recorder.model import ParsedBatch, StoredBatch
from catalog.mtconnect_recorder.storage import DurableRecorderStore

try:
    # Phase 0/1 applications place ``catalog`` directly on sys.path and import
    # recorder models through this established package name.  Supporting both
    # names avoids rejecting the same dataclass solely because Python loaded it
    # through two compatible module paths.
    from mtconnect_recorder.model import ParsedBatch as LegacyParsedBatch
except ModuleNotFoundError:  # pragma: no cover - only one import layout is active
    LegacyParsedBatch = ParsedBatch

_PARSED_BATCH_TYPES = (ParsedBatch, LegacyParsedBatch)

from .errors import FederationValidationError
from .models import StorageBatch
from .outbox import SQLiteOutbox
from .providers import (
    ProviderError,
    ProviderErrorCode,
    StorageReadOutcome,
    StorageWriteOutcome,
)


class RecorderDeliveryTransitionError(OSError):
    """Recorder files are durable, but prepared delivery still needs reconciliation."""

    def __init__(self, stored: StoredBatch, created: bool, message: str) -> None:
        self.stored = stored
        self.created = created
        super().__init__(message)


class FilesystemRecorderStorageProvider:
    """Composition wrapper; it intentionally delegates every recorder write."""

    durable = True

    def __init__(self, data_dir: Path, outbox: SQLiteOutbox | None = None) -> None:
        self.store = DurableRecorderStore(data_dir)
        self.outbox = outbox

    def store_batch(self, *, source_name: str, requested_from: int, xml_text: str,
                    batch: ParsedBatch,
                    initial_values: Mapping[str, Mapping[str, Any]] | None = None,
                    session_id: str | None = None, destination_id: str | None = None,
                    now: datetime | None = None) -> StoredBatch:
        if self.outbox is not None and (
            session_id is None or destination_id is None or now is None
        ):
            raise ValueError(
                "session_id, destination_id, and explicit now are required with an outbox"
            )
        stored, _ = self._store_batch_with_status(
            source_name=source_name, requested_from=requested_from, xml_text=xml_text,
            batch=batch, initial_values=initial_values, session_id=session_id,
            destination_id=destination_id, now=now,
        )
        return stored

    def _store_batch_with_status(self, *, source_name: str, requested_from: int,
                                 xml_text: str, batch: ParsedBatch,
                                 initial_values: Mapping[str, Mapping[str, Any]] | None,
                                 session_id: str | None, destination_id: str | None,
                                 now: datetime | None) -> tuple[StoredBatch, bool]:
        prepared_id: int | None = None
        delivery_created = True
        if self.outbox is not None:
            raw_digest = sha256(xml_text.encode("utf-8")).hexdigest()
            prepared, delivery_created = self.prepare_delivery(
                source_name=source_name,
                instance_id=batch.header.instance_id,
                requested_from=requested_from,
                first=batch.first_observation_sequence,
                last=batch.last_observation_sequence,
                next_sequence=batch.header.next_sequence,
                digest=raw_digest,
                count=len(batch.observations),
                session_id=session_id,
                destination_id=destination_id,
                now=now,
            )
            prepared_id = prepared.outbox_id
        stored = self.store.store_batch(source_name=source_name, requested_from=requested_from,
                                        xml_text=xml_text, batch=batch,
                                        initial_values=initial_values)
        if self.outbox is not None:
            try:
                self.outbox.activate(prepared_id, now=now)
            except Exception as exc:
                raise RecorderDeliveryTransitionError(
                    stored, delivery_created,
                    "recorder data is durable; delivery reconciliation is required"
                ) from exc
        return stored, delivery_created

    def write(self, batch: StorageBatch, payload: Any) -> StorageWriteOutcome:
        """StorageProvider entry point for a parsed local recorder payload."""
        if not isinstance(payload, dict) or not isinstance(
            payload.get("batch"), _PARSED_BATCH_TYPES
        ):
            return StorageWriteOutcome(batch, False, False, ProviderError(
                ProviderErrorCode.INVALID, "recorder payload requires a parsed batch", "payload"))
        try:
            xml_text = str(payload["xml_text"])
            actual_hash = sha256(xml_text.encode("utf-8")).hexdigest()
            declared_hash = batch.content_hash.removeprefix("sha256:")
            if actual_hash != declared_hash:
                return StorageWriteOutcome(batch, False, False, ProviderError(
                    ProviderErrorCode.CONFLICT,
                    "content hash does not match recorder raw input",
                    "content_hash",
                ))
            delivery = payload.get("delivery")
            if self.outbox is not None and not isinstance(delivery, dict):
                return StorageWriteOutcome(batch, False, False, ProviderError(
                    ProviderErrorCode.INVALID,
                    "outbox-enabled recorder payload requires delivery metadata",
                    "payload.delivery",
                ))
            _, created = self._store_batch_with_status(
                source_name=str(payload["source_name"]),
                requested_from=int(payload["requested_from"]),
                xml_text=xml_text,
                batch=payload["batch"],
                initial_values=payload.get("initial_values"),
                session_id=batch.session_id if self.outbox is not None else None,
                destination_id=(delivery or {}).get("destination_id"),
                now=(delivery or {}).get("now"),
            )
        except RecorderDeliveryTransitionError as exc:
            return StorageWriteOutcome(batch, exc.created, True, ProviderError(
                ProviderErrorCode.RECONCILIATION_REQUIRED, str(exc)))
        except FederationValidationError as exc:
            code = (ProviderErrorCode.CONFLICT
                    if "conflict" in exc.code else ProviderErrorCode.INVALID)
            return StorageWriteOutcome(batch, False, False, ProviderError(
                code, exc.message, exc.field))
        except (KeyError, TypeError, ValueError) as exc:
            return StorageWriteOutcome(batch, False, False, ProviderError(
                ProviderErrorCode.INVALID, str(exc)))
        except (OSError, sqlite3.Error) as exc:
            return StorageWriteOutcome(batch, False, False, ProviderError(
                ProviderErrorCode.IO_ERROR, str(exc)))
        return StorageWriteOutcome(batch, created, True)

    def retrieve(self, session_id: str, batch_id: str) -> StorageReadOutcome:
        # Recorder archives are discovered by source sequence, never by backend paths.
        return StorageReadOutcome(None, None, ProviderError(
            ProviderErrorCode.NOT_FOUND, "batch identity is not indexed by this adapter"))

    def enqueue_delivery(self, *, source_name: str, instance_id: int,
                         requested_from: int, first: int | None, last: int | None,
                         next_sequence: int, digest: str, count: int,
                         session_id: str, destination_id: str,
                         now: datetime) -> bool:
        """Persist delivery metadata, including stable coordinates for empty batches."""
        entry, created = self.prepare_delivery(
            source_name=source_name, instance_id=instance_id,
            requested_from=requested_from, first=first, last=last,
            next_sequence=next_sequence, digest=digest, count=count,
            session_id=session_id, destination_id=destination_id, now=now,
        )
        self.outbox.activate(entry.outbox_id, now=now)
        return created

    def prepare_delivery(self, *, source_name: str, instance_id: int,
                         requested_from: int, first: int | None, last: int | None,
                         next_sequence: int, digest: str, count: int,
                         session_id: str, destination_id: str,
                         now: datetime):
        """Persist immutable delivery routing before recorder filesystem mutation."""
        if self.outbox is None:
            raise ValueError("delivery enqueue requires an outbox")
        identity = (
            f"{source_name}:{instance_id}:request-{requested_from}:"
            f"sequences-{first}-{last}:next-{next_sequence}"
        )
        payload = {"source_name": source_name, "agent_instance_id": instance_id,
                   "requested_from": requested_from, "first_sequence": first,
                   "last_sequence": last, "next_sequence": next_sequence,
                   "observation_count": count, "raw_sha256": digest}
        return self.outbox.prepare(session_id=session_id, destination_id=destination_id,
                                   schema_id="fcp.recorder.delivery.v1", payload=payload,
                                   idempotency_key=identity,
                                   content_hash=f"sha256:{digest}", now=now)

    def reconcile(self, *, now: datetime) -> int:
        """Enqueue every complete durable batch missing after a crash.

        Incomplete raw prepares are left for the recorder's established recovery
        path; they become eligible on a later reconciliation after derived files
        have been durably regenerated.
        """
        if self.outbox is None:
            raise ValueError("reconciliation requires an outbox")
        created = 0
        for entry in self.outbox.prepared():
            intent = entry.payload
            source_name = str(intent["source_name"])
            instance_id = int(intent["agent_instance_id"])
            refs = self.store.iter_raw_batches(source_name=source_name,
                                               instance_id=instance_id)
            for ref in refs:
                if (ref.requested_from != intent["requested_from"]
                        or ref.next_sequence != intent["next_sequence"]
                        or ref.raw_sha256 != intent["raw_sha256"]):
                    continue
                relative = ref.raw_path.relative_to(self.store.raw_root)
                full_suffix = f"-{ref.raw_sha256}.xml.gz"
                short_suffix = f"-{ref.raw_sha256[:12]}.xml.gz"
                if ref.raw_path.name.endswith(full_suffix):
                    base = ref.raw_path.name.removesuffix(full_suffix)
                elif ref.raw_path.name.endswith(short_suffix):
                    base = ref.raw_path.name.removesuffix(short_suffix)
                else:
                    continue
                full_hashed_base = f"{base}-{ref.raw_sha256}"
                short_hashed_base = f"{base}-{ref.raw_sha256[:12]}"
                observation_candidates = (
                    self.store.observation_root
                    / relative.parent
                    / f"{full_hashed_base}.ndjson",
                    self.store.observation_root
                    / relative.parent
                    / f"{short_hashed_base}.ndjson",
                    self.store.observation_root / relative.parent / f"{base}.ndjson",
                )
                normalized_candidates = (
                    self.store.normalized_root
                    / relative.parent
                    / f"{full_hashed_base}.jsonl",
                    self.store.normalized_root
                    / relative.parent
                    / f"{short_hashed_base}.jsonl",
                    self.store.normalized_root / relative.parent / f"{base}.jsonl",
                )
                # A crash may leave derived files from several raw variants in
                # the same sequence slot.  Only activate the prepared delivery
                # when both derived files belong to the same naming generation
                # (full hash, transitional short hash, or legacy unhashed).
                if any(
                    observation.is_file() and normalized.is_file()
                    for observation, normalized in zip(
                        observation_candidates,
                        normalized_candidates,
                        strict=True,
                    )
                ):
                    self.outbox.activate(entry.outbox_id, now=now)
                    created += 1
                break
        return created
