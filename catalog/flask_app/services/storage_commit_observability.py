"""Read-only operator projection of coordinator-committed storage data.

This service deliberately reads the coordinator-owned authoritative storage
manifest rather than scanning provider files. Provider-local files may be
prepared, stale, orphaned, or otherwise not Federation-committed; only committed
manifest batch items are valid acceptance evidence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import current_app

from catalog.federation.manifest import ManifestItemKind
from catalog.federation.models import CommitState
from catalog.federation.phase_d_control import PhaseDControlPlane

_RECORDER_SCHEMA_NAME = "fcp.mtconnect.observations"
_DEFAULT_STORAGE_CONTROL_DATABASE = "data/federation/storage/control.sqlite3"
_MAX_RECENT_BATCHES = 20


def _timestamp(value: object) -> str | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sequence_range(first: object, last: object) -> str | None:
    if (
        isinstance(first, bool)
        or isinstance(last, bool)
        or not isinstance(first, int)
        or not isinstance(last, int)
        or first < 0
        or last < first
    ):
        return None
    return f"{first}\u2013{last}"


def _empty_view(*, state: str, message: str) -> dict[str, Any]:
    return {
        "state": state,
        "message": message,
        "committed_batches": 0,
        "recorder_batches": 0,
        "dataset_count": 0,
        "latest_committed_at": None,
        "datasets": [],
        "recent_batches": [],
        "manifest_heads": [],
    }


def build_storage_commit_view(
    control_plane: object,
    *,
    session_id: str,
    max_recent_batches: int = _MAX_RECENT_BATCHES,
) -> dict[str, Any]:
    """Build bounded public-safe acceptance evidence from authoritative manifests."""

    if not isinstance(session_id, str) or not session_id:
        return _empty_view(
            state="unavailable",
            message="Trusted Federation storage context is unavailable.",
        )
    if (
        isinstance(max_recent_batches, bool)
        or not isinstance(max_recent_batches, int)
        or max_recent_batches <= 0
        or max_recent_batches > 100
    ):
        raise ValueError("max_recent_batches must be between 1 and 100")

    snapshot_loader = getattr(control_plane, "snapshot", None)
    manifest_loader = getattr(control_plane, "manifest", None)
    if not callable(snapshot_loader) or not callable(manifest_loader):
        return _empty_view(
            state="unavailable",
            message="Authoritative storage catalogue is unavailable.",
        )

    snapshot = snapshot_loader(session_id)
    groups = getattr(snapshot, "groups", None)
    if not isinstance(groups, dict) or not groups:
        return _empty_view(
            state="empty",
            message="No logical storage group is available yet.",
        )

    datasets: dict[tuple[str, str], dict[str, Any]] = {}
    recent: list[dict[str, Any]] = []
    manifest_heads: list[dict[str, Any]] = []
    total_batches = 0
    recorder_batches = 0
    latest_commit: datetime | None = None

    for group_id in sorted(groups):
        manifest = manifest_loader(session_id, group_id)
        manifest_heads.append(
            {
                "group_id": group_id,
                "revision": getattr(manifest, "revision", None),
                "manifest_hash": getattr(manifest, "manifest_hash", None),
            }
        )
        items = getattr(manifest, "items", ())
        for item in items if isinstance(items, tuple) else ():
            if (
                getattr(item, "kind", None) is not ManifestItemKind.BATCH
                or getattr(item, "commit_state", None) is not CommitState.COMMITTED
            ):
                continue
            dataset_id = getattr(item, "dataset_id", None)
            batch_id = getattr(item, "item_id", None)
            schema_name = getattr(item, "schema_name", None)
            schema_version = getattr(item, "schema_version", None)
            committed_at = getattr(item, "committed_at", None)
            if (
                not isinstance(dataset_id, str)
                or not dataset_id
                or not isinstance(batch_id, str)
                or not batch_id
                or not isinstance(schema_name, str)
                or not schema_name
            ):
                continue

            total_batches += 1
            is_recorder = schema_name == _RECORDER_SCHEMA_NAME
            if is_recorder:
                recorder_batches += 1
            if isinstance(committed_at, datetime) and committed_at.tzinfo is not None:
                normalized_commit = committed_at.astimezone(timezone.utc)
                if latest_commit is None or normalized_commit > latest_commit:
                    latest_commit = normalized_commit
            else:
                normalized_commit = None

            key = (group_id, dataset_id)
            aggregate = datasets.get(key)
            if aggregate is None:
                aggregate = {
                    "group_id": group_id,
                    "dataset_id": dataset_id,
                    "schema_name": schema_name,
                    "schema_version": schema_version,
                    "is_recorder": is_recorder,
                    "batch_count": 0,
                    "total_bytes": 0,
                    "first_sequence": None,
                    "latest_sequence": None,
                    "latest_committed_at": None,
                    "latest_batch_id": None,
                    "latest_content_hash": None,
                    "source_ids": set(),
                }
                datasets[key] = aggregate

            aggregate["batch_count"] += 1
            size_bytes = getattr(item, "size_bytes", 0)
            if (
                isinstance(size_bytes, int)
                and not isinstance(size_bytes, bool)
                and size_bytes >= 0
            ):
                aggregate["total_bytes"] += size_bytes
            source_id = getattr(item, "source_id", None)
            if isinstance(source_id, str) and source_id:
                aggregate["source_ids"].add(source_id)
            first_sequence = getattr(item, "first_sequence", None)
            last_sequence = getattr(item, "last_sequence", None)
            if isinstance(first_sequence, int) and not isinstance(first_sequence, bool):
                current_first = aggregate["first_sequence"]
                if current_first is None or first_sequence < current_first:
                    aggregate["first_sequence"] = first_sequence
            if isinstance(last_sequence, int) and not isinstance(last_sequence, bool):
                current_last = aggregate["latest_sequence"]
                if current_last is None or last_sequence > current_last:
                    aggregate["latest_sequence"] = last_sequence

            previous_latest = aggregate["latest_committed_at"]
            if normalized_commit is not None and (
                previous_latest is None or normalized_commit > previous_latest
            ):
                aggregate["latest_committed_at"] = normalized_commit
                aggregate["latest_batch_id"] = batch_id
                aggregate["latest_content_hash"] = getattr(item, "content_hash", None)

            recent.append(
                {
                    "group_id": group_id,
                    "dataset_id": dataset_id,
                    "batch_id": batch_id,
                    "schema_name": schema_name,
                    "schema_version": schema_version,
                    "is_recorder": is_recorder,
                    "source_id": source_id if isinstance(source_id, str) else None,
                    "sequence_range": _sequence_range(first_sequence, last_sequence),
                    "first_sequence": (
                        first_sequence if isinstance(first_sequence, int) else None
                    ),
                    "last_sequence": (
                        last_sequence if isinstance(last_sequence, int) else None
                    ),
                    "size_bytes": size_bytes if isinstance(size_bytes, int) else 0,
                    "content_hash": getattr(item, "content_hash", None),
                    "committed_at": normalized_commit,
                    "committed_at_label": _timestamp(normalized_commit),
                    "acknowledgement_count": len(
                        getattr(item, "acknowledged_provider_ids", ())
                        if isinstance(
                            getattr(item, "acknowledged_provider_ids", ()), tuple
                        )
                        else ()
                    ),
                    "manifest_revision": getattr(manifest, "revision", None),
                }
            )

    public_datasets: list[dict[str, Any]] = []
    for aggregate in datasets.values():
        sources = sorted(aggregate.pop("source_ids"))
        first_sequence = aggregate["first_sequence"]
        latest_sequence = aggregate["latest_sequence"]
        public_datasets.append(
            {
                **aggregate,
                "source_label": (
                    sources[0]
                    if len(sources) == 1
                    else ("Multiple sources" if sources else "Not recorded")
                ),
                "sequence_range": _sequence_range(first_sequence, latest_sequence),
                "latest_committed_at_label": _timestamp(
                    aggregate["latest_committed_at"]
                ),
            }
        )

    public_datasets.sort(
        key=lambda item: (
            item["latest_committed_at"]
            or datetime.min.replace(tzinfo=timezone.utc),
            item["dataset_id"],
        ),
        reverse=True,
    )
    recent.sort(
        key=lambda item: (
            item["committed_at"] or datetime.min.replace(tzinfo=timezone.utc),
            item["dataset_id"],
            item["batch_id"],
        ),
        reverse=True,
    )
    recent = recent[:max_recent_batches]

    if not total_batches:
        return {
            **_empty_view(
                state="empty",
                message=(
                    "No coordinator-committed batches are visible yet. Recorder-local "
                    "capture does not count as Federation storage delivery."
                ),
            ),
            "manifest_heads": manifest_heads,
        }

    return {
        "state": "committed",
        "message": (
            "These batches are present in the coordinator-owned committed manifest. "
            "Refresh this page while the recorder runs; the latest sequence and batch "
            "count should advance after successful Federation delivery."
        ),
        "committed_batches": total_batches,
        "recorder_batches": recorder_batches,
        "dataset_count": len(public_datasets),
        "latest_committed_at": _timestamp(latest_commit),
        "datasets": public_datasets,
        "recent_batches": recent,
        "manifest_heads": manifest_heads,
    }


def current_storage_commit_view(onboarding_service: object) -> dict[str, Any]:
    """Resolve the current trusted session and its local authoritative manifest."""

    try:
        context_loader = getattr(onboarding_service, "authorized_context", None)
        context = context_loader() if callable(context_loader) else None
        if context is None:
            return _empty_view(
                state="unavailable",
                message="Connect this device to its trusted Federation to inspect commits.",
            )
        binding = getattr(context, "binding", None)
        session_id = getattr(binding, "internal_session_id", None)
        if not isinstance(session_id, str) or not session_id:
            return _empty_view(
                state="unavailable",
                message="Trusted Federation storage context is unavailable.",
            )

        database = Path(
            str(
                current_app.config.get(
                    "FEDERATION_STORAGE_AUTHORITY_STORAGE_DATABASE",
                    _DEFAULT_STORAGE_CONTROL_DATABASE,
                )
            )
        )
        if not database.exists():
            return _empty_view(
                state="empty",
                message=(
                    "The authoritative storage catalogue has not been created yet; "
                    "no Federation-committed data can be verified on this device."
                ),
            )
        return build_storage_commit_view(
            PhaseDControlPlane(database),
            session_id=session_id,
        )
    except Exception as exc:  # noqa: BLE001 - operator projection fails closed
        current_app.logger.warning(
            "Committed storage observability unavailable (%s)",
            type(exc).__name__,
        )
        return _empty_view(
            state="unavailable",
            message="Committed storage evidence could not be read safely.",
        )


__all__ = ["build_storage_commit_view", "current_storage_commit_view"]
