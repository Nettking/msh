"""Mirror committed Federation recorder telemetry into the local product UI.

The bridge deliberately discovers data through the coordinator-owned manifest
and reads it through the authenticated storage authority.  It never scans a
provider or accepts a remote path as a local filesystem path.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask

from catalog.common.artifact_refresh import request_artifact_catalog_refresh
from catalog.federation.errors import FederationOperationError
from catalog.federation.storage_catalog import (
    CommittedBatchPage,
    CommittedBatchReference,
)
from catalog.federation.telemetry_mirror import (
    RECORDER_DATASET_SCHEMA_NAME,
    RECORDER_DATASET_SCHEMA_VERSION,
    FederatedTelemetryMirror,
    TelemetryMirrorIngestState,
)
from catalog.mtconnect_recorder.federation_node import select_storage_authority

_PAGE_SIZE = 20
_DEFAULT_MAX_PAGES = 5
_DEFAULT_MAX_BATCHES = 100
_HARD_MAX_PAGES = 20
_HARD_MAX_BATCHES = _PAGE_SIZE * _HARD_MAX_PAGES

_STORAGE_GROUP_CONFIG_KEYS = (
    "FEDERATED_TELEMETRY_STORAGE_GROUP_ID",
    "FEDERATION_STORAGE_GROUP_ID",
    "FCP_FEDERATION_STORAGE_GROUP",
    "RECORDER_FEDERATION_STORAGE_GROUP_ID",
)

RefreshCallback = Callable[..., bool]
Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise TypeError("telemetry bridge clock must return a timezone-aware value")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _error_code(exc: BaseException) -> str:
    value = getattr(exc, "code", None)
    return value if isinstance(value, str) and value else type(exc).__name__


class FederatedTelemetryProductBridge:
    """Bounded, thread-safe synchronization of manifest-backed telemetry."""

    def __init__(
        self,
        app: Flask,
        onboarding_service: object,
        *,
        mirror: FederatedTelemetryMirror | None = None,
        refresh_callback: RefreshCallback | None = None,
        clock: Clock = _utc_now,
    ) -> None:
        self.app = app
        self.onboarding_service = onboarding_service
        self._mirror = mirror
        self._refresh_callback = refresh_callback
        self._clock = clock
        self._sync_lock = threading.Lock()
        self._state_lock = threading.RLock()
        self._needs_rebuild = True
        self._resume_scope: tuple[str, str, str] | None = None
        self._resume_cursor: str | None = None
        self._resume_manifest: tuple[int, str] | None = None
        self._refresh_pending = False
        self._state: dict[str, object] = {
            "status": "not-started",
            "authority": None,
            "group": None,
            "manifest": None,
            "discovered": 0,
            "downloaded": 0,
            "already": 0,
            "last_error": None,
            "last_sync": None,
        }

    def snapshot(self) -> dict[str, object]:
        """Return a detached snapshot suitable for routes and diagnostics."""

        with self._state_lock:
            result = dict(self._state)
            manifest = result.get("manifest")
            if isinstance(manifest, dict):
                result["manifest"] = dict(manifest)
            return result

    def _set_state(self, **values: object) -> None:
        with self._state_lock:
            state = dict(self._state)
            state.update(values)
            self._state = state

    def _configured_group(self) -> str | None:
        for key in _STORAGE_GROUP_CONFIG_KEYS:
            configured = self.app.config.get(key)
            if configured is not None and (value := str(configured).strip()):
                return value
        return None

    def _positive_bound(self, key: str, default: int, hard_limit: int) -> int:
        value = self.app.config.get(key, default)
        if isinstance(value, bool):
            raise FederationOperationError(
                "invalid-telemetry-bridge-config",
                f"{key} must be a positive integer",
            )
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise FederationOperationError(
                "invalid-telemetry-bridge-config",
                f"{key} must be a positive integer",
            ) from exc
        if parsed < 1 or parsed > hard_limit:
            raise FederationOperationError(
                "invalid-telemetry-bridge-config",
                f"{key} must be between 1 and {hard_limit}",
            )
        return parsed

    def _ensure_mirror(self) -> FederatedTelemetryMirror:
        mirror = self._mirror
        if mirror is not None:
            return mirror
        configured = self.app.config.get(
            "FEDERATED_TELEMETRY_MIRROR_DIRECTORY",
            "data/federation/shared",
        )
        mirror = FederatedTelemetryMirror(Path(str(configured)))
        self._mirror = mirror
        return mirror

    def _request_refresh(self) -> bool:
        callback = self._refresh_callback
        if callback is None:
            # Prefer the catalog owned by this Flask application.  The module-
            # level callback is retained for non-Flask producers, but can point
            # at another app when more than one application factory is used in
            # the same process (for example, tests or an embedded deployment).
            catalog = self.app.config.get("ARTIFACT_CATALOG")
            start_rescan = getattr(catalog, "start_background_rescan_if_idle", None)
            if callable(start_rescan):
                callback = start_rescan
            else:
                return request_artifact_catalog_refresh(
                    reason="federated_telemetry_mirror_updated"
                )
        try:
            return bool(callback(reason="federated_telemetry_mirror_updated"))
        except Exception as exc:  # noqa: BLE001 - refresh remains best effort
            self.app.logger.info(
                "Federated telemetry catalog refresh unavailable (%s)",
                type(exc).__name__,
            )
            return False

    @staticmethod
    def _session_id(runtime_state: object, context: object) -> str:
        binding = getattr(context, "binding", None)
        session_id = getattr(binding, "internal_session_id", None)
        if not isinstance(session_id, str) or not session_id:
            raise FederationOperationError(
                "trusted-federation-context-incomplete",
                "trusted Federation context has no session identity",
            )
        runtime_binding = getattr(runtime_state, "binding", None)
        runtime_session_id = getattr(runtime_binding, "internal_session_id", None)
        if runtime_session_id is not None and runtime_session_id != session_id:
            raise FederationOperationError(
                "federation-session-mismatch",
                "connected relay state does not match the trusted context",
            )
        return session_id

    @staticmethod
    def _eligible(reference: CommittedBatchReference) -> bool:
        return bool(
            reference.schema_name == RECORDER_DATASET_SCHEMA_NAME
            and reference.schema_version == RECORDER_DATASET_SCHEMA_VERSION
            and reference.dataset_id.startswith("mtconnect:")
        )

    @staticmethod
    def _validate_page(
        page: object,
        *,
        session_id: str,
        group_id: str,
        requested_limit: int,
    ) -> CommittedBatchPage:
        if not isinstance(page, CommittedBatchPage):
            raise FederationOperationError(
                "storage-catalog-response-invalid",
                "storage authority returned an invalid committed-batch page",
            )
        if (
            page.session_id != session_id
            or page.group_id != group_id
            or page.dataset_id is not None
            or len(page.batches) > requested_limit
        ):
            raise FederationOperationError(
                "storage-catalog-scope-mismatch",
                "storage authority returned a page outside the requested scope",
            )
        return page

    def _reset_resume(self) -> None:
        self._resume_scope = None
        self._resume_cursor = None
        self._resume_manifest = None

    def sync(self, runtime_state: object, context: object) -> dict[str, object]:
        """Run one bounded synchronization pass and return its public snapshot."""

        files_changed = False
        discovered = 0
        downloaded = 0
        already = 0
        authority_node_id: str | None = None
        group_id: str | None = None
        manifest_value: dict[str, object] | None = None

        with self._sync_lock:
            previous_last_sync = self.snapshot()["last_sync"]
            self._set_state(
                status="syncing",
                discovered=0,
                downloaded=0,
                already=0,
                last_error=None,
            )
            try:
                session_id = self._session_id(runtime_state, context)
                runtime = getattr(self.onboarding_service, "relay_runtime", None)
                if runtime is None:
                    raise FederationOperationError(
                        "pairing-relay-runtime-unavailable",
                        "the authenticated relay runtime is unavailable",
                    )
                status = runtime.coordinator_status()
                if not isinstance(status, dict):
                    raise FederationOperationError(
                        "coordinator-status-invalid",
                        "coordinator status must be an object",
                    )
                mirror = self._ensure_mirror()
                if self._needs_rebuild:
                    rebuild = getattr(mirror, "rebuild_materializations", None)
                    if callable(rebuild):
                        rebuilt = rebuild()
                        files_changed = bool(rebuilt) or files_changed
                    self._needs_rebuild = False
                requested_group = self._configured_group()
                selection = select_storage_authority(
                    status,
                    session_id=session_id,
                    requested_group=requested_group,
                )
                authority_node_id = selection.authority_node_id
                group_id = selection.group_id or requested_group
                if (
                    selection.state != "ready"
                    or authority_node_id is None
                    or selection.group_id is None
                ):
                    self._reset_resume()
                    self._set_state(
                        status=selection.state,
                        authority=authority_node_id,
                        group=group_id,
                        manifest=None,
                        discovered=0,
                        downloaded=0,
                        already=0,
                        last_error=selection.state,
                        last_sync=previous_last_sync,
                    )
                    return self.snapshot()
                group_id = selection.group_id

                max_pages = self._positive_bound(
                    "FEDERATED_TELEMETRY_MAX_PAGES_PER_SYNC",
                    _DEFAULT_MAX_PAGES,
                    _HARD_MAX_PAGES,
                )
                max_batches = self._positive_bound(
                    "FEDERATED_TELEMETRY_MAX_BATCHES_PER_SYNC",
                    _DEFAULT_MAX_BATCHES,
                    _HARD_MAX_BATCHES,
                )

                scope = (session_id, authority_node_id, group_id)
                if self._resume_scope != scope:
                    self._reset_resume()
                cursor = self._resume_cursor
                manifest_identity = self._resume_manifest
                seen_cursors = {cursor} if cursor is not None else set()
                pages = 0

                while pages < max_pages and discovered < max_batches:
                    requested_limit = min(_PAGE_SIZE, max_batches - discovered)
                    page = self._validate_page(
                        runtime.list_committed_batches(
                            runtime_state,
                            authority_node_id=authority_node_id,
                            group_id=group_id,
                            dataset_id=None,
                            limit=requested_limit,
                            cursor=cursor,
                        ),
                        session_id=session_id,
                        group_id=group_id,
                        requested_limit=requested_limit,
                    )
                    current_manifest = (page.manifest_revision, page.manifest_hash)
                    if (
                        manifest_identity is not None
                        and current_manifest != manifest_identity
                    ):
                        raise FederationOperationError(
                            "storage-catalog-manifest-changed",
                            "paginated storage catalog changed manifest identity",
                        )
                    manifest_identity = current_manifest
                    manifest_value = {
                        "revision": page.manifest_revision,
                        "hash": page.manifest_hash,
                    }
                    pages += 1
                    discovered += len(page.batches)

                    for reference in page.batches:
                        if not self._eligible(reference):
                            continue
                        if mirror.contains(reference):
                            already += 1
                            continue
                        content = runtime.read_committed_batch(
                            runtime_state,
                            authority_node_id=authority_node_id,
                            reference=reference,
                        )
                        downloaded += 1
                        try:
                            result = mirror.ingest(reference, content)
                        except Exception:
                            # Ingest may have durably committed its index before a
                            # materialization failure. Repair locally next pass.
                            self._needs_rebuild = True
                            raise
                        if result.state is TelemetryMirrorIngestState.STORED:
                            files_changed = True
                        elif result.state is TelemetryMirrorIngestState.ALREADY_STORED:
                            already += 1
                        else:
                            raise FederationOperationError(
                                "telemetry-mirror-result-invalid",
                                "telemetry mirror returned an unknown ingest state",
                            )

                    next_cursor = page.next_cursor
                    if next_cursor is None:
                        cursor = None
                        break
                    if next_cursor in seen_cursors:
                        raise FederationOperationError(
                            "storage-catalog-cursor-cycle",
                            "storage authority returned a repeated pagination cursor",
                        )
                    seen_cursors.add(next_cursor)
                    cursor = next_cursor

                if cursor is None:
                    self._reset_resume()
                    sync_status = "up-to-date"
                else:
                    self._resume_scope = scope
                    self._resume_cursor = cursor
                    self._resume_manifest = manifest_identity
                    sync_status = "syncing"
                self._set_state(
                    status=sync_status,
                    authority=authority_node_id,
                    group=group_id,
                    manifest=manifest_value,
                    discovered=discovered,
                    downloaded=downloaded,
                    already=already,
                    last_error=None,
                    last_sync=_stamp(self._clock()),
                )
                return self.snapshot()
            except Exception as exc:
                self._reset_resume()
                self._set_state(
                    status="error",
                    authority=authority_node_id,
                    group=group_id,
                    manifest=manifest_value,
                    discovered=discovered,
                    downloaded=downloaded,
                    already=already,
                    last_error=_error_code(exc),
                    last_sync=previous_last_sync,
                )
                raise
            finally:
                self._refresh_pending = self._refresh_pending or files_changed
                if self._refresh_pending and self._request_refresh():
                    self._refresh_pending = False


__all__ = ["FederatedTelemetryProductBridge"]
