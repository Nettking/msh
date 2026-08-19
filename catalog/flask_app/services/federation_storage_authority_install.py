"""Flask lifecycle integration for the creator's logical-storage authority.

The authority is what makes a Federation able to accept recorder and JSONL
publication at all. Normal full FCP startup supervises it automatically, while
the existing creator-only check prevents a joined member from self-promoting.

The supervised runtime uses the same reviewed trusted-network relay client as
physical pairing and reconciles only the creator's own relay/session state from
the local coordinator database. It does not add a new enrollment authority or
weaken the generic node transport policy.
"""

from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass
from pathlib import Path

from flask import Flask

from catalog.common.federation_paths import DEFAULT_COORDINATOR_DATABASE
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.storage_failover import StorageAuthoritySettings

from .storage_commit_observability import current_storage_commit_view
from .trusted_storage_authority_runtime import run_trusted_storage_authority

_EXTENSION_KEY = "federation_storage_authority"
_RETRY_SECONDS = 5.0


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class FederationStorageAuthoritySnapshot:
    """What an operator needs to tell working from silently-not-running."""

    status: str
    enabled: bool
    ready_group_ids: tuple[str, ...] = ()
    last_error_code: str | None = None


class FederationStorageAuthorityMonitor:
    """Supervise one logical-storage authority for this device's session."""

    def __init__(self, app: Flask, onboarding_service: object) -> None:
        self.app = app
        self.onboarding_service = onboarding_service
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._async_stop: asyncio.Event | None = None
        self._snapshot = FederationStorageAuthoritySnapshot(
            status="not-started",
            enabled=False,
        )

    # ---- observable state ------------------------------------------------

    def snapshot(self) -> FederationStorageAuthoritySnapshot:
        with self._lock:
            return self._snapshot

    def _set_snapshot(
        self,
        status: str,
        *,
        enabled: bool,
        ready_group_ids: tuple[str, ...] = (),
        error_code: str | None = None,
    ) -> None:
        with self._lock:
            self._snapshot = FederationStorageAuthoritySnapshot(
                status=status,
                enabled=enabled,
                ready_group_ids=ready_group_ids,
                last_error_code=error_code,
            )

    def _on_announced(self, announcement: CapabilityAnnouncement) -> None:
        groups = announcement.properties.get("group_ids")
        ready = tuple(
            item
            for item in (groups if isinstance(groups, list) else ())
            if isinstance(item, str) and item
        )
        self._set_snapshot(
            "ready" if announcement.status is CapabilityStatus.READY else "no-groups",
            enabled=True,
            ready_group_ids=ready,
        )

    # ---- configuration ---------------------------------------------------

    def _enabled(self) -> bool:
        return bool(self.app.config.get("FEDERATION_STORAGE_AUTHORITY_ENABLED", False))

    def _authorized_context(self):
        loader = getattr(self.onboarding_service, "authorized_context", None)
        context = loader() if callable(loader) else None
        if context is None:
            raise FederationOperationError(
                "storage-authority-federation-required",
                "a trusted Federation connection is required before the storage "
                "authority can run",
                "binding",
            )
        return context

    def build_settings(self) -> StorageAuthoritySettings:
        """Compose settings from current authenticated app configuration."""

        if not self._enabled():
            raise FederationOperationError(
                "storage-authority-disabled",
                "the Federation logical-storage authority is not enabled",
            )
        context = self._authorized_context()
        binding = getattr(context, "binding", None)
        session_id = getattr(binding, "internal_session_id", None)
        if not isinstance(session_id, str) or not session_id:
            raise FederationValidationError(
                "invalid-storage-authority-context",
                "session_id",
                "the authorized Federation session is missing",
            )
        relay_url = str(
            self.app.config.get("FEDERATION_STORAGE_AUTHORITY_RELAY_URL", "")
        ).strip()
        if not relay_url:
            raise FederationValidationError(
                "storage-authority-relay-required",
                "relay_url",
                "the authority needs the Federation relay address; set "
                "FCP_FEDERATION_STORAGE_AUTHORITY_RELAY or FCP_PAIRING_RELAY_URL",
            )
        return StorageAuthoritySettings(
            relay_control_database=str(
                self.app.config["FEDERATION_STORAGE_AUTHORITY_CONTROL_DATABASE"]
            ),
            storage_control_database=str(
                self.app.config["FEDERATION_STORAGE_AUTHORITY_STORAGE_DATABASE"]
            ),
            publication_database=str(
                self.app.config["FEDERATION_STORAGE_AUTHORITY_PUBLICATION_DATABASE"]
            ),
            failover_database=str(
                self.app.config["FEDERATION_STORAGE_AUTHORITY_FAILOVER_DATABASE"]
            ),
            state_dir=str(self.app.config["FEDERATION_STORAGE_AUTHORITY_STATE_DIR"]),
            relay=relay_url,
            display_name=str(
                self.app.config["FEDERATION_STORAGE_AUTHORITY_DISPLAY_NAME"]
            ),
            session_id=session_id,
            allow_insecure_local=bool(
                self.app.config.get(
                    "FEDERATION_STORAGE_AUTHORITY_ALLOW_INSECURE_LOCAL", False
                )
            ),
            scan_interval=float(
                self.app.config["FEDERATION_STORAGE_AUTHORITY_SCAN_INTERVAL_SECONDS"]
            ),
            lease_seconds=float(
                self.app.config["FEDERATION_STORAGE_AUTHORITY_LEASE_SECONDS"]
            ),
        )

    def session_creator_state(self) -> str:
        """Report whether an authority started here can be used at all.

        A publisher accepts the logical-storage capability only from the node
        that created the session, so an authority anywhere else advertises
        something nothing will ever select. Report that instead of running a
        connection whose announcements are ignored.
        """

        try:
            context = self._authorized_context()
        except (FederationOperationError, FederationValidationError):
            return "unknown"
        binding = getattr(context, "binding", None)
        coordinator = getattr(context, "coordinator", None)
        session_id = getattr(binding, "internal_session_id", None)
        device_id = getattr(binding, "device_id", None)
        store = getattr(coordinator, "store", None)
        getter = getattr(store, "get_session", None)
        if not callable(getter) or not isinstance(session_id, str):
            return "unknown"
        try:
            session = getter(session_id)
        except Exception:  # noqa: BLE001 - a status probe must not raise
            return "unknown"
        creator = getattr(session, "created_by_node_id", None)
        if not isinstance(creator, str) or not creator:
            return "unknown"
        return "creator" if creator == device_id else "not-creator"

    # ---- lifecycle -------------------------------------------------------

    async def _run_authority(self, settings: StorageAuthoritySettings) -> None:
        stop = asyncio.Event()
        with self._lock:
            self._async_stop = stop
        self._set_snapshot("starting", enabled=True)
        await run_trusted_storage_authority(
            settings,
            stop=stop,
            on_announced=self._on_announced,
        )

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                with self.app.app_context():
                    settings = self.build_settings()
            except Exception as exc:  # noqa: BLE001 - retry boundary is deliberate
                self._set_snapshot(
                    "waiting",
                    enabled=self._enabled(),
                    error_code=str(getattr(exc, "code", type(exc).__name__)),
                )
                if self._stop.wait(_RETRY_SECONDS):
                    return
                continue

            loop = asyncio.new_event_loop()
            with self._lock:
                self._loop = loop
            try:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self._run_authority(settings))
            except Exception as exc:  # noqa: BLE001 - authority stays restartable
                self._set_snapshot(
                    "retrying",
                    enabled=True,
                    error_code=str(getattr(exc, "code", type(exc).__name__)),
                )
                if self._stop.wait(_RETRY_SECONDS):
                    return
            finally:
                with self._lock:
                    self._async_stop = None
                    self._loop = None
                loop.close()
                asyncio.set_event_loop(None)
        self._set_snapshot("stopped", enabled=self._enabled())

    def start(self) -> None:
        if not self._enabled():
            self._set_snapshot("disabled", enabled=False)
            return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
        # Refuse before allocating a thread when this device could never serve
        # a usable authority, so the status names the reason instead of looping.
        with self.app.app_context():
            if self.session_creator_state() == "not-creator":
                self._set_snapshot(
                    "not-session-creator",
                    enabled=True,
                    error_code="storage-authority-not-session-creator",
                )
                return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="fcp-federation-storage-authority",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            loop = self._loop
            async_stop = self._async_stop
        if loop is not None and async_stop is not None:
            loop.call_soon_threadsafe(async_stop.set)


def install_federation_storage_authority(
    app: Flask,
    *,
    onboarding_service: object,
) -> FederationStorageAuthorityMonitor:
    """Install supervision of the creator's logical-storage authority."""

    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_ENABLED",
        _env_bool("FCP_FEDERATION_STORAGE_AUTHORITY_ENABLED", False),
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_RELAY_URL",
        os.getenv("FCP_FEDERATION_STORAGE_AUTHORITY_RELAY")
        or os.getenv("FCP_PAIRING_RELAY_URL", ""),
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_CONTROL_DATABASE",
        os.getenv("FCP_FEDERATION_COORDINATOR_DATABASE", DEFAULT_COORDINATOR_DATABASE),
    )
    storage_root = Path(
        os.getenv("FCP_FEDERATION_STORAGE_AUTHORITY_DIR", "data/federation/storage")
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_STORAGE_DATABASE",
        str(storage_root / "control.sqlite3"),
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_PUBLICATION_DATABASE",
        str(storage_root / "publication.sqlite3"),
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_FAILOVER_DATABASE",
        str(storage_root / "failover.sqlite3"),
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_STATE_DIR",
        os.getenv("FCP_FEDERATION_NODE_STATE_DIR", "data/federation/device"),
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_DISPLAY_NAME",
        os.getenv("FCP_DEVICE_NAME", "This FCP device"),
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_SCAN_INTERVAL_SECONDS",
        float(os.getenv("FCP_FEDERATION_STORAGE_AUTHORITY_SCAN_SECONDS", "2.0")),
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_LEASE_SECONDS",
        float(os.getenv("FCP_FEDERATION_STORAGE_AUTHORITY_LEASE_SECONDS", "300.0")),
    )

    monitor = FederationStorageAuthorityMonitor(app, onboarding_service)
    app.extensions[_EXTENSION_KEY] = monitor

    @app.before_request
    def _start_federation_storage_authority() -> None:
        monitor.start()

    def _storage_commit_view() -> dict[str, object]:
        return current_storage_commit_view(onboarding_service)

    @app.context_processor
    def _storage_commit_observability_context() -> dict[str, object]:
        # Expose a callable rather than eagerly reading the manifest on every
        # template render. The storage detail page invokes it only when needed.
        return {"federation_storage_commit_view": _storage_commit_view}

    return monitor


__all__ = [
    "FederationStorageAuthorityMonitor",
    "FederationStorageAuthoritySnapshot",
    "install_federation_storage_authority",
]
