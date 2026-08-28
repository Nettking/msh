"""Flask lifecycle integration for the creator's logical-storage authority.

The authority is what makes a Federation able to accept recorder and JSONL
publication at all. Normal full FCP startup supervises it automatically, while
the existing creator-only check prevents a joined member from self-promoting.

A full FCP device owns one authenticated relay connection per node identity. The
storage authority therefore runs on the pairing runtime's existing connection
and event loop instead of authenticating a second client with the creator's same
identity. This prevents the relay's intentional same-node connection replacement
from turning normal startup into a reconnect loop.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flask import Flask

from catalog.common.federation_paths import DEFAULT_COORDINATOR_DATABASE
from catalog.federation.errors import (
    AuthenticationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.storage_failover import StorageAuthoritySettings

from .federation_pairing_service import RemotePairingState
from .storage_commit_observability import current_storage_commit_view
from .trusted_storage_authority_runtime import run_trusted_storage_authority

_EXTENSION_KEY = "federation_storage_authority"
_RETRY_SECONDS = 5.0
#: Ceiling for this supervisor's own restart wait.
#
# Every restart rebuilds the authority settings and the shared relay context for
# the creator's logical-storage authority. A fixed wait answered a condition
# that keeps failing -- an unreachable relay, an unopenable control database --
# by repeating that work at a constant rate forever. The first wait is unchanged
# so ordinary recovery stays as prompt as it was; only a persistent failure
# backs off. This matches the reconnect driver, which already counts its
# attempts and backs off to the same ceiling.
_MAX_RETRY_SECONDS = 60.0


def _restart_delay_seconds(consecutive_failures: int) -> float:
    """Bounded exponential wait before restarting the storage authority."""

    exponent = max(0, consecutive_failures - 1)
    return min(_RETRY_SECONDS * 2.0**exponent, _MAX_RETRY_SECONDS)
_AI_BRIDGE_EXTENSION_KEY = "federated_ai_product_bridge"


def _env_bytes(name: str) -> int | None:
    """Read an optional byte count from the environment.

    Unset means "not configured", which each consumer interprets for itself.
    A present but malformed value is a configuration error and is refused at
    startup rather than being quietly dropped.
    """

    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise FederationValidationError(
            "invalid-storage-authority-allocation",
            name,
            "must be a non-negative integer number of bytes",
        ) from exc
    if parsed < 0:
        raise FederationValidationError(
            "invalid-storage-authority-allocation",
            name,
            "must be a non-negative integer number of bytes",
        )
    return parsed


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
    # Consecutive supervisor restarts. A last error code alone cannot separate
    # one blip from an authority that has been restarting all day, which is the
    # difference between "retrying" and "not running".
    consecutive_failures: int = 0


@dataclass(frozen=True)
class _SharedRelayContext:
    client: object
    loop: asyncio.AbstractEventLoop
    message_source: object | None
    bridge: object | None


class _StorageAwareRelayView:
    """Keep product methods on the upstream endpoint but read after storage."""

    def __init__(self, upstream: object, downstream: object) -> None:
        self._upstream = upstream
        self._downstream = downstream

    def __getattr__(self, name: str) -> Any:
        return getattr(self._upstream, name)

    async def receive_other(self, *, timeout: float | None = None):
        receive = getattr(self._downstream, "receive_other")
        return await receive(timeout=timeout)

    async def close(self) -> None:
        close = getattr(self._upstream, "close", None)
        if callable(close):
            await close()


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
        self._future: concurrent.futures.Future[None] | None = None
        self._snapshot = FederationStorageAuthoritySnapshot(
            status="not-started",
            enabled=False,
        )
        # Cleared by an announcement, which is the authority proving it ran --
        # never by merely managing to compose settings again.
        self._restart_failures = 0

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
        consecutive_failures: int = 0,
    ) -> None:
        with self._lock:
            self._snapshot = FederationStorageAuthoritySnapshot(
                status=status,
                enabled=enabled,
                ready_group_ids=ready_group_ids,
                last_error_code=error_code,
                consecutive_failures=consecutive_failures,
            )

    def _record_restart_failure(self) -> float:
        """Count one supervisor restart and report how long to wait.

        A restart that is never counted cannot be told apart from a first
        blip, and a wait that never grows answers a persistent failure by
        repeating its most expensive part at a constant rate.
        """

        with self._lock:
            self._restart_failures += 1
            failures = self._restart_failures
        return _restart_delay_seconds(failures)

    def _restart_count(self) -> int:
        with self._lock:
            return self._restart_failures

    def _on_announced(self, announcement: CapabilityAnnouncement) -> None:
        groups = announcement.properties.get("group_ids")
        ready = tuple(
            item
            for item in (groups if isinstance(groups, list) else ())
            if isinstance(item, str) and item
        )
        # Reaching an announcement is the authority proving it actually ran, so
        # this is the one thing that clears the restart ladder. Composing
        # settings again is not evidence: an authority that starts and dies
        # before announcing must keep escalating rather than reset each time.
        with self._lock:
            self._restart_failures = 0
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
        coordinator_database = str(
            self.app.config["FEDERATION_STORAGE_AUTHORITY_CONTROL_DATABASE"]
        )
        return StorageAuthoritySettings(
            relay_control_database=coordinator_database,
            storage_control_database=coordinator_database,
            publication_database=str(
                self.app.config["FEDERATION_STORAGE_AUTHORITY_PUBLICATION_DATABASE"]
            ),
            failover_database=str(
                self.app.config["FEDERATION_STORAGE_AUTHORITY_FAILOVER_DATABASE"]
            ),
            acknowledgements_database=str(
                self.app.config[
                    "FEDERATION_STORAGE_AUTHORITY_ACKNOWLEDGEMENTS_DATABASE"
                ]
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
            storage_budget_bytes=self._optional_bytes(
                "FEDERATION_STORAGE_AUTHORITY_BUDGET_BYTES"
            ),
            storage_floor_bytes=self._optional_bytes(
                "FEDERATION_STORAGE_AUTHORITY_FLOOR_BYTES"
            ),
        )

    def _optional_bytes(self, key: str) -> int | None:
        """Read an optional byte count from application configuration.

        A malformed value is refused rather than defaulted. A storage budget
        that silently became "unbounded" because someone mistyped it is the
        outcome the allocation exists to prevent, and a floor that silently
        became the derived default would hide an operator's deliberate choice.
        """

        value = self.app.config.get(key)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise FederationValidationError(
                "invalid-storage-authority-allocation",
                key,
                "must be a non-negative integer number of bytes",
            )
        return value

    def session_creator_state(self) -> str:
        """Report whether an authority started here can be used at all."""

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

    # ---- shared relay composition ---------------------------------------

    def _shared_relay_context(
        self,
        settings: StorageAuthoritySettings,
    ) -> _SharedRelayContext | None:
        """Borrow the already-connected product relay without retargeting it.

        The saved-membership monitor is the sole owner of connection establishment
        and relay-route selection for the full workbench. Storage must wait for
        that connection and compose behind it; using the authority's own relay
        setting here can differ from the product route (for example Compose DNS
        versus the host's Tailscale address) and would make ``ensure_connected``
        replace the same node identity's live WebSocket on every retry.

        Tiny unit-test fakes and direct low-level compositions may not expose a
        runtime; those retain the legacy owned-client path.
        """

        del settings  # Shared mode deliberately does not choose a relay route.
        runtime = getattr(self.onboarding_service, "relay_runtime", None)
        if runtime is None:
            return None
        connected_client = getattr(runtime, "_connected_client", None)
        start_loop = getattr(runtime, "_start_loop", None)
        if not all(callable(item) for item in (connected_client, start_loop)):
            raise FederationOperationError(
                "storage-authority-shared-relay-required",
                "the installed Federation runtime does not expose its shared relay connection",
                "connection",
            )

        context = self._authorized_context()
        binding = getattr(context, "binding", None)
        device_id = getattr(binding, "device_id", None)
        session_id = getattr(binding, "internal_session_id", None)
        if not isinstance(device_id, str) or not isinstance(session_id, str):
            raise FederationValidationError(
                "invalid-storage-authority-context",
                "binding",
                "the trusted Federation binding is incomplete",
            )

        client = connected_client()
        if getattr(client, "node_id", None) != device_id:
            raise AuthenticationError(
                "storage-authority-shared-identity-mismatch",
                "the shared relay client does not use the Federation creator identity",
                "node_id",
            )
        current_relay_url = getattr(runtime, "_relay_url", None)
        if not isinstance(current_relay_url, str) or not current_relay_url:
            raise FederationOperationError(
                "storage-authority-shared-relay-not-ready",
                "the saved-membership runtime has not established its relay route yet",
                "connection",
            )
        state = RemotePairingState(current_relay_url, binding)
        loop = start_loop()

        bridge = self.app.extensions.get(_AI_BRIDGE_EXTENSION_KEY)
        transport_context = getattr(bridge, "_transport_context", None)
        source = None
        if callable(transport_context):
            # The AI bridge may revalidate connectivity, but the state passed to
            # it names the runtime's current route, never the storage-specific
            # configuration. Revalidation therefore cannot replace the live
            # connection merely because two equivalent relay addresses differ.
            source, bridge_loop = transport_context(runtime, state)
            if bridge_loop is not loop:
                raise FederationOperationError(
                    "storage-authority-relay-loop-mismatch",
                    "product relay consumers are not running on one shared event loop",
                    "connection",
                )
        return _SharedRelayContext(
            client=client,
            loop=loop,
            message_source=source,
            bridge=bridge,
        )

    @staticmethod
    def _install_storage_view(
        shared: _SharedRelayContext,
        downstream: object,
    ) -> _StorageAwareRelayView | None:
        bridge = shared.bridge
        upstream = shared.message_source
        lock = getattr(bridge, "_endpoint_lock", None)
        if bridge is None or upstream is None or lock is None:
            return None
        with lock:
            if getattr(bridge, "_endpoint", None) is not upstream:
                raise FederationOperationError(
                    "storage-authority-message-source-changed",
                    "the shared Federation message source changed during storage startup",
                    "connection",
                )
            view = _StorageAwareRelayView(upstream, downstream)
            bridge._endpoint = view
            return view

    @staticmethod
    def _restore_storage_view(
        shared: _SharedRelayContext,
        view: _StorageAwareRelayView | None,
    ) -> None:
        if view is None:
            return
        bridge = shared.bridge
        lock = getattr(bridge, "_endpoint_lock", None)
        if bridge is None or lock is None:
            return
        with lock:
            if getattr(bridge, "_endpoint", None) is view:
                bridge._endpoint = shared.message_source

    # ---- lifecycle -------------------------------------------------------

    async def _run_authority(
        self,
        settings: StorageAuthoritySettings,
        *,
        shared: _SharedRelayContext | None = None,
    ) -> None:
        stop = asyncio.Event()
        with self._lock:
            self._async_stop = stop
        self._set_snapshot("starting", enabled=True)
        view: _StorageAwareRelayView | None = None

        def expose_downstream(source: object) -> None:
            nonlocal view
            if shared is not None:
                view = self._install_storage_view(shared, source)

        try:
            await run_trusted_storage_authority(
                settings,
                stop=stop,
                on_announced=self._on_announced,
                client=(None if shared is None else shared.client),  # type: ignore[arg-type]
                message_source=(None if shared is None else shared.message_source),
                on_message_source=(None if shared is None else expose_downstream),
            )
        finally:
            if shared is not None:
                self._restore_storage_view(shared, view)

    def _wait_retry(self, delay: float | None = None) -> bool:
        return self._stop.wait(_RETRY_SECONDS if delay is None else delay)

    def _run_owned_attempt(self, settings: StorageAuthoritySettings) -> None:
        """Legacy low-level path for tests without an installed pairing runtime."""

        loop = asyncio.new_event_loop()
        with self._lock:
            self._loop = loop
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._run_authority(settings))
        finally:
            with self._lock:
                self._async_stop = None
                self._loop = None
            loop.close()
            asyncio.set_event_loop(None)

    def _run_shared_attempt(
        self,
        settings: StorageAuthoritySettings,
        shared: _SharedRelayContext,
    ) -> None:
        with self._lock:
            self._loop = shared.loop
        future = asyncio.run_coroutine_threadsafe(
            self._run_authority(settings, shared=shared),
            shared.loop,
        )
        with self._lock:
            self._future = future
        try:
            future.result()
        finally:
            with self._lock:
                self._future = None
                self._async_stop = None
                self._loop = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                with self.app.app_context():
                    settings = self.build_settings()
                    shared = self._shared_relay_context(settings)
            except Exception as exc:  # noqa: BLE001 - retry boundary is deliberate
                delay = self._record_restart_failure()
                self._set_snapshot(
                    "waiting",
                    enabled=self._enabled(),
                    error_code=str(getattr(exc, "code", type(exc).__name__)),
                    consecutive_failures=self._restart_count(),
                )
                if self._wait_retry(delay):
                    return
                continue

            try:
                if shared is None:
                    self._run_owned_attempt(settings)
                else:
                    self._run_shared_attempt(settings, shared)
            except (asyncio.CancelledError, concurrent.futures.CancelledError):
                delay = self._record_restart_failure()
                self._set_snapshot(
                    "retrying",
                    enabled=True,
                    error_code="storage-authority-cancelled",
                    consecutive_failures=self._restart_count(),
                )
                if self._wait_retry(delay):
                    return
            except Exception as exc:  # noqa: BLE001 - authority stays restartable
                delay = self._record_restart_failure()
                self._set_snapshot(
                    "retrying",
                    enabled=True,
                    error_code=str(getattr(exc, "code", type(exc).__name__)),
                    consecutive_failures=self._restart_count(),
                )
                if self._wait_retry(delay):
                    return
        self._set_snapshot("stopped", enabled=self._enabled())

    def start(self) -> None:
        if not self._enabled():
            self._set_snapshot("disabled", enabled=False)
            return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
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
            future = self._future
        if loop is not None and async_stop is not None:
            loop.call_soon_threadsafe(async_stop.set)
        elif future is not None:
            future.cancel()


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
    app.config["FEDERATION_STORAGE_AUTHORITY_STORAGE_DATABASE"] = str(
        app.config["FEDERATION_STORAGE_AUTHORITY_CONTROL_DATABASE"]
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
        "FEDERATION_STORAGE_AUTHORITY_ACKNOWLEDGEMENTS_DATABASE",
        str(storage_root / "recorder_ingest_acknowledgements.sqlite3"),
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
    # Disk this device offers the Federation, and the free space it never
    # consumes. Both are unset by default: an unset budget leaves the device
    # bounded by the floor alone, and an unset floor is derived from the
    # volume and platform rather than guessed at.
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_BUDGET_BYTES",
        _env_bytes("FCP_FEDERATION_STORAGE_AUTHORITY_BUDGET_BYTES"),
    )
    app.config.setdefault(
        "FEDERATION_STORAGE_AUTHORITY_FLOOR_BYTES",
        _env_bytes("FCP_FEDERATION_STORAGE_AUTHORITY_FLOOR_BYTES"),
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
        return {"federation_storage_commit_view": _storage_commit_view}

    return monitor


__all__ = [
    "FederationStorageAuthorityMonitor",
    "FederationStorageAuthoritySnapshot",
    "install_federation_storage_authority",
]
