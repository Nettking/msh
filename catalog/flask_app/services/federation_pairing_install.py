"""Application-factory installation for authenticated Federation pairing."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, current_app, request

from .capability_recovery_adapters import fresh_capability_inspection_adapters
from .federated_ai_product_bridge import FederatedAIProductBridge
from .federation_contribution_publication import publish_local_contributions
from .federation_pairing_service import (
    PairingAwareCapabilityOnboardingService,
    RemotePairingState,
    RemotePairingStore,
)
from .recorder_federation_publication_install import (
    install_recorder_federation_publication,
)
from .resilient_pairing_runtime import ResilientPairingRelayRuntime
from .server_setup_service import load_settings

_RECONNECT_EXTENSION_KEY = "federation_saved_membership_reconnect"
_RETAINED_STARTUP_CHECK_KEY = "capability_onboarding_startup_checked"
_CONTRIBUTION_RECONCILE_EXTENSION_KEY = "capability_contribution_startup_reconciled"
_PROVIDER_SURFACE_CONFIG_KEY = "PROVIDER_OPERATOR_SURFACE"
_LOCAL_RELAY_CONFIG_KEY = "CAPABILITY_ONBOARDING_LOCAL_RELAY_URL"
_DEFAULT_COMPOSE_LOCAL_RELAY_URL = "ws://relay:8765"
_CONNECTED_CHECK_SECONDS = 15.0
_MAX_RETRY_SECONDS = 60.0
_CONTRIBUTION_MUTATION_ENDPOINTS = frozenset(
    {
        "capability_contribution_web.save_contributions",
        "capability_contribution_web.suspend_contribution",
        "capability_contribution_web.reconcile_contributions",
    }
)
_PROVIDER_MUTATION_ENDPOINTS = frozenset(
    {
        "provider_federation_web.provider_action_html",
        "provider_federation_web.provider_action_api",
    }
)
_TERMINAL_RECONNECT_CODES = frozenset(
    {
        "malformed-remote-pairing-state",
        "not-session-member",
        "pairing-actor-mismatch",
        "pairing-membership-mismatch",
        "pairing-membership-missing",
        "revoked-node",
        "unknown-node",
    }
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _build_service(app: Flask) -> PairingAwareCapabilityOnboardingService:
    identity_directory = Path(
        app.config["CAPABILITY_ONBOARDING_IDENTITY_DIRECTORY"]
    )
    state_database = Path(
        app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"]
    )
    remote_path = Path(
        app.config.get(
            "CAPABILITY_ONBOARDING_REMOTE_PAIRING_PATH",
            state_database.with_name("remote_pairing.json"),
        )
    )
    device_name = str(app.config["CAPABILITY_ONBOARDING_DEVICE_NAME"])
    runtime = ResilientPairingRelayRuntime(
        state_directory=identity_directory,
        display_name=device_name,
    )
    return PairingAwareCapabilityOnboardingService(
        identity_directory=identity_directory,
        state_database=state_database,
        coordinator_database=app.config[
            "CAPABILITY_ONBOARDING_COORDINATOR_DATABASE"
        ],
        device_name=device_name,
        discovery_sources=app.config.get(
            "CAPABILITY_ONBOARDING_DISCOVERY_SOURCES",
            (),
        ),
        setup_loader=app.config.get(
            "CAPABILITY_ONBOARDING_SETUP_LOADER",
            load_settings,
        ),
        remote_store=RemotePairingStore(remote_path),
        relay_runtime=runtime,
    )


class LazyPairingOnboardingService(PairingAwareCapabilityOnboardingService):
    """Resolve the real service only after final application configuration."""

    def __init__(self) -> None:
        object.__setattr__(self, "_lazy_lock", threading.RLock())
        object.__setattr__(self, "_lazy_instance", None)

    def _lazy_delegate(self) -> PairingAwareCapabilityOnboardingService:
        instance = object.__getattribute__(self, "_lazy_instance")
        if instance is not None:
            return instance
        lock = object.__getattribute__(self, "_lazy_lock")
        with lock:
            instance = object.__getattribute__(self, "_lazy_instance")
            if instance is None:
                instance = _build_service(current_app._get_current_object())
                object.__setattr__(self, "_lazy_instance", instance)
            return instance

    def __getattribute__(self, name: str) -> Any:
        if name in {
            "_lazy_lock",
            "_lazy_instance",
            "_lazy_delegate",
            "__class__",
            "__dict__",
            "__repr__",
        }:
            return object.__getattribute__(self, name)
        return getattr(object.__getattribute__(self, "_lazy_delegate")(), name)

    def __repr__(self) -> str:
        instance = object.__getattribute__(self, "_lazy_instance")
        return (
            "LazyPairingOnboardingService(pending)"
            if instance is None
            else f"LazyPairingOnboardingService({instance!r})"
        )


class SavedFederationReconnectMonitor:
    """Keep one trusted Federation membership present on the authenticated relay.

    Remote members reconnect to their persisted relay root. The local Federation
    creator also opens an authenticated outbound relay connection through the
    Compose-internal relay root (or an explicitly configured replacement). This
    makes coordinator liveness symmetric: a member is reported online because a
    real authenticated connection and heartbeat exist, not because the local UI
    special-cases itself.
    """

    def __init__(
        self,
        app: Flask,
        service: LazyPairingOnboardingService,
    ) -> None:
        self.app = app
        self.service = service
        self.ai_bridge = FederatedAIProductBridge(app, service)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._state: dict[str, object] = {
            "status": "not-started",
            "attempts": 0,
            "last_error_code": None,
        }

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return dict(self._state)

    def _set_state(
        self,
        status: str,
        *,
        attempts: int,
        error_code: str | None = None,
    ) -> None:
        with self._lock:
            self._state = {
                "status": status,
                "attempts": attempts,
                "last_error_code": error_code,
            }

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="msh-federation-auto-reconnect",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def request_contribution_refresh(self) -> None:
        """Wake the monitor after contribution or provider authority changes."""

        self._wake.set()

    def _local_relay_url(self) -> str:
        configured = self.app.config.get(
            _LOCAL_RELAY_CONFIG_KEY,
            _DEFAULT_COMPOSE_LOCAL_RELAY_URL,
        )
        value = str(configured).strip()
        if not value:
            return _DEFAULT_COMPOSE_LOCAL_RELAY_URL
        return value

    def _connected_state_and_context(self) -> tuple[RemotePairingState, object] | None:
        remote = self.service.remote_store.load()
        if remote is not None:
            context = self.service.authorized_context()
            if context is None:
                raise RuntimeError("saved remote Federation context is unavailable")
            return remote, context

        # A local Federation creator previously had no relay client at all. That
        # made every remote member see the owner as offline while the owner's UI
        # locally overrode itself to online. Revalidate the existing membership,
        # then establish a real authenticated connection using the same identity.
        context = self.service.authorized_context()
        if context is None:
            return None
        binding = getattr(context, "binding", None)
        if binding is None:
            return None
        local_state = RemotePairingState(self._local_relay_url(), binding)
        self.service.relay_runtime.ensure_connected(local_state)
        return local_state, context

    def _publish_contributions(
        self,
        runtime_state: RemotePairingState,
        context: object,
    ) -> None:
        # The app's one-shot startup reconciliation must run first. Otherwise a
        # persisted active intent with stale evidence could briefly be advertised
        # as ready before the existing fail-closed suspension path fences it.
        if not self.app.extensions.get(_CONTRIBUTION_RECONCILE_EXTENSION_KEY):
            return
        from .capability_contribution_service import (
            get_capability_contribution_service,
        )

        binding = getattr(context, "binding", None)
        credentials = getattr(context, "credentials", None)
        identity = getattr(credentials, "identity", None)
        session_id = getattr(binding, "internal_session_id", None)
        node_id = getattr(identity, "node_id", None)
        if not isinstance(session_id, str) or not isinstance(node_id, str):
            raise TypeError("trusted Federation context is incomplete")
        publish_local_contributions(
            contribution_service=get_capability_contribution_service(),
            runtime=self.service.relay_runtime,
            runtime_state=runtime_state,
            session_id=session_id,
            node_id=node_id,
            now=_utc_now(),
        )

    def _sync_remote_ai(
        self,
        runtime_state: RemotePairingState,
        context: object,
    ) -> None:
        if not self.app.extensions.get(_CONTRIBUTION_RECONCILE_EXTENSION_KEY):
            return
        self.ai_bridge.sync(runtime_state, context)

    def _run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                with self.app.app_context():
                    resolved = self._connected_state_and_context()
                    if resolved is None:
                        self._set_state(
                            "no-saved-membership",
                            attempts=failures,
                        )
                        return
                    runtime_state, context = resolved
                    try:
                        self._publish_contributions(runtime_state, context)
                    except Exception as exc:  # noqa: BLE001 - metadata sync is fail-closed
                        # Publication never grants authority. A failed metadata
                        # refresh must not tear down a valid membership connection;
                        # the next bounded monitor pass retries it.
                        self.app.logger.info(
                            "Federation capability metadata refresh unavailable (%s)",
                            type(exc).__name__,
                        )
                    try:
                        self._sync_remote_ai(runtime_state, context)
                    except Exception as exc:  # noqa: BLE001 - provider sync is fail-closed
                        # Provider enrollment/health/runtime composition is a
                        # separate authority path. Failure must not weaken or tear
                        # down the already valid Federation membership.
                        self.app.logger.info(
                            "Federation remote AI authority refresh unavailable (%s)",
                            type(exc).__name__,
                        )
            except Exception as exc:  # noqa: BLE001 - network retry boundary
                failures += 1
                code = str(getattr(exc, "code", type(exc).__name__))
                if code in _TERMINAL_RECONNECT_CODES:
                    self._set_state(
                        "membership-needs-pairing",
                        attempts=failures,
                        error_code=code,
                    )
                    self.app.logger.warning(
                        "Saved Federation membership requires pairing (%s)",
                        code,
                    )
                    return
                self._set_state(
                    "retrying",
                    attempts=failures,
                    error_code=code,
                )
                delay = min(
                    _MAX_RETRY_SECONDS,
                    float(2 ** min(failures - 1, 6)),
                )
                if self._stop.wait(delay):
                    return
                continue

            was_retrying = failures > 0
            failures = 0
            self._set_state("connected", attempts=0)
            if was_retrying:
                self.app.logger.info(
                    "Saved Federation membership reconnected automatically"
                )
            self._wake.wait(_CONNECTED_CHECK_SECONDS)
            self._wake.clear()
            if self._stop.is_set():
                return


def install_federation_pairing(app: Flask) -> LazyPairingOnboardingService:
    """Install config-aware onboarding and automatic saved reconnection."""

    service = LazyPairingOnboardingService()
    app.config["CAPABILITY_ONBOARDING_SERVICE"] = service
    if app.config.get("CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS") is None:
        app.config["CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS"] = (
            fresh_capability_inspection_adapters
        )
    app.extensions["federation_pairing_service"] = service
    monitor = SavedFederationReconnectMonitor(app, service)
    app.extensions[_RECONNECT_EXTENSION_KEY] = monitor
    app.extensions["federated_ai_product_bridge"] = monitor.ai_bridge
    install_recorder_federation_publication(
        app,
        onboarding_service=service,
    )

    @app.before_request
    def _start_saved_membership_reconnect() -> None:
        # This hook is registered before the retained one-shot reconnect hook.
        # The monitor owns retries, so prevent the old hook from marking a
        # transient first failure as permanently checked.
        app.extensions[_RETAINED_STARTUP_CHECK_KEY] = True
        monitor.start()

    @app.after_request
    def _wake_contribution_publication(response):
        # Local contribution intent and explicit provider decisions are persisted
        # by their authorities first. Wake the authenticated synchronization loop
        # afterwards so peers observe the resulting metadata/health promptly.
        if (
            request.method == "POST"
            and request.endpoint
            in (_CONTRIBUTION_MUTATION_ENDPOINTS | _PROVIDER_MUTATION_ENDPOINTS)
            and response.status_code < 500
        ):
            monitor.request_contribution_refresh()
        return response

    @app.context_processor
    def _provider_operator_availability() -> dict[str, bool]:
        return {
            "provider_operator_available": (
                app.config.get(_PROVIDER_SURFACE_CONFIG_KEY) is not None
            )
        }

    return service


__all__ = [
    "LazyPairingOnboardingService",
    "SavedFederationReconnectMonitor",
    "install_federation_pairing",
]
