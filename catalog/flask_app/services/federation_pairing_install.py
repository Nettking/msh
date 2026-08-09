"""Application-factory installation for authenticated Federation pairing."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from flask import Flask, current_app

from .capability_recovery_adapters import fresh_capability_inspection_adapters
from .federation_pairing_service import (
    PairingAwareCapabilityOnboardingService,
    RemotePairingStore,
)
from .federation_update_events import FederationUpdateEventProcessor
from .federation_update_handoff import HostUpdateHandoff
from .resilient_pairing_runtime import ResilientPairingRelayRuntime
from .server_setup_service import load_settings

_RECONNECT_EXTENSION_KEY = "federation_saved_membership_reconnect"
_RETAINED_STARTUP_CHECK_KEY = "capability_onboarding_startup_checked"
_CONNECTED_CHECK_SECONDS = 2.0
_MAX_RETRY_SECONDS = 60.0
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
    """Keep one saved remote Federation membership connected automatically."""

    def __init__(
        self,
        app: Flask,
        service: LazyPairingOnboardingService,
    ) -> None:
        self.app = app
        self.service = service
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._update_processor: FederationUpdateEventProcessor | None = None
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

    def _processor(self) -> FederationUpdateEventProcessor:
        processor = self._update_processor
        if processor is not None:
            return processor
        onboarding_database = Path(
            self.app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"]
        )
        federation_root = onboarding_database.parent.parent
        handoff_directory = Path(
            self.app.config.get(
                "FEDERATION_UPDATE_HANDOFF_DIR",
                federation_root / "update-agent",
            )
        )
        processor_state = Path(
            self.app.config.get(
                "FEDERATION_UPDATE_PROCESSOR_STATE",
                federation_root / "update-events" / "processor.json",
            )
        )
        processor = FederationUpdateEventProcessor(
            self.service,
            HostUpdateHandoff(handoff_directory),
            processor_state,
        )
        self._update_processor = processor
        return processor

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

    def _run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                with self.app.app_context():
                    remote = self.service.remote_store.load()
                    if remote is None:
                        self._set_state(
                            "no-saved-remote-membership",
                            attempts=failures,
                        )
                        return
                    context = self.service.authorized_context()
                    if context is None:
                        raise RuntimeError(
                            "saved remote Federation context is unavailable"
                        )
                    try:
                        self._processor().process(context)
                    except Exception as exc:  # noqa: BLE001 - update path fails closed independently
                        self.app.logger.warning(
                            "Federation update event processing unavailable (%s)",
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
            if self._stop.wait(_CONNECTED_CHECK_SECONDS):
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

    @app.before_request
    def _start_saved_membership_reconnect() -> None:
        # This hook is registered before the retained one-shot reconnect hook.
        # The monitor owns retries, so prevent the old hook from marking a
        # transient first failure as permanently checked.
        app.extensions[_RETAINED_STARTUP_CHECK_KEY] = True
        monitor.start()

    return service


__all__ = [
    "LazyPairingOnboardingService",
    "SavedFederationReconnectMonitor",
    "install_federation_pairing",
]
