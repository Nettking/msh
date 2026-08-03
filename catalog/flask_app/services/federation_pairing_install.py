"""Application-factory installation for authenticated Federation pairing."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from flask import Flask, current_app

from .federation_pairing_service import (
    PairingAwareCapabilityOnboardingService,
    PairingRelayRuntime,
    RemotePairingStore,
)
from .server_setup_service import load_settings


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
    runtime = PairingRelayRuntime(
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


def install_federation_pairing(app: Flask) -> LazyPairingOnboardingService:
    """Install a config-aware lazy service before any CFI getter is resolved."""

    service = LazyPairingOnboardingService()
    app.config["CAPABILITY_ONBOARDING_SERVICE"] = service
    app.extensions["federation_pairing_service"] = service
    return service


__all__ = [
    "LazyPairingOnboardingService",
    "install_federation_pairing",
]
