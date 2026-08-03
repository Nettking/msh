"""Application-factory installation for authenticated Federation pairing."""

from __future__ import annotations

from pathlib import Path

from flask import Flask

from .federation_pairing_service import (
    PairingAwareCapabilityOnboardingService,
    PairingRelayRuntime,
    RemotePairingStore,
)
from .server_setup_service import load_settings


def install_federation_pairing(app: Flask) -> PairingAwareCapabilityOnboardingService:
    """Install one pairing-aware service before any CFI service is resolved."""

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
    service = PairingAwareCapabilityOnboardingService(
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
        setup_loader=load_settings,
        remote_store=RemotePairingStore(remote_path),
        relay_runtime=runtime,
    )
    app.config["CAPABILITY_ONBOARDING_SERVICE"] = service
    app.extensions["federation_pairing_service"] = service
    return service


__all__ = ["install_federation_pairing"]
