"""Capability-first federation discovery and verified onboarding."""

from .authority import SessionOnboardingAuthority, VerifiedJoinMaterial
from .service import FederationOnboardingDiscoveryService
from .sources import (
    ConfiguredFederationDiscoveryAdapter,
    FederationDiscoveryCandidate,
    FederationDiscoverySource,
    RelayFederationDiscoveryAdapter,
)

__all__ = [
    "ConfiguredFederationDiscoveryAdapter",
    "FederationDiscoveryCandidate",
    "FederationDiscoverySource",
    "FederationOnboardingDiscoveryService",
    "RelayFederationDiscoveryAdapter",
    "SessionOnboardingAuthority",
    "VerifiedJoinMaterial",
]
