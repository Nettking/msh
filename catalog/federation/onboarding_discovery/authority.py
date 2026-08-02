"""Narrow capability-first adapter over existing federation authorities."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthenticationError, FederationOperationError
from catalog.federation.models import NodeIdentity, Session
from catalog.federation.onboarding_compat import (
    federation_id_from_session_id,
    federation_id_matches_session,
)
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class VerifiedJoinMaterial:
    """Private authority material retained behind a safe discovery handle."""

    internal_session_id: str
    enrollment_token: str | None = field(default=None, repr=False, compare=False)
    invitation_token: str | None = field(default=None, repr=False, compare=False)


class SessionOnboardingAuthority:
    """Compose enrollment, invitation, and session authority without replacing it."""

    def __init__(
        self,
        coordinator: SessionCoordinator,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.coordinator = coordinator
        self._clock = clock

    def _registered_identity(self, identity: NodeIdentity) -> NodeIdentity | None:
        if self.coordinator.store.get_node(identity.node_id) is None:
            return None
        registered = self.coordinator.public_identity(identity.node_id)
        if registered.public_key != identity.public_key:
            raise AuthenticationError(
                "onboarding-identity-mismatch",
                "the enrolled node identity does not match this device",
                "device_id",
            )
        return registered

    def _ensure_enrolled(
        self,
        identity: NodeIdentity,
        *,
        enrollment_token: str | None,
    ) -> None:
        if self._registered_identity(identity) is not None:
            return
        if enrollment_token is None:
            raise AuthenticationError(
                "onboarding-enrollment-required",
                "the selected federation requires authenticated enrollment",
                "device_id",
            )
        self.coordinator.enroll_node(identity, token=enrollment_token)

    def _binding(self, session: Session, identity: NodeIdentity) -> FederationSessionBinding:
        return FederationSessionBinding(
            federation_id=federation_id_from_session_id(session.session_id),
            internal_session_id=session.session_id,
            device_id=identity.node_id,
            state=FederationConnectionState.CONNECTED,
            revision=session.revision,
            trusted=True,
            created_at=session.created_at,
            last_verified_at=self._clock(),
        )

    def verified_join(
        self,
        identity: NodeIdentity,
        material: VerifiedJoinMaterial,
        *,
        request_id: str,
        expected_federation_id: str,
    ) -> FederationSessionBinding:
        """Join through existing token authorities or reuse current membership."""

        if not federation_id_matches_session(
            expected_federation_id,
            material.internal_session_id,
        ):
            raise FederationOperationError(
                "onboarding-federation-binding-mismatch",
                "the selected federation no longer matches its session authority",
                "federation_id",
            )
        self._ensure_enrolled(
            identity,
            enrollment_token=material.enrollment_token,
        )
        if material.internal_session_id in self.coordinator.session_ids_for_node(
            identity.node_id
        ):
            session = self.coordinator.store.get_session(material.internal_session_id)
            if session is None:
                raise FederationOperationError(
                    "onboarding-session-unavailable",
                    "the trusted federation session is unavailable",
                    "internal_session_id",
                )
            return self._binding(session, identity)
        if material.invitation_token is None:
            raise FederationOperationError(
                "onboarding-invitation-required",
                "the selected federation requires a current invitation",
                "discovery_id",
            )
        session = self.coordinator.join_session(
            node_id=identity.node_id,
            token=material.invitation_token,
            request_id=request_id,
            expected_session_id=material.internal_session_id,
        )
        return self._binding(session, identity)

    def reconnect(
        self,
        identity: NodeIdentity,
        binding: FederationSessionBinding,
    ) -> FederationSessionBinding:
        """Revalidate existing membership without minting enrollment or invitations."""

        if binding.device_id != identity.node_id or not binding.trusted:
            raise AuthenticationError(
                "onboarding-untrusted-binding",
                "automatic reconnect requires this device's trusted binding",
                "device_id",
            )
        if not federation_id_matches_session(
            binding.federation_id,
            binding.internal_session_id,
        ):
            raise FederationOperationError(
                "onboarding-federation-binding-mismatch",
                "the saved federation binding is inconsistent",
                "federation_id",
            )
        self._registered_identity(identity)
        self.coordinator.store.require_membership(
            session_id=binding.internal_session_id,
            node_id=identity.node_id,
        )
        session = self.coordinator.store.get_session(binding.internal_session_id)
        if session is None:
            raise FederationOperationError(
                "onboarding-session-unavailable",
                "the trusted federation session is unavailable",
                "internal_session_id",
            )
        return self._binding(session, identity)

    def create_local(
        self,
        identity: NodeIdentity,
        *,
        display_name: str,
        request_id: str,
        session_id: str | None = None,
    ) -> FederationSessionBinding:
        """Create a local federation through the existing enrollment/session authority."""

        if self._registered_identity(identity) is None:
            token = self.coordinator.create_enrollment_token(
                ttl_seconds=60,
                max_uses=1,
            )["token"]
            self.coordinator.enroll_node(identity, token=token)
        session = self.coordinator.create_session(
            actor_node_id=identity.node_id,
            display_name=display_name,
            request_id=request_id,
            session_id=session_id,
        )
        return self._binding(session, identity)
