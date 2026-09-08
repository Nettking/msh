"""Configured C03 Flask contexts use the existing authenticated relay owner.

This module owns no replica, voter socket, or writable coordinator store. Initial
C03 bootstrap supplies the trusted binding separately. An unavailable explicitly
bound relay never falls back to an independent local authority.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
)
from catalog.federation.models import CapabilityAnnouncement, Session, SessionEvent
from catalog.federation.onboarding_compat import federation_id_matches_session
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationDiscoveryResult,
    FederationSessionBinding,
)
from catalog.federation.session_leadership import SessionLeadership
from catalog.node.identity import NodeCredentials

from .capability_onboarding_service import AuthorizedOnboardingContext
from .federation_pairing_service import (
    PairingAwareCapabilityOnboardingService,
    RemoteCoordinatorFacade,
    RemotePairingState,
    _validate_relay_url,
)
from .resilient_pairing_runtime import ResilientPairingRelayRuntime


def _bootstrap_required() -> FederationOperationError:
    return FederationOperationError(
        "c03-bootstrap-binding-required",
        "configured replicated Federation requires its trusted bootstrap binding",
        "binding",
    )


class C03RelayCoordinatorFacade(RemoteCoordinatorFacade):
    """Coordinator-shaped member operations with no local journal writer."""

    def __init__(
        self,
        runtime: ResilientPairingRelayRuntime,
        state: RemotePairingState,
        *,
        credentials: NodeCredentials,
        database: Path | str,
    ) -> None:
        super().__init__(runtime, state)
        self.runtime = runtime
        self.credentials = credentials
        # Existing local storage-control readers need their separate DB path.
        # `store` remains this facade; it exposes no SQL or journal write method.
        self.database = Path(database)
        binding = state.binding
        if (
            not binding.trusted
            or binding.device_id != credentials.identity.node_id
            or not federation_id_matches_session(
                binding.federation_id, binding.internal_session_id
            )
        ):
            raise AuthenticationError(
                "pairing-membership-mismatch",
                "the trusted binding must match this device and Federation",
                "binding",
            )

    def _require_actor(self, session_id: str, actor_node_id: str) -> None:
        if (
            session_id != self.state.binding.internal_session_id
            or actor_node_id != self.credentials.identity.node_id
        ):
            raise AuthenticationError(
                "pairing-actor-mismatch",
                "the request must use the authenticated bound device and session",
                "actor_node_id",
            )

    def _status(self) -> dict[str, Any]:
        status = super()._status()
        sessions = status.get("sessions")
        if not isinstance(sessions, list) or not any(
            isinstance(value, dict)
            and value.get("session_id") == self.state.binding.internal_session_id
            for value in sessions
        ):
            raise AuthenticationError(
                "pairing-membership-missing",
                "the saved Federation membership is no longer active",
                "binding",
            )
        return status

    def require_membership(self, *, session_id: str, node_id: str) -> None:
        self._require_actor(session_id, node_id)
        self._status()

    def append_event(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        request_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> tuple[SessionEvent, bool]:
        self._require_actor(session_id, actor_node_id)
        return self.runtime.append_event_result(
            self.state,
            session_id=session_id,
            event_type=event_type,
            payload=payload,
            request_id=request_id,
        )

    def announce_capability(
        self,
        capability: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        request_id: str,
    ) -> tuple[CapabilityAnnouncement, bool]:
        self._require_actor(capability.session_id, actor_node_id)
        return self.runtime.announce_capability_result(
            self.state, capability, request_id=request_id
        )

    def create_pairing_material(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        ttl_seconds: int = 600,
        request_id: str,
    ) -> dict[str, Any]:
        self._require_actor(session_id, actor_node_id)
        # The authenticated relay rechecks leadership and quorum before issuing.
        return self.runtime.create_pairing_material(
            self.state,
            session_id=session_id,
            ttl_seconds=ttl_seconds,
            request_id=request_id,
        )

    def authenticated_session_authority(
        self, *, session_id: str, actor_node_id: str
    ) -> tuple[Session, SessionLeadership]:
        self._require_actor(session_id, actor_node_id)
        return self.runtime.session_authority(
            self.state, session_id=session_id
        )

    def get_session(self, session_id: str) -> Session:
        session, _ = self.authenticated_session_authority(
            session_id=session_id, actor_node_id=self.credentials.identity.node_id
        )
        return session

    def session_leadership(self, session_id: str) -> SessionLeadership:
        _, leadership = self.authenticated_session_authority(
            session_id=session_id, actor_node_id=self.credentials.identity.node_id
        )
        return leadership

    def require_session_leader(
        self, *, session_id: str, actor_node_id: str
    ) -> SessionLeadership:
        self._require_actor(session_id, actor_node_id)
        leadership = self.session_leadership(session_id)
        if leadership.leader_node_id != actor_node_id:
            raise AuthorizationError(
                "federation-leader-required",
                "operation requires the current Federation leader",
                "actor_node_id",
            )
        return leadership


class C03PairingOnboardingService(PairingAwareCapabilityOnboardingService):
    """Use the paired relay for every configured C03 Flask context."""

    def __init__(self, *, local_relay_url: Callable[[], str], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._local_relay_url = local_relay_url

    def _bound_state(self) -> tuple[NodeCredentials, RemotePairingState]:
        credentials = self.identity_or_none()
        remote = self.remote_store.load()
        binding = remote.binding if remote is not None else self.binding_store.load()
        if credentials is None or binding is None:
            raise _bootstrap_required()
        if remote is not None:
            state = remote
        else:
            relay_url = self._local_relay_url()
            if not isinstance(relay_url, str) or not relay_url.strip():
                raise FederationOperationError(
                    "c03-relay-binding-required",
                    "configured replicated Federation requires an explicit relay binding",
                    "binding",
                )
            state = RemotePairingState(_validate_relay_url(relay_url), binding)
        return credentials, state

    def _facade(
        self, credentials: NodeCredentials, state: RemotePairingState
    ) -> C03RelayCoordinatorFacade:
        return C03RelayCoordinatorFacade(
            self.relay_runtime,
            state,
            credentials=credentials,
            database=self._coordinator_database,
        )

    @property
    def coordinator(self) -> C03RelayCoordinatorFacade:
        credentials, state = self._bound_state()
        return self._facade(credentials, state)

    @property
    def authority(self):
        # Initial enrollment/session creation belongs to the single C03 bootstrap
        # owner, not the legacy local onboarding authority in another process.
        raise _bootstrap_required()

    def authorized_context(self) -> AuthorizedOnboardingContext | None:
        if self.identity_or_none() is None:
            return None
        credentials, state = self._bound_state()
        coordinator = self._facade(credentials, state)
        status = coordinator.status(actor_node_id=credentials.identity.node_id)
        session = next(
            value
            for value in status["sessions"]
            if isinstance(value, dict)
            and value.get("session_id") == state.binding.internal_session_id
        )
        revision = session.get("revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise FederationOperationError(
                "invalid-coordinator-status",
                "the authenticated session revision is unavailable",
            )
        binding = replace(
            state.binding,
            state=FederationConnectionState.CONNECTED,
            revision=revision,
            last_verified_at=self._clock(),
        )
        return AuthorizedOnboardingContext(
            credentials=credentials,
            binding=binding,
            coordinator=coordinator,  # type: ignore[arg-type]
        )

    def retained_context_for_read_only_projection(
        self,
    ) -> AuthorizedOnboardingContext | None:
        if self.identity_or_none() is None or self.binding_or_none() is None:
            return None
        credentials, state = self._bound_state()
        return AuthorizedOnboardingContext(
            credentials=credentials,
            binding=state.binding,
            coordinator=self._facade(credentials, state),  # type: ignore[arg-type]
        )

    def reconnect(self) -> FederationSessionBinding:
        context = self.authorized_context()
        if context is None:
            raise _bootstrap_required()
        remote = self.remote_store.load()
        if remote is not None:
            self.remote_store.save(RemotePairingState(remote.relay_url, context.binding))
        else:
            self.binding_store.save(context.binding)
        return context.binding

    def discover(self) -> tuple[FederationDiscoveryResult, ...]:
        # Existing C03 bindings reconnect; new devices redeem signed pairing codes.
        # Do not enter the legacy discovery path that can create local authority.
        self._last_results = ()
        return ()

    def connect(
        self,
        *,
        request_id: str,
        discovery_id: str | None = None,
        verification_code: str | None = None,
    ) -> FederationSessionBinding:
        if discovery_id is not None or verification_code is not None:
            raise FederationOperationError(
                "c03-signed-pairing-required",
                "join the configured replicated Federation with its signed pairing code",
                "binding",
            )
        return self.reconnect()

    def _host_pairing_material(
        self, *, ttl_seconds: int
    ) -> tuple[AuthorizedOnboardingContext, dict[str, Any], dict[str, Any]]:
        context = self.authorized_context()
        if context is None:
            raise _bootstrap_required()
        material = context.coordinator.create_pairing_material(
            session_id=context.binding.internal_session_id,
            actor_node_id=context.credentials.identity.node_id,
            ttl_seconds=ttl_seconds,
            request_id=f"pairing-invite-{os.urandom(12).hex()}",
        )
        return context, material["enrollment"], material["invitation"]


__all__ = ["C03PairingOnboardingService", "C03RelayCoordinatorFacade"]
