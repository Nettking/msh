"""Authenticated C03 provider operator proxy without local authority stores."""

from __future__ import annotations

import os
import uuid

from flask import Flask

from catalog.capabilities.operator_surface import (
    ProviderOperatorAction,
    ProviderOperatorSurface,
)
from catalog.capabilities.operator_wire import (
    operator_snapshot_from_dict,
    operator_view_from_dict,
)
from catalog.federation.errors import AuthorizationError, FederationOperationError
from catalog.node.client import RelayRemoteError

from .c03_pairing_onboarding import (
    C03PairingOnboardingService,
    C03RelayCoordinatorFacade,
)


class C03ProviderOperatorSurface(ProviderOperatorSurface):
    """Only public operator results cross the paired authenticated connection.

The local base surface owns stores; this proxy intentionally has none and
overrides its entire public interface instead of constructing a second writer.
"""

    def __init__(self, coordinator: C03RelayCoordinatorFacade):
        self.coordinator = coordinator
        self.session_id = coordinator.state.binding.internal_session_id
        self.actor_node_id = coordinator.credentials.identity.node_id

    async def _remote(self, message_type, payload, command_id=None):
        coordinator = self.coordinator
        coordinator._require_actor(self.session_id, self.actor_node_id)
        client = await coordinator.runtime._bound_client(coordinator.state, self.session_id)
        try:
            return await client.request(
                message_type, session_id=self.session_id, payload=payload,
                request_id=command_id,
            )
        except RelayRemoteError as exc:
            # Preserve the route's authorization status without trusting error
            # text or treating arbitrary remote failures as successful actions.
            if exc.code in {
                "provider-operator-action-not-authorized", "provider-enrollment-not-authorized",
                "federation-leader-required", "federation-leader-not-local",
                "federation-quorum-leader-required", "not-session-member", "node-revoked",
            }:
                raise AuthorizationError(exc.code, "provider operation is not authorized", exc.field) from exc
            raise

    def _request(self, message_type, payload, *, response_key, command_id=None):
        result = self.coordinator.runtime._submit(
            self._remote(message_type, payload, command_id)
        )
        if not isinstance(result, dict) or set(result) != {response_key}:
            raise FederationOperationError(
                "invalid-provider-operator-response", "relay returned an invalid operator result",
            )
        return result[response_key]

    def view(self):
        return operator_view_from_dict(
            self._request("provider.operator.view", {}, response_key="view"),
            session_id=self.session_id, actor_node_id=self.actor_node_id,
        )

    def now(self):
        return self.view().generated_at

    def get(self, capability_id):
        for provider in self.view().providers:
            if provider.capability_id == capability_id:
                return provider
        raise FederationOperationError(
            "unknown-operator-provider", "provider is not visible in the bound session", "capability_id",
        )

    def execute(
        self, action, *, capability_id, expected_revision=None, reason_code=None, command_id=None,
    ):
        action = ProviderOperatorAction(action)
        command_id = f"operator-{action.value}-{uuid.uuid4().hex}" if command_id is None else command_id
        result = self._request(
            "provider.operator.execute",
            {"action": action.value, "capability_id": capability_id,
             "expected_revision": expected_revision, "reason_code": reason_code},
            response_key="provider", command_id=command_id,
        )
        return operator_snapshot_from_dict(
            result, session_id=self.session_id, capability_id=capability_id,
        )


def uses_c03_provider_authority(app: Flask) -> bool:
    return bool(
        app.config.get("FCP_REPLICATED_CONTROL_PLANE_CONFIG")
        or os.environ.get("FCP_REPLICATED_CONTROL_PLANE_CONFIG")
        or isinstance(app.config.get("CAPABILITY_ONBOARDING_SERVICE"), C03PairingOnboardingService)
    )


def c03_provider_operator_surface(app: Flask) -> C03ProviderOperatorSurface | None:
    """Fail closed on unavailable/mixed C03 bindings; never fall back to SQL."""
    onboarding = app.config.get("CAPABILITY_ONBOARDING_SERVICE")
    if not isinstance(onboarding, C03PairingOnboardingService):
        return None
    try:
        context = onboarding.authorized_context()
    except Exception:  # noqa: BLE001 - availability boundary must never select a local writer
        return None
    if context is None or not isinstance(context.coordinator, C03RelayCoordinatorFacade):
        return None
    coordinator = context.coordinator
    configured = app.config.get("PROVIDER_OPERATOR_SURFACE")
    if configured is not None and (
        not isinstance(configured, C03ProviderOperatorSurface)
        or configured.session_id != context.binding.internal_session_id
        or configured.actor_node_id != context.credentials.identity.node_id
        or configured.coordinator.runtime is not coordinator.runtime
        or configured.coordinator.database != coordinator.database
        or configured.coordinator.state.relay_url != coordinator.state.relay_url
    ):
        return None
    # Do not cache a render-time authority decision. Every operation returns to
    # the live relay and is rechecked under its owned runtime lock.
    return C03ProviderOperatorSurface(coordinator)
