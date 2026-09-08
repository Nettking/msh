"""Human-auth authority resolution for replicated Federation leader failover.

The base SSO service retains legacy creator-backed semantics for standalone and
non-replicated coordinators.  This subclass follows the durable current
operational leader whenever the connected coordinator exposes
``session_leadership``.  Immutable creator provenance remains in the session
row and is never rewritten.
"""

from __future__ import annotations

from catalog.federation.errors import FederationOperationError

from .federation import FederationHumanAuthService


class CurrentLeaderFederationHumanAuthService(FederationHumanAuthService):
    """Resolve credential/sign-in authority from durable Federation leadership."""

    def _leader_node_id(self, context: object) -> str | None:
        _federation_id, session_id, _node_id, _credentials, coordinator = (
            self._context_parts(context)
        )
        resolver = getattr(coordinator, "session_leadership", None)
        if callable(resolver):
            try:
                leadership = resolver(session_id)
            except FederationOperationError:
                raise
            except Exception as exc:  # noqa: BLE001 - authority resolution must fail closed
                raise FederationOperationError(
                    "human-auth-leadership-unavailable",
                    "the current Federation human-auth authority cannot be resolved",
                    "session_id",
                ) from exc
            value = getattr(leadership, "leader_node_id", None)
            if isinstance(value, str) and value:
                return value
            raise FederationOperationError(
                "human-auth-leadership-unavailable",
                "the current Federation human-auth authority is missing",
                "session_id",
            )
        return super()._leader_node_id(context)


__all__ = ["CurrentLeaderFederationHumanAuthService"]
