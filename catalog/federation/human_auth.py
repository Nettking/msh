"""Coordinator-level authority rules for Federation human-auth metadata."""

from __future__ import annotations

from typing import Any

from .errors import AuthorizationError, FederationValidationError
from .models import Session

AUTHORITY_EVENT = "human_auth.authority.published"
MEMBER_ENDPOINT_EVENT = "human_auth.member_endpoint.published"
USER_EVENT = "human_auth.user.changed"
AUTHORITY_SCHEMA = "fcp.human-auth.authority.v1"
MEMBER_ENDPOINT_SCHEMA = "fcp.human-auth.member-endpoint.v1"
USER_SCHEMA = "fcp.human-auth.user.v1"

_LEADER_ONLY_EVENTS = frozenset({AUTHORITY_EVENT, USER_EVENT})
_SUPPORTED_EVENTS = frozenset(
    {AUTHORITY_EVENT, MEMBER_ENDPOINT_EVENT, USER_EVENT}
)
_SCHEMAS = {
    AUTHORITY_EVENT: AUTHORITY_SCHEMA,
    MEMBER_ENDPOINT_EVENT: MEMBER_ENDPOINT_SCHEMA,
    USER_EVENT: USER_SCHEMA,
}


def enforce_human_auth_event_authority(
    *,
    session: Session,
    actor_node_id: str,
    event_type: str,
    payload: dict[str, Any],
    authority_node_id: str | None = None,
) -> None:
    """Fail closed when human-auth control metadata exceeds node authority.

    Human credentials never enter this event family. These events contain only
    public sign-in routing metadata and non-secret user authorization state.

    ``session.created_by_node_id`` remains immutable creator provenance. When a
    durable leadership service is available, callers pass its current fenced
    leader as ``authority_node_id`` so human sign-in authority can survive a
    creator/coordinator host loss. Legacy callers omit it and preserve the
    original creator-backed rule.
    """

    if event_type not in _SUPPORTED_EVENTS:
        return
    expected_schema = _SCHEMAS[event_type]
    if not isinstance(payload, dict) or payload.get("schema") != expected_schema:
        raise FederationValidationError(
            "invalid-human-auth-event-schema",
            "payload.schema",
            f"expected {expected_schema}",
        )
    if event_type in _LEADER_ONLY_EVENTS:
        expected_authority = authority_node_id or session.created_by_node_id
        if actor_node_id != expected_authority:
            raise AuthorizationError(
                "human-auth-authority-required",
                (
                    "only the Federation creator may publish authority or user state"
                    if authority_node_id is None
                    else "only the current Federation human-auth authority may publish authority or user state"
                ),
                "actor_node_id",
            )
        return
    advertised_node_id = payload.get("node_id")
    if advertised_node_id != actor_node_id:
        raise AuthorizationError(
            "human-auth-node-mismatch",
            "a Federation member may advertise a human-auth endpoint only for itself",
            "node_id",
        )


__all__ = [
    "AUTHORITY_EVENT",
    "AUTHORITY_SCHEMA",
    "MEMBER_ENDPOINT_EVENT",
    "MEMBER_ENDPOINT_SCHEMA",
    "USER_EVENT",
    "USER_SCHEMA",
    "enforce_human_auth_event_authority",
]
