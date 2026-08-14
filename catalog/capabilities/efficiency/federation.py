"""Federation-wide sharing of execution observations.

Knowledge has to leave the machine that produced it, or every node relearns
the same thing. The transport already exists: the authoritative session event
log. A member appends one ``capability.execution.observed`` event about its own
execution; the current operational leader applies accepted events into the
shared learning store and is the only writer of derived scheduling state.

Two boundaries are enforced here and nowhere else:

*authority*
    a member may report only executions it performed itself. This mirrors the
    existing human-auth event rule: the payload's ``node_id`` must equal the
    authenticated actor. Nothing in this module can grant, widen or bypass
    federation authorization; a rejected event simply is not learned from.

*plausibility*
    telemetry is bounded, deduplicated by ``(session, job, attempt)``, checked
    against the session it claims, and rejected when it is stale or from the
    future. A member can therefore slow its own selection down by reporting
    honestly bad numbers, but cannot fabricate authority over another node or
    replay one lucky execution into a reputation.

This is not federated machine learning. No model is exchanged and no user data
leaves a node - only bounded scalar facts about how long the federation's own
work took.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from catalog.federation.errors import (
    AuthorizationError,
    FederationValidationError,
)

from .contracts import (
    EXECUTION_OBSERVATION_SCHEMA,
    MAX_OBSERVATION_BYTES,
    ExecutionObservation,
    canonical_json,
)
from .policy import EfficiencyPolicy
from .store import SQLiteExecutionLearningStore

EXECUTION_OBSERVATION_EVENT = "capability.execution.observed"

#: How far ahead of the coordinator's clock a report may be stamped.
MAX_CLOCK_SKEW_SECONDS = 300


def observation_event_payload(observation: ExecutionObservation) -> dict[str, Any]:
    """Render one observation as an authoritative session-event payload.

    ``node_id`` is lifted to the top level because that is the field the
    authority check compares against the authenticated actor.
    """

    if not isinstance(observation, ExecutionObservation):
        raise FederationValidationError(
            "invalid-observation", "observation", "must be an ExecutionObservation"
        )
    payload = {
        "schema": EXECUTION_OBSERVATION_SCHEMA,
        "node_id": observation.node_id,
        "session_id": observation.session_id,
        "observation": observation.to_dict(),
    }
    encoded = canonical_json(payload, "payload")
    if len(encoded.encode("utf-8")) > MAX_OBSERVATION_BYTES:
        raise FederationValidationError(
            "message-too-large", "payload", f"exceeds {MAX_OBSERVATION_BYTES} bytes"
        )
    return payload


def enforce_observation_authority(
    *, actor_node_id: str, payload: Any
) -> ExecutionObservation:
    """Validate one reported observation and bind it to its reporter.

    Raises :class:`AuthorizationError` when a member reports an execution it
    did not perform, and :class:`FederationValidationError` when the payload
    is malformed.
    """

    if not isinstance(payload, dict):
        raise FederationValidationError(
            "invalid-object", "payload", "must be an object"
        )
    if payload.get("schema") != EXECUTION_OBSERVATION_SCHEMA:
        raise FederationValidationError(
            "invalid-observation-event-schema",
            "payload.schema",
            f"expected {EXECUTION_OBSERVATION_SCHEMA}",
        )
    observation = ExecutionObservation.from_dict(payload.get("observation"))
    if payload.get("node_id") != actor_node_id:
        raise AuthorizationError(
            "execution-observation-node-mismatch",
            "a member may report only its own execution observations",
            "node_id",
        )
    if observation.node_id != actor_node_id:
        raise AuthorizationError(
            "execution-observation-node-mismatch",
            "a member may report only its own execution observations",
            "observation.node_id",
        )
    return observation


@dataclass(frozen=True)
class ObservationIngestOutcome:
    """Why one reported observation was or was not learned from."""

    accepted: bool
    reason: str
    observation: ExecutionObservation | None = None


class FederationObservationIngest:
    """Coordinator-side application of member-reported observations."""

    def __init__(
        self,
        store: SQLiteExecutionLearningStore,
        *,
        policy: EfficiencyPolicy | None = None,
        max_clock_skew_seconds: int = MAX_CLOCK_SKEW_SECONDS,
    ) -> None:
        self.store = store
        self.policy = policy or store.policy
        self.max_clock_skew_seconds = int(max_clock_skew_seconds)

    def apply(
        self,
        *,
        actor_node_id: str,
        session_id: str,
        payload: Any,
        now: datetime,
    ) -> ObservationIngestOutcome:
        """Validate, bound-check and record one reported observation."""

        observation = enforce_observation_authority(
            actor_node_id=actor_node_id, payload=payload
        )
        if observation.session_id != session_id:
            raise AuthorizationError(
                "execution-observation-session-mismatch",
                "an observation must belong to the reporting session",
                "session_id",
            )
        skew = timedelta(seconds=self.max_clock_skew_seconds)
        if observation.observed_at > now + skew:
            return ObservationIngestOutcome(
                False, "observation-from-future", observation
            )
        age = (now - observation.observed_at).total_seconds()
        if age > self.policy.max_observation_age_seconds:
            return ObservationIngestOutcome(False, "observation-too-old", observation)
        recorded = self.store.record(observation, now=now)
        return ObservationIngestOutcome(
            recorded,
            "recorded" if recorded else "duplicate-observation",
            observation,
        )


__all__ = [
    "EXECUTION_OBSERVATION_EVENT",
    "MAX_CLOCK_SKEW_SECONDS",
    "FederationObservationIngest",
    "ObservationIngestOutcome",
    "enforce_observation_authority",
    "observation_event_payload",
]
