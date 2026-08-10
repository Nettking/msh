from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from catalog.flask_app.services.federation_contribution_publication import (
    plan_contribution_publications,
)

NOW = datetime(2026, 8, 8, 21, 0, tzinfo=timezone.utc)


def _candidate(node_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        candidate_id="ollama-configured",
        device_id=node_id,
        capability_type="language-model",
        capability_protocol="fcp-language-model",
        display_label="AI Explainer — This computer",
        capacity_envelope={"protocol_version": "1.0"},
    )


def _intent(node_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        candidate_id="ollama-configured",
        device_id=node_id,
        activation_state="active",
        decision_revision=1,
        decided_at=NOW,
    )


def test_same_local_candidate_id_is_device_scoped_after_remote_collision() -> None:
    remote = {
        "session_id": "session-one",
        "capability_id": "ollama-configured",
        "node_id": "node-one",
        "type": "language-model",
        "protocol": "fcp-language-model",
        "protocol_version": "1.0",
        "status": "ready",
        "properties": {
            "kind": "capability-first-candidate",
            "candidate_id": "ollama-configured",
            "label": "AI Explainer — This computer",
        },
    }

    publications = plan_contribution_publications(
        session_id="session-one",
        node_id="node-two",
        candidates=(_candidate("node-two"),),
        intents=(_intent("node-two"),),
        authorized_status={"capabilities": [remote]},
        now=NOW,
    )

    assert len(publications) == 1
    announcement = publications[0].announcement
    assert announcement.node_id == "node-two"
    assert announcement.capability_id != "ollama-configured"
    assert announcement.capability_id.startswith("member-capability-")
    assert announcement.properties["candidate_id"] == "ollama-configured"

    converged_status = {"capabilities": [remote, announcement.to_dict()]}
    assert plan_contribution_publications(
        session_id="session-one",
        node_id="node-two",
        candidates=(_candidate("node-two"),),
        intents=(_intent("node-two"),),
        authorized_status=converged_status,
        now=NOW,
    ) == ()
