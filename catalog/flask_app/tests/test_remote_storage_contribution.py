from __future__ import annotations

from types import SimpleNamespace

from flask import Flask

from catalog.federation.onboarding_models import ContributionActivationState
from catalog.flask_app.services.local_capability_candidates import (
    local_contribution_components,
)


def _candidate() -> SimpleNamespace:
    return SimpleNamespace(
        capability_type="storage",
        capacity_envelope={
            "authority": "candidate-only",
            "provider_id": "fcp-local-data-storage",
        },
    )


def test_remote_session_without_creator_metadata_saves_storage_as_pending(
    tmp_path,
) -> None:
    device_id = "node-joined-device"
    session_id = "session-remote"

    class ReadOnlyRemoteStore:
        def require_membership(self, *, session_id: str, node_id: str) -> None:
            assert session_id == "session-remote"
            assert node_id == "node-joined-device"

        def get_session(self, requested_session_id: str) -> object:
            assert requested_session_id == session_id
            # The authenticated relay projection may intentionally omit creator
            # authority metadata. Absence must never be interpreted as authority.
            return SimpleNamespace(session_id=session_id)

    context = SimpleNamespace(
        credentials=SimpleNamespace(
            identity=SimpleNamespace(node_id=device_id),
        ),
        binding=SimpleNamespace(internal_session_id=session_id),
        coordinator=SimpleNamespace(store=ReadOnlyRemoteStore()),
    )
    onboarding = SimpleNamespace(authorized_context=lambda: context)

    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_STORAGE_PROBE_DIRECTORY"] = (
        tmp_path / "storage-probe"
    )
    with app.app_context():
        _sources, adapters = local_contribution_components(onboarding)
        storage = adapters[1]
        enabled = storage.enable(_candidate())
        disabled = storage.disable(_candidate())

    assert enabled.activation_state is ContributionActivationState.PENDING
    assert enabled.authority_confirmed is False
    assert "creator" in str(enabled.reason).lower()
    assert disabled.activation_state is ContributionActivationState.INACTIVE
