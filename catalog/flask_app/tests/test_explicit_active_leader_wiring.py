from catalog.flask_app import federation_routes
from catalog.flask_app.services import federation_active_leader_runtime as active
from catalog.flask_app.services import federation_pairing_install as pairing_install
from catalog.flask_app.services.federation_active_leader_provider_runtime import (
    ActiveLeaderProviderEnrollmentService,
    ActiveLeaderProviderOperatorSurface,
)
from catalog.flask_app.services.pending_contribution_approval import (
    CapabilityFirstProviderEnrollmentService,
    CapabilityFirstProviderOperatorSurface,
)
from catalog.mtconnect_recorder import managed_service


def test_web_routes_use_explicit_active_leader_service_factories() -> None:
    assert (
        federation_routes.get_federation_update_service
        is active.get_active_update_service
    )
    assert (
        federation_routes.get_federation_capability_request_service
        is active.get_active_capability_request_service
    )


def test_background_processors_use_explicit_active_leader_classes() -> None:
    assert (
        pairing_install.FederationUpdateEventProcessor
        is active.ActiveLeaderFederationUpdateEventProcessor
    )
    assert (
        pairing_install.FederationCapabilityRequestProcessor
        is active.ActiveLeaderFederationCapabilityRequestProcessor
    )
    assert (
        managed_service.FederationUpdateEventProcessor
        is active.ActiveLeaderFederationUpdateEventProcessor
    )


def test_capability_first_provider_surface_uses_explicit_active_leader_adapters() -> (
    None
):
    assert issubclass(
        CapabilityFirstProviderEnrollmentService,
        ActiveLeaderProviderEnrollmentService,
    )
    assert issubclass(
        CapabilityFirstProviderOperatorSurface,
        ActiveLeaderProviderOperatorSurface,
    )
