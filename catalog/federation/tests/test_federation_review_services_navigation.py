from types import SimpleNamespace

from catalog.federation.projections.service_core import FederationProjectionCore


def test_review_services_action_opens_contribution_review() -> None:
    inactive_contribution = SimpleNamespace(
        policy_state="allowed",
        activation_state="inactive",
    )
    snapshot = SimpleNamespace(
        onboarding=SimpleNamespace(
            available=True,
            federation_id="federation-local",
            connection_state="connected",
            trusted=True,
            contributions=(inactive_contribution,),
        ),
        benchmarks=SimpleNamespace(results=()),
        providers=SimpleNamespace(available=True, providers=()),
    )

    action = FederationProjectionCore._next_action(snapshot)

    assert action is not None
    assert action.label == "Review services"
    assert action.url == "/onboarding?step=contributions"
