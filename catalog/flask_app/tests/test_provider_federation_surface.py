from __future__ import annotations

from datetime import datetime, timedelta, timezone

from flask import Flask, url_for

from catalog.capabilities.operator_surface import (
    ProviderActivationState,
    ProviderOperatorAction,
    ProviderOperatorSnapshot,
    ProviderOperatorSurface,
    ProviderOperatorView,
)
from catalog.flask_app.app import create_app
from catalog.flask_app.provider_federation_routes import provider_federation_web

NOW = datetime(2026, 8, 2, 16, tzinfo=timezone.utc)


class StubOperatorSurface(ProviderOperatorSurface):
    def __init__(self) -> None:
        self.calls: list[tuple[object, dict[str, object]]] = []
        self.snapshot = ProviderOperatorSnapshot(
            session_id="session-f85-web",
            capability_id="provider-one",
            node_id="node-provider-one",
            capability_type="synthetic-compute",
            protocol="msh-synthetic",
            protocol_version="1.0",
            discovered=True,
            announcement_status="ready",
            announced_at=NOW,
            enrollment_state="not-requested",
            enrollment_revision=None,
            enrollment_reason_code="enrollment-not-requested",
            health_state="absent",
            health_reason_code="health-record-absent",
            provider_status=None,
            provider_generation=None,
            report_revision=None,
            reported_at=None,
            expires_at=None,
            max_concurrent_jobs=None,
            active_jobs=None,
            queue_depth=None,
            utilization_millis=None,
            activation_state=ProviderActivationState.UNAVAILABLE,
            activation_reason_code="enrollment-not-requested",
            inventory_compatible=None,
            can_manage=True,
            allowed_actions=(ProviderOperatorAction.REQUEST,),
        )

    def view(self) -> ProviderOperatorView:
        return ProviderOperatorView(
            session_id="session-f85-web",
            actor_node_id="node-owner-one",
            is_owner=True,
            generated_at=NOW,
            providers=(self.snapshot,),
        )

    def execute(self, action, **kwargs):
        self.calls.append((action, dict(kwargs)))
        return self.snapshot


def minimal_app(surface: ProviderOperatorSurface | None = None) -> Flask:
    app = Flask(
        __name__,
        template_folder="../templates",
    )
    app.config.update(TESTING=True, SECRET_KEY="f85-test-secret")
    if surface is not None:
        app.config["PROVIDER_OPERATOR_SURFACE"] = surface

    def endpoint_url(endpoint: str, fallback: str) -> str:
        return url_for(endpoint) if endpoint in app.view_functions else fallback

    app.context_processor(lambda: {"endpoint_url": endpoint_url})
    for endpoint, path in (
        ("web.overview", "/overview"),
        ("web.guide", "/guide"),
        ("web.live", "/live"),
        ("web.playback", "/playback"),
        ("web.rescan", "/rescan"),
    ):
        app.add_url_rule(
            path,
            endpoint=endpoint,
            view_func=lambda: "ok",
            methods=["GET", "POST"],
        )
    app.register_blueprint(provider_federation_web)
    return app


def csrf(client) -> str:
    response = client.get("/provider-federation/api/providers")
    assert response.status_code == 200
    value = response.get_json()["view"]["csrf_token"]
    assert isinstance(value, str) and len(value) >= 32
    return value


def test_unconfigured_surface_fails_closed() -> None:
    app = minimal_app()
    client = app.test_client()

    api = client.get("/provider-federation/api/providers")
    page = client.get("/provider-federation")

    assert api.status_code == 503
    assert api.get_json() == {
        "ok": False,
        "error": {"code": "provider-operator-unavailable"},
    }
    assert page.status_code == 503
    assert b"Provider controls are unavailable" in page.data


def test_json_surface_uses_server_bound_context_and_safe_fields() -> None:
    surface = StubOperatorSurface()
    client = minimal_app(surface).test_client()

    response = client.get("/provider-federation/api/providers")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["view"]["session_id"] == "session-f85-web"
    assert payload["view"]["actor_node_id"] == "node-owner-one"
    serialized = response.get_data(as_text=True)
    for forbidden in (
        "properties",
        "attributes",
        "endpoint",
        "credential",
        "descriptor_fingerprint",
        "handler_id",
    ):
        assert forbidden not in serialized


def test_json_controls_require_csrf_and_reject_context_override() -> None:
    surface = StubOperatorSurface()
    client = minimal_app(surface).test_client()

    missing = client.post(
        "/provider-federation/api/providers/provider-one/request",
        json={},
    )
    assert missing.status_code == 403
    assert missing.get_json()["error"]["code"] == "provider-operator-csrf-rejected"

    token = csrf(client)
    override = client.post(
        "/provider-federation/api/providers/provider-one/request",
        json={
            "actor_node_id": "node-attacker",
            "session_id": "session-other",
        },
        headers={"X-CSRF-Token": token},
    )
    assert override.status_code == 400
    assert override.get_json()["error"]["code"] == (
        "operator-context-override-forbidden"
    )
    assert not surface.calls


def test_json_and_html_controls_share_one_surface_command() -> None:
    surface = StubOperatorSurface()
    client = minimal_app(surface).test_client()
    token = csrf(client)

    api = client.post(
        "/provider-federation/api/providers/provider-one/request",
        json={"command_id": "web-request-one"},
        headers={"X-CSRF-Token": token},
    )
    assert api.status_code == 200
    assert api.get_json()["provider"]["capability_id"] == "provider-one"

    html = client.post(
        "/provider-federation/providers/provider-one/request",
        data={"_csrf_token": token, "command_id": "html-request-one"},
    )
    assert html.status_code == 302
    assert html.headers["Location"].endswith("/provider-federation")

    assert surface.calls == [
        (
            ProviderOperatorAction.REQUEST,
            {
                "capability_id": "provider-one",
                "expected_revision": None,
                "reason_code": None,
                "command_id": "web-request-one",
            },
        ),
        (
            ProviderOperatorAction.REQUEST,
            {
                "capability_id": "provider-one",
                "expected_revision": None,
                "reason_code": None,
                "command_id": "html-request-one",
            },
        ),
    ]


def test_html_page_renders_safe_provider_status() -> None:
    surface = StubOperatorSurface()
    client = minimal_app(surface).test_client()

    response = client.get("/provider-federation")

    assert response.status_code == 200
    assert b"Provider federation" in response.data
    assert b"provider-one" in response.data
    assert b"Actor identity is bound by the server" in response.data
    assert b"<dt>Properties</dt>" not in response.data
    assert b"handler_id" not in response.data


def test_real_app_factory_registers_provider_federation_blueprint() -> None:
    app = create_app()
    assert "provider_federation_web.index" in app.view_functions
    assert "provider_federation_web.providers_api" in app.view_functions
    rules = {str(rule) for rule in app.url_map.iter_rules()}
    assert "/provider-federation" in rules
    assert "/provider-federation/api/providers" in rules
