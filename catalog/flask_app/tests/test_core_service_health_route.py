from __future__ import annotations

from flask import Flask

from catalog.flask_app import federation_recorder_routes as routes


def test_core_health_route_reports_degradation_without_http_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        routes,
        "core_service_health_snapshot",
        lambda **_kwargs: {
            "status": "degraded",
            "services": [
                {
                    "service": "flask",
                    "liveness": "alive",
                    "readiness": "ready",
                    "dependency": "degraded",
                    "code": "flask-dependency-degraded",
                    "message": "The FCP web/control surface is serving.",
                },
                {
                    "service": "relay",
                    "liveness": "unavailable",
                    "readiness": "not_ready",
                    "dependency": "unavailable",
                    "code": "relay-listener-unavailable",
                    "message": "The Federation relay listener is not reachable.",
                },
                {
                    "service": "recorder",
                    "liveness": "alive",
                    "readiness": "ready",
                    "dependency": "healthy",
                    "code": "recorder-ready",
                    "message": "The managed recorder is ready.",
                },
            ],
        },
    )
    app = Flask(__name__)
    app.register_blueprint(routes.federation_recorder_web)

    response = app.test_client().get("/federation/health")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Pragma"] == "no-cache"
    assert response.get_json()["status"] == "degraded"
    services = {item["service"]: item for item in response.get_json()["services"]}
    assert services["flask"]["readiness"] == "ready"
    assert services["flask"]["dependency"] == "degraded"
    assert services["relay"]["readiness"] == "not_ready"
