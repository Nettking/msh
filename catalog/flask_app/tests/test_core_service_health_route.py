from __future__ import annotations

from flask import Flask

from catalog.flask_app import federation_recorder_routes as routes


def test_core_health_route_reports_degradation_without_http_failure(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def snapshot(**kwargs):
        captured.update(kwargs)
        return {
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
        }

    monkeypatch.setattr(routes, "core_service_health_snapshot", snapshot)
    app = Flask(__name__)
    app.register_blueprint(routes.federation_recorder_web)

    response = app.test_client().get("/federation/health")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Pragma"] == "no-cache"
    assert captured["listener_probe"] is routes.bounded_relay_listener_probe
    assert response.get_json()["status"] == "degraded"
    services = {item["service"]: item for item in response.get_json()["services"]}
    assert services["flask"]["readiness"] == "ready"
    assert services["flask"]["dependency"] == "degraded"
    assert services["relay"]["readiness"] == "not_ready"
