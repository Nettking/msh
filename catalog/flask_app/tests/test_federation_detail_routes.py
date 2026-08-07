from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from flask import redirect

from catalog.flask_app import capability_benchmark_routes as benchmark_routes
from catalog.flask_app import federation_routes
from catalog.flask_app.app import create_app

FEDERATION_PAGES = {
    "/federation/device": "This device",
    "/federation/devices": "Devices",
    "/federation/services": "Services",
    "/federation/benchmarks": "Benchmarks",
    "/federation/storage": "Storage",
    "/federation/jobs": "Jobs",
    "/federation/activity": "Activity",
    "/federation/settings": "Settings",
}


def _app():
    app = create_app()
    app.config.update(TESTING=True)
    return app


def test_federation_detail_route_is_registered_read_only() -> None:
    app = _app()
    rule = next(
        rule
        for rule in app.url_map.iter_rules()
        if rule.endpoint == "federation_web.detail"
    )

    assert str(rule) == "/federation/<page_name>"
    assert rule.methods == {"GET", "HEAD", "OPTIONS"}


@pytest.mark.parametrize(("path", "title"), FEDERATION_PAGES.items())
def test_all_federation_detail_pages_render_read_only(
    path: str,
    title: str,
) -> None:
    client = _app().test_client()

    response = client.get(path)
    head = client.head(path)
    page = response.get_data(as_text=True)

    assert response.status_code == 200
    assert head.status_code == 200
    assert head.data == b""
    assert f"<h2>{title}</h2>" in page
    assert f'href="{path}"' in page
    assert 'aria-current="page"' in page
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Pragma"] == "no-cache"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Referrer-Policy"] == "same-origin"


def test_benchmark_detail_offers_explicit_bounded_rerun(monkeypatch) -> None:
    now = datetime(2026, 8, 7, 18, 20, tzinfo=timezone.utc)
    snapshot = SimpleNamespace(device_id="node-local")
    result = SimpleNamespace(
        run_id="run-local-1",
        device_id="node-local",
        benchmark_id="benchmark.ai.ollama-inference.v1",
        target_service_id="ollama-configured",
        finished_at=now,
    )

    class FakeBenchmarkService:
        def __init__(self) -> None:
            self.run_calls: list[tuple[str, str]] = []
            self.onboarding_service = SimpleNamespace(
                authorized_context=lambda: SimpleNamespace(
                    credentials=SimpleNamespace(
                        identity=SimpleNamespace(node_id="node-local")
                    )
                )
            )
            self.inspection_service = SimpleNamespace(load=lambda: snapshot)

        def view_model(self, _snapshot, *, connected: bool):
            assert connected is True
            return (
                {"state": "complete"},
                [
                    {
                        "benchmark_id": result.benchmark_id,
                        "target_service_id": result.target_service_id,
                        "can_run": True,
                    }
                ],
                True,
            )

        def list_results(self):
            return (result,)

        def run(self, *, benchmark_id: str, target_service_id: str):
            self.run_calls.append((benchmark_id, target_service_id))
            return SimpleNamespace(state=SimpleNamespace(value="passed"))

    service = FakeBenchmarkService()
    projection = {
        "schema": "msh.federation-product-view.v1",
        "page": "benchmarks",
        "active_section": "benchmarks",
        "title": "Benchmarks",
        "intro": "Benchmark results are evidence only and never grant authority.",
        "state": "connected",
        "state_label": "Connected",
        "generated_at": now.isoformat(),
        "updated_label": "Updated just now",
        "sections": [
            {"key": "benchmarks", "label": "Benchmarks", "url": "/federation/benchmarks"}
        ],
        "recommended_action": None,
        "degraded": {
            "visible": False,
            "title": "No current issue",
            "message": "No current issue",
            "action": {"label": "Open overview", "url": "/federation"},
        },
        "notice": None,
        "summary_cards": [],
        "items": [
            {
                "key": result.run_id,
                "label": "Benchmark.Ai.Ollama Inference.V1",
                "detail": "Current suitability evidence",
                "state": "passed",
                "state_label": "Current",
                "details": [
                    {"label": "Target", "value": result.target_service_id},
                    {"label": "Expires", "value": "2026-08-06T17:09:57Z"},
                ],
            }
        ],
        "technical_details": [],
    }

    monkeypatch.setattr(
        federation_routes,
        "get_capability_benchmark_service",
        lambda: service,
    )
    monkeypatch.setattr(
        federation_routes,
        "_safe_projection",
        lambda _page: projection,
    )
    monkeypatch.setattr(
        benchmark_routes,
        "get_capability_benchmark_service",
        lambda: service,
    )

    client = _app().test_client()
    response = client.get("/federation/benchmarks")
    page = response.get_data(as_text=True)

    assert response.status_code == 200
    assert service.run_calls == []
    assert 'action="/onboarding/benchmarks/run"' in page
    assert 'name="benchmark_id" value="benchmark.ai.ollama-inference.v1"' in page
    assert 'name="target_service_id" value="ollama-configured"' in page
    assert ">Run again</button>" in page
    assert "Recorded expiry" in page
    assert "grants no authority" in page

    with client.session_transaction() as browser:
        csrf = str(browser["capability_onboarding_csrf_token"])
    rerun = client.post(
        "/onboarding/benchmarks/run",
        data={
            "_csrf_token": csrf,
            "benchmark_id": result.benchmark_id,
            "target_service_id": result.target_service_id,
        },
    )

    assert rerun.status_code == 303
    assert service.run_calls == [
        (result.benchmark_id, result.target_service_id)
    ]


def test_overview_navigation_links_resolve_to_registered_pages() -> None:
    client = _app().test_client()
    overview = client.get("/federation").get_data(as_text=True)

    for path in FEDERATION_PAGES:
        assert f'href="{path}"' in overview
        assert client.get(path).status_code == 200


def test_detail_routes_strip_browser_supplied_context() -> None:
    client = _app().test_client()

    response = client.get(
        "/federation/devices?session_id=attacker&actor_node_id=attacker"
        "&include_technical=1"
    )

    assert response.status_code in {301, 302, 303, 307, 308}
    assert response.headers["Location"] == "/federation/devices"


def test_detail_routes_dispatch_before_legacy_startup_gate() -> None:
    app = _app()

    @app.before_request
    def simulated_legacy_gate():
        return redirect("/startup")

    response = app.test_client().get("/federation/services")

    assert response.status_code == 200
    assert response.headers.get("Location") is None


def test_unknown_federation_detail_page_is_not_exposed() -> None:
    response = _app().test_client().get("/federation/not-a-page")

    assert response.status_code == 404
