from __future__ import annotations

import pytest
from flask import redirect

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


@pytest.mark.parametrize(("path", "title"), FEDERATION_PAGES.items())
def test_all_federation_detail_pages_render_read_only(
    path: str,
    title: str,
) -> None:
    client = _app().test_client()

    response = client.get(path)
    head = client.head(path)
    rejected_write = client.post(path)
    page = response.get_data(as_text=True)

    assert response.status_code == 200
    assert head.status_code == 200
    assert head.data == b""
    assert rejected_write.status_code == 405
    assert f"<h2>{title}</h2>" in page
    assert f'href="{path}"' in page
    assert 'aria-current="page"' in page
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Pragma"] == "no-cache"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Referrer-Policy"] == "same-origin"


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
