from __future__ import annotations

from pathlib import Path

import pytest
from flask import Flask, redirect
from werkzeug.exceptions import NotFound

from catalog.flask_app.docs_routes import _safe_path, docs_web


def _app(docs_root: Path) -> Flask:
    app = Flask(
        __name__,
        template_folder="../templates",
        static_folder="../static",
    )
    app.config.update(
        TESTING=True,
        FCP_DOCS_ROOT=str(docs_root),
    )
    app.register_blueprint(docs_web)
    return app


def test_docs_index_keeps_real_tree_behind_development_navigation(tmp_path: Path) -> None:
    (tmp_path / "index.md").write_text("# Documentation Home\n", encoding="utf-8")
    guide = tmp_path / "getting-started"
    guide.mkdir()
    (guide / "setup.md").write_text(
        "# Install FCP on a Laptop\n\nSetup details.",
        encoding="utf-8",
    )
    app = _app(tmp_path)

    response = app.test_client().get("/docs")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "FCP Documentation" in body
    assert "Development &amp; all files" in body
    assert "Getting Started" in body
    assert "Install FCP on a Laptop" in body
    assert "/docs/getting-started/setup.md" in body
    assert "2 Markdown files" not in body


def test_docs_render_markdown_and_rewrite_local_links(tmp_path: Path) -> None:
    guide = tmp_path / "guide"
    images = tmp_path / "images"
    guide.mkdir()
    images.mkdir()
    (images / "diagram.png").write_bytes(b"not-a-real-image")
    (guide / "setup.md").write_text(
        "# Setup\n\n## Install\n\nReady.",
        encoding="utf-8",
    )
    (guide / "overview.md").write_text(
        """# Overview

[Continue to setup](setup.md#install)

![Architecture](../images/diagram.png)

| Feature | Status |
| --- | --- |
| Docs | Ready |

!!! note
    This is a documentation note.
""",
        encoding="utf-8",
    )
    app = _app(tmp_path)

    response = app.test_client().get("/docs/guide/overview.md")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "<table>" in body
    assert 'class="admonition note"' in body
    assert 'href="/docs/guide/setup.md#install"' in body
    assert 'src="/docs/_assets/images/diagram.png"' in body
    assert "On this page" in body


def test_docs_asset_route_is_limited_to_document_images(tmp_path: Path) -> None:
    (tmp_path / "image.png").write_bytes(b"image")
    (tmp_path / "secret.txt").write_text("secret", encoding="utf-8")
    app = _app(tmp_path)
    client = app.test_client()

    image_response = client.get("/docs/_assets/image.png")
    secret_response = client.get("/docs/_assets/secret.txt")

    assert image_response.status_code == 200
    assert image_response.headers["X-Content-Type-Options"] == "nosniff"
    assert secret_response.status_code == 404


def test_docs_reject_path_traversal(tmp_path: Path) -> None:
    app = _app(tmp_path)

    with app.test_request_context("/docs"), pytest.raises(NotFound):
        _safe_path("../outside.md")


def test_docs_are_available_before_a_later_runtime_gate(tmp_path: Path) -> None:
    (tmp_path / "index.md").write_text("# Available Early\n", encoding="utf-8")
    app = _app(tmp_path)

    @app.before_request
    def runtime_gate():
        return redirect("/startup")

    docs_response = app.test_client().get("/docs/index.md")
    other_response = app.test_client().get("/not-docs")

    assert docs_response.status_code == 200
    assert "Available Early" in docs_response.get_data(as_text=True)
    assert other_response.status_code == 302
    assert other_response.headers["Location"] == "/startup"


def test_missing_docs_root_has_a_clear_landing_page(tmp_path: Path) -> None:
    missing = tmp_path / "not-created"
    app = _app(missing)

    response = app.test_client().get("/docs")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "documentation folder could not be found" in body
