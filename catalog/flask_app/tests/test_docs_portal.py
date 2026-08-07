from __future__ import annotations

from pathlib import Path

from flask import Flask

from catalog.flask_app.docs_routes import docs_web


def _app(docs_root: Path) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.config["TESTING"] = True
    app.config["MSH_DOCS_ROOT"] = str(docs_root)
    app.register_blueprint(docs_web)
    return app


def _write_doc(root: Path, relative: str, content: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_docs_home_is_task_first_not_a_repository_or_marketing_landing_page(
    tmp_path: Path,
) -> None:
    _write_doc(tmp_path, "quick_start.md", "# Quick start\n\nStart here.\n")
    _write_doc(tmp_path, "one_command_setup.md", "# One-command setup\n")
    _write_doc(tmp_path, "getting_started.md", "# Getting started with MSH\n")
    _write_doc(tmp_path, "federated_session_network.md", "# Federation\n")
    _write_doc(tmp_path, "implementation/history.md", "# Historical delivery note\n")

    response = _app(tmp_path).test_client().get("/docs")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Get started with MSH" in html
    assert "Install MSH, create or join a Federation" in html
    assert "Start here" in html
    assert "First-run path" in html
    assert "Common tasks" in html
    assert "Reference" in html
    assert "Development &amp; all files" in html

    assert 'href="/docs/quick_start.md">Quick start</a>' in html
    assert 'href="/docs/one_command_setup.md">Installation</a>' in html
    assert 'href="/docs/getting_started.md">Understand MSH</a>' in html
    assert 'href="/docs/federated_session_network.md">Federation</a>' in html

    assert "Build confidence before you build complexity." not in html
    assert "documents available" not in html
    assert "Markdown files" not in html
    assert "Choose a document from the navigation panel" not in html


def test_docs_home_primary_action_has_explicit_readable_contrast() -> None:
    css_path = Path(__file__).resolve().parents[1] / "static" / "css" / "docs-home.css"
    css = css_path.read_text(encoding="utf-8")

    assert ".docs-main .docs-home__button--primary" in css
    assert "color: #fff;" in css
    assert "background: var(--docs-ink-950);" in css


def test_docs_document_keeps_curated_navigation_and_toc(tmp_path: Path) -> None:
    _write_doc(
        tmp_path,
        "quick_start.md",
        "# Quick start\n\n## First run\n\nStart MSH.\n\n## Next step\n\nOpen onboarding.\n",
    )

    response = _app(tmp_path).test_client().get("/docs/quick_start.md")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Quick start" in html
    assert "First run" in html
    assert "Next step" in html
    assert "On this page" in html
    assert "Start here" in html
    assert "Use MSH" in html
    assert "Understand MSH" in html
    assert "Development &amp; all files" in html
    assert 'class="docs-nav-link is-active"' in html
    assert 'aria-current="page"' in html
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Referrer-Policy"] == "same-origin"


def test_docs_home_retains_safe_missing_root_state(tmp_path: Path) -> None:
    missing = tmp_path / "not-created"

    response = _app(missing).test_client().get("/docs")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Documentation unavailable" in html
    assert "The documentation folder could not be found." in html
