from __future__ import annotations

from pathlib import Path

from flask import Flask

from catalog.flask_app import ai_routes
from catalog.flask_app.ai_routes import ai_web
from catalog.flask_app.routes import web
from catalog.flask_app.services.ai_answer_formatting_service import render_safe_markdown


def _app() -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "test"
    app.register_blueprint(web)
    app.register_blueprint(ai_web)
    return app


def test_safe_markdown_formats_answers_and_escapes_model_html() -> None:
    rendered = render_safe_markdown(
        "## Direct answer\n\n"
        "Use **this flow** and `safe_mode`.\n\n"
        "- First\n"
        "- <script>alert('no')</script>\n\n"
        "```python\n"
        "print('<safe>')\n"
        "```"
    )

    assert "<h2>Direct answer</h2>" in rendered
    assert "<strong>this flow</strong>" in rendered
    assert "<code>safe_mode</code>" in rendered
    assert "<ul><li>First</li>" in rendered
    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered
    assert '<pre><code class="language-python">print(&#x27;&lt;safe&gt;&#x27;)</code></pre>' in rendered


def test_ai_page_starts_as_chat_with_model_calls_enabled(monkeypatch) -> None:
    monkeypatch.setattr(ai_routes, "_ai_defaults", lambda: ("smollm2:360m", "http://laptop:11434", "Laptop"))
    response = _app().test_client().get("/ai")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'data-ai-chat' in html
    assert "Ask a question about how MSH works." in html
    assert "How does data flow?" in html
    assert 'name="dry_run" value="1" checked' not in html
    assert "Retrieved context" not in html


def test_ai_ask_returns_formatted_json_with_collapsed_evidence_data(monkeypatch) -> None:
    monkeypatch.setattr(ai_routes, "_ai_defaults", lambda: ("smollm2:360m", "http://laptop:11434", "Laptop"))
    monkeypatch.setattr(
        ai_routes,
        "_answer_question",
        lambda *_args, **_kwargs: {
            "answer": "## Answer\n\n- One\n- Two",
            "context": "--- README.md:1-4 ---\nContext",
            "sources": ["README.md:1-4"],
            "error": "",
        },
    )

    response = _app().test_client().post(
        "/ai/ask",
        data={"question": "How?", "model": "smollm2:360m"},
        headers={"Accept": "application/json"},
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["answer_html"] == "<h2>Answer</h2>\n<ul><li>One</li><li>Two</li></ul>"
    assert payload["sources"] == ["README.md:1-4"]
    assert payload["context"].startswith("--- README.md")
    assert payload["provider_name"] == "Laptop"


def test_ai_chat_script_has_waiting_and_optional_source_interactions() -> None:
    script = Path("catalog/flask_app/static/js/ai-explainer.js").read_text(encoding="utf-8")
    template = Path("catalog/flask_app/templates/ai_explainer.html").read_text(encoding="utf-8")

    assert 'Working with ${provider}' in script
    assert 'form.setAttribute("aria-busy", "true")' in script
    assert 'headers: { Accept: "application/json" }' in script
    assert 'label.textContent = "Inspect sources"' in script
    assert '<details class="ai-chat-evidence">' in template
    assert "<summary>View retrieved excerpts</summary>" in template
