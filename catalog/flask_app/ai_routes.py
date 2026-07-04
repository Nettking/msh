from __future__ import annotations

from flask import Blueprint, render_template_string, request

from catalog.ai.ollama_client import DEFAULT_MODEL, OllamaError, chat
from catalog.ai.prompts import SYSTEM_PROMPT, build_prompt
from catalog.ai.rag import format_context, retrieve
from catalog.ai.repo_index import build_chunks, repo_root_from

ai_web = Blueprint("ai_web", __name__)

_PAGE = """
<!doctype html>
<title>MSH AI Explainer</title>
<h1>MSH AI Explainer</h1>
<p>Read-only local explainer for system-understanding questions. It retrieves repository context and sends only that context to Ollama.</p>
<form method="post" action="{{ url_for('ai_web.ai_ask') }}">
  <label for="question">Question</label><br>
  <textarea id="question" name="question" rows="4" cols="100">{{ question }}</textarea><br>
  <label for="model">Model</label><br>
  <input id="model" name="model" value="{{ model }}" size="40"><br>
  <label><input type="checkbox" name="dry_run" value="1" {% if dry_run %}checked{% endif %}> Show retrieved context only</label><br><br>
  <button type="submit">Ask</button>
</form>
{% if error %}<h2>Error</h2><pre>{{ error }}</pre>{% endif %}
{% if answer %}<h2>Answer</h2><pre style="white-space: pre-wrap">{{ answer }}</pre>{% endif %}
{% if sources %}
<h2>Sources</h2>
<ul>{% for source in sources %}<li><code>{{ source }}</code></li>{% endfor %}</ul>
{% endif %}
{% if context %}<h2>Retrieved context</h2><pre style="white-space: pre-wrap">{{ context }}</pre>{% endif %}
"""


def _answer_question(question: str, *, model: str, dry_run: bool) -> dict[str, object]:
    root = repo_root_from()
    chunks = build_chunks(root)
    selected = retrieve(question, chunks, limit=8)
    sources = [f"{chunk.path}:{chunk.start_line}-{chunk.end_line}" for chunk in selected]
    context = format_context(selected)
    if not selected:
        return {"answer": "", "context": "", "sources": [], "error": "No relevant repository context found."}
    if dry_run:
        return {"answer": "", "context": context, "sources": sources, "error": ""}
    try:
        prompt = build_prompt(question, context)
        answer = chat(prompt=prompt, system_prompt=SYSTEM_PROMPT, model=model)
    except OllamaError as exc:
        return {"answer": "", "context": context, "sources": sources, "error": str(exc)}
    return {"answer": answer, "context": "", "sources": sources, "error": ""}


@ai_web.get("/ai")
def ai_page():
    return render_template_string(
        _PAGE,
        question="How does data flow through MSH?",
        model=DEFAULT_MODEL,
        dry_run=True,
        answer="",
        context="",
        sources=[],
        error="",
    )


@ai_web.post("/ai/ask")
def ai_ask():
    question = (request.form.get("question") or "").strip()
    model = (request.form.get("model") or DEFAULT_MODEL).strip()
    dry_run = request.form.get("dry_run") == "1"
    if not question:
        result = {"answer": "", "context": "", "sources": [], "error": "Question is required."}
    else:
        result = _answer_question(question, model=model, dry_run=dry_run)
    return render_template_string(
        _PAGE,
        question=question,
        model=model,
        dry_run=dry_run,
        **result,
    )
