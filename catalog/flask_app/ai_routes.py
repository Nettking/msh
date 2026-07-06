from __future__ import annotations

from flask import Blueprint, render_template, request

from catalog.ai.grounding import append_grounding_warning
from catalog.ai.ollama_client import DEFAULT_MODEL, OllamaError, chat
from catalog.ai.prompts import SYSTEM_PROMPT, build_extractive_prompt, build_prompt
from catalog.ai.rag import format_context, retrieve
from catalog.ai.repo_index import load_or_build_chunks, repo_root_from
from catalog.ai.symbols import build_symbols

from .services.server_setup_service import load_settings

ai_web = Blueprint("ai_web", __name__)


def _default_model() -> str:
    try:
        settings = load_settings()
    except Exception:
        return DEFAULT_MODEL
    if settings.configured and settings.ai_enabled and settings.ai_model:
        return settings.ai_model
    return DEFAULT_MODEL


def _answer_question(question: str, *, model: str, dry_run: bool, extractive: bool) -> dict[str, object]:
    root = repo_root_from()
    chunks = load_or_build_chunks(root)
    symbols = build_symbols(root)
    selected = retrieve(question, chunks, limit=8, symbols=symbols)
    sources = [chunk.source_label() for chunk in selected]
    context = format_context(selected)
    if not selected:
        return {"answer": "", "context": "", "sources": [], "error": "No relevant repository context found."}
    if dry_run:
        return {"answer": "", "context": context, "sources": sources, "error": ""}
    try:
        prompt_builder = build_extractive_prompt if extractive else build_prompt
        prompt = prompt_builder(question, context, sources=sources)
        answer = chat(prompt=prompt, system_prompt=SYSTEM_PROMPT, model=model)
    except OllamaError as exc:
        return {"answer": "", "context": context, "sources": sources, "error": str(exc)}
    return {"answer": append_grounding_warning(answer, selected), "context": "", "sources": sources, "error": ""}


def _render_ai_page(
    *,
    question: str,
    model: str,
    dry_run: bool,
    extractive: bool,
    answer: str = "",
    context: str = "",
    sources: list[str] | None = None,
    error: str = "",
):
    return render_template(
        "ai_explainer.html",
        question=question,
        model=model,
        dry_run=dry_run,
        extractive=extractive,
        answer=answer,
        context=context,
        sources=sources or [],
        error=error,
    )


@ai_web.get("/ai")
def ai_page():
    return _render_ai_page(
        question="How does data flow through MSH?",
        model=_default_model(),
        dry_run=True,
        extractive=True,
    )


@ai_web.post("/ai/ask")
def ai_ask():
    question = (request.form.get("question") or "").strip()
    model = (request.form.get("model") or _default_model()).strip()
    dry_run = request.form.get("dry_run") == "1"
    extractive = request.form.get("extractive") == "1"
    if not question:
        result = {"answer": "", "context": "", "sources": [], "error": "Question is required."}
    else:
        result = _answer_question(question, model=model, dry_run=dry_run, extractive=extractive)
    return _render_ai_page(
        question=question,
        model=model,
        dry_run=dry_run,
        extractive=extractive,
        **result,
    )
