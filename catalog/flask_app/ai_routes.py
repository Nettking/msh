from __future__ import annotations

from flask import Blueprint, jsonify, render_template, request

from catalog.ai.grounding import append_grounding_warning
from catalog.ai.ollama_client import DEFAULT_BASE_URL, DEFAULT_MODEL, OllamaError, chat
from catalog.ai.prompts import SYSTEM_PROMPT, build_extractive_prompt, build_prompt
from catalog.ai.rag import format_context, retrieve
from catalog.ai.repo_index import load_or_build_chunks, repo_root_from
from catalog.ai.symbols import build_symbols

from .services.ai_answer_formatting_service import render_safe_markdown
from .services.server_setup_service import ai_provider_label, load_settings

ai_web = Blueprint("ai_web", __name__)


def _ai_defaults() -> tuple[str, str, str]:
    try:
        settings = load_settings()
    except Exception:
        return DEFAULT_MODEL, DEFAULT_BASE_URL, "Default Ollama"
    if settings.configured and settings.user_setup_complete and settings.ai_enabled and settings.ai_model:
        return settings.ai_model, settings.ollama_base_url, ai_provider_label(settings)
    return DEFAULT_MODEL, DEFAULT_BASE_URL, "Default Ollama"


def _answer_question(
    question: str,
    *,
    model: str,
    base_url: str,
    dry_run: bool,
    extractive: bool,
) -> dict[str, object]:
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
        answer = chat(prompt=prompt, system_prompt=SYSTEM_PROMPT, model=model, base_url=base_url)
    except OllamaError as exc:
        return {"answer": "", "context": context, "sources": sources, "error": str(exc)}
    return {
        "answer": append_grounding_warning(answer, selected),
        "context": context,
        "sources": sources,
        "error": "",
    }


def _render_ai_page(
    *,
    question: str,
    model: str,
    dry_run: bool,
    extractive: bool,
    ai_provider_name: str,
    ollama_base_url: str,
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
        ai_provider_name=ai_provider_name,
        ollama_base_url=ollama_base_url,
        answer=answer,
        answer_html=render_safe_markdown(answer),
        context=context,
        sources=sources or [],
        error=error,
    )


@ai_web.get("/ai")
def ai_page():
    model, base_url, provider_name = _ai_defaults()
    return _render_ai_page(
        question="",
        model=model,
        dry_run=False,
        extractive=False,
        ai_provider_name=provider_name,
        ollama_base_url=base_url,
    )


@ai_web.post("/ai/ask")
def ai_ask():
    default_model, base_url, provider_name = _ai_defaults()
    question = (request.form.get("question") or "").strip()
    model = (request.form.get("model") or default_model).strip()
    dry_run = request.form.get("dry_run") == "1"
    extractive = request.form.get("extractive") == "1"
    if not question:
        result = {"answer": "", "context": "", "sources": [], "error": "Question is required."}
    else:
        result = _answer_question(
            question,
            model=model,
            base_url=base_url,
            dry_run=dry_run,
            extractive=extractive,
        )
    if request.accept_mimetypes.best == "application/json":
        payload = {
            **result,
            "answer_html": render_safe_markdown(str(result["answer"])),
            "question": question,
            "model": model,
            "provider_name": provider_name,
            "dry_run": dry_run,
            "extractive": extractive,
        }
        status = 400 if not question else (502 if result["error"] else 200)
        return jsonify(payload), status
    return _render_ai_page(
        question=question,
        model=model,
        dry_run=dry_run,
        extractive=extractive,
        ai_provider_name=provider_name,
        ollama_base_url=base_url,
        **result,
    )
