from __future__ import annotations

import threading
from uuid import uuid4

from flask import (
    Blueprint,
    jsonify,
    render_template,
    request,
)

from catalog.ai.grounding import append_grounding_warning
from catalog.ai.ollama_client import DEFAULT_BASE_URL, DEFAULT_MODEL
from catalog.ai.prompts import SYSTEM_PROMPT, build_extractive_prompt, build_prompt
from catalog.ai.rag import format_context, retrieve
from catalog.ai.repo_index import load_or_build_chunks, repo_root_from
from catalog.ai.runtime import AIRuntimePolicy, LanguageModelProvider, LanguageModelRuntime
from catalog.ai.runtime_manager import ConfiguredLanguageModelRuntimeManager
from catalog.ai.runtime_contracts import AIModality, AIRuntimeError, AIRuntimeRequest
from catalog.ai.symbols import build_symbols

from .services.ai_answer_formatting_service import render_safe_markdown
from .services.capability_ai_service import ai_provider_label
from .services.capability_config_service import (
    CapabilityConfigError,
    load_capability_config,
)
from .services.capability_startup_transition_service import (
    get_capability_startup_transition_service,
)

ai_web = Blueprint("ai_web", __name__)
AI_RUNTIME_SESSION_ID = "local-ai"
AI_RUNTIME_MANAGER = ConfiguredLanguageModelRuntimeManager(
    session_id=AI_RUNTIME_SESSION_ID,
    policy=AIRuntimePolicy(max_pending_requests=32, max_fallbacks=1),
)
_ACTIVE_RUNTIME_LOCK = threading.RLock()
_ACTIVE_RUNTIME_KEY: tuple[object, ...] | None = None
_ACTIVE_RUNTIME: LanguageModelRuntime | None = None


def _active_capability_providers() -> tuple[LanguageModelProvider, ...]:
    """Return only providers activated through capability contribution authority."""

    return AI_RUNTIME_MANAGER.additional_providers()


def _active_capability_runtime() -> LanguageModelRuntime | None:
    """Reuse one bounded runtime containing only currently active contributions."""

    global _ACTIVE_RUNTIME, _ACTIVE_RUNTIME_KEY

    providers = _active_capability_providers()
    if not providers:
        with _ACTIVE_RUNTIME_LOCK:
            _ACTIVE_RUNTIME = None
            _ACTIVE_RUNTIME_KEY = None
        return None
    key: tuple[object, ...] = (
        id(AI_RUNTIME_MANAGER),
        *tuple(
            (provider.capability_id, id(provider), tuple(provider.models))
            for provider in providers
        ),
    )
    with _ACTIVE_RUNTIME_LOCK:
        if _ACTIVE_RUNTIME is not None and _ACTIVE_RUNTIME_KEY == key:
            return _ACTIVE_RUNTIME
        _ACTIVE_RUNTIME = LanguageModelRuntime(
            session_id=AI_RUNTIME_MANAGER.session_id,
            providers=providers,
            policy=AI_RUNTIME_MANAGER.policy,
        )
        _ACTIVE_RUNTIME_KEY = key
        return _ACTIVE_RUNTIME


def _active_capability_defaults() -> tuple[str, str] | None:
    """Choose a deterministic safe label/model from active local AI authority."""

    providers = sorted(
        _active_capability_providers(),
        key=lambda provider: provider.capability_id,
    )
    for provider in providers:
        models = tuple(provider.models)
        if models:
            return models[0], provider.display_name
    return None


def _ai_defaults() -> tuple[str, str, str]:
    active = _active_capability_defaults()
    if active is not None:
        model, provider_name = active
        return model, DEFAULT_BASE_URL, provider_name
    try:
        config = load_capability_config()
    except (CapabilityConfigError, OSError, TypeError, ValueError):
        return DEFAULT_MODEL, DEFAULT_BASE_URL, "No active AI contribution"
    return (
        config.ai_model or DEFAULT_MODEL,
        config.ollama_base_url or DEFAULT_BASE_URL,
        f"No active AI contribution ({ai_provider_label(config)} configured)",
    )


def register_language_model_provider(provider: LanguageModelProvider) -> None:
    """Register an additional session-bound provider for normal AI requests."""
    AI_RUNTIME_MANAGER.register(provider)


def unregister_language_model_provider(capability_id: str) -> bool:
    return AI_RUNTIME_MANAGER.unregister(capability_id)


def _build_ai_runtime(
    *,
    model: str,
    base_url: str,
    provider_name: str,
) -> LanguageModelRuntime | None:
    """Return only the runtime composed from live contribution authority."""

    del model, base_url, provider_name
    return _active_capability_runtime()


def _empty_result(*, error: str = "") -> dict[str, object]:
    return {
        "answer": "",
        "context": "",
        "sources": [],
        "error": error,
        "error_code": "",
        "selection_status": None,
        "selected_provider_name": "",
        "provider_attempts": [],
        "provider_status": [],
    }


def _answer_question(
    question: str,
    *,
    model: str,
    base_url: str,
    dry_run: bool,
    extractive: bool,
    provider_name: str = "Configured provider",
    runtime: LanguageModelRuntime | None = None,
) -> dict[str, object]:
    root = repo_root_from()
    chunks = load_or_build_chunks(root)
    symbols = build_symbols(root)
    selected = retrieve(question, chunks, limit=8, symbols=symbols)
    sources = [chunk.source_label() for chunk in selected]
    context = format_context(selected)
    if not selected:
        result = _empty_result(error="No relevant repository context found.")
        result.update({"context": "", "sources": []})
        return result
    runtime = runtime or _build_ai_runtime(
        model=model,
        base_url=base_url,
        provider_name=provider_name,
    )
    if dry_run:
        result = _empty_result()
        result.update(
            {
                "context": context,
                "sources": sources,
                "provider_status": (
                    [] if runtime is None else list(runtime.provider_status())
                ),
            }
        )
        return result
    if runtime is None:
        result = _empty_result(
            error=(
                "No active AI contribution is available. Enable a language-model "
                "contribution in Device setup and try again."
            )
        )
        result.update(
            {
                "context": context,
                "sources": sources,
                "error_code": "no-active-ai-provider",
            }
        )
        return result
    prompt_builder = build_extractive_prompt if extractive else build_prompt
    prompt = prompt_builder(question, context, sources=sources)
    request_token = uuid4().hex
    logical_request = AIRuntimeRequest(
        request_id=f"request-{request_token}",
        session_id=runtime.session_id,
        idempotency_key=f"request-{request_token}",
        model=model,
        modality=AIModality.TEXT,
        prompt=prompt,
        system_prompt=SYSTEM_PROMPT.strip(),
        timeout_seconds=120,
    )
    try:
        runtime_result = runtime.execute(logical_request)
    except AIRuntimeError as exc:
        payload = exc.to_dict()
        return {
            "answer": "",
            "context": context,
            "sources": sources,
            "error": exc.message,
            "error_code": exc.code,
            "selection_status": payload["selection"],
            "selected_provider_name": (
                ""
                if exc.selection is None
                else exc.selection.selected_provider_name or ""
            ),
            "provider_attempts": payload["attempts"],
            "provider_status": list(runtime.provider_status()),
        }
    return {
        "answer": append_grounding_warning(runtime_result.content, selected),
        "context": context,
        "sources": sources,
        "error": "",
        "error_code": "",
        "selection_status": runtime_result.selection.to_dict(),
        "selected_provider_name": runtime_result.provider_name,
        "provider_attempts": [item.to_dict() for item in runtime_result.attempts],
        "provider_status": list(runtime.provider_status()),
    }


def _render_ai_page(
    *,
    question: str,
    model: str,
    dry_run: bool,
    extractive: bool,
    ai_provider_name: str,
    answer: str = "",
    context: str = "",
    sources: list[str] | None = None,
    error: str = "",
    error_code: str = "",
    selection_status: dict[str, object] | None = None,
    selected_provider_name: str = "",
    provider_attempts: list[dict[str, object]] | None = None,
    provider_status: list[dict[str, object]] | None = None,
):
    return render_template(
        "ai_explainer.html",
        question=question,
        model=model,
        dry_run=dry_run,
        extractive=extractive,
        ai_provider_name=ai_provider_name,
        selected_provider_name=selected_provider_name or ai_provider_name,
        selection_status=selection_status,
        provider_attempts=provider_attempts or [],
        provider_status=provider_status or [],
        answer=answer,
        answer_html=render_safe_markdown(answer),
        context=context,
        sources=sources or [],
        error=error,
        error_code=error_code,
    )


@ai_web.app_context_processor
def inject_live_ai_capability_startup_flags() -> dict[str, object]:
    """Keep the workbench AI page discoverable without granting AI authority."""

    try:
        flags = get_capability_startup_transition_service().capability_flags()
    except Exception:  # noqa: BLE001 - retain the existing fail-closed processor
        return {}
    if not bool(flags.get("completed")):
        return {}
    live_flags = dict(flags)
    live_flags["language_model"] = bool(live_flags.get("workbench"))
    live_flags["degraded"] = False
    return {"capability_startup_flags": live_flags}


@ai_web.get("/ai")
def ai_page():
    model, base_url, provider_name = _ai_defaults()
    runtime = _build_ai_runtime(
        model=model,
        base_url=base_url,
        provider_name=provider_name,
    )
    return _render_ai_page(
        question="",
        model=model,
        dry_run=False,
        extractive=False,
        ai_provider_name=provider_name,
        provider_status=[] if runtime is None else list(runtime.provider_status()),
    )


@ai_web.post("/ai/ask")
def ai_ask():
    default_model, base_url, provider_name = _ai_defaults()
    question = (request.form.get("question") or "").strip()
    model = (request.form.get("model") or default_model).strip()
    dry_run = request.form.get("dry_run") == "1"
    extractive = request.form.get("extractive") == "1"
    if not question:
        result = _empty_result(error="Question is required.")
    else:
        result = _answer_question(
            question,
            model=model,
            base_url=base_url,
            provider_name=provider_name,
            dry_run=dry_run,
            extractive=extractive,
        )
    effective_provider = str(result.get("selected_provider_name") or provider_name)
    if request.accept_mimetypes.best == "application/json":
        payload = {
            **result,
            "answer_html": render_safe_markdown(str(result["answer"])),
            "question": question,
            "model": model,
            "provider_name": effective_provider,
            "dry_run": dry_run,
            "extractive": extractive,
        }
        if not question:
            status = 400
        elif result.get("error_code") == "no-active-ai-provider":
            status = 503
        else:
            status = 502 if result["error"] else 200
        return jsonify(payload), status
    return _render_ai_page(
        question=question,
        model=model,
        dry_run=dry_run,
        extractive=extractive,
        ai_provider_name=provider_name,
        **result,
    )
