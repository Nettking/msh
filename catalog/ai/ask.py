"""Command-line entry point for the read-only MSH AI explainer.

Usage:
    python -m catalog.ai.ask "How does data flow through MSH?"
    python -m catalog.ai.ask --dry-run "What does /control do?"
    python -m catalog.ai.ask --show-sources "Where are workflow artifacts stored?"
    python -m catalog.ai.ask --extractive "What does /control do?"
"""

from __future__ import annotations

import argparse
from pathlib import Path
from uuid import uuid4

from .grounding import append_grounding_warning
from .ollama_client import DEFAULT_BASE_URL, DEFAULT_MODEL
from .ollama_provider import OllamaLanguageModelProvider
from .prompts import SYSTEM_PROMPT, build_extractive_prompt, build_prompt
from .rag import format_context, retrieve
from .repo_index import load_or_build_chunks, repo_root_from
from .runtime import AIRuntimePolicy, LanguageModelRuntime
from .runtime_contracts import AIModality, AIRuntimeError, AIRuntimeRequest
from .symbols import build_symbols

CLI_AI_SESSION_ID = "local-ai-cli"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ask a read-only question about the MSH repository.")
    parser.add_argument("question", help="Question to ask about the repository.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Ollama model name.")
    parser.add_argument("--root", type=Path, default=None, help="Repository root. Defaults to auto-detection.")
    parser.add_argument("--limit", type=int, default=8, help="Number of context chunks to retrieve.")
    parser.add_argument("--max-context-chars", type=int, default=12000, help="Maximum context characters sent to Ollama.")
    parser.add_argument("--no-cache", action="store_true", help="Do not read or write the local repository index cache.")
    parser.add_argument("--rebuild-index", action="store_true", help="Force rebuilding the local repository index cache.")
    parser.add_argument("--show-sources", action="store_true", help="Print retrieved source labels before the answer.")
    parser.add_argument("--extractive", action="store_true", help="Use a stricter source-extractive answer prompt.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print retrieved context only. Does not call Ollama.",
    )
    return parser.parse_args()


def _bounded_context(context: str, max_chars: int) -> str:
    if max_chars <= 0 or len(context) <= max_chars:
        return context
    return context[:max_chars].rstrip() + "\n\n[context truncated by --max-context-chars]"


def _runtime(model: str) -> LanguageModelRuntime:
    provider = OllamaLanguageModelProvider(
        session_id=CLI_AI_SESSION_ID,
        display_name="Default Ollama",
        base_url=DEFAULT_BASE_URL,
        models=(model,),
    )
    return LanguageModelRuntime(
        session_id=CLI_AI_SESSION_ID,
        providers=(provider,),
        policy=AIRuntimePolicy(max_fallbacks=0),
    )


def main() -> int:
    args = parse_args()
    root = repo_root_from(args.root)
    chunks = load_or_build_chunks(root, use_cache=not args.no_cache, rebuild=args.rebuild_index)
    symbols = build_symbols(root)
    selected = retrieve(args.question, chunks, limit=args.limit, symbols=symbols)

    if not selected:
        print("No relevant repository context found.")
        return 1

    context = _bounded_context(format_context(selected), args.max_context_chars)
    source_labels = [chunk.source_label() for chunk in selected]
    if args.show_sources:
        print("Sources:")
        for source in source_labels:
            print(f"- {source}")
        print()
    if args.dry_run:
        print(context)
        return 0

    prompt_builder = build_extractive_prompt if args.extractive else build_prompt
    prompt = prompt_builder(args.question, context, sources=source_labels)
    request_token = uuid4().hex
    logical_request = AIRuntimeRequest(
        request_id=f"request-{request_token}",
        session_id=CLI_AI_SESSION_ID,
        idempotency_key=f"request-{request_token}",
        model=args.model,
        modality=AIModality.TEXT,
        prompt=prompt,
        system_prompt=SYSTEM_PROMPT.strip(),
        timeout_seconds=120,
    )
    try:
        result = _runtime(args.model).execute(logical_request)
    except AIRuntimeError as exc:
        print(f"{exc.code}: {exc.message}")
        return 2

    print(append_grounding_warning(result.content, selected))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
