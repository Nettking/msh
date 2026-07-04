"""Command-line entry point for the read-only MSH AI explainer.

Usage:
    python -m catalog.ai.ask "How does data flow through MSH?"
    python -m catalog.ai.ask --dry-run "What does /control do?"
    python -m catalog.ai.ask --show-sources "Where are workflow artifacts stored?"
"""

from __future__ import annotations

import argparse
from pathlib import Path

from .ollama_client import DEFAULT_MODEL, OllamaError, chat
from .prompts import SYSTEM_PROMPT, build_prompt
from .rag import format_context, retrieve
from .repo_index import load_or_build_chunks, repo_root_from


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


def main() -> int:
    args = parse_args()
    root = repo_root_from(args.root)
    chunks = load_or_build_chunks(root, use_cache=not args.no_cache, rebuild=args.rebuild_index)
    selected = retrieve(args.question, chunks, limit=args.limit)

    if not selected:
        print("No relevant repository context found.")
        return 1

    context = _bounded_context(format_context(selected), args.max_context_chars)
    if args.show_sources:
        print("Sources:")
        for chunk in selected:
            print(f"- {chunk.source_label()}")
        print()
    if args.dry_run:
        print(context)
        return 0

    prompt = build_prompt(args.question, context)
    try:
        answer = chat(prompt=prompt, system_prompt=SYSTEM_PROMPT, model=args.model)
    except OllamaError as exc:
        print(exc)
        return 2

    print(answer)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
