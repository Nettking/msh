"""Command-line entry point for the read-only MSH AI explainer.

Usage:
    python -m catalog.ai.ask "How does data flow through MSH?"
    python -m catalog.ai.ask --dry-run "What does /control do?"
"""

from __future__ import annotations

import argparse
from pathlib import Path

from .ollama_client import DEFAULT_MODEL, OllamaError, chat
from .prompts import SYSTEM_PROMPT, build_prompt
from .rag import format_context, retrieve
from .repo_index import build_chunks, repo_root_from


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ask a read-only question about the MSH repository.")
    parser.add_argument("question", help="Question to ask about the repository.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Ollama model name.")
    parser.add_argument("--root", type=Path, default=None, help="Repository root. Defaults to auto-detection.")
    parser.add_argument("--limit", type=int, default=8, help="Number of context chunks to retrieve.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print retrieved context only. Does not call Ollama.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = repo_root_from(args.root)
    chunks = build_chunks(root)
    selected = retrieve(args.question, chunks, limit=args.limit)

    if not selected:
        print("No relevant repository context found.")
        return 1

    context = format_context(selected)
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
