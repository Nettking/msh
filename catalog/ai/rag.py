"""Simple lexical retrieval for the read-only AI explainer."""

from __future__ import annotations

import re

from .repo_index import Chunk, tokenize

PATH_QUERY_RE = re.compile(r"\b[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)+(?:\.[A-Za-z0-9_]+)?\b")
ROUTE_QUERY_RE = re.compile(r"(?<!\w)/[A-Za-z0-9_./<>-]+")


def _query_paths(question: str) -> set[str]:
    return {match.group(0).strip("`.,:;)]") for match in PATH_QUERY_RE.finditer(question)}


def _query_routes(question: str) -> set[str]:
    return {match.group(0).strip("`.,:;)]") for match in ROUTE_QUERY_RE.finditer(question)}


def _literal_bonus(question: str, chunk: Chunk) -> int:
    """Boost exact path, route, decorator, and function-name matches."""
    bonus = 0
    chunk_text = chunk.text
    question_lower = question.lower()
    chunk_text_lower = chunk_text.lower()
    chunk_path_lower = chunk.path.lower()

    for path in _query_paths(question):
        path_lower = path.lower()
        if path_lower == chunk_path_lower:
            bonus += 100
        elif chunk_path_lower.endswith(path_lower) or path_lower.endswith(chunk_path_lower):
            bonus += 60
        elif path_lower in chunk_path_lower:
            bonus += 40

    for route in _query_routes(question):
        route_lower = route.lower()
        if route_lower in chunk_text_lower:
            bonus += 80
        decorator_fragments = (
            f'route("{route_lower}"',
            f"route('{route_lower}'",
            f'get("{route_lower}"',
            f"get('{route_lower}'",
            f'post("{route_lower}"',
            f"post('{route_lower}'",
        )
        if any(fragment in chunk_text_lower for fragment in decorator_fragments):
            bonus += 40

    for token in tokenize(question):
        if token in {"control", "playback", "status", "startup", "strategies", "exploration", "machine", "live"}:
            if f"def {token}" in chunk_text_lower:
                bonus += 50
            if f"/{token}" in chunk_text_lower:
                bonus += 40

    if "route" in question_lower and "@web." in chunk_text_lower:
        bonus += 20
    return bonus


def score_chunk(question: str, chunk: Chunk) -> int:
    """Score a chunk by token overlap plus exact path/route boosts."""
    question_tokens = tokenize(question)
    text_tokens = tokenize(chunk.text)
    path_tokens = tokenize(chunk.path.replace("/", " "))
    lexical_score = len(question_tokens & text_tokens) + (2 * len(question_tokens & path_tokens))
    return lexical_score + _literal_bonus(question, chunk)


def retrieve(question: str, chunks: list[Chunk], limit: int = 8) -> list[Chunk]:
    """Return the most relevant chunks for a question."""
    scored = [(score_chunk(question, chunk), chunk) for chunk in chunks]
    scored = [(score, chunk) for score, chunk in scored if score > 0]
    scored.sort(key=lambda item: item[0], reverse=True)
    return [chunk for _, chunk in scored[:limit]]


def format_context(chunks: list[Chunk]) -> str:
    """Format retrieved chunks for the language model."""
    return "\n\n".join(chunk.format_for_prompt() for chunk in chunks)
