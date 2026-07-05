"""Retrieval for the read-only AI explainer."""

from __future__ import annotations

import re

from .repo_index import Chunk, tokenize
from .symbols import Symbol, matching_symbols

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


def _chunk_overlaps_symbol(chunk: Chunk, symbol: Symbol) -> bool:
    if chunk.path != symbol.path:
        return False
    return chunk.start_line <= symbol.end_line and chunk.end_line >= symbol.start_line


def _symbol_chunks(question: str, chunks: list[Chunk], symbols: list[Symbol] | None) -> list[Chunk]:
    if not symbols:
        return []
    selected: list[Chunk] = []
    seen: set[tuple[str, int, int]] = set()
    for symbol in matching_symbols(question, symbols):
        for chunk in chunks:
            key = (chunk.path, chunk.start_line, chunk.end_line)
            if key not in seen and _chunk_overlaps_symbol(chunk, symbol):
                selected.append(chunk)
                seen.add(key)
    return selected


def score_chunk(question: str, chunk: Chunk) -> int:
    """Score a chunk by token overlap plus exact path/route boosts."""
    question_tokens = tokenize(question)
    text_tokens = tokenize(chunk.text)
    path_tokens = tokenize(chunk.path.replace("/", " "))
    lexical_score = len(question_tokens & text_tokens) + (2 * len(question_tokens & path_tokens))
    return lexical_score + _literal_bonus(question, chunk)


def retrieve(question: str, chunks: list[Chunk], limit: int = 8, symbols: list[Symbol] | None = None) -> list[Chunk]:
    """Return the most relevant chunks for a question."""
    prioritized = _symbol_chunks(question, chunks, symbols)
    prioritized_keys = {(chunk.path, chunk.start_line, chunk.end_line) for chunk in prioritized}

    scored = [(score_chunk(question, chunk), chunk) for chunk in chunks]
    scored = [
        (score, chunk)
        for score, chunk in scored
        if score > 0 and (chunk.path, chunk.start_line, chunk.end_line) not in prioritized_keys
    ]
    scored.sort(key=lambda item: item[0], reverse=True)
    lexical = [chunk for _, chunk in scored]
    return (prioritized + lexical)[:limit]


def format_context(chunks: list[Chunk]) -> str:
    """Format retrieved chunks for the language model."""
    return "\n\n".join(chunk.format_for_prompt() for chunk in chunks)
