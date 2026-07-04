"""Simple lexical retrieval for the read-only AI explainer."""

from __future__ import annotations

from .repo_index import Chunk, tokenize


def score_chunk(question_tokens: set[str], chunk: Chunk) -> int:
    """Score a chunk by token overlap with a small path bonus."""
    text_tokens = tokenize(chunk.text)
    path_tokens = tokenize(chunk.path.replace("/", " "))
    return len(question_tokens & text_tokens) + (2 * len(question_tokens & path_tokens))


def retrieve(question: str, chunks: list[Chunk], limit: int = 8) -> list[Chunk]:
    """Return the most relevant chunks for a question."""
    question_tokens = tokenize(question)
    scored = [
        (score_chunk(question_tokens, chunk), chunk)
        for chunk in chunks
    ]
    scored = [(score, chunk) for score, chunk in scored if score > 0]
    scored.sort(key=lambda item: item[0], reverse=True)
    return [chunk for _, chunk in scored[:limit]]


def format_context(chunks: list[Chunk]) -> str:
    """Format retrieved chunks for the language model."""
    return "\n\n".join(chunk.format_for_prompt() for chunk in chunks)
