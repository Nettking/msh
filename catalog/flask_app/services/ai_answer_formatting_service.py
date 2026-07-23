"""Conservative Markdown rendering for AI Explainer answers."""

from __future__ import annotations

from html import escape
import re


_FENCE_RE = re.compile(r"^```(?P<language>[A-Za-z0-9_+-]*)\s*$")
_HEADING_RE = re.compile(r"^(?P<marks>#{1,3})\s+(?P<text>.+)$")
_UNORDERED_RE = re.compile(r"^\s*[-*]\s+(?P<text>.+)$")
_ORDERED_RE = re.compile(r"^\s*\d+\.\s+(?P<text>.+)$")
_INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")
_STRONG_RE = re.compile(r"\*\*(.+?)\*\*")


def _render_inline(text: str) -> str:
    """Escape arbitrary text, then add a deliberately small inline subset."""
    escaped = escape(text, quote=True)
    code_fragments: list[str] = []

    def stash_code(match: re.Match[str]) -> str:
        code_fragments.append(f"<code>{match.group(1)}</code>")
        return f"\x00CODE{len(code_fragments) - 1}\x00"

    rendered = _INLINE_CODE_RE.sub(stash_code, escaped)
    rendered = _STRONG_RE.sub(r"<strong>\1</strong>", rendered)
    for index, fragment in enumerate(code_fragments):
        rendered = rendered.replace(f"\x00CODE{index}\x00", fragment)
    return rendered


def _consume_list(lines: list[str], start: int, pattern: re.Pattern[str], tag: str) -> tuple[str, int]:
    items: list[str] = []
    index = start
    while index < len(lines):
        match = pattern.match(lines[index])
        if not match:
            break
        items.append(f"<li>{_render_inline(match.group('text'))}</li>")
        index += 1
    return f"<{tag}>{''.join(items)}</{tag}>", index


def render_safe_markdown(text: str) -> str:
    """Render common answer structure without allowing model-supplied HTML."""
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    rendered: list[str] = []
    paragraph: list[str] = []
    index = 0

    def flush_paragraph() -> None:
        if paragraph:
            rendered.append(f"<p>{_render_inline(' '.join(paragraph))}</p>")
            paragraph.clear()

    while index < len(lines):
        line = lines[index]
        fence = _FENCE_RE.match(line)
        if fence:
            flush_paragraph()
            language = fence.group("language")
            code_lines: list[str] = []
            index += 1
            while index < len(lines) and not _FENCE_RE.match(lines[index]):
                code_lines.append(lines[index])
                index += 1
            language_class = f' class="language-{language}"' if language else ""
            rendered.append(
                f"<pre><code{language_class}>{escape(chr(10).join(code_lines), quote=True)}</code></pre>"
            )
            index += 1
            continue

        heading = _HEADING_RE.match(line)
        if heading:
            flush_paragraph()
            level = len(heading.group("marks"))
            rendered.append(f"<h{level}>{_render_inline(heading.group('text'))}</h{level}>")
            index += 1
            continue

        if _UNORDERED_RE.match(line):
            flush_paragraph()
            list_html, index = _consume_list(lines, index, _UNORDERED_RE, "ul")
            rendered.append(list_html)
            continue

        if _ORDERED_RE.match(line):
            flush_paragraph()
            list_html, index = _consume_list(lines, index, _ORDERED_RE, "ol")
            rendered.append(list_html)
            continue

        if not line.strip():
            flush_paragraph()
            index += 1
            continue

        paragraph.append(line.strip())
        index += 1

    flush_paragraph()
    return "\n".join(rendered)
