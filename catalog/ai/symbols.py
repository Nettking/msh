"""Repository symbol indexing for the AI explainer.

The symbol index is intentionally small and standard-library only. It extracts
Python functions, classes, and Flask-style route decorators so route and symbol
questions can retrieve implementation chunks before general lexical search.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .repo_index import iter_indexed_files

ROUTE_DECORATOR_NAMES = {"route", "get", "post", "put", "patch", "delete"}


@dataclass(frozen=True)
class Symbol:
    kind: str
    name: str
    path: str
    start_line: int
    end_line: int
    detail: str = ""

    def label(self) -> str:
        return f"{self.kind}:{self.name} {self.path}:{self.start_line}-{self.end_line}"


def _decorator_name(decorator: ast.AST) -> str | None:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    if isinstance(target, ast.Attribute):
        return target.attr
    if isinstance(target, ast.Name):
        return target.id
    return None


def _route_path(decorator: ast.AST) -> str | None:
    if not isinstance(decorator, ast.Call):
        return None
    name = _decorator_name(decorator)
    if name not in ROUTE_DECORATOR_NAMES:
        return None
    if not decorator.args:
        return None
    first = decorator.args[0]
    if isinstance(first, ast.Constant) and isinstance(first.value, str) and first.value.startswith("/"):
        return first.value
    return None


def _node_end_line(node: ast.AST) -> int:
    end_lineno = getattr(node, "end_lineno", None)
    lineno = getattr(node, "lineno", 1)
    return int(end_lineno or lineno)


def _symbol_start_line(node: ast.AST) -> int:
    decorator_lines = [getattr(decorator, "lineno", None) for decorator in getattr(node, "decorator_list", [])]
    decorator_lines = [line for line in decorator_lines if isinstance(line, int)]
    if decorator_lines:
        return min(decorator_lines)
    return int(getattr(node, "lineno", 1))


def extract_python_symbols(path: Path, root: Path) -> list[Symbol]:
    """Extract functions, classes, and Flask routes from a Python file."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, SyntaxError, UnicodeDecodeError):
        return []

    rel = path.relative_to(root).as_posix()
    symbols: list[Symbol] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            start_line = _symbol_start_line(node)
            end_line = _node_end_line(node)
            symbols.append(Symbol("function", node.name, rel, start_line, end_line))
            for decorator in node.decorator_list:
                route = _route_path(decorator)
                if route:
                    symbols.append(Symbol("flask_route", route, rel, start_line, end_line, detail=node.name))
        elif isinstance(node, ast.ClassDef):
            symbols.append(Symbol("class", node.name, rel, _symbol_start_line(node), _node_end_line(node)))
    return sorted(symbols, key=lambda item: (item.path, item.start_line, item.kind, item.name))


def build_symbols(root: Path) -> list[Symbol]:
    """Build a symbol index from indexed Python files."""
    symbols: list[Symbol] = []
    for path in iter_indexed_files(root):
        if path.suffix == ".py":
            symbols.extend(extract_python_symbols(path, root))
    return symbols


def matching_symbols(question: str, symbols: list[Symbol]) -> list[Symbol]:
    """Return symbols directly mentioned by route, path, or name."""
    question_lower = question.lower()
    matches: list[Symbol] = []
    for symbol in symbols:
        path_lower = symbol.path.lower()
        name_lower = symbol.name.lower()
        detail_lower = symbol.detail.lower()
        if symbol.kind == "flask_route" and name_lower in question_lower:
            matches.append(symbol)
            continue
        if path_lower in question_lower:
            matches.append(symbol)
            continue
        if name_lower and name_lower in question_lower:
            matches.append(symbol)
            continue
        if detail_lower and detail_lower in question_lower:
            matches.append(symbol)
    return matches
