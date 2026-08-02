"""Read-only repository documentation browser for the Flask application."""

from __future__ import annotations

import posixpath
import re
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit, urlunsplit

import markdown
from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    make_response,
    render_template,
    request,
    send_file,
    url_for,
)
from markdown.extensions import Extension
from markdown.treeprocessors import Treeprocessor

DOC_SUFFIXES = {".md", ".markdown"}
ASSET_SUFFIXES = {
    ".gif",
    ".jpeg",
    ".jpg",
    ".png",
    ".svg",
    ".webp",
}


docs_web = Blueprint("docs_web", __name__, url_prefix="/docs")


def _repository_root() -> Path:
    configured = current_app.config.get("MSH_REPOSITORY_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[2]


def _docs_root() -> Path:
    configured = current_app.config.get("MSH_DOCS_ROOT")
    if configured:
        root = Path(configured).expanduser()
        if not root.is_absolute():
            root = _repository_root() / root
        return root.resolve()
    return (_repository_root() / "docs").resolve()


def _prettify(value: str) -> str:
    value = re.sub(r"^\d+[\s_.-]*", "", value)
    value = value.replace("-", " ").replace("_", " ")
    return value.strip().title() or value


def _plain_heading(value: str) -> str:
    value = re.sub(r"!\[([^]]*)]\([^)]*\)", r"\1", value)
    value = re.sub(r"\[([^]]+)]\([^)]*\)", r"\1", value)
    return value.replace("`", "").replace("*", "").strip()


def _document_title(path: Path) -> str:
    fallback = _prettify(path.stem)
    try:
        with path.open("r", encoding="utf-8") as handle:
            for _ in range(80):
                line = handle.readline()
                if not line:
                    break
                match = re.match(r"^#\s+(.+?)\s*$", line)
                if match:
                    return _plain_heading(match.group(1)) or fallback
    except (OSError, UnicodeError):
        return fallback
    return fallback


def _safe_path(relative_path: str) -> Path:
    decoded = unquote(relative_path).replace("\\", "/").strip("/")
    pure = PurePosixPath(decoded)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        abort(404)

    root = _docs_root()
    candidate = (root / Path(*pure.parts)).resolve()
    if candidate != root and root not in candidate.parents:
        abort(404)
    return candidate


def _is_readable_document(path: Path) -> bool:
    return (
        path.is_file()
        and not path.is_symlink()
        and path.suffix.casefold() in DOC_SUFFIXES
    )


def _resolve_document(relative_path: str) -> Path | None:
    clean = relative_path.strip("/")
    root = _docs_root()

    if not clean:
        candidates = [root / "index.md", root / "README.md"]
    else:
        base = _safe_path(clean)
        if base.suffix.casefold() in DOC_SUFFIXES:
            candidates = [base]
        elif base.suffix:
            candidates = []
        else:
            candidates = [
                base.with_suffix(".md"),
                base / "index.md",
                base / "README.md",
            ]

    for candidate in candidates:
        if _is_readable_document(candidate):
            return candidate
    return None


def _relative_document_path(path: Path) -> str:
    return path.relative_to(_docs_root()).as_posix()


def _build_tree() -> tuple[dict[str, object], int]:
    root = _docs_root()
    total = 0

    def walk(directory: Path, relative: str = "") -> dict[str, object]:
        nonlocal total
        node: dict[str, object] = {
            "name": "Documentation" if not relative else _prettify(directory.name),
            "path": relative,
            "dirs": [],
            "files": [],
        }
        if not directory.is_dir() or directory.is_symlink():
            return node

        try:
            entries = sorted(
                directory.iterdir(),
                key=lambda entry: (not entry.is_dir(), entry.name.casefold()),
            )
        except OSError:
            return node

        for entry in entries:
            if entry.name.startswith(".") or entry.is_symlink():
                continue
            entry_relative = f"{relative}/{entry.name}" if relative else entry.name
            if entry.is_dir():
                child = walk(entry, entry_relative)
                if child["dirs"] or child["files"]:
                    node["dirs"].append(child)
                continue
            if entry.suffix.casefold() not in DOC_SUFFIXES:
                continue
            total += 1
            node["files"].append(
                {
                    "name": _document_title(entry),
                    "path": entry_relative,
                    "url": url_for(
                        "docs_web.document",
                        document_path=entry_relative,
                    ),
                }
            )
        return node

    return walk(root), total


def _breadcrumbs(document_path: str, title: str) -> list[dict[str, str | None]]:
    parts = PurePosixPath(document_path).parts
    crumbs: list[dict[str, str | None]] = [
        {"label": "Documentation", "url": url_for("docs_web.index")}
    ]
    for part in parts[:-1]:
        crumbs.append({"label": _prettify(part), "url": None})
    crumbs.append({"label": title, "url": None})
    return crumbs


def _append_query_fragment(url: str, query: str, fragment: str) -> str:
    return urlunsplit(("", "", url, query, fragment))


class _DocumentationLinkProcessor(Treeprocessor):
    def __init__(self, markdown_instance, document_path: str):
        super().__init__(markdown_instance)
        self.document_path = document_path

    def run(self, root):  # type: ignore[no-untyped-def]
        for element in root.iter():
            if element.tag == "a":
                self._rewrite(element, "href", is_image=False)
            elif element.tag == "img":
                self._rewrite(element, "src", is_image=True)
        return root

    def _rewrite(self, element, attribute: str, *, is_image: bool) -> None:  # type: ignore[no-untyped-def]
        value = element.get(attribute)
        if not value:
            return
        parsed = urlsplit(value)
        if parsed.scheme or parsed.netloc or value.startswith(("/", "#")):
            return
        if not parsed.path:
            return

        parent = PurePosixPath(self.document_path).parent.as_posix()
        target = posixpath.normpath(posixpath.join(parent, unquote(parsed.path)))
        if target == ".." or target.startswith("../"):
            element.set(attribute, "#")
            return

        suffix = PurePosixPath(target).suffix.casefold()
        if is_image:
            if suffix not in ASSET_SUFFIXES:
                return
            target_url = url_for("docs_web.asset", asset_path=target)
        else:
            document = _resolve_document(target)
            if document is not None:
                target_url = url_for(
                    "docs_web.document",
                    document_path=_relative_document_path(document),
                )
            elif suffix in ASSET_SUFFIXES:
                target_url = url_for("docs_web.asset", asset_path=target)
            else:
                return

        element.set(
            attribute,
            _append_query_fragment(target_url, parsed.query, parsed.fragment),
        )


class _DocumentationLinkExtension(Extension):
    def __init__(self, document_path: str):
        self.document_path = document_path
        super().__init__()

    def extendMarkdown(self, md):  # type: ignore[no-untyped-def, N802]
        md.treeprocessors.register(
            _DocumentationLinkProcessor(md, self.document_path),
            "msh_documentation_links",
            1,
        )


def _render_document(path: Path) -> tuple[str, str, str]:
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        abort(404)

    document_path = _relative_document_path(path)
    renderer = markdown.Markdown(
        extensions=[
            "extra",
            "toc",
            "sane_lists",
            "admonition",
            _DocumentationLinkExtension(document_path),
        ],
        extension_configs={"toc": {"permalink": True}},
        output_format="html5",
    )
    html = renderer.convert(source)
    return _document_title(path), html, renderer.toc


def _viewer_response(**context: object) -> Response:
    response = make_response(render_template("docs_viewer.html", **context))
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    return response


def _render_index() -> Response:
    tree, document_count = _build_tree()
    return _viewer_response(
        tree=tree,
        document_count=document_count,
        docs_root_exists=_docs_root().is_dir(),
        current_path="",
        document=None,
        document_title="MSH Documentation",
        document_html="",
        document_toc="",
        breadcrumbs=[{"label": "Documentation", "url": None}],
    )


def _render_document_response(document_path: str) -> Response:
    path = _resolve_document(document_path)
    if path is None:
        abort(404)

    canonical_path = _relative_document_path(path)
    title, html, toc = _render_document(path)
    tree, document_count = _build_tree()
    return _viewer_response(
        tree=tree,
        document_count=document_count,
        docs_root_exists=True,
        current_path=canonical_path,
        document=canonical_path,
        document_title=title,
        document_html=html,
        document_toc=toc,
        breadcrumbs=_breadcrumbs(canonical_path, title),
    )


@docs_web.before_app_request
def _serve_read_only_docs_before_runtime_gate() -> Response | None:
    """Keep GET-only docs available while the runtime startup gate is active.

    The main web blueprint owns a global startup gate. Documentation is a
    read-only repository surface with no runtime authority, so its matched
    view is dispatched before that gate. Register this blueprint before the
    main web blueprint so this hook runs first.
    """

    if request.blueprint != docs_web.name or request.method not in {"GET", "HEAD"}:
        return None
    endpoint = request.endpoint or ""
    view = current_app.view_functions.get(endpoint)
    if view is None:
        return None
    return current_app.ensure_sync(view)(**(request.view_args or {}))


@docs_web.get("/", strict_slashes=False)
def index() -> Response:
    return _render_index()


@docs_web.get("/_assets/<path:asset_path>")
def asset(asset_path: str) -> Response:
    path = _safe_path(asset_path)
    if (
        not path.is_file()
        or path.is_symlink()
        or path.suffix.casefold() not in ASSET_SUFFIXES
    ):
        abort(404)
    response = make_response(send_file(path, conditional=True, max_age=3600))
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@docs_web.get("/<path:document_path>")
def document(document_path: str) -> Response:
    return _render_document_response(document_path)
