"""
Markdown Docs Viewer
---------------------
A small Flask app that walks a folder of .md files (with arbitrary
subfolders) and serves them through a single reading UI with a
left-hand navigation tree that mirrors the folder structure.

Run:
    pip install -r requirements.txt
    python app.py
Then open http://127.0.0.1:5000/

Put your own markdown files (and subfolders) inside the docs/ folder.
"""

import re
from pathlib import Path

from flask import Flask, abort, redirect, render_template, url_for
import markdown

app = Flask(__name__)

# Root folder that holds all the markdown content. Change this if you
# want to point the viewer at a different directory.
DOCS_ROOT = Path(__file__).parent / "docs"

MD_EXTENSIONS = [
    "extra",        # tables, footnotes, fenced code, etc.
    "codehilite",   # syntax highlighting for fenced code blocks
    "toc",          # adds ids to headings + lets us build a page TOC
    "sane_lists",
    "admonition",
]


# --------------------------------------------------------------------------
# Tree building
# --------------------------------------------------------------------------

def build_tree(root: Path) -> dict:
    """Recursively walk `root` and build a nested dict describing every
    folder/subfolder that contains (directly or indirectly) at least one
    markdown file, plus the markdown files themselves.

    Shape:
        {"name": str, "path": str, "dirs": [tree, ...], "files": [file, ...]}
        file = {"name": str, "path": "a/b/c.md", "filename": "c.md"}
    """
    node = {"name": root.name, "path": "", "dirs": [], "files": []}

    def walk(dir_path: Path, rel_path: str, node: dict):
        try:
            entries = sorted(
                dir_path.iterdir(),
                key=lambda p: (p.is_file(), p.name.lower()),
            )
        except FileNotFoundError:
            return

        for entry in entries:
            if entry.name.startswith("."):
                continue

            entry_rel = f"{rel_path}/{entry.name}" if rel_path else entry.name

            if entry.is_dir():
                child = {"name": entry.name, "path": entry_rel, "dirs": [], "files": []}
                walk(entry, entry_rel, child)
                # Only keep folders that (eventually) contain markdown files.
                if child["dirs"] or child["files"]:
                    node["dirs"].append(child)
            elif entry.suffix.lower() in (".md", ".markdown"):
                node["files"].append(
                    {
                        "name": prettify(entry.stem),
                        "path": entry_rel,
                        "filename": entry.name,
                    }
                )

    walk(root, "", node)
    return node


def find_first_file(node: dict):
    """Depth-first search for the first markdown file, used so `/` can
    redirect straight into some readable content."""
    for f in node["files"]:
        return f["path"]
    for d in node["dirs"]:
        result = find_first_file(d)
        if result:
            return result
    return None


def prettify(stem: str) -> str:
    """Turn 'getting-started' or '01_intro' into 'Getting Started' / 'Intro'."""
    stem = re.sub(r"^\d+[\s_.-]*", "", stem)  # drop leading numeric ordering
    stem = stem.replace("-", " ").replace("_", " ")
    return stem.strip().title() or stem


def extract_title(text: str, fallback: str) -> str:
    """Use the first H1 in the document as the page title if present."""
    match = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
    return match.group(1).strip() if match else fallback


def build_breadcrumbs(filepath: str):
    parts = filepath.split("/")
    crumbs = []
    accum = ""
    for i, part in enumerate(parts):
        accum = f"{accum}/{part}" if accum else part
        is_last = i == len(parts) - 1
        label = part.rsplit(".", 1)[0] if is_last else part
        crumbs.append(
            {
                "label": prettify(label),
                "path": accum,
                "is_file": is_last,
            }
        )
    return crumbs


def safe_resolve(filepath: str) -> Path:
    """Resolve a requested path safely inside DOCS_ROOT (no path traversal)."""
    root = DOCS_ROOT.resolve()
    candidate = (root / filepath).resolve()
    if root != candidate and root not in candidate.parents:
        abort(403)
    return candidate


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------

@app.route("/")
def home():
    tree = build_tree(DOCS_ROOT)
    first = find_first_file(tree)
    if first:
        return redirect(url_for("view", filepath=first))
    return render_template(
        "index.html",
        tree=tree,
        content=None,
        current_path="",
        title="No documents found",
        breadcrumbs=[],
    )


@app.route("/view/<path:filepath>")
def view(filepath):
    full_path = safe_resolve(filepath)

    if not full_path.exists() or full_path.suffix.lower() not in (".md", ".markdown"):
        abort(404)

    text = full_path.read_text(encoding="utf-8")
    html_content = markdown.markdown(text, extensions=MD_EXTENSIONS)

    tree = build_tree(DOCS_ROOT)
    title = extract_title(text, prettify(full_path.stem))
    breadcrumbs = build_breadcrumbs(filepath)

    return render_template(
        "index.html",
        tree=tree,
        content=html_content,
        current_path=filepath,
        title=title,
        breadcrumbs=breadcrumbs,
    )


@app.errorhandler(404)
def not_found(_e):
    tree = build_tree(DOCS_ROOT)
    return (
        render_template(
            "index.html",
            tree=tree,
            content=(
                "<h2>Page not found</h2>"
                "<p>That document doesn't exist. Pick something from the "
                "navigation on the left.</p>"
            ),
            current_path="",
            title="Not found",
            breadcrumbs=[],
        ),
        404,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
