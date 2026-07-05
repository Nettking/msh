from __future__ import annotations

from pathlib import Path

from catalog.ai.rag import retrieve
from catalog.ai.repo_index import build_chunks, iter_indexed_files, repo_root_from, should_index
from catalog.ai.symbols import build_symbols


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_repo_root_from_finds_root(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write(root / "README.md", "# Test repo\n")
    (root / "catalog").mkdir()

    assert repo_root_from(root / "catalog") == root


def test_should_index_includes_docs_and_catalog_but_excludes_data_and_results(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write(root / "README.md", "# Test repo\n")
    docs_file = _write(root / "docs" / "architecture.md", "Architecture\n")
    code_file = _write(root / "catalog" / "common" / "helper.py", "def helper():\n    return True\n")
    data_file = _write(root / "data" / "2026-01-01.jsonl", "{}\n")
    result_file = _write(root / "results" / "out.csv", "a,b\n")

    assert should_index(docs_file, root)
    assert should_index(code_file, root)
    assert not should_index(data_file, root)
    assert not should_index(result_file, root)


def test_iter_indexed_files_excludes_legacy_and_binary_like_outputs(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    readme = _write(root / "README.md", "# Test repo\n")
    included = _write(root / "catalog" / "runner" / "README.md", "Runner docs\n")
    _write(root / "legacy" / "old.md", "Old docs\n")
    _write(root / "catalog" / "cache" / "part.parquet", "not real parquet\n")

    paths = {path.relative_to(root).as_posix() for path in iter_indexed_files(root)}

    assert readme.relative_to(root).as_posix() in paths
    assert included.relative_to(root).as_posix() in paths
    assert "legacy/old.md" not in paths
    assert "catalog/cache/part.parquet" not in paths


def test_retrieve_prefers_matching_text_and_path(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write(root / "README.md", "Overview\n")
    _write(root / "docs" / "architecture.md", "The playback pipeline creates timeline exports for Flask views.\n")
    _write(root / "catalog" / "ai" / "notes.md", "Local AI explainer notes.\n")

    chunks = build_chunks(root)
    selected = retrieve("How does playback create timeline exports?", chunks, limit=1)

    assert selected
    assert selected[0].path == "docs/architecture.md"


def test_retrieve_boosts_exact_repo_path(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write(root / "README.md", "Control documentation overview.\n")
    _write(root / "catalog" / "flask_app" / "routes.py", "@web.route('/control')\ndef control():\n    return 'ok'\n")

    chunks = build_chunks(root)
    selected = retrieve("what does /control do in catalog/flask_app/routes.py", chunks, limit=1)

    assert selected
    assert selected[0].path == "catalog/flask_app/routes.py"


def test_retrieve_boosts_route_literals(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write(root / "README.md", "Control documentation overview.\n")
    _write(root / "catalog" / "flask_app" / "routes.py", "@web.route('/control')\ndef control():\n    return 'ok'\n")

    chunks = build_chunks(root)
    selected = retrieve("what does the /control route do", chunks, limit=1)

    assert selected
    assert selected[0].path == "catalog/flask_app/routes.py"


def test_symbol_aware_retrieval_prioritizes_matching_route_chunk(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write(root / "README.md", "Control documentation overview.\n")
    _write(
        root / "catalog" / "flask_app" / "routes.py",
        "@web.route('/control')\ndef control():\n    return 'ok'\n\n@web.route('/status')\ndef status():\n    return 'status'\n",
    )

    chunks = build_chunks(root)
    symbols = build_symbols(root)
    selected = retrieve("explain /control", chunks, limit=1, symbols=symbols)

    assert selected
    assert selected[0].path == "catalog/flask_app/routes.py"
    assert "def control" in selected[0].text


def test_retrieve_returns_empty_when_no_context_matches(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write(root / "README.md", "Overview\n")

    chunks = build_chunks(root)

    assert retrieve("unrelated quantum banana", chunks) == []
