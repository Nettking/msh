from __future__ import annotations

from catalog.ai.grounding import append_grounding_warning, referenced_paths, unsupported_references
from catalog.ai.repo_index import Chunk


def test_unsupported_references_detects_invented_repo_file_paths() -> None:
    chunks = [Chunk(path="catalog/flask_app/routes.py", start_line=1, end_line=10, text="control route")]
    answer = "See catalog/flask_app/routes.py and catalog/workflows/workflow_controller.py."

    assert unsupported_references(answer, chunks) == {"catalog/workflows/workflow_controller.py"}


def test_referenced_paths_ignores_routes_and_slash_separated_phrases() -> None:
    answer = "POST /control/action, machine/day summaries, and phase/date/progress are not file paths."

    assert referenced_paths(answer) == set()


def test_append_grounding_warning_mentions_unsupported_repo_files() -> None:
    chunks = [Chunk(path="catalog/flask_app/routes.py", start_line=1, end_line=10, text="control route")]
    answer = "The control route is in catalog/common/artifact_registry.py."

    guarded = append_grounding_warning(answer, chunks)

    assert "Grounding warning" in guarded
    assert "catalog/common/artifact_registry.py" in guarded


def test_append_grounding_warning_leaves_supported_answer_unchanged() -> None:
    chunks = [Chunk(path="catalog/flask_app/routes.py", start_line=1, end_line=10, text="control route")]
    answer = "The control route is in catalog/flask_app/routes.py."

    assert append_grounding_warning(answer, chunks) == answer
