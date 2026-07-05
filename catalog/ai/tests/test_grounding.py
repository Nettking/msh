from __future__ import annotations

from catalog.ai.grounding import append_grounding_warning, unsupported_references
from catalog.ai.repo_index import Chunk


def test_unsupported_references_detects_invented_file_paths() -> None:
    chunks = [Chunk(path="catalog/flask_app/routes.py", start_line=1, end_line=10, text="control route")]
    answer = "See catalog/flask_app/routes.py and controllers/workflows/workflow_controller.py."

    assert unsupported_references(answer, chunks) == {"controllers/workflows/workflow_controller.py"}


def test_append_grounding_warning_mentions_unsupported_paths() -> None:
    chunks = [Chunk(path="catalog/flask_app/routes.py", start_line=1, end_line=10, text="control route")]
    answer = "The control route is in controllers/status_controller.py."

    guarded = append_grounding_warning(answer, chunks)

    assert "Grounding warning" in guarded
    assert "controllers/status_controller.py" in guarded


def test_append_grounding_warning_leaves_supported_answer_unchanged() -> None:
    chunks = [Chunk(path="catalog/flask_app/routes.py", start_line=1, end_line=10, text="control route")]
    answer = "The control route is in catalog/flask_app/routes.py."

    assert append_grounding_warning(answer, chunks) == answer
