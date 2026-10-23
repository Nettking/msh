"""Each normative synthetic vector must point to executable assertions."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path


def test_every_normative_vector_has_unique_existing_executable_coverage():
    root = Path(__file__).resolve().parents[3]
    matrix = json.loads(
        Path(__file__).with_name("acceptance_vectors.json").read_text(encoding="utf-8")
    )
    assert matrix["schema"] == "fcp.process_twin.synthetic_acceptance.v1"
    spec = (root / matrix["specification"]).read_text(encoding="utf-8")
    normative = set(re.findall(r"\| (BV-\d{2}) \|", spec))
    ids = [row["id"] for row in matrix["vectors"]]
    assert len(ids) == len(set(ids)) == 49
    assert set(ids) == normative
    for row in matrix["vectors"]:
        assert row["tests"]
        for selector in row["tests"]:
            path = (root / selector["file"]).resolve()
            assert path.is_relative_to(root) and path.is_file()
            functions = {
                node.name
                for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
                if isinstance(node, ast.FunctionDef)
            }
            assert (
                selector["function"].startswith("test_")
                and selector["function"] in functions
            )
