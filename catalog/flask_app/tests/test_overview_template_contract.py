from pathlib import Path
import re


def test_overview_template_is_snapshot_driven() -> None:
    template = Path('catalog/flask_app/templates/overview.html').read_text(encoding='utf-8')

    forbidden_patterns = [
        r"\{\{\s*total\b",
        r"\{\{\s*derived_total\b",
        r"\{\{\s*source_total\b",
        r"\{\{\s*playback_count\b",
        r"\{\{\s*read_errors\b",
        r"\{\{\s*hidden_workflow_copies\b",
        r"\{\{\s*hidden_internal_metadata\b",
        r"\{%\s*if\s+warnings\b",
        r"\{%\s*for\s+a\s+in\s+artifacts\b",
        r"<th>\s*Category\s*</th>",
        r"<th>\s*Analysis\s*</th>",
        r"<th>\s*File\s*</th>",
        r"<th>\s*Kind\s*</th>",
        r"<th>\s*Path\s*</th>",
    ]

    for pattern in forbidden_patterns:
        assert re.search(pattern, template) is None, f'overview.html should not contain legacy token pattern: {pattern}'

    assert 'overview.headline.' in template
    assert 'overview.activity.' in template
    assert 'overview.runtime.' in template
    assert 'overview.readiness' in template
    assert 'overview.quick_links' in template
    assert 'overview.warnings' in template
