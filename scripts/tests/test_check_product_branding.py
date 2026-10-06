from __future__ import annotations

from scripts.check_product_branding import LEGACY, searchable_text


def test_exact_host_name_and_path_are_not_treated_as_product_branding() -> None:
    docs = "docs/implementation/nitro_artifact_archive.md"
    fixture = "scripts/windows/tests/test_recorder_pause_resume_guard.py"

    host_phrase = LEGACY.upper() + "-to-Nitro Recorder backup"
    host_path = "C:\\" + LEGACY + "\\git\\data"

    assert LEGACY not in searchable_text(docs, host_phrase).casefold()
    assert LEGACY not in searchable_text(fixture, host_path).casefold()


def test_host_allowlist_does_not_hide_adjacent_or_unrelated_mentions() -> None:
    docs = "docs/implementation/nitro_artifact_archive.md"
    fixture = "scripts/windows/tests/test_recorder_pause_resume_guard.py"

    adjacent_phrase = LEGACY.upper() + "-branded product"
    adjacent_path = "C:\\" + LEGACY + "\\gitlab\\data"

    assert LEGACY in searchable_text(docs, adjacent_phrase).casefold()
    assert LEGACY in searchable_text(fixture, adjacent_path).casefold()
