from __future__ import annotations

from catalog.flask_app.services.operator_strategy_service import OperatorStrategyService
from catalog.flask_app.services.operator_support_service import OperatorSupportService
from catalog.flask_app.services.strategy_comparison_service import StrategyComparisonService


def test_support_card_ids_are_stable_across_page_loads(tmp_path):
    notes = OperatorStrategyService(tmp_path / "notes.json")
    note = notes.add_from_form({"raw_statement": "Chatter appeared, so I inspected the insert."})
    assert notes.update_structure(
        note.id,
        {"issue": "chatter", "decision": "Inspect the insert.", "reusable_strategy": "on"},
    )[0]
    service = OperatorSupportService(note_service=notes)

    first_ids = [card["id"] for card in service.support_cards()]
    second_ids = [card["id"] for card in service.support_cards()]

    assert first_ids == second_ids
    assert "template-chatter" in first_ids
    assert f"note-{note.id}" in first_ids


def test_strategy_comparison_excludes_unreviewed_capture_notes(tmp_path):
    notes = OperatorStrategyService(tmp_path / "notes.json")
    notes.add_from_form({"raw_statement": "Unreviewed field note."})
    reviewed = notes.add_from_form({"raw_statement": "Reviewed field note."})
    assert notes.update_structure(reviewed.id, {"issue": "chatter", "decision": "Inspect insert."})[0]

    comparisons = StrategyComparisonService(note_service=notes).comparisons()

    assert len(comparisons) == 1
    assert comparisons[0]["situation"] == "chatter"
    assert comparisons[0]["total_notes"] == 1
