from __future__ import annotations

import pytest

from catalog.flask_app.services.operator_strategy_service import OperatorStrategyError, OperatorStrategyService


def test_operator_statement_capture_requires_only_raw_statement(tmp_path):
    service = OperatorStrategyService(tmp_path / "records.json")

    record = service.add_from_form({"raw_statement": "The machine drifts when it is cold."})

    assert record.raw_statement == "The machine drifts when it is cold."
    assert record.decision == "The machine drifts when it is cold."
    assert record.review_status == "captured"
    assert service.recent_records()[0]["raw_statement"] == record.raw_statement


def test_operator_strategy_quick_save_still_supports_decision(tmp_path):
    service = OperatorStrategyService(tmp_path / "records.json")

    record = service.add_from_form({"decision": "Reduced feed override when chatter appeared."})

    assert record.decision == "Reduced feed override when chatter appeared."
    assert record.raw_statement == "Reduced feed override when chatter appeared."


def test_operator_strategy_save_requires_statement_or_decision(tmp_path):
    service = OperatorStrategyService(tmp_path / "records.json")

    with pytest.raises(OperatorStrategyError, match="statement or decision/action is required"):
        service.add_from_form({"goal": "Avoid scrap"})


def test_operator_note_can_be_structured_later(tmp_path):
    service = OperatorStrategyService(tmp_path / "records.json")
    record = service.add_from_form({"raw_statement": "Chatter appeared during finishing, so feed was reduced."})

    ok, message = service.update_structure(
        record.id,
        {
            "issue": "chatter",
            "observation": "chatter appeared during finishing",
            "possible_cause": "cutting force too high",
            "decision": "reduce feed",
            "rationale": "lower cutting force may reduce chatter",
            "reusable_strategy": "on",
        },
    )

    assert ok, message
    structured = service.get_record(record.id)
    assert structured is not None
    assert structured["issue"] == "chatter"
    assert structured["review_status"] == "reusable"
    assert structured["reusable_strategy"] is True
