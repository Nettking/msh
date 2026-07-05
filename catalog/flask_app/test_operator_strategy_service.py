from __future__ import annotations

import pytest

from catalog.flask_app.services.operator_strategy_service import OperatorStrategyError, OperatorStrategyService


def test_operator_strategy_quick_save_requires_only_decision(tmp_path):
    service = OperatorStrategyService(tmp_path / "records.json")

    record = service.add_from_form({"decision": "Reduced feed override when chatter appeared."})

    assert record.decision == "Reduced feed override when chatter appeared."
    assert record.goal == ""
    assert record.strategy_name == ""
    assert record.strategy_situation == ""
    assert service.recent_records()[0]["decision"] == record.decision


def test_operator_strategy_quick_save_still_requires_decision(tmp_path):
    service = OperatorStrategyService(tmp_path / "records.json")

    with pytest.raises(OperatorStrategyError, match="Decision/action is required"):
        service.add_from_form({"goal": "Avoid scrap"})
