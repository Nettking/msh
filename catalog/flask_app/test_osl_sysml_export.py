from __future__ import annotations

from catalog.flask_app.services.operator_strategy_service import OperatorStrategyService
from catalog.flask_app.services.osl_export_service import OslExportService


class Form(dict):
    def getlist(self, key: str):
        value = self.get(key, [])
        return value if isinstance(value, list) else [value]


def test_osl_export_preview_is_sysml(tmp_path):
    notes = OperatorStrategyService(tmp_path / "notes.json")
    note = notes.add_from_form(
        Form(
            {
                "issue": "chatter",
                "decision": "Inspect insert before changing offsets.",
                "action_type": "inspect_part",
                "rationale": "Safe first check before changing dimensions.",
                "risk": "Inspection adds downtime.",
                "worked": "yes",
                "outcome": "Surface improved.",
                "reusable_strategy": "on",
            }
        )
    )
    notes.mark_reusable(note.id, reusable=True)

    preview = OslExportService(note_service=notes, export_path=tmp_path / "operator_strategies.sysml").preview()

    assert "package MSH_OperatorStrategies" in preview
    assert "part def OperatorStrategy" in preview
    assert "part Chatter_" in preview
    assert "Inspect insert before changing offsets" in preview
    assert "schema:" not in preview
