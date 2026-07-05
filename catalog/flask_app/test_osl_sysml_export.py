from __future__ import annotations

from catalog.flask_app.services.operator_strategy_service import OperatorStrategyService
from catalog.flask_app.services.osl_export_service import OslExportService


class Form(dict):
    def getlist(self, key: str):
        value = self.get(key, [])
        return value if isinstance(value, list) else [value]


def test_osl_export_preview_is_sysml_with_osl_keywords(tmp_path):
    notes = OperatorStrategyService(tmp_path / "notes.json")
    note = notes.add_from_form(
        Form(
            {
                "issue": "chatter",
                "strategy_situation": "finishing with long tool overhang",
                "observation": "vibration increased during finishing",
                "possible_cause": "tool wear or resonance",
                "decision": "Inspect insert before changing offsets.",
                "action_type": "inspect_part",
                "rationale": "Safe first check before changing dimensions.",
                "risk": "Inspection adds downtime.",
                "alternative_strategy": "Change spindle speed.",
                "evidence": "surface finish worsened",
                "confidence": "High",
                "trace_target": "inspection recommendation",
                "worked": "yes",
                "outcome": "Surface improved.",
                "reusable_strategy": "on",
            }
        )
    )
    notes.mark_reusable(note.id, reusable=True)

    preview = OslExportService(note_service=notes, export_path=tmp_path / "operator_strategies.sysml").preview()

    assert "package MSH_OperatorStrategies" in preview
    assert "part def OSLStrategy" in preview
    assert "part Chatter_" in preview
    assert "osl.context" in preview
    assert "osl.trigger" in preview
    assert "osl.observation" in preview
    assert "osl.hypothesis" in preview
    assert "osl.strategy_action" in preview
    assert "osl.rationale" in preview
    assert "osl.tradeoff" in preview
    assert "osl.alternative_strategy" in preview
    assert "osl.evidence" in preview
    assert "osl.confidence" in preview
    assert "osl.outcome" in preview
    assert "osl.dt_artifact_link" in preview
    assert "Inspect insert before changing offsets" in preview
    assert "schema:" not in preview
