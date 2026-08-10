from __future__ import annotations

from catalog.flask_app.services.operator_strategy_service import OperatorStrategyService
from catalog.flask_app.services.osl_export_service import OslExportService


class Form(dict):
    def getlist(self, key: str):
        value = self.get(key, [])
        return value if isinstance(value, list) else [value]


def test_osl_export_preview_uses_paper_keyword_style(tmp_path):
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

    assert "package FCP_OperatorStrategies" in preview
    assert "import OSLCore::*" in preview
    assert "import OSLMetadata::*" in preview
    assert "#operator_strategy Chatter" in preview
    assert "#strategy_situation ChatterPath" in preview
    assert "#situation Chatter" in preview
    assert "#context FinishingWithLongToolOverhang" in preview
    assert "#observation VibrationIncreasedDuringFinishing" in preview
    assert "#hypothesis ToolWearOrResonance" in preview
    assert "#decision InspectInsertBeforeChangingOffsets" in preview
    assert "#operator_action InspectPart" in preview
    assert "#rationale SafeFirstCheckBeforeChangingDimensions" in preview
    assert "#expected_outcome SurfaceImproved" in preview
    assert "#trade_off InspectionAddsDowntime" in preview
    assert "#risk InspectionAddsDowntime" in preview
    assert "attribute confidence: ConfidenceLevel = High" in preview
    assert "attribute evidenceStatus: EvidenceStatus = SourceBacked" in preview
    assert "#evidence SurfaceFinishWorsened" in preview
    assert "#recommendation InspectionRecommendation" in preview
    assert "part def OSLStrategy" not in preview
    assert "osl.context" not in preview
    assert "schema:" not in preview
