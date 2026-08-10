from __future__ import annotations

from catalog.flask_app.services.operator_strategy_service import OperatorStrategyService
from catalog.flask_app.services.operator_support_service import OperatorSupportService
from catalog.flask_app.services.osl_export_service import OslExportService
from catalog.flask_app.services.recommender_artifact_service import RecommenderArtifactService
from catalog.flask_app.services.source_inventory_service import SourceInventoryService


class Form(dict):
    def getlist(self, key: str):
        value = self.get(key, [])
        return value if isinstance(value, list) else [value]


def test_operator_strategy_lifecycle(tmp_path):
    sources = SourceInventoryService(tmp_path / "sources.json")
    assert sources.add_machine_from_form({"name": "Mazak i500"})[0]
    machine_id = sources.status_model()["machines"][0]["id"]
    assert sources.add_vibration_sensor_from_form({"name": "Spindle vibration", "machine_id": machine_id, "enabled": "on"})[0]
    sensor_id = sources.status_model()["vibration_sensors"][0]["id"]

    notes = OperatorStrategyService(tmp_path / "notes.json")
    record = notes.add_from_form(
        Form(
            {
                "issue": "chatter",
                "machine_id": machine_id,
                "sensor_ids": [sensor_id],
                "decision": "Reduced feed override when chatter appeared.",
                "action_type": "reduce_feed",
                "reusable_strategy": "on",
                "worked": "yes",
                "outcome": "Chatter reduced.",
            }
        )
    )
    assert record.decision
    assert record.sensor_ids == [sensor_id]

    assert notes.update_outcome(record.id, {"worked": "yes", "outcome": "Surface improved.", "reusable_strategy": "on"})[0]
    assert notes.mark_reusable(record.id, reusable=True)[0]

    cards = OperatorSupportService(note_service=notes).support_cards()
    assert cards
    assert any("Reduced feed" in card["suggested_action"] for card in cards)

    count, path = OslExportService(note_service=notes, export_path=tmp_path / "operator_strategies.sysml").export_reusable()
    assert count == 1
    assert path.endswith("operator_strategies.sysml")
    assert "package FCP_OperatorStrategies" in (tmp_path / "operator_strategies.sysml").read_text(encoding="utf-8")

    count, path = RecommenderArtifactService(note_service=notes, output_path=tmp_path / "rec.json").generate()
    assert count == 1
    assert path.endswith("rec.json")
