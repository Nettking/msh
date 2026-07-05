from __future__ import annotations

from catalog.flask_app.services.operator_strategy_service import OperatorStrategyService
from catalog.flask_app.services.source_inventory_service import SourceInventoryService


class Form(dict):
    def getlist(self, key: str):
        value = self.get(key, [])
        return value if isinstance(value, list) else [value]


def main() -> None:
    sources = SourceInventoryService()
    sources.add_machine_from_form({"name": "Mazak i500", "machine_type": "CNC machining centre", "controller": "MTConnect"})
    inventory = sources.status_model()
    machine_id = inventory["machines"][0]["id"] if inventory["machines"] else ""
    sources.add_vibration_sensor_from_form({"name": "Spindle vibration", "machine_id": machine_id, "source": "Observer Phoenix", "channel": "VibrationVelocityRMS", "axis": "spindle", "unit": "mm/s", "enabled": "on"})
    inventory = sources.status_model()
    sensor_id = inventory["vibration_sensors"][0]["id"] if inventory["vibration_sensors"] else ""

    notes = OperatorStrategyService()
    examples = [
        Form({
            "issue": "chatter",
            "strategy_situation": "chatter during finishing",
            "machine_id": machine_id,
            "sensor_ids": [sensor_id],
            "decision": "Reduced feed override when chatter appeared during finishing pass.",
            "possible_cause": "excessive cutting load or tool wear",
            "action_type": "reduce_feed",
            "rationale": "Safe first response while checking whether the surface improved.",
            "risk": "Cycle time increases and resonance may remain.",
            "alternative_strategy": "Change spindle speed to move away from resonance.",
            "confidence": "High",
            "reusable_strategy": "on",
            "worked": "yes",
            "outcome": "Chatter reduced and surface finish improved.",
        }),
        Form({
            "issue": "first_part_deviation",
            "strategy_situation": "cold start first part deviation",
            "machine_id": machine_id,
            "decision": "Waited for warm-up before changing offsets.",
            "possible_cause": "thermal drift",
            "action_type": "manual_note",
            "rationale": "This machine often drifts during the first 30 minutes.",
            "risk": "Waiting delays production, but avoids premature offset correction.",
            "confidence": "Medium",
            "reusable_strategy": "on",
            "worked": "partly",
            "outcome": "Second measurement was closer to target after warm-up.",
        }),
    ]
    for example in examples:
        notes.add_from_form(example)
    print("Seeded MSH operator support demo data.")


if __name__ == "__main__":
    main()
