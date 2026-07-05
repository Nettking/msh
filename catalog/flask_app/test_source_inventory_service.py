from __future__ import annotations

from catalog.flask_app.services.source_inventory_service import SourceInventoryService


def test_add_machine_and_vibration_sensor(tmp_path):
    service = SourceInventoryService(tmp_path / "sources.json")

    ok, message = service.add_machine_from_form(
        {
            "name": "Mazak i500",
            "machine_type": "CNC machining centre",
            "controller": "MTConnect",
        }
    )
    assert ok, message

    status = service.status_model()
    machine_id = status["machines"][0]["id"]

    ok, message = service.add_vibration_sensor_from_form(
        {
            "name": "Spindle vibration",
            "machine_id": machine_id,
            "source": "Observer Phoenix",
            "channel": "VibrationVelocityRMS",
            "axis": "spindle",
            "unit": "mm/s",
            "sampling_rate_hz": "1000",
            "enabled": "on",
        }
    )
    assert ok, message

    status = service.status_model()
    assert status["machine_count"] == 1
    assert status["sensor_count"] == 1
    assert status["enabled_sensor_count"] == 1
    assert status["vibration_sensors"][0]["machine_name"] == "Mazak i500"


def test_machine_with_sensor_must_not_be_deleted_first(tmp_path):
    service = SourceInventoryService(tmp_path / "sources.json")
    assert service.add_machine_from_form({"name": "Mazak i500"})[0]
    machine_id = service.status_model()["machines"][0]["id"]
    assert service.add_vibration_sensor_from_form({"name": "Spindle vibration", "machine_id": machine_id})[0]

    ok, message = service.delete_machine(machine_id)
    assert not ok
    assert "Remove vibration sensors" in message

    sensor_id = service.status_model()["vibration_sensors"][0]["id"]
    assert service.delete_vibration_sensor(sensor_id)[0]
    assert service.delete_machine(machine_id)[0]
    assert service.status_model()["machine_count"] == 0
