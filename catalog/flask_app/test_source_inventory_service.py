from __future__ import annotations

from catalog.flask_app.services.source_inventory_service import SourceInventoryService


def test_add_machine_and_vibration_sensor(tmp_path):
    service = SourceInventoryService(tmp_path / "sources.json")

    ok, message = service.add_machine_from_form(
        {
            "name": "Mazak i500",
            "machine_type": "CNC machining centre",
            "controller": "MTConnect",
            "mtconnect_url": "http://10.0.0.20:5000",
            "vpn_test_host": "10.0.0.20",
            "vpn_test_port": "5000",
        }
    )
    assert ok, message

    status = service.status_model()
    machine = status["machines"][0]
    machine_id = machine["id"]
    assert machine["mtconnect_url"] == "http://10.0.0.20:5000"
    assert machine["vpn_test_host"] == "10.0.0.20"
    assert machine["vpn_test_port"] == "5000"

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


def test_existing_machines_are_normalized_with_connection_fields(tmp_path):
    path = tmp_path / "sources.json"
    path.write_text('{"machines":[{"id":"m1","name":"Old machine"}],"vibration_sensors":[]}', encoding="utf-8")

    machine = SourceInventoryService(path).status_model()["machines"][0]

    assert machine["mtconnect_url"] == ""
    assert machine["vpn_test_host"] == ""
    assert machine["vpn_test_port"] == ""
