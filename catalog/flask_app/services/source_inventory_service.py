from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4


DEFAULT_SOURCE_INVENTORY_PATH = Path("data") / "source_config" / "machines_and_sensors.json"


class SourceInventoryError(RuntimeError):
    pass


@dataclass(frozen=True)
class MachineConfig:
    id: str
    name: str
    machine_type: str
    controller: str
    notes: str
    created_at: str
    mtconnect_url: str = ""
    vpn_test_host: str = ""
    vpn_test_port: str = ""

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class VibrationSensorConfig:
    id: str
    name: str
    machine_id: str
    source: str
    channel: str
    axis: str
    unit: str
    sampling_rate_hz: str
    enabled: bool
    notes: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


class SourceInventoryService:
    def __init__(self, inventory_path: Path | str = DEFAULT_SOURCE_INVENTORY_PATH) -> None:
        self.inventory_path = Path(inventory_path)

    def load(self) -> dict[str, Any]:
        if not self.inventory_path.exists():
            return _empty_inventory()
        try:
            payload = json.loads(self.inventory_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SourceInventoryError(f"Could not read source inventory: {self.inventory_path}") from exc
        if not isinstance(payload, dict):
            raise SourceInventoryError(f"Source inventory has unexpected shape: {self.inventory_path}")
        payload.setdefault("schema", "fcp.source_inventory.v1")
        payload.setdefault("machines", [])
        payload.setdefault("vibration_sensors", [])
        payload.setdefault("updated_at", "")
        payload["machines"] = [_normalize_machine(item) for item in payload["machines"] if isinstance(item, dict)]
        payload["vibration_sensors"] = [_normalize_sensor(item) for item in payload["vibration_sensors"] if isinstance(item, dict)]
        return payload

    def status_model(self) -> dict[str, Any]:
        inventory = self.load()
        machines = sorted(inventory["machines"], key=lambda item: str(item.get("name") or item.get("id") or ""))
        machine_by_id = {str(machine.get("id") or ""): machine for machine in machines}
        sensors = sorted(inventory["vibration_sensors"], key=lambda item: str(item.get("name") or item.get("id") or ""))
        for sensor in sensors:
            machine = machine_by_id.get(str(sensor.get("machine_id") or ""))
            sensor["machine_name"] = machine.get("name") if machine else "Unassigned"
        return {
            "inventory_path": str(self.inventory_path),
            "machines": machines,
            "vibration_sensors": sensors,
            "machine_count": len(machines),
            "sensor_count": len(sensors),
            "enabled_sensor_count": len([sensor for sensor in sensors if sensor.get("enabled")]),
            "updated_at": inventory.get("updated_at") or "n/a",
        }

    def add_machine_from_form(self, form: Any) -> tuple[bool, str]:
        name = _text(form, "name")
        if not name:
            return False, "Machine name is required."
        inventory = self.load()
        if any(str(machine.get("name") or "").strip().lower() == name.lower() for machine in inventory["machines"]):
            return False, f"Machine already exists: {name}"
        machine = MachineConfig(
            id=_safe_id(form.get("id")) or uuid4().hex[:12],
            name=name,
            machine_type=_text(form, "machine_type"),
            controller=_text(form, "controller"),
            notes=_text(form, "notes"),
            created_at=_now_utc(),
            mtconnect_url=_text(form, "mtconnect_url"),
            vpn_test_host=_text(form, "vpn_test_host"),
            vpn_test_port=_text(form, "vpn_test_port"),
        )
        inventory["machines"].append(machine.to_dict())
        self._write(inventory)
        return True, f"Added machine: {name}"

    def delete_machine(self, machine_id: str) -> tuple[bool, str]:
        machine_id = str(machine_id or "").strip()
        inventory = self.load()
        machine = next((item for item in inventory["machines"] if str(item.get("id") or "") == machine_id), None)
        if not machine:
            return False, "Machine was not found."
        used_by = [sensor for sensor in inventory["vibration_sensors"] if str(sensor.get("machine_id") or "") == machine_id]
        if used_by:
            return False, "Remove vibration sensors assigned to this machine before deleting it."
        inventory["machines"] = [item for item in inventory["machines"] if str(item.get("id") or "") != machine_id]
        self._write(inventory)
        return True, f"Removed machine: {machine.get('name') or machine_id}"

    def add_vibration_sensor_from_form(self, form: Any) -> tuple[bool, str]:
        name = _text(form, "name")
        if not name:
            return False, "Sensor name is required."
        machine_id = _text(form, "machine_id")
        inventory = self.load()
        machine_ids = {str(machine.get("id") or "") for machine in inventory["machines"]}
        if machine_id and machine_id not in machine_ids:
            return False, "Selected machine was not found."
        sensor = VibrationSensorConfig(
            id=_safe_id(form.get("id")) or uuid4().hex[:12],
            name=name,
            machine_id=machine_id,
            source=_text(form, "source") or "Observer Phoenix",
            channel=_text(form, "channel"),
            axis=_text(form, "axis"),
            unit=_text(form, "unit") or "mm/s",
            sampling_rate_hz=_text(form, "sampling_rate_hz"),
            enabled=_coerce_bool(form.get("enabled")),
            notes=_text(form, "notes"),
            created_at=_now_utc(),
        )
        inventory["vibration_sensors"].append(sensor.to_dict())
        self._write(inventory)
        return True, f"Added vibration sensor: {name}"

    def delete_vibration_sensor(self, sensor_id: str) -> tuple[bool, str]:
        sensor_id = str(sensor_id or "").strip()
        inventory = self.load()
        sensor = next((item for item in inventory["vibration_sensors"] if str(item.get("id") or "") == sensor_id), None)
        if not sensor:
            return False, "Vibration sensor was not found."
        inventory["vibration_sensors"] = [item for item in inventory["vibration_sensors"] if str(item.get("id") or "") != sensor_id]
        self._write(inventory)
        return True, f"Removed vibration sensor: {sensor.get('name') or sensor_id}"

    def _write(self, inventory: dict[str, Any]) -> None:
        inventory = dict(inventory)
        inventory["schema"] = "fcp.source_inventory.v1"
        inventory["updated_at"] = _now_utc()
        inventory.setdefault("machines", [])
        inventory.setdefault("vibration_sensors", [])
        inventory["machines"] = [_normalize_machine(item) for item in inventory["machines"] if isinstance(item, dict)]
        inventory["vibration_sensors"] = [_normalize_sensor(item) for item in inventory["vibration_sensors"] if isinstance(item, dict)]
        self.inventory_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.inventory_path.with_suffix(self.inventory_path.suffix + ".tmp")
        tmp.write_text(json.dumps(inventory, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(self.inventory_path)


def machine_label(machine_id: str) -> str:
    """Resolve a local machine ID to the name an operator would recognize.

    Machine IDs live in this device's inventory, which is not Federation state.
    Records that travel between devices denormalize the label at capture time so
    the receiving device shows a machine name instead of an opaque ID it has
    never seen. An empty result means the caller should fall back to the ID.
    """

    machine_id = str(machine_id or "").strip()
    if not machine_id:
        return ""
    try:
        inventory = SourceInventoryService().load()
    except (SourceInventoryError, OSError):
        return ""
    for machine in inventory.get("machines", []):
        if str(machine.get("id") or "") == machine_id:
            return str(machine.get("name") or "").strip()
    return ""


def _empty_inventory() -> dict[str, Any]:
    return {
        "schema": "fcp.source_inventory.v1",
        "updated_at": "",
        "machines": [],
        "vibration_sensors": [],
    }


def _normalize_machine(machine: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(machine)
    normalized.setdefault("mtconnect_url", "")
    normalized.setdefault("vpn_test_host", "")
    normalized.setdefault("vpn_test_port", "")
    return normalized


def _normalize_sensor(sensor: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(sensor)
    normalized["enabled"] = _coerce_bool(sensor.get("enabled"))
    return normalized


def _text(form: Any, name: str) -> str:
    return str(form.get(name) or "").strip()


def _safe_id(value: Any) -> str:
    candidate = str(value or "").strip()
    return "".join(character for character in candidate if character.isalnum() or character in {"-", "_"})[:64]


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
