from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4


DEFAULT_FIRST_PART_PATH = Path("data") / "quality" / "first_part_checks.json"
CHECKLIST_ITEMS = [
    "correct_program_loaded",
    "correct_material_confirmed",
    "tool_list_verified",
    "workholding_checked",
    "offset_checked",
    "warmup_status_checked",
    "first_part_measured",
    "operator_signed_off",
]


class FirstPartService:
    def __init__(self, path: Path | str = DEFAULT_FIRST_PART_PATH) -> None:
        self.path = Path(path)

    def recent(self, limit: int = 30) -> list[dict[str, Any]]:
        records = self._load()["checks"]
        records.sort(key=lambda item: str(item.get("signed_at") or item.get("created_at") or ""), reverse=True)
        return records[: max(1, limit)]

    def add_from_form(self, form: Any) -> tuple[bool, str]:
        signed = bool(form.get("operator_signed_off"))
        payload = self._load()
        payload["checks"].append(
            {
                "id": uuid4().hex,
                "created_at": _now_utc(),
                "signed_at": _now_utc() if signed else "",
                "machine_id": _text(form, "machine_id"),
                "part_id": _text(form, "part_id"),
                "program_id": _text(form, "program_id"),
                "checklist": {item: bool(form.get(item)) for item in CHECKLIST_ITEMS},
                "measurement_notes": _text(form, "measurement_notes"),
                "quality_deviation_note": _text(form, "quality_deviation_note"),
                "signed_by": _text(form, "signed_by") or "operator",
                "status": "signed_off" if signed else "not_signed_off",
            }
        )
        self._write(payload)
        return True, "First-part check saved."

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema": "fcp.first_part_checks.v1", "updated_at": "", "checks": []}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"schema": "fcp.first_part_checks.v1", "updated_at": "", "checks": []}
        if not isinstance(payload, dict):
            return {"schema": "fcp.first_part_checks.v1", "updated_at": "", "checks": []}
        payload.setdefault("checks", [])
        payload["checks"] = [item for item in payload["checks"] if isinstance(item, dict)]
        return payload

    def _write(self, payload: dict[str, Any]) -> None:
        payload = dict(payload)
        payload["schema"] = "fcp.first_part_checks.v1"
        payload["updated_at"] = _now_utc()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(self.path)


def _text(form: Any, name: str) -> str:
    return str(form.get(name) or "").strip()


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
