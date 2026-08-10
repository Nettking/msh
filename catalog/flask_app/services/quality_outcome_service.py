from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4


DEFAULT_QUALITY_OUTCOME_PATH = Path("data") / "quality" / "quality_outcomes.json"


class QualityOutcomeService:
    def __init__(self, path: Path | str = DEFAULT_QUALITY_OUTCOME_PATH) -> None:
        self.path = Path(path)

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        records = self._load()["outcomes"]
        records.sort(key=lambda item: str(item.get("recorded_at") or ""), reverse=True)
        return records[: max(1, limit)]

    def add_from_form(self, form: Any) -> tuple[bool, str]:
        result = _text(form, "result")
        if not result:
            return False, "Quality result is required."
        payload = self._load()
        payload["outcomes"].append(
            {
                "id": uuid4().hex,
                "recorded_at": _now_utc(),
                "operator_note_id": _text(form, "operator_note_id"),
                "machine_id": _text(form, "machine_id"),
                "part_id": _text(form, "part_id"),
                "measurement_type": _text(form, "measurement_type"),
                "measurement_value": _text(form, "measurement_value"),
                "tolerance": _text(form, "tolerance"),
                "surface_finish_note": _text(form, "surface_finish_note"),
                "result": result,
                "linked_action": _text(form, "linked_action"),
                "conclusion": _text(form, "conclusion"),
            }
        )
        self._write(payload)
        return True, "Quality outcome saved."

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema": "fcp.quality_outcomes.v1", "updated_at": "", "outcomes": []}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"schema": "fcp.quality_outcomes.v1", "updated_at": "", "outcomes": []}
        if not isinstance(payload, dict):
            return {"schema": "fcp.quality_outcomes.v1", "updated_at": "", "outcomes": []}
        payload.setdefault("outcomes", [])
        payload["outcomes"] = [item for item in payload["outcomes"] if isinstance(item, dict)]
        return payload

    def _write(self, payload: dict[str, Any]) -> None:
        payload = dict(payload)
        payload["schema"] = "fcp.quality_outcomes.v1"
        payload["updated_at"] = _now_utc()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(self.path)


def _text(form: Any, name: str) -> str:
    return str(form.get(name) or "").strip()


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
