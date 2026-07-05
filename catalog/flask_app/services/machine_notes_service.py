from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4


DEFAULT_MACHINE_NOTES_PATH = Path("data") / "source_config" / "machine_notes.json"


class MachineNotesService:
    def __init__(self, path: Path | str = DEFAULT_MACHINE_NOTES_PATH) -> None:
        self.path = Path(path)

    def recent(self, limit: int = 100) -> list[dict[str, Any]]:
        notes = self._load()["notes"]
        notes.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return notes[: max(1, limit)]

    def add_from_form(self, form: Any) -> tuple[bool, str]:
        text = _text(form, "note")
        if not text:
            return False, "Machine note is required."
        payload = self._load()
        payload["notes"].append(
            {
                "id": uuid4().hex,
                "created_at": _now_utc(),
                "machine_id": _text(form, "machine_id"),
                "applies_when": _text(form, "applies_when"),
                "note": text,
                "caution": _text(form, "caution"),
                "source": _text(form, "source") or "operator experience",
            }
        )
        self._write(payload)
        return True, "Machine note saved."

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema": "msh.machine_notes.v1", "updated_at": "", "notes": []}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"schema": "msh.machine_notes.v1", "updated_at": "", "notes": []}
        if not isinstance(payload, dict):
            return {"schema": "msh.machine_notes.v1", "updated_at": "", "notes": []}
        payload.setdefault("notes", [])
        payload["notes"] = [item for item in payload["notes"] if isinstance(item, dict)]
        return payload

    def _write(self, payload: dict[str, Any]) -> None:
        payload = dict(payload)
        payload["schema"] = "msh.machine_notes.v1"
        payload["updated_at"] = _now_utc()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(self.path)


def _text(form: Any, name: str) -> str:
    return str(form.get(name) or "").strip()


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
