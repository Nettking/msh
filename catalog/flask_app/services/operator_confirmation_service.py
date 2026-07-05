from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4


DEFAULT_CONFIRMATIONS_PATH = Path("data") / "operator_confirmations" / "confirmations.json"


class OperatorConfirmationService:
    def __init__(self, path: Path | str = DEFAULT_CONFIRMATIONS_PATH) -> None:
        self.path = Path(path)

    def list_confirmations(self, limit: int = 50) -> list[dict[str, Any]]:
        records = self._load()["confirmations"]
        records.sort(key=lambda item: str(item.get("confirmed_at") or ""), reverse=True)
        return records[: max(1, limit)]

    def add_from_form(self, form: Any) -> tuple[bool, str]:
        support_card_id = _text(form, "support_card_id")
        status = _text(form, "confirmation_status") or _text(form, "status")
        if not support_card_id:
            return False, "Support card id is required."
        if status not in {"accepted", "rejected", "do_later", "needs_supervisor", "already_done"}:
            return False, "Select a valid confirmation status."
        payload = self._load()
        payload["confirmations"].append(
            {
                "id": uuid4().hex,
                "support_card_id": support_card_id,
                "operator_note_id": _text(form, "operator_note_id"),
                "confirmation_status": status,
                "confirmed_by": _text(form, "confirmed_by") or "operator",
                "confirmed_at": _now_utc(),
                "comment": _text(form, "comment"),
            }
        )
        self._write(payload)
        return True, "Operator confirmation saved."

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema": "msh.operator_confirmations.v1", "updated_at": "", "confirmations": []}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"schema": "msh.operator_confirmations.v1", "updated_at": "", "confirmations": []}
        if not isinstance(payload, dict):
            return {"schema": "msh.operator_confirmations.v1", "updated_at": "", "confirmations": []}
        payload.setdefault("confirmations", [])
        payload["confirmations"] = [item for item in payload["confirmations"] if isinstance(item, dict)]
        return payload

    def _write(self, payload: dict[str, Any]) -> None:
        payload = dict(payload)
        payload["schema"] = "msh.operator_confirmations.v1"
        payload["updated_at"] = _now_utc()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(self.path)


def _text(form: Any, name: str) -> str:
    return str(form.get(name) or "").strip()


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
