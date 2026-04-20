from __future__ import annotations

"""Global operator scope persisted for the whole app instance (not per-user)."""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class OperatorScope:
    start_date: str | None
    end_date: str | None
    selected_session_id: str | None
    updated_at: str | None

    @property
    def is_active(self) -> bool:
        return bool(self.start_date and self.end_date)

    @property
    def label(self) -> str:
        if not self.is_active:
            return "No shared scope"
        return f"{self.start_date}..{self.end_date}"


class OperatorScopeService:
    def __init__(self) -> None:
        self._path = Path("results") / "workflows" / "operator_scope.json"

    def get(self) -> OperatorScope:
        if not self._path.exists():
            return OperatorScope(None, None, None, None)
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return OperatorScope(None, None, None, None)
        return OperatorScope(
            start_date=payload.get("start_date"),
            end_date=payload.get("end_date"),
            selected_session_id=payload.get("selected_session_id"),
            updated_at=payload.get("updated_at"),
        )

    def set(self, *, start_date: str, end_date: str, selected_session_id: str | None = None) -> OperatorScope:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        updated_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        payload = {
            "start_date": start_date,
            "end_date": end_date,
            "selected_session_id": selected_session_id,
            "updated_at": updated_at,
        }
        self._path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return OperatorScope(start_date, end_date, selected_session_id, updated_at)

    def clear(self) -> OperatorScope:
        if self._path.exists():
            self._path.unlink()
        return OperatorScope(None, None, None, None)


_SCOPE: OperatorScopeService | None = None


def get_operator_scope_service() -> OperatorScopeService:
    global _SCOPE
    if _SCOPE is None:
        _SCOPE = OperatorScopeService()
    return _SCOPE
