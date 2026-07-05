"""Local storage service for operator strategy field-capture records.

These records are research/field notes, not telemetry. They are stored as JSON
under data/operator_strategy_records so recursive JSONL telemetry discovery does
not ingest them as machine samples.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_RECORDS_PATH = Path("data") / "operator_strategy_records" / "operator_strategies.json"
DEFAULT_TIMEZONE = "Europe/Oslo"


class OperatorStrategyError(RuntimeError):
    """Raised when an operator strategy record cannot be created or stored."""


@dataclass(frozen=True)
class OperatorStrategyRecord:
    id: str
    captured_at: str
    decision_time: str
    decision_time_mode: str
    decision_time_local: str
    decision_timezone: str
    strategy_name: str
    strategy_situation: str
    machine: str
    process: str
    operation: str
    observation: str
    trigger: str
    context: str
    hypothesis: str
    goal: str
    decision: str
    rationale: str
    expected_outcome: str
    risk: str
    trade_off: str
    confidence: str
    evidence: str
    trace_target: str
    notes: str

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


class OperatorStrategyService:
    def __init__(self, records_path: Path | str = DEFAULT_RECORDS_PATH) -> None:
        self.records_path = Path(records_path)

    def load_records(self) -> list[dict[str, Any]]:
        if not self.records_path.exists():
            return []
        try:
            payload = json.loads(self.records_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise OperatorStrategyError(f"Could not read operator strategy records: {self.records_path}") from exc
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict) and isinstance(payload.get("records"), list):
            return [item for item in payload["records"] if isinstance(item, dict)]
        raise OperatorStrategyError(f"Operator strategy records file has unexpected shape: {self.records_path}")

    def recent_records(self, limit: int = 25) -> list[dict[str, Any]]:
        records = self.load_records()
        records.sort(key=lambda item: str(item.get("decision_time") or item.get("captured_at") or ""), reverse=True)
        return records[: max(1, limit)]

    def add_from_form(self, form: Any) -> OperatorStrategyRecord:
        captured_at_dt = datetime.now(timezone.utc).replace(microsecond=0)
        captured_at = _format_utc(captured_at_dt)
        decision_time_mode = str(form.get("decision_time_mode") or "now").strip().lower()
        if decision_time_mode not in {"now", "custom"}:
            decision_time_mode = "now"

        timezone_name = str(form.get("decision_timezone") or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
        if decision_time_mode == "now":
            decision_time_dt = captured_at_dt
            decision_time_local = ""
        else:
            decision_time_local = str(form.get("decision_time_local") or "").strip()
            if not decision_time_local:
                raise OperatorStrategyError("Select a custom decision/action time, or use Now.")
            decision_time_dt = _parse_local_datetime(decision_time_local, timezone_name)

        record = OperatorStrategyRecord(
            id=uuid4().hex,
            captured_at=captured_at,
            decision_time=_format_utc(decision_time_dt),
            decision_time_mode=decision_time_mode,
            decision_time_local=decision_time_local,
            decision_timezone=timezone_name,
            strategy_name=_text(form, "strategy_name"),
            strategy_situation=_text(form, "strategy_situation"),
            machine=_text(form, "machine"),
            process=_text(form, "process"),
            operation=_text(form, "operation"),
            observation=_text(form, "observation"),
            trigger=_text(form, "trigger"),
            context=_text(form, "context"),
            hypothesis=_text(form, "hypothesis"),
            goal=_text(form, "goal"),
            decision=_text(form, "decision"),
            rationale=_text(form, "rationale"),
            expected_outcome=_text(form, "expected_outcome"),
            risk=_text(form, "risk"),
            trade_off=_text(form, "trade_off"),
            confidence=_text(form, "confidence"),
            evidence=_text(form, "evidence"),
            trace_target=_text(form, "trace_target"),
            notes=_text(form, "notes"),
        )
        _validate_record(record)
        records = self.load_records()
        records.append(record.to_dict())
        self._write_records(records)
        return record

    def delete(self, record_id: str) -> bool:
        record_id = record_id.strip()
        records = self.load_records()
        kept = [record for record in records if str(record.get("id") or "") != record_id]
        if len(kept) == len(records):
            return False
        self._write_records(kept)
        return True

    def _write_records(self, records: list[dict[str, Any]]) -> None:
        self.records_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema": "msh.operator_strategy_records.v1",
            "updated_at": _format_utc(datetime.now(timezone.utc).replace(microsecond=0)),
            "records": records,
        }
        tmp = self.records_path.with_suffix(self.records_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(self.records_path)
        return None


def _text(form: Any, name: str) -> str:
    return str(form.get(name) or "").strip()


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_local_datetime(value: str, timezone_name: str) -> datetime:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise OperatorStrategyError(f"Unknown decision timezone: {timezone_name}") from exc
    try:
        naive = datetime.fromisoformat(value)
    except ValueError as exc:
        raise OperatorStrategyError("Custom decision/action time must be a valid date and time.") from exc
    if naive.tzinfo is not None:
        return naive.astimezone(timezone.utc)
    return naive.replace(tzinfo=tz).astimezone(timezone.utc)


def _validate_record(record: OperatorStrategyRecord) -> None:
    if not record.decision:
        raise OperatorStrategyError("Decision/action is required.")
