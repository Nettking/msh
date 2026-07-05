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
SCHEMA_VERSION = "msh.operator_strategy_records.v2"


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
    issue: str = ""
    possible_cause: str = ""
    action_type: str = "manual_note"
    worked: str = "unknown"
    outcome: str = ""
    reusable_strategy: bool = False
    alternative_strategy: str = ""
    operator_confirmation_required: bool = False
    quality_result_id: str = ""
    first_part_approval_id: str = ""
    machine_id: str = ""
    sensor_ids: list[str] | None = None
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        payload = self.__dict__.copy()
        payload["sensor_ids"] = list(self.sensor_ids or [])
        return payload


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
            records = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, dict) and isinstance(payload.get("records"), list):
            records = [item for item in payload["records"] if isinstance(item, dict)]
        else:
            raise OperatorStrategyError(f"Operator strategy records file has unexpected shape: {self.records_path}")
        return [_normalize_record(item) for item in records]

    def recent_records(self, limit: int = 25) -> list[dict[str, Any]]:
        records = self.load_records()
        records.sort(key=lambda item: str(item.get("decision_time") or item.get("captured_at") or ""), reverse=True)
        return records[: max(1, limit)]

    def reusable_records(self, limit: int = 100) -> list[dict[str, Any]]:
        records = [record for record in self.load_records() if record.get("reusable_strategy")]
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
            issue=_text(form, "issue"),
            possible_cause=_text(form, "possible_cause"),
            action_type=_text(form, "action_type") or _infer_action_type(_text(form, "decision")),
            worked=_text(form, "worked") or "unknown",
            outcome=_text(form, "outcome"),
            reusable_strategy=bool(form.get("reusable_strategy")),
            alternative_strategy=_text(form, "alternative_strategy"),
            operator_confirmation_required=bool(form.get("operator_confirmation_required")),
            quality_result_id=_text(form, "quality_result_id"),
            first_part_approval_id=_text(form, "first_part_approval_id"),
            machine_id=_text(form, "machine_id"),
            sensor_ids=_split_ids(form.getlist("sensor_ids") if hasattr(form, "getlist") else form.get("sensor_ids")),
        )
        _validate_record(record)
        records = self.load_records()
        records.append(record.to_dict())
        self._write_records(records)
        return record

    def update_outcome(self, record_id: str, form: Any) -> tuple[bool, str]:
        records = self.load_records()
        for record in records:
            if str(record.get("id") or "") == record_id:
                record["worked"] = _text(form, "worked") or record.get("worked") or "unknown"
                record["outcome"] = _text(form, "outcome") or record.get("outcome") or ""
                record["quality_result_id"] = _text(form, "quality_result_id") or record.get("quality_result_id") or ""
                record["reusable_strategy"] = _bool_from_form(form, "reusable_strategy", bool(record.get("reusable_strategy")))
                record["updated_at"] = _format_utc(datetime.now(timezone.utc).replace(microsecond=0))
                self._write_records(records)
                return True, "Operator note outcome updated."
        return False, "Operator note was not found."

    def mark_reusable(self, record_id: str, reusable: bool = True) -> tuple[bool, str]:
        records = self.load_records()
        for record in records:
            if str(record.get("id") or "") == record_id:
                record["reusable_strategy"] = reusable
                record["updated_at"] = _format_utc(datetime.now(timezone.utc).replace(microsecond=0))
                self._write_records(records)
                return True, "Operator note marked reusable." if reusable else "Operator note marked not reusable."
        return False, "Operator note was not found."

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
            "schema": SCHEMA_VERSION,
            "updated_at": _format_utc(datetime.now(timezone.utc).replace(microsecond=0)),
            "records": [_normalize_record(record) for record in records],
        }
        tmp = self.records_path.with_suffix(self.records_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(self.records_path)
        return None


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    defaults: dict[str, Any] = {
        "issue": "",
        "possible_cause": "",
        "action_type": _infer_action_type(str(record.get("decision") or "")),
        "worked": "unknown",
        "outcome": "",
        "reusable_strategy": False,
        "alternative_strategy": "",
        "operator_confirmation_required": False,
        "quality_result_id": "",
        "first_part_approval_id": "",
        "machine_id": "",
        "sensor_ids": [],
        "schema_version": SCHEMA_VERSION,
    }
    for key, value in defaults.items():
        normalized.setdefault(key, value)
    normalized["sensor_ids"] = _split_ids(normalized.get("sensor_ids"))
    normalized["reusable_strategy"] = bool(normalized.get("reusable_strategy"))
    normalized["operator_confirmation_required"] = bool(normalized.get("operator_confirmation_required"))
    return normalized


def _split_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value).replace(";", ",").split(",")
    return [str(item).strip() for item in raw if str(item).strip()]


def _bool_from_form(form: Any, name: str, default: bool = False) -> bool:
    if hasattr(form, "get") and form.get(name) is None:
        return default
    value = form.get(name) if hasattr(form, "get") else None
    return str(value).lower() in {"1", "true", "yes", "on"}


def _infer_action_type(decision: str) -> str:
    lowered = decision.lower()
    if "tool" in lowered or "insert" in lowered:
        return "change_tool" if "change" in lowered or "replace" in lowered else "inspect_part"
    if "offset" in lowered:
        return "change_offset"
    if "feed" in lowered:
        return "reduce_feed" if "reduc" in lowered or "lower" in lowered else "feed_adjustment"
    if "pause" in lowered or "stop" in lowered:
        return "pause_production"
    if "supervisor" in lowered:
        return "call_supervisor"
    return "manual_note"


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
