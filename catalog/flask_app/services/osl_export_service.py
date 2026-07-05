from __future__ import annotations

from pathlib import Path
from typing import Any
import re

from .operator_strategy_service import OperatorStrategyService


DEFAULT_OSL_EXPORT_PATH = Path("data") / "osl" / "operator_strategies.yaml"


class OslExportService:
    def __init__(self, note_service: OperatorStrategyService | None = None, export_path: Path | str = DEFAULT_OSL_EXPORT_PATH) -> None:
        self.note_service = note_service or OperatorStrategyService()
        self.export_path = Path(export_path)

    def export_reusable(self) -> tuple[int, str]:
        records = self.note_service.reusable_records(limit=500)
        self.export_path.parent.mkdir(parents=True, exist_ok=True)
        self.export_path.write_text(_records_to_yaml(records), encoding="utf-8")
        return len(records), self.export_path.as_posix()

    def preview(self) -> str:
        return _records_to_yaml(self.note_service.reusable_records(limit=50))


def _records_to_yaml(records: list[dict[str, Any]]) -> str:
    lines = ["# Generated from MSH Operator Notes", "schema: msh.osl.operator_strategies.v1", "strategies:"]
    if not records:
        lines.append("  []")
        return "\n".join(lines) + "\n"
    for record in records:
        identifier = _slug(record.get("strategy_name") or record.get("issue") or record.get("strategy_situation") or record.get("id") or "operator_strategy")
        lines.extend(
            [
                f"  - id: {identifier}_{str(record.get('id') or '')[:8]}",
                f"    source_note_id: {record.get('id', '')}",
                "    situation:",
                f"      issue: {_q(record.get('issue') or record.get('strategy_situation'))}",
                f"      context: {_q(record.get('context'))}",
                f"      machine_id: {_q(record.get('machine_id') or record.get('machine'))}",
                "    observation:",
                f"      text: {_q(record.get('observation'))}",
                f"      trigger: {_q(record.get('trigger'))}",
                "    hypothesis:",
                f"      possible_cause: {_q(record.get('possible_cause') or record.get('hypothesis'))}",
                "    action:",
                f"      type: {_q(record.get('action_type'))}",
                f"      text: {_q(record.get('decision'))}",
                f"      requires_confirmation: {str(bool(record.get('operator_confirmation_required'))).lower()}",
                f"    rationale: {_q(record.get('rationale') or record.get('goal'))}",
                f"    risk: {_q(record.get('risk') or record.get('trade_off'))}",
                f"    alternative_strategy: {_q(record.get('alternative_strategy'))}",
                "    evidence:",
                f"      text: {_q(record.get('evidence'))}",
                "    outcome:",
                f"      worked: {_q(record.get('worked'))}",
                f"      text: {_q(record.get('outcome'))}",
                f"    confidence: {_q(record.get('confidence'))}",
            ]
        )
    return "\n".join(lines) + "\n"


def _slug(value: Any) -> str:
    text = str(value or "operator_strategy").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or "operator_strategy"


def _q(value: Any) -> str:
    text = str(value or "").replace('"', '\\"')
    return f'"{text}"'
