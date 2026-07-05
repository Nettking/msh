from __future__ import annotations

from pathlib import Path
from typing import Any
import re

from .operator_strategy_service import OperatorStrategyService


DEFAULT_SYSML_EXPORT_PATH = Path("data") / "sysml" / "operator_strategies.sysml"


class OslExportService:
    def __init__(self, note_service: OperatorStrategyService | None = None, export_path: Path | str = DEFAULT_SYSML_EXPORT_PATH) -> None:
        self.note_service = note_service or OperatorStrategyService()
        self.export_path = Path(export_path)

    def export_reusable(self) -> tuple[int, str]:
        records = self.note_service.reusable_records(limit=500)
        self.export_path.parent.mkdir(parents=True, exist_ok=True)
        self.export_path.write_text(_records_to_sysml(records), encoding="utf-8")
        return len(records), self.export_path.as_posix()

    def preview(self) -> str:
        return _records_to_sysml(self.note_service.reusable_records(limit=50))


def _records_to_sysml(records: list[dict[str, Any]]) -> str:
    lines = [
        "// Generated from MSH Operator Notes",
        "// OSL is the internal structure; SysML is the export format.",
        "package MSH_OperatorStrategies {",
        "",
        "  part def OperatorStrategy {",
        "    attribute sourceNoteId : String;",
        "    attribute issue : String;",
        "    attribute machineRef : String;",
        "    attribute observation : String;",
        "    attribute possibleCause : String;",
        "    attribute actionType : String;",
        "    attribute actionText : String;",
        "    attribute rationale : String;",
        "    attribute risk : String;",
        "    attribute alternativeStrategy : String;",
        "    attribute outcome : String;",
        "    attribute confidence : String;",
        "  }",
        "",
    ]
    if not records:
        lines.extend(["  // No reusable operator notes have been marked for export yet.", "", "}"])
        return "\n".join(lines) + "\n"
    for record in records:
        name = _sysml_identifier(record.get("strategy_name") or record.get("issue") or record.get("strategy_situation") or record.get("id"))
        short_id = str(record.get("id") or "")[:8]
        usage_name = f"{name}_{short_id}" if short_id else name
        lines.extend(
            [
                f"  part {usage_name} : OperatorStrategy {{",
                f"    // sourceNoteId = {_text(record.get('id'))}",
                f"    // issue = {_text(record.get('issue') or record.get('strategy_situation'))}",
                f"    // machineRef = {_text(record.get('machine_id') or record.get('machine'))}",
                f"    // observation = {_text(record.get('observation'))}",
                f"    // possibleCause = {_text(record.get('possible_cause') or record.get('hypothesis'))}",
                f"    // actionType = {_text(record.get('action_type'))}",
                f"    // actionText = {_text(record.get('decision'))}",
                f"    // rationale = {_text(record.get('rationale') or record.get('goal'))}",
                f"    // risk = {_text(record.get('risk') or record.get('trade_off'))}",
                f"    // alternativeStrategy = {_text(record.get('alternative_strategy'))}",
                f"    // outcome = {_text(record.get('outcome'))}",
                f"    // confidence = {_text(record.get('confidence'))}",
                "  }",
                "",
            ]
        )
    lines.append("}")
    return "\n".join(lines) + "\n"


def _sysml_identifier(value: Any) -> str:
    text = str(value or "OperatorStrategy").strip()
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", text) if part]
    if not parts:
        return "OperatorStrategy"
    identifier = "".join(part[:1].upper() + part[1:] for part in parts)
    if identifier[0].isdigit():
        identifier = f"Strategy{identifier}"
    return identifier


def _text(value: Any) -> str:
    return str(value or "").replace("\n", " ")
