from __future__ import annotations

from pathlib import Path
from typing import Any
import re

from .operator_strategy_service import OperatorStrategyService


DEFAULT_SYSML_EXPORT_PATH = Path("data") / "sysml" / "operator_strategies.sysml"
VALID_CONFIDENCE = {"Unknown", "Low", "Medium", "High"}


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
        "// Method: coded CNC strategy statement -> OSL keywords -> SysML artefact.",
        "// Keyword style follows systems-paper/sysml/osl-core.sysml and cnc-chatter-keywords-example.sysml.",
        "package MSH_OperatorStrategies {",
        "",
        "  import OSLCore::*;",
        "  import OSLMetadata::*;",
        "",
    ]
    if not records:
        lines.extend(["  // No reusable operator notes have been marked for export yet.", "", "}"])
        return "\n".join(lines) + "\n"
    for record in records:
        strategy_name = _sysml_identifier(record.get("strategy_name") or record.get("issue") or record.get("strategy_situation") or record.get("id"))
        path_name = f"{strategy_name}Path"
        lines.extend(_record_to_keyword_strategy(record, strategy_name, path_name))
    lines.append("}")
    return "\n".join(lines) + "\n"


def _record_to_keyword_strategy(record: dict[str, Any], strategy_name: str, path_name: str) -> list[str]:
    situation = record.get("issue") or record.get("strategy_situation") or "OperatorSituation"
    context = record.get("context") or record.get("strategy_situation") or record.get("machine_id") or record.get("machine")
    observation = record.get("observation") or record.get("issue") or "OperatorObservation"
    trigger = record.get("trigger")
    hypothesis = record.get("possible_cause") or record.get("hypothesis")
    goal = record.get("goal") or record.get("expected_outcome") or record.get("outcome")
    decision = record.get("decision") or "OperatorDecision"
    action = record.get("action_type") or record.get("decision") or "OperatorAction"
    rationale = record.get("rationale") or record.get("goal")
    expected_outcome = record.get("expected_outcome") or record.get("outcome")
    trade_off = record.get("trade_off") or record.get("risk")
    risk = record.get("risk")
    evidence = record.get("evidence")
    trace_target = record.get("trace_target") or record.get("action_type")
    confidence = _confidence(record.get("confidence"))
    evidence_status = "SourceBacked" if evidence else "Unknown"

    lines = [
        f"  #operator_strategy {strategy_name} {{",
        f"    // source_note_id = {_clean(record.get('id'))}",
        f"    #strategy_situation {path_name} {{",
        f"      #situation {_sysml_identifier(situation)};",
    ]
    lines.extend(_keyword_lines("context", context))
    lines.extend(_keyword_lines("observation", observation))
    lines.extend(_keyword_lines("trigger", trigger))
    lines.extend(_keyword_lines("hypothesis", hypothesis))
    lines.extend(_keyword_lines("goal", goal))
    lines.extend(_keyword_lines("decision", decision))
    lines.extend(_keyword_lines("operator_action", action))
    lines.extend(_keyword_lines("rationale", rationale))
    lines.extend(_keyword_lines("expected_outcome", expected_outcome))
    lines.extend(_keyword_lines("trade_off", trade_off))
    lines.extend(_keyword_lines("risk", risk))
    lines.append(f"      attribute confidence: ConfidenceLevel = {confidence};")
    lines.append(f"      attribute evidenceStatus: EvidenceStatus = {evidence_status};")
    lines.extend(_keyword_lines("evidence", evidence))
    lines.extend(_dt_keyword_lines(trace_target))
    lines.append("    }")
    lines.append("  }")
    lines.append("")
    return lines


def _keyword_lines(keyword: str, value: Any) -> list[str]:
    if not _clean(value):
        return []
    return [f"      #{keyword} {_sysml_identifier(value)};"]


def _dt_keyword_lines(value: Any) -> list[str]:
    text = _clean(value)
    if not text:
        return []
    lowered = text.lower()
    if "rule" in lowered or "monitor" in lowered:
        keyword = "monitoring_rule"
    elif "dashboard" in lowered:
        keyword = "dashboard"
    elif "explanation" in lowered:
        keyword = "explanation"
    elif "validation" in lowered or "outcome" in lowered:
        keyword = "validation_case"
    elif "recommend" in lowered or "action" in lowered or "feed" in lowered or "inspect" in lowered:
        keyword = "recommendation"
    else:
        keyword = "dt_artifact"
    return [f"      #{keyword} {_sysml_identifier(value)};"]


def _confidence(value: Any) -> str:
    candidate = str(value or "Unknown").strip().title()
    return candidate if candidate in VALID_CONFIDENCE else "Unknown"


def _sysml_identifier(value: Any) -> str:
    text = _clean(value) or "OperatorStrategy"
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", text) if part]
    if not parts:
        return "OperatorStrategy"
    identifier = "".join(part[:1].upper() + part[1:] for part in parts)
    if identifier[0].isdigit():
        identifier = f"Strategy{identifier}"
    return identifier


def _clean(value: Any) -> str:
    return str(value or "").replace("\n", " ").strip()
