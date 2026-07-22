from __future__ import annotations

from collections import defaultdict
from typing import Any

from .operator_strategy_service import OperatorStrategyService


class StrategyComparisonService:
    def __init__(self, note_service: OperatorStrategyService | None = None) -> None:
        self.note_service = note_service or OperatorStrategyService()

    def comparisons(self) -> list[dict[str, Any]]:
        notes = [
            note
            for note in self.note_service.recent_records(limit=500)
            if note.get("review_status") in {"structured", "reusable"}
        ]
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for note in notes:
            key = _situation_key(note)
            grouped[key].append(note)
        comparisons: list[dict[str, Any]] = []
        for situation, items in sorted(grouped.items()):
            actions: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in items:
                actions[str(item.get("decision") or "unrecorded action")].append(item)
            comparisons.append(
                {
                    "situation": situation,
                    "total_notes": len(items),
                    "strategies": [_strategy_summary(action, action_notes) for action, action_notes in actions.items()],
                }
            )
        return comparisons


def _situation_key(note: dict[str, Any]) -> str:
    return str(note.get("issue") or note.get("strategy_situation") or "general operator situation").strip().lower()


def _strategy_summary(action: str, notes: list[dict[str, Any]]) -> dict[str, Any]:
    worked_yes = len([note for note in notes if note.get("worked") == "yes"])
    worked_partly = len([note for note in notes if note.get("worked") == "partly"])
    worked_no = len([note for note in notes if note.get("worked") == "no"])
    confidence_values = [str(note.get("confidence") or "unknown") for note in notes]
    reusable_count = len([note for note in notes if note.get("reusable_strategy")])
    risks = [str(note.get("risk") or note.get("trade_off") or "") for note in notes if note.get("risk") or note.get("trade_off")]
    return {
        "action": action,
        "note_count": len(notes),
        "worked_yes": worked_yes,
        "worked_partly": worked_partly,
        "worked_no": worked_no,
        "confidence": ", ".join(sorted(set(confidence_values))) or "unknown",
        "reusable_count": reusable_count,
        "risk_or_tradeoff": risks[0] if risks else "No risk/trade-off recorded.",
        "evidence": f"{len(notes)} note(s), {worked_yes} worked, {worked_partly} partly, {worked_no} failed.",
    }
