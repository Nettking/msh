from __future__ import annotations

from typing import Any

from .operator_strategy_service import OperatorStrategyService
from .operator_support_model import SupportCard, action_requires_confirmation


CAUSE_LIBRARY: dict[str, list[str]] = {
    "chatter": ["tool wear", "resonance", "excessive cutting load", "material variation", "poor coolant"],
    "spindle_load": ["tool wear", "excessive cutting load", "chip evacuation", "material variation"],
    "surface_finish": ["tool wear", "chatter", "incorrect feed", "coolant issue"],
    "first_part_deviation": ["wrong offset", "thermal drift", "workholding", "tool length mismatch"],
}


DEFAULT_ACTIONS: dict[str, dict[str, str]] = {
    "chatter": {
        "action": "Inspect insert before changing offsets; consider spindle speed change before reducing feed.",
        "action_type": "inspect_part",
        "rationale": "Chatter plus surface finish change often indicates wear or resonance. Inspection is a safe first check.",
        "risk": "Inspection causes short downtime; feed reduction may increase cycle time.",
        "alternative": "Change spindle speed to move away from resonance.",
    },
    "spindle_load": {
        "action": "Inspect tool condition and chip evacuation before changing offsets.",
        "action_type": "inspect_part",
        "rationale": "Increasing load can be caused by wear, chip build-up, or cutting condition changes.",
        "risk": "Stopping for inspection may interrupt production, but avoids hidden quality loss.",
        "alternative": "Reduce feed temporarily if the process must continue under supervision.",
    },
    "surface_finish": {
        "action": "Check insert, coolant, and vibration before making dimensional offset changes.",
        "action_type": "inspect_part",
        "rationale": "Surface finish degradation is often a symptom, not the root cause.",
        "risk": "Offset changes may not fix finish and can move dimensions away from target.",
        "alternative": "Compare previous similar operator notes before acting.",
    },
    "first_part_deviation": {
        "action": "Hold production until first-part measurement and setup checklist are reviewed.",
        "action_type": "pause_production",
        "rationale": "First-part approval is the control point before repeated quality loss.",
        "risk": "Short delay, but prevents repeating a setup error.",
        "alternative": "Call supervisor if offset/tool/program uncertainty remains.",
    },
}


class OperatorSupportService:
    def __init__(self, note_service: OperatorStrategyService | None = None) -> None:
        self.note_service = note_service or OperatorStrategyService()

    def support_cards(self) -> list[dict[str, Any]]:
        notes = self.note_service.recent_records(limit=100)
        cards: list[SupportCard] = []
        for issue_key in ["chatter", "spindle_load", "surface_finish", "first_part_deviation"]:
            cards.append(self._card_from_issue(issue_key, notes))
        reusable = [note for note in notes if note.get("reusable_strategy")]
        for note in reusable[:5]:
            cards.append(self._card_from_reusable_note(note))
        return [card.to_dict() for card in cards]

    def _card_from_issue(self, issue_key: str, notes: list[dict[str, Any]]) -> SupportCard:
        action = DEFAULT_ACTIONS[issue_key]
        related = [note for note in notes if issue_key in _note_text(note)]
        evidence = [f"{len(related)} related operator note(s) found."] if related else ["No local operator notes yet; using seed rule template."]
        return SupportCard(
            id=f"template-{issue_key.replace('_', '-')}",
            issue=issue_key.replace("_", " ").title(),
            observation=f"Potential {issue_key.replace('_', ' ')} situation.",
            possible_causes=CAUSE_LIBRARY[issue_key],
            suggested_action=action["action"],
            rationale=action["rationale"],
            risk=action["risk"],
            alternative_action=action["alternative"],
            evidence=evidence,
            action_type=action["action_type"],
            requires_confirmation=action_requires_confirmation(action["action_type"]),
        )

    def _card_from_reusable_note(self, note: dict[str, Any]) -> SupportCard:
        action_type = str(note.get("action_type") or "manual_note")
        return SupportCard(
            id=f"note-{note.get('id')}",
            issue=str(note.get("issue") or note.get("strategy_situation") or "Reusable operator strategy"),
            observation=str(note.get("observation") or note.get("context") or "Similar previous operator note exists."),
            possible_causes=[str(note.get("possible_cause") or note.get("hypothesis") or "operator experience")],
            suggested_action=str(note.get("decision") or "Review previous operator note."),
            rationale=str(note.get("rationale") or note.get("goal") or "Marked reusable by operator."),
            risk=str(note.get("risk") or note.get("trade_off") or "Unknown risk; operator judgment required."),
            alternative_action=str(note.get("alternative_strategy") or "No alternative recorded."),
            evidence=[f"Operator note {note.get('id')} worked={note.get('worked', 'unknown')}"],
            action_type=action_type,
            requires_confirmation=bool(note.get("operator_confirmation_required")) or action_requires_confirmation(action_type),
        )


def _note_text(note: dict[str, Any]) -> str:
    values = [str(note.get(key) or "") for key in ["issue", "strategy_situation", "context", "decision", "observation", "notes"]]
    return " ".join(values).lower()
