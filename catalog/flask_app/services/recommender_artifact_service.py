from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from .federated_operator_strategy_service import FederatedOperatorStrategyService


DEFAULT_RECOMMENDER_PATH = Path("data") / "osl" / "recommender_artifacts.json"


class RecommenderArtifactService:
    def __init__(self, note_service: FederatedOperatorStrategyService | None = None, output_path: Path | str = DEFAULT_RECOMMENDER_PATH) -> None:
        self.note_service = note_service or FederatedOperatorStrategyService()
        self.output_path = Path(output_path)

    def generate(self) -> tuple[int, str]:
        artifacts = [self._artifact_from_note(note) for note in self.note_service.reusable_records(limit=500)]
        payload = {"schema": "fcp.recommender_artifacts.v1", "artifacts": artifacts}
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        return len(artifacts), self.output_path.as_posix()

    def preview(self) -> list[dict[str, Any]]:
        return [self._artifact_from_note(note) for note in self.note_service.reusable_records(limit=50)]

    def _artifact_from_note(self, note: dict[str, Any]) -> dict[str, Any]:
        issue = str(note.get("issue") or note.get("strategy_situation") or "operator situation")
        action = str(note.get("decision") or "review operator note")
        return {
            "id": f"rec_{str(note.get('id') or '')[:12]}",
            "source_note_id": note.get("id"),
            "match": {"issue": issue, "machine_id": note.get("machine_id", ""), "action_type": note.get("action_type", "manual_note")},
            "recommendation": action,
            "reason": note.get("rationale") or note.get("goal") or "Derived from reusable operator note.",
            "risk": note.get("risk") or note.get("trade_off") or "Risk not recorded.",
            "alternative": note.get("alternative_strategy") or "No alternative recorded.",
            "requires_confirmation": bool(note.get("operator_confirmation_required")),
            "evidence": [note.get("evidence") or f"operator note {note.get('id')}"]
        }
