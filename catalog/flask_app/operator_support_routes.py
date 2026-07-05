from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for

from .services.first_part_service import CHECKLIST_ITEMS, FirstPartService
from .services.machine_notes_service import MachineNotesService
from .services.operator_confirmation_service import OperatorConfirmationService
from .services.operator_support_service import OperatorSupportService
from .services.osl_export_service import OslExportService
from .services.quality_outcome_service import QualityOutcomeService
from .services.recommender_artifact_service import RecommenderArtifactService
from .services.source_inventory_service import SourceInventoryService
from .services.strategy_comparison_service import StrategyComparisonService


operator_support_web = Blueprint("operator_support_web", __name__)


def _source_inventory() -> dict[str, object]:
    try:
        return SourceInventoryService().status_model()
    except Exception:
        return {"machines": [], "vibration_sensors": []}


@operator_support_web.get("/assist")
def assist():
    cards = OperatorSupportService().support_cards()
    confirmations = OperatorConfirmationService().list_confirmations(limit=20)
    return render_template("assist.html", cards=cards, confirmations=confirmations)


@operator_support_web.post("/assist/confirm")
def confirm_assist_action():
    ok, message = OperatorConfirmationService().add_from_form(request.form)
    flash(message, "success" if ok else "error")
    return redirect(url_for("operator_support_web.assist"))


@operator_support_web.get("/first-part")
def first_part():
    service = FirstPartService()
    return render_template("first_part.html", checks=service.recent(limit=30), checklist_items=CHECKLIST_ITEMS, source_inventory=_source_inventory(), records_path=service.path.as_posix())


@operator_support_web.post("/first-part/save")
def save_first_part():
    ok, message = FirstPartService().add_from_form(request.form)
    flash(message, "success" if ok else "error")
    return redirect(url_for("operator_support_web.first_part"))


@operator_support_web.get("/strategy-comparison")
def strategy_comparison():
    return render_template("strategy_comparison.html", comparisons=StrategyComparisonService().comparisons())


@operator_support_web.get("/quality-outcomes")
def quality_outcomes():
    service = QualityOutcomeService()
    return render_template("quality_outcomes.html", outcomes=service.recent(limit=50), source_inventory=_source_inventory(), records_path=service.path.as_posix())


@operator_support_web.post("/quality-outcomes/save")
def save_quality_outcome():
    ok, message = QualityOutcomeService().add_from_form(request.form)
    flash(message, "success" if ok else "error")
    return redirect(url_for("operator_support_web.quality_outcomes"))


@operator_support_web.get("/machine-notes")
def machine_notes():
    service = MachineNotesService()
    return render_template("machine_notes.html", notes=service.recent(limit=100), source_inventory=_source_inventory(), records_path=service.path.as_posix())


@operator_support_web.post("/machine-notes/save")
def save_machine_note():
    ok, message = MachineNotesService().add_from_form(request.form)
    flash(message, "success" if ok else "error")
    return redirect(url_for("operator_support_web.machine_notes"))


@operator_support_web.get("/osl-export")
def osl_export():
    service = OslExportService()
    return render_template("osl_export.html", preview=service.preview(), export_path=service.export_path.as_posix())


@operator_support_web.post("/osl-export/run")
def run_osl_export():
    count, path = OslExportService().export_reusable()
    flash(f"Exported {count} reusable operator note(s) to {path}.", "success")
    return redirect(url_for("operator_support_web.osl_export"))


@operator_support_web.get("/recommender-artifacts")
def recommender_artifacts():
    service = RecommenderArtifactService()
    return render_template("recommender_artifacts.html", artifacts=service.preview(), output_path=service.output_path.as_posix())


@operator_support_web.post("/recommender-artifacts/generate")
def generate_recommender_artifacts():
    count, path = RecommenderArtifactService().generate()
    flash(f"Generated {count} recommender artifact(s) at {path}.", "success")
    return redirect(url_for("operator_support_web.recommender_artifacts"))


@operator_support_web.get("/learning")
def learning():
    cards = OperatorSupportService().support_cards()
    comparisons = StrategyComparisonService().comparisons()
    outcomes = QualityOutcomeService().recent(limit=10)
    return render_template("learning.html", cards=cards, comparisons=comparisons, outcomes=outcomes)
