from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for

from .services.first_part_service import CHECKLIST_ITEMS, FirstPartService
from .services.operator_confirmation_service import OperatorConfirmationService
from .services.operator_support_service import OperatorSupportService
from .services.source_inventory_service import SourceInventoryService


operator_support_web = Blueprint("operator_support_web", __name__)


def _source_inventory() -> dict[str, object]:
    try:
        return SourceInventoryService().status_model()
    except Exception:
        return {"machines": []}


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
    return render_template(
        "first_part.html",
        checks=service.recent(limit=30),
        checklist_items=CHECKLIST_ITEMS,
        source_inventory=_source_inventory(),
        records_path=service.path.as_posix(),
    )


@operator_support_web.post("/first-part/save")
def save_first_part():
    ok, message = FirstPartService().add_from_form(request.form)
    flash(message, "success" if ok else "error")
    return redirect(url_for("operator_support_web.first_part"))
