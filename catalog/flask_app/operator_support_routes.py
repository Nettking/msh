from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for

from .services.operator_confirmation_service import OperatorConfirmationService
from .services.operator_support_service import OperatorSupportService


operator_support_web = Blueprint("operator_support_web", __name__)


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
