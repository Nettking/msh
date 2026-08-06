"""Web surface for durable multi-file JSONL uploads and analysis handoff."""

from __future__ import annotations

import secrets
from typing import Any

from flask import (
    Blueprint,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from .services.data_upload_service import DataUploadError, get_data_upload_service


data_upload_web = Blueprint(
    "data_upload_web",
    __name__,
    url_prefix="/data-upload",
    template_folder="templates",
)

_CSRF_SESSION_KEY = "data_upload_csrf_token"


def _csrf_token() -> str:
    value = session.get(_CSRF_SESSION_KEY)
    if not isinstance(value, str) or len(value) < 32:
        value = secrets.token_urlsafe(32)
        session[_CSRF_SESSION_KEY] = value
    return value


def _validate_csrf() -> None:
    supplied = request.headers.get("X-CSRF-Token") or request.form.get("_csrf_token")
    expected = session.get(_CSRF_SESSION_KEY)
    if (
        not isinstance(supplied, str)
        or not isinstance(expected, str)
        or not secrets.compare_digest(supplied, expected)
    ):
        raise DataUploadError(
            "upload-csrf-rejected",
            "The upload request expired. Refresh the page and try again.",
        )


def _selected_batch(batch_id: str | None) -> dict[str, Any] | None:
    if not batch_id:
        return None
    try:
        return get_data_upload_service().batch(batch_id)
    except DataUploadError:
        return None


@data_upload_web.get("/")
def index():
    service = get_data_upload_service()
    selected_id = request.args.get("batch", "").strip()
    return render_template(
        "data_upload.html",
        upload_batches=service.list_batches(limit=20),
        selected_batch=_selected_batch(selected_id),
        data_upload_csrf_token=_csrf_token(),
        max_files=service.max_files,
        max_total_bytes=service.max_total_bytes,
    )


@data_upload_web.post("/")
def upload():
    try:
        _validate_csrf()
        batch = get_data_upload_service().enqueue(request.files.getlist("files"))
    except DataUploadError as exc:
        flash(exc.message, "error")
        return redirect(url_for("data_upload_web.index"))
    flash(
        f"Upload received. Importing {batch['file_count']} JSONL file(s) in the background.",
        "success",
    )
    return redirect(url_for("data_upload_web.index", batch=batch["batch_id"]))


@data_upload_web.get("/<batch_id>/status.json")
def status(batch_id: str):
    try:
        batch = get_data_upload_service().batch(batch_id)
    except DataUploadError as exc:
        return jsonify({"ok": False, "error": {"code": exc.code}}), 404
    return jsonify({"ok": True, "batch": batch})


@data_upload_web.post("/<batch_id>/analyze")
def analyze(batch_id: str):
    try:
        _validate_csrf()
        batch = get_data_upload_service().request_analysis(batch_id)
    except DataUploadError as exc:
        flash(exc.message, "error")
        return redirect(url_for("data_upload_web.index", batch=batch_id))
    flash(
        "Background analysis requested. Open Diagnostics to follow runtime progress.",
        "success",
    )
    return redirect(url_for("data_upload_web.index", batch=batch["batch_id"]))


__all__ = ["data_upload_web"]
