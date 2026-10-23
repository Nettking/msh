"""Protected historical process-twin API and local workbench page."""

from __future__ import annotations

import re
from pathlib import Path

import click
from flask import (
    Blueprint,
    current_app,
    jsonify,
    make_response,
    render_template,
    request,
)
from flask_security import current_user
from flask_wtf.csrf import generate_csrf, validate_csrf
from werkzeug.exceptions import HTTPException
from wtforms.validators import ValidationError

from catalog.digital_twin.contracts import (
    MAX_REQUEST,
    TwinError,
    check_id,
    identifier,
    now,
    require,
    strict_json,
)
from catalog.digital_twin.records import TwinStore

from .auth.models import User, db
from .services.process_twin_inputs import InventoryInputRegistry
from .services.process_twin_service import API, OPERATIONS, WORKFLOW, ProcessTwinService

process_twin_api = Blueprint("process_twin_api", __name__, url_prefix=API)
process_twin_web = Blueprint("process_twin_web", __name__)


def _service():
    directory = current_app.config.get("FCP_PROCESS_TWIN_STORE")
    if directory is None:
        directory = (
            Path(current_app.config.get("FCP_DATA_ROOT", "data")) / "process_twins"
        )
    registry = current_app.extensions.get("process_twin_input_registry")
    if registry is None:

        def local_authorized(actor, _record):
            user_id = current_user.id
            require(
                not (db.session.new or db.session.dirty or db.session.deleted),
                "AUTHORIZATION_UNAVAILABLE",
                503,
            )
            # End an auth read transaction before checking current roles again.
            # Never roll back pending owner writes or mint a replacement grant.
            db.session.rollback()
            fresh = db.session.get(User, user_id, populate_existing=True)
            return (
                fresh is not None
                and str(fresh.get_id()) == actor
                and fresh.is_active
                and fresh.has_permission("data.read")
            )

        registry = InventoryInputRegistry(
            current_app.config.get("FCP_DATA_ROOT", "data"),
            local_authorized,
            blocked_roots=tuple(
                current_app.config[k]
                for k in (
                    "FEDERATED_TELEMETRY_MIRROR_DIRECTORY",
                    "FEDERATED_JSONL_MIRROR_DIRECTORY",
                )
                if current_app.config.get(k)
            ),
        )
    return ProcessTwinService(directory, registry)


def _actor():
    user = current_user
    require(
        user is not None and user.is_authenticated and user.is_active,
        "AUTHENTICATION_REQUIRED",
        401,
    )
    return str(user.get_id())


def _permission(permission):
    require(current_user.has_permission(permission), "PERMISSION_REQUIRED", 403)


def _meta(state="AVAILABLE", reasons=()):
    return {
        "request_id": identifier("cmd"),
        "evaluated_at": now(),
        "contract_version": 1,
        "technical_state": state,
        "technical_reason_codes": list(reasons),
    }


def _response(data, status=200, location=None, *, state="AVAILABLE", reasons=()):
    response = jsonify(
        schema="fcp.process_twin.response.v1", data=data, meta=_meta(state, reasons)
    )
    response.status_code = status
    if location:
        response.headers["Location"] = location
    return response


def _error(error):
    messages = {
        400: "Forespørselen har ugyldig format.",
        401: "Logg inn for å fortsette.",
        403: "Du har ikke rettighet til denne handlingen.",
        404: "Innholdet er ikke tilgjengelig.",
        405: "Denne handlingen støttes ikke på adressen.",
        409: "Grunnlaget eller bestillingsstatusen krever ny kontroll.",
        413: "Innholdet overskrider tillatt størrelse.",
        415: "Forespørselen må være JSON.",
        422: "Valget oppfyller ikke reglene for dette grunnlaget.",
        500: "En intern feil oppstod. Resultatet må avklares gjennom bestillingsstatus.",
        503: "En nødvendig tjeneste eller kilde er utilgjengelig. Ingen reservekilde er brukt.",
        504: "Byggingen overskred tidsgrensen og ble ikke publisert.",
        507: "Det er ikke tilstrekkelig lagringsplass. Ingen eksisterende data er slettet.",
    }
    specific = {
        "CSRF_INVALID": "Skjemaets sikkerhetskontroll er utløpt eller ugyldig. Last siden på nytt.",
        "INVALID_CURSOR": "Sidepekeren er ugyldig. Åpne første side igjen.",
        "COMMAND_RUNNING": "En bygging pågår. Kontroller status før du sender på nytt.",
        "BUILD_BUSY": "En bygging pågår. Kontroller status før du sender på nytt.",
        "IDEMPOTENCY_CONFLICT": "Bestillingsnøkkelen er allerede brukt med annet innhold.",
        "VIEW_INDEX_UNAVAILABLE": "Visningsgrunnlaget mangler. Historikken er bevart.",
        "NO_DEFINITION_CHANGE": "Definisjonen inneholder ingen endring.",
        "BUILD_ABANDONED": "Byggingen ble avbrutt og kan ikke publiseres. En ny kjøring krever en ny bestilling.",
    }
    for names, message in (
        (
            ("DEFINITION_CHANGED", "SOURCE_CHANGED", "LIST_CHANGED"),
            "Grunnlaget er endret. Last inn valget på nytt før en ny bestilling.",
        ),
        (
            ("PROJECTION_REVISION_MISMATCH", "REBUILD_DIFFERENT_RESULT"),
            "Revisjonene samsvarer ikke. Resultatet ble ikke publisert.",
        ),
        (
            ("INVALID_TIME_WINDOW", "LEGACY_WINDOW_NOT_DAY_ALIGNED"),
            "Tidsvinduet oppfyller ikke reglene for denne kilden.",
        ),
        (
            (
                "DATA_MODE_NOT_SUPPORTED",
                "MULTI_DATASET_NOT_SUPPORTED",
                "PROPERTY_NOT_SUPPORTED",
                "VIEW_NOT_SUPPORTED",
                "BINDING_MISMATCH",
            ),
            "Valget støttes ikke av denne leveransen eller kildebindingen.",
        ),
        (
            (
                "INPUT_LIMIT_EXCEEDED",
                "RECORD_LIMIT_EXCEEDED",
                "ARTIFACT_LIMIT_EXCEEDED",
                "MEMORY_LIMIT_EXCEEDED",
            ),
            "Bestillingen overskrider ressursgrensen og ble ikke publisert.",
        ),
        (
            (
                "OVERLAP_UNRESOLVED",
                "INPUT_SCHEMA_UNSUPPORTED",
                "TIME_POLICY_NOT_SUPPORTED",
            ),
            "Kildegrunnlaget oppfyller ikke kontrakten.",
        ),
        (
            ("SOURCE_INTEGRITY_FAILED", "SOURCE_CONTENT_INVALID"),
            "Kildeinnholdet kunne ikke verifiseres eller leses. Resultatet ble ikke publisert.",
        ),
        (
            ("CLOCK_UNSAFE",),
            "Bygging kan ikke utføres mens fristgrunnlaget er uavklart.",
        ),
    ):
        specific.update({name: message for name in names})
    retry = (
        "REAUTHENTICATE"
        if error.status in (401, 403)
        else "CORRECT_REQUEST"
        if error.status < 500
        else "NEW_EXPLICIT_REQUEST"
        if error.command_id
        else "NONE"
    )
    if error.code in ("COMMAND_RUNNING", "BUILD_BUSY"):
        retry = "WAIT_FOR_COMMAND"
    response = jsonify(
        schema="fcp.process_twin.response.v1",
        error={
            "code": error.code,
            "message": specific.get(error.code, messages[error.status]),
            "field": error.field,
            "command_id": error.command_id,
            "retry_mode": retry,
        },
        meta=_meta(
            "UNAVAILABLE" if error.status >= 500 else "NOT_EVALUATED", [error.code]
        ),
    )
    response.status_code = error.status
    if retry == "WAIT_FOR_COMMAND":
        response.headers["Retry-After"] = "1"
    return response


for blueprint in (process_twin_api, process_twin_web):
    blueprint.register_error_handler(TwinError, _error)


@process_twin_api.errorhandler(HTTPException)
def _http_error(error):
    code = {
        400: "INVALID_REQUEST",
        401: "AUTHENTICATION_REQUIRED",
        403: "PERMISSION_REQUIRED",
        404: "ROUTE_NOT_FOUND",
        405: "METHOD_NOT_ALLOWED",
        413: "REQUEST_TOO_LARGE",
    }.get(error.code, "INTERNAL_ERROR")
    return _error(
        TwinError(
            code, error.code if error.code in (400, 401, 403, 404, 405, 413) else 500
        )
    )


def _preflight():
    require(
        current_app.config.get("FCP_AUTH_ENABLED", True),
        "AUTHENTICATION_UNAVAILABLE",
        503,
    )
    _actor()
    _permission("data.read")
    if request.method == "OPTIONS":
        response = make_response("", 204)
        response.headers["Allow"] = ", ".join(sorted(request.url_rule.methods))
        return response
    if request.method == "POST":
        permission = (
            "workflow.run"
            if request.endpoint
            in ("process_twin_api.create_twin", "process_twin_api.revise_twin")
            else "analysis.run"
        )
        _permission(permission)
        try:
            validate_csrf(request.headers.get("X-CSRFToken"))
        except ValidationError:
            raise TwinError("CSRF_INVALID") from None
    return None


process_twin_api.before_request(_preflight)
process_twin_web.before_request(_preflight)


@process_twin_api.after_request
@process_twin_web.after_request
def _private(response):
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
    )
    return response


def _key():
    key = request.headers.get("Idempotency-Key")
    require(
        isinstance(key, str) and re.fullmatch(r"[A-Za-z0-9_-]{16,128}", key) is not None
    )
    return key


def _body():
    require(not request.args)
    require(
        request.content_length is None or request.content_length <= MAX_REQUEST,
        "REQUEST_TOO_LARGE",
        413,
    )
    require(request.mimetype == "application/json", "UNSUPPORTED_MEDIA_TYPE", 415)
    # Bounded stream read also covers requests without Content-Length.
    raw = request.stream.read(MAX_REQUEST + 1)
    require(len(raw) <= MAX_REQUEST, "REQUEST_TOO_LARGE", 413)
    return strict_json(raw)


def _query(extra=()):
    require(set(request.args) <= {"limit", "cursor", *extra})
    require(all(len(request.args.getlist(k)) == 1 for k in request.args))
    limit = request.args.get("limit", "100")
    require(re.fullmatch(r"[1-9][0-9]{0,2}", limit) is not None and int(limit) <= 500)
    cursor = request.args.get("cursor")
    require(cursor is None or 1 <= len(cursor) <= 4096, "INVALID_CURSOR")
    return {"limit": int(limit), "cursor": cursor}


def _post(operation, target=None):
    body = _body()
    key = _key()

    def authorize_operation():
        _actor()
        _permission("data.read")
        _permission("workflow.run" if operation in WORKFLOW else "analysis.run")

    result, status, location = _service().execute(
        _actor(),
        operation,
        body,
        key,
        target=target,
        authorize_operation=authorize_operation,
    )
    if operation in WORKFLOW:
        result = {k: v for k, v in result.items() if k != "definition"}
    return _response(result, status, location)


@process_twin_api.get("/inputs")
def inputs():
    options = _query()
    service = _service()
    return _response(
        service.page(service.inputs(_actor()), _actor(), "inputs", **options)
    )


@process_twin_api.post("/dataset-revisions")
def prepare_dataset():
    return _post("PREPARE_DATASET")


@process_twin_api.get("/dataset-revisions/<ref>")
def dataset(ref):
    require(not request.args)
    return _response(_service().dataset(ref, _actor()))


@process_twin_api.post("/twins")
def create_twin():
    return _post("CREATE_TWIN")


@process_twin_api.post("/twins/<twin>/revisions")
def revise_twin(twin):
    check_id(twin, "tw")
    return _post("REVISE_TWIN", twin)


@process_twin_api.get("/twins")
def twins():
    return _response(_service().list_objects("definition", _actor(), **_query()))


@process_twin_api.get("/twins/<twin>/revisions/<ref>")
def definition(twin, ref):
    require(not request.args)
    result = _service().definition(twin, ref, _actor())
    return _response({k: v for k, v in result.items() if k != "definition"})


@process_twin_api.get("/twins/<twin>/snapshots")
def snapshots(twin):
    return _response(_service().list_objects("snapshot", _actor(), twin, **_query()))


@process_twin_api.post("/twins/<twin>/snapshots")
def build_snapshot(twin):
    check_id(twin, "tw")
    return _post("BUILD_SNAPSHOT", twin)


@process_twin_api.get("/snapshots/<snapshot>")
def snapshot_manifest(snapshot):
    require(not request.args)
    service = _service()
    manifest = service.snapshot(snapshot, _actor())
    index_ok = service.store.index_available(snapshot)
    return _response(
        manifest,
        state="AVAILABLE" if index_ok else "PARTIAL",
        reasons=[] if index_ok else ["VIEW_INDEX_UNAVAILABLE"],
    )


@process_twin_api.get("/snapshots/<snapshot>/timeline")
def timeline(snapshot):
    check_id(snapshot, "sn")
    options = _query(("view",))
    return _response(
        _service().timeline(
            snapshot, _actor(), view=request.args.get("view"), **options
        )
    )


@process_twin_api.get("/snapshots/<snapshot>/episodes/<ref>")
def episode(snapshot, ref):
    check_id(snapshot, "sn")
    require(not request.args)
    return _response(_service().detail(snapshot, _actor(), "episode", ref))


@process_twin_api.get("/snapshots/<snapshot>/records/<ref>")
def record(snapshot, ref):
    check_id(snapshot, "sn")
    require(not request.args)
    return _response(_service().detail(snapshot, _actor(), "record", ref))


@process_twin_api.get("/snapshots/<snapshot>/links")
def links(snapshot):
    check_id(snapshot, "sn")
    options = _query()
    service = _service()
    service.get(snapshot, _actor(), "snapshot")
    rows = [value for value, _scopes in service.store.objects("link", snapshot)]
    return _response(
        service.page(rows, _actor(), snapshot + ":links", snapshot=snapshot, **options)
    )


@process_twin_api.get("/snapshots/<snapshot>/evidence/<ref>")
def evidence(snapshot, ref):
    check_id(snapshot, "sn")
    require(not request.args)
    return _response(_service().detail(snapshot, _actor(), "evidence", ref))


@process_twin_api.post("/snapshots/<snapshot>/rebuild-index")
def rebuild(snapshot):
    check_id(snapshot, "sn")
    return _post("REBUILD_INDEX", snapshot)


def _command_response(command):
    _permission("workflow.run" if command["operation"] in WORKFLOW else "analysis.run")
    return _response(
        {
            "command_id": command["id"],
            "operation": command["operation"],
            "status": command["status"],
            "created_at": command["created"],
            "finished_at": command["finished"],
            "result_location": command["location"],
            "original_http_status": command["http"],
            "error": command["response"] if command["status"] == "FAILED" else None,
        }
    )


@process_twin_api.get("/commands/by-key")
def command_by_key():
    require(
        set(request.args) == {"operation"}
        and len(request.args.getlist("operation")) == 1
    )
    operation = request.args["operation"]
    require(operation in OPERATIONS)
    _permission("workflow.run" if operation in WORKFLOW else "analysis.run")
    return _command_response(
        _service().command(_actor(), operation=operation, key=_key())
    )


@process_twin_api.get("/commands/<ref>")
def command(ref):
    require(not request.args)
    return _command_response(_service().command(_actor(), command_id=ref))


@process_twin_web.get("/process-twins")
def index():
    require(not request.args)
    return render_template(
        "process_twin.html",
        process_twin_private=True,
        process_twin_csrf=generate_csrf(),
        can_analyze=current_user.has_permission("analysis.run"),
        can_define=current_user.has_permission("workflow.run"),
    )


def install_process_twins(app):
    app.register_blueprint(process_twin_api)
    app.register_blueprint(process_twin_web)

    @app.before_request
    def unknown_process_twin_route():
        if request.path.startswith(API + "/") and request.url_rule is None:
            routing = request.routing_exception
            if routing is not None and routing.code == 405:
                response = _error(TwinError("METHOD_NOT_ALLOWED", 405))
                response.headers["Allow"] = ", ".join(sorted(routing.valid_methods))
            else:
                response = _error(TwinError("ROUTE_NOT_FOUND", 404))
            return _private(response)
        return None

    @app.cli.command("process-twin-init")
    def initialize_process_twin_store():
        """Explicitly initialize the private manifest/index store; no source access."""
        directory = app.config.get(
            "FCP_PROCESS_TWIN_STORE",
            Path(app.config.get("FCP_DATA_ROOT", "data")) / "process_twins",
        )
        TwinStore(directory).initialize()
        click.echo("Process-twin store initialized. No sources or permissions changed.")
