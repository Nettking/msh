"""Flask application factory and webapp-first runtime startup entrypoint."""

from __future__ import annotations

import os

from flask import Flask, current_app, request

from catalog.capabilities.benchmarking.policy import (
    DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS,
)
from catalog.common.artifact_refresh import register_artifact_catalog_refresh
from catalog.federation.onboarding_models import ContributionDesiredState
from catalog.orchestrator.pipeline import get_runtime_manager, start_runtime_background

from .ai_routes import ai_web
from .capability_benchmark_routes import capability_benchmark_web
from .capability_contribution_routes import capability_contribution_web
from .capability_inspection_routes import capability_inspection_web
from .capability_onboarding_routes import capability_onboarding_web
from .capability_startup_transition_routes import (
    capability_startup_transition_web,
)
from .data_upload_routes import data_upload_web
from .docs_routes import docs_web
from .federation_pairing_routes import federation_pairing_web
from .federation_routes import federation_web
from .operator_strategy_routes import operator_strategy_web
from .operator_support_routes import operator_support_web
from .provider_federation_routes import provider_federation_web
from .routes import web
from .server_setup_routes import server_setup_web
from .services.capability_benchmark_service import get_capability_benchmark_service
from .services.capability_contribution_service import (
    get_capability_contribution_service,
)
from .services.capability_inspection_service import get_capability_inspection_service
from .services.capability_startup_transition_service import (
    get_capability_startup_transition_service,
)
from .services.catalog_service import ArtifactCatalog
from .services.federation_pairing_install import install_federation_pairing
from .services.onboarding_view_normalizer import normalize_onboarding_view_model
from .services.recorder_control_service import get_recorder_control_service
from .services.run_once_capability_evidence import install_run_once_capability_evidence
from .services.server_setup_service import (
    AI_MODEL_CHOICES,
    AI_PROVIDER_MODES,
    DEPLOYMENT_MODES,
    ServerSetupError,
    load_settings,
    ollama_status,
)
from .source_routes import source_web

_CONTRIBUTION_RECONCILE_EXTENSION_KEY = "capability_contribution_startup_reconciled"


def _resume_persisted_contributions_safely() -> tuple[int, int]:
    """Reconcile saved intent only when saved capability evidence is accepted.

    Returns ``(reconciled, suspended)``. Inspection and benchmarks are never run
    here. If saved evidence is stale, enabled contributions are fenced through
    the existing suspension path instead of being reactivated from stale input.
    """

    contribution = get_capability_contribution_service()
    if not contribution.has_persisted_intents():
        return 0, 0

    inspection = get_capability_inspection_service()
    snapshot = inspection.load()
    if snapshot is None:
        return 0, 0

    evidence_current = inspection.state(snapshot) == "current"
    if evidence_current:
        benchmarks = get_capability_benchmark_service()
        _summary, _cards, evidence_current = benchmarks.view_model(
            snapshot,
            connected=True,
        )

    if evidence_current:
        return len(tuple(contribution.reconcile())), 0

    suspended = 0
    for intent in contribution.intents():
        desired = getattr(intent, "desired_state", None)
        if desired is not ContributionDesiredState.ENABLED:
            continue
        try:
            contribution.suspend(intent.candidate_id)
        except Exception as exc:  # noqa: BLE001 - fencing must remain best effort
            current_app.logger.info(
                "Contribution suspension unavailable during stale-evidence "
                "recovery (%s)",
                type(exc).__name__,
            )
            continue
        suspended += 1
    return 0, suspended


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SECRET_KEY"] = os.getenv("MSH_FLASK_SECRET", "msh-dev")
    if app.config.get("MAX_CONTENT_LENGTH") is None:
        app.config["MAX_CONTENT_LENGTH"] = int(
            os.getenv("MSH_UPLOAD_MAX_REQUEST_BYTES", str(1100 * 1024 * 1024))
        )
    app.config.setdefault(
        "DATA_UPLOAD_DATABASE",
        os.getenv("MSH_DATA_UPLOAD_DATABASE", "data/imports/uploads.sqlite3"),
    )
    app.config.setdefault(
        "DATA_UPLOAD_STAGING_DIRECTORY",
        os.getenv("MSH_DATA_UPLOAD_STAGING_DIR", "data/imports/staging"),
    )
    app.config.setdefault(
        "DATA_UPLOAD_PUBLISHED_DIRECTORY",
        os.getenv("MSH_DATA_UPLOAD_PUBLISHED_DIR", "data/uploads"),
    )
    app.config.setdefault(
        "DATA_UPLOAD_MAX_FILES",
        int(os.getenv("MSH_DATA_UPLOAD_MAX_FILES", "50")),
    )
    app.config.setdefault(
        "DATA_UPLOAD_MAX_FILE_BYTES",
        int(os.getenv("MSH_DATA_UPLOAD_MAX_FILE_BYTES", str(512 * 1024 * 1024))),
    )
    app.config.setdefault(
        "DATA_UPLOAD_MAX_TOTAL_BYTES",
        int(os.getenv("MSH_DATA_UPLOAD_MAX_TOTAL_BYTES", str(1024 * 1024 * 1024))),
    )
    app.config.setdefault(
        "DATA_UPLOAD_MAX_LINE_BYTES",
        int(os.getenv("MSH_DATA_UPLOAD_MAX_LINE_BYTES", str(4 * 1024 * 1024))),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_IDENTITY_DIRECTORY",
        os.getenv(
            "MSH_FEDERATION_NODE_STATE_DIR",
            "data/federation/device",
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_STATE_DATABASE",
        os.getenv(
            "MSH_FEDERATION_ONBOARDING_DATABASE",
            "data/federation/onboarding/onboarding.sqlite3",
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_TRANSITION_DATABASE",
        os.getenv(
            "MSH_FEDERATION_TRANSITION_DATABASE",
            app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_BENCHMARK_DATABASE",
        os.getenv(
            "MSH_FEDERATION_BENCHMARK_DATABASE",
            app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_CONTRIBUTION_DATABASE",
        os.getenv(
            "MSH_FEDERATION_CONTRIBUTION_DATABASE",
            app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_COORDINATOR_DATABASE",
        os.getenv(
            "MSH_FEDERATION_COORDINATOR_DATABASE",
            "data/federation/relay/control.sqlite3",
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_DEVICE_NAME",
        os.getenv("MSH_DEVICE_NAME", "This MSH device"),
    )
    app.config.setdefault("CAPABILITY_ONBOARDING_DISCOVERY_SOURCES", ())
    app.config.setdefault("CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS", None)
    app.config.setdefault("CAPABILITY_ONBOARDING_CONTRIBUTION_SOURCES", None)
    app.config.setdefault("CAPABILITY_ONBOARDING_CONTRIBUTION_ADAPTERS", None)
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_PAIRING_RELAY_URL",
        os.getenv("MSH_PAIRING_RELAY_URL", ""),
    )
    remote_pairing_path = os.getenv("MSH_FEDERATION_REMOTE_PAIRING_PATH", "")
    if remote_pairing_path:
        app.config.setdefault(
            "CAPABILITY_ONBOARDING_REMOTE_PAIRING_PATH",
            remote_pairing_path,
        )
    # The frozen snapshot/result contracts retain an expiry timestamp, while the
    # installed product uses explicit refresh plus dependency/version invalidation.
    # A long default still keeps newly written compatibility metadata sensible.
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_INSPECTION_TTL_SECONDS",
        int(
            os.getenv(
                "MSH_INSPECTION_TTL_SECONDS",
                str(DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS),
            )
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_CONTRIBUTION_TTL_SECONDS",
        int(os.getenv("MSH_CONTRIBUTION_TTL_SECONDS", "900")),
    )
    app.jinja_env.globals["normalize_onboarding_view_model"] = (
        normalize_onboarding_view_model
    )
    install_federation_pairing(app)
    install_run_once_capability_evidence(app)

    catalog = ArtifactCatalog()
    app.config["ARTIFACT_CATALOG"] = catalog

    @app.context_processor
    def inject_catalog_freshness() -> dict[str, object]:
        return {"artifact_catalog_freshness": catalog.freshness()}

    @app.context_processor
    def inject_server_setup() -> dict[str, object]:
        try:
            settings = load_settings()
            setup_error = ""
        except ServerSetupError as exc:
            settings = None
            setup_error = str(exc)

        ai_status = None
        if (
            settings is not None
            and settings.configured
            and settings.user_setup_complete
            and settings.deployment_mode != "recorder-only"
            and request.path == "/startup"
        ):
            ai_status = ollama_status(settings)

        recorder_status = get_recorder_control_service().status(settings)
        return {
            "server_setup_settings": settings,
            "server_setup_error": setup_error,
            "server_setup_modes": DEPLOYMENT_MODES,
            "server_setup_ai_choices": AI_MODEL_CHOICES,
            "server_setup_ai_provider_modes": AI_PROVIDER_MODES,
            "server_setup_ollama_status": ai_status,
            "server_setup_recorder_status": recorder_status,
        }

    register_artifact_catalog_refresh(
        lambda reason: catalog.start_background_rescan_if_idle(reason=reason)
    )
    catalog.start_background_rescan_if_idle(reason="startup")
    get_runtime_manager().mark_app_started()

    @app.before_request
    def reconcile_persisted_contributions_from_current_evidence():
        """Own the single automatic contribution reconciliation for this app."""

        endpoint = request.endpoint or ""
        if endpoint.startswith("static") or app.extensions.get(
            _CONTRIBUTION_RECONCILE_EXTENSION_KEY
        ):
            return
        # Set this before doing any work so the retained CFI-5/CFI-6 hooks see
        # the reconciliation as owned here and cannot execute a second path.
        app.extensions[_CONTRIBUTION_RECONCILE_EXTENSION_KEY] = True
        try:
            reconciled, suspended = _resume_persisted_contributions_safely()
        except Exception as exc:  # noqa: BLE001 - startup must remain available
            app.logger.info(
                "Capability contribution startup reconcile unavailable (%s)",
                type(exc).__name__,
            )
            return
        if suspended:
            app.logger.info(
                "Suspended %d enabled contribution(s) because saved capability "
                "evidence requires explicit refresh",
                suspended,
            )
        elif reconciled:
            app.logger.info(
                "Reconciled %d persisted contribution choice(s) from saved "
                "capability evidence",
                reconciled,
            )

    @app.before_request
    def resolve_completed_capability_runtime_choice():
        """Repair a stale legacy runtime choice before compatibility gates run."""

        runtime_manager = get_runtime_manager()
        if not runtime_manager.requires_startup_choice():
            return
        try:
            flags = get_capability_startup_transition_service().capability_flags()
        except Exception as exc:  # noqa: BLE001 - legacy gate remains fail-closed
            app.logger.info(
                "Capability runtime request handoff unavailable (%s)",
                type(exc).__name__,
            )
            return
        if not bool(flags.get("completed")) or not bool(flags.get("runtime")):
            return
        accepted, _message = runtime_manager.choose_startup_mode(
            "continue_existing"
        )
        if not accepted:
            app.logger.warning(
                "Completed capability runtime could not clear legacy startup choice"
            )
        return

    # CFI-6 owns capability-first startup and migration before CFI-5 and the
    # retained role-first compatibility gates. It persists intent only and
    # delegates every operational authority to the already registered services.
    app.register_blueprint(docs_web)
    app.register_blueprint(federation_web)
    app.register_blueprint(federation_pairing_web)
    app.register_blueprint(capability_startup_transition_web)
    app.register_blueprint(capability_contribution_web)
    app.register_blueprint(capability_benchmark_web)
    app.register_blueprint(capability_inspection_web)
    app.register_blueprint(capability_onboarding_web)
    app.register_blueprint(server_setup_web)
    app.register_blueprint(web)
    app.register_blueprint(data_upload_web)
    app.register_blueprint(source_web)
    app.register_blueprint(operator_strategy_web)
    app.register_blueprint(operator_support_web)
    app.register_blueprint(ai_web)
    app.register_blueprint(provider_federation_web)
    return app


def _start_runtime_from_capability_state(
    app: Flask,
    setup: object | None = None,
) -> str:
    """Start runtime without reviving the retired legacy startup prompt.

    A completed capability-first transition already made the safe startup choice:
    preserve existing sessions and continue processing. Older installations that
    have not completed that transition retain the explicit legacy choice.
    """

    with app.app_context():
        flags = get_capability_startup_transition_service().capability_flags(setup)
    if not bool(flags.get("runtime")):
        return "disabled"

    runtime_manager = get_runtime_manager()
    if runtime_manager.requires_startup_choice():
        if not bool(flags.get("completed")):
            return "legacy-choice-required"
        accepted, _message = runtime_manager.choose_startup_mode(
            "continue_existing"
        )
        return "started" if accepted else "failed"

    start_runtime_background()
    return "started"


if __name__ == "__main__":
    app = create_app()
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"

    setup = None
    try:
        setup = load_settings()
    except ServerSetupError as exc:
        print(
            f"[orchestrator] setup needs attention before runtime starts: {exc}",
            flush=True,
        )

    runtime_start_state = "disabled"
    if os.getenv("MSH_SKIP_ORCHESTRATION", "0") != "1":
        try:
            runtime_start_state = _start_runtime_from_capability_state(app, setup)
        except Exception as exc:  # noqa: BLE001 - corrupt state must fail closed
            runtime_start_state = "repair"
            print(
                "[orchestrator] capability-first startup state needs repair: "
                f"{type(exc).__name__}",
                flush=True,
            )

    if runtime_start_state == "started":
        print(
            "[orchestrator] capability-first startup: Flask available "
            "immediately, runtime starts in background",
            flush=True,
        )
    elif runtime_start_state == "legacy-choice-required":
        print(
            "[orchestrator] runtime progress choice remains available through "
            "the controlled legacy fallback",
            flush=True,
        )
    elif runtime_start_state == "failed":
        print(
            "[orchestrator] capability-first runtime continuation could not be "
            "applied safely",
            flush=True,
        )
    elif (
        setup is None
        or not getattr(setup, "configured", False)
        or not getattr(setup, "user_setup_complete", False)
    ):
        print(
            "[orchestrator] capability-first onboarding required at /onboarding; "
            "runtime will remain idle",
            flush=True,
        )
    elif runtime_start_state != "repair":
        print(
            "[orchestrator] runtime disabled by capability intent, environment, "
            "or repair state",
            flush=True,
        )

    print(f"[orchestrator] starting Flask app on http://{host}:{port}", flush=True)
    app.run(host=host, port=port, debug=debug, threaded=True)
