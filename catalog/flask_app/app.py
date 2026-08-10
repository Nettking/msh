"""Flask application factory and capability-first runtime startup entrypoint."""

from __future__ import annotations

import os

from flask import Flask, current_app, request

from catalog.capabilities.benchmarking.policy import (
    DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS,
)
from catalog.common.artifact_refresh import register_artifact_catalog_refresh
from catalog.federation.onboarding_models import ContributionDesiredState
from catalog.orchestrator.capability_startup import (
    prepare_capability_runtime,
    start_capability_runtime_background,
)
from catalog.orchestrator.pipeline import get_runtime_manager

from .ai_routes import ai_web
from .capability_benchmark_routes import capability_benchmark_web
from .capability_contribution_routes import capability_contribution_web
from .capability_inspection_routes import capability_inspection_web
from .capability_onboarding_routes import capability_onboarding_web
from .capability_product_routes import install_capability_product_routes
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
from .services.run_once_capability_evidence import install_run_once_capability_evidence
from .services.startup_contribution_reconcile import (
    run_startup_contribution_reconcile,
)
from .source_routes import source_web


def _resume_persisted_contributions_safely() -> tuple[int, int]:
    """Reconcile saved intent only when saved capability evidence is accepted."""

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
    app.config["SECRET_KEY"] = os.getenv("FCP_FLASK_SECRET", "fcp-dev")
    if app.config.get("MAX_CONTENT_LENGTH") is None:
        app.config["MAX_CONTENT_LENGTH"] = int(
            os.getenv("FCP_UPLOAD_MAX_REQUEST_BYTES", str(1100 * 1024 * 1024))
        )
    app.config.setdefault(
        "DATA_UPLOAD_DATABASE",
        os.getenv("FCP_DATA_UPLOAD_DATABASE", "data/imports/uploads.sqlite3"),
    )
    app.config.setdefault(
        "DATA_UPLOAD_STAGING_DIRECTORY",
        os.getenv("FCP_DATA_UPLOAD_STAGING_DIR", "data/imports/staging"),
    )
    app.config.setdefault(
        "DATA_UPLOAD_PUBLISHED_DIRECTORY",
        os.getenv("FCP_DATA_UPLOAD_PUBLISHED_DIR", "data/uploads"),
    )
    app.config.setdefault(
        "DATA_UPLOAD_MAX_FILES",
        int(os.getenv("FCP_DATA_UPLOAD_MAX_FILES", "50")),
    )
    app.config.setdefault(
        "DATA_UPLOAD_MAX_FILE_BYTES",
        int(os.getenv("FCP_DATA_UPLOAD_MAX_FILE_BYTES", str(512 * 1024 * 1024))),
    )
    app.config.setdefault(
        "DATA_UPLOAD_MAX_TOTAL_BYTES",
        int(os.getenv("FCP_DATA_UPLOAD_MAX_TOTAL_BYTES", str(1024 * 1024 * 1024))),
    )
    app.config.setdefault(
        "DATA_UPLOAD_MAX_LINE_BYTES",
        int(os.getenv("FCP_DATA_UPLOAD_MAX_LINE_BYTES", str(4 * 1024 * 1024))),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_IDENTITY_DIRECTORY",
        os.getenv(
            "FCP_FEDERATION_NODE_STATE_DIR",
            "data/federation/device",
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_STATE_DATABASE",
        os.getenv(
            "FCP_FEDERATION_ONBOARDING_DATABASE",
            "data/federation/onboarding/onboarding.sqlite3",
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_TRANSITION_DATABASE",
        os.getenv(
            "FCP_FEDERATION_TRANSITION_DATABASE",
            app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_BENCHMARK_DATABASE",
        os.getenv(
            "FCP_FEDERATION_BENCHMARK_DATABASE",
            app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_CONTRIBUTION_DATABASE",
        os.getenv(
            "FCP_FEDERATION_CONTRIBUTION_DATABASE",
            app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_COORDINATOR_DATABASE",
        os.getenv(
            "FCP_FEDERATION_COORDINATOR_DATABASE",
            "data/federation/relay/control.sqlite3",
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_DEVICE_NAME",
        os.getenv("FCP_DEVICE_NAME", "This FCP device"),
    )
    app.config.setdefault("CAPABILITY_ONBOARDING_DISCOVERY_SOURCES", ())
    app.config.setdefault("CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS", None)
    app.config.setdefault("CAPABILITY_ONBOARDING_CONTRIBUTION_SOURCES", None)
    app.config.setdefault("CAPABILITY_ONBOARDING_CONTRIBUTION_ADAPTERS", None)
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_PAIRING_RELAY_URL",
        os.getenv("FCP_PAIRING_RELAY_URL", ""),
    )
    remote_pairing_path = os.getenv("FCP_FEDERATION_REMOTE_PAIRING_PATH", "")
    if remote_pairing_path:
        app.config.setdefault(
            "CAPABILITY_ONBOARDING_REMOTE_PAIRING_PATH",
            remote_pairing_path,
        )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_INSPECTION_TTL_SECONDS",
        int(
            os.getenv(
                "FCP_INSPECTION_TTL_SECONDS",
                str(DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS),
            )
        ),
    )
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_CONTRIBUTION_TTL_SECONDS",
        int(os.getenv("FCP_CONTRIBUTION_TTL_SECONDS", "900")),
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

    register_artifact_catalog_refresh(
        lambda reason: catalog.start_background_rescan_if_idle(reason=reason)
    )
    catalog.start_background_rescan_if_idle(reason="startup")
    get_runtime_manager().mark_app_started()

    @app.before_request
    def reconcile_persisted_contributions_from_current_evidence():
        endpoint = request.endpoint or ""
        if endpoint.startswith("static"):
            return
        attempted, result, error = run_startup_contribution_reconcile(
            app.extensions,
            _resume_persisted_contributions_safely,
        )
        if not attempted:
            return
        if error is not None:
            app.logger.info(
                "Capability contribution startup reconcile unavailable (%s)",
                type(error).__name__,
            )
            return
        assert result is not None
        reconciled, suspended = result
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
    def prepare_completed_capability_runtime():
        """Resolve historical runtime namespace state from capability intent only."""

        runtime_manager = get_runtime_manager()
        if not runtime_manager.requires_startup_choice():
            return
        try:
            flags = get_capability_startup_transition_service().capability_flags()
        except Exception as exc:  # noqa: BLE001 - startup remains fail-closed
            app.logger.info(
                "Capability runtime preparation unavailable (%s)",
                type(exc).__name__,
            )
            return
        if not bool(flags.get("completed")) or not bool(flags.get("runtime")):
            return
        try:
            prepare_capability_runtime(runtime_manager)
        except Exception as exc:  # noqa: BLE001 - runtime startup remains isolated
            app.logger.warning(
                "Capability runtime preparation failed (%s)",
                type(exc).__name__,
            )

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
    install_capability_product_routes(app)
    return app


def _start_runtime_from_capability_state(app: Flask) -> str:
    """Start runtime only from completed persisted capability intent."""

    with app.app_context():
        flags = get_capability_startup_transition_service().capability_flags()
    if not bool(flags.get("completed")) or not bool(flags.get("runtime")):
        return "disabled"
    start_capability_runtime_background()
    return "started"


if __name__ == "__main__":
    app = create_app()
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"

    runtime_start_state = "disabled"
    if os.getenv("FCP_SKIP_ORCHESTRATION", "0") != "1":
        try:
            runtime_start_state = _start_runtime_from_capability_state(app)
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
    elif runtime_start_state == "disabled":
        print(
            "[orchestrator] runtime remains idle until completed capability intent "
            "enables it",
            flush=True,
        )

    print(f"[orchestrator] starting Flask app on http://{host}:{port}", flush=True)
    app.run(host=host, port=port, debug=debug, threaded=True)
