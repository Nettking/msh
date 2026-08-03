"""Flask application factory and webapp-first runtime startup entrypoint."""

from __future__ import annotations

import os

from flask import Flask, request

from catalog.common.artifact_refresh import register_artifact_catalog_refresh
from catalog.orchestrator.pipeline import get_runtime_manager, start_runtime_background

from .ai_routes import ai_web
from .capability_benchmark_routes import capability_benchmark_web
from .capability_inspection_routes import capability_inspection_web
from .capability_onboarding_routes import capability_onboarding_web
from .docs_routes import docs_web
from .federation_routes import federation_web
from .operator_strategy_routes import operator_strategy_web
from .operator_support_routes import operator_support_web
from .provider_federation_routes import provider_federation_web
from .routes import web
from .server_setup_routes import server_setup_web
from .services.catalog_service import ArtifactCatalog
from .services.recorder_control_service import get_recorder_control_service
from .services.server_setup_service import (
    AI_MODEL_CHOICES,
    AI_PROVIDER_MODES,
    DEPLOYMENT_MODES,
    ServerSetupError,
    load_settings,
    ollama_status,
    runtime_should_start,
)
from .source_routes import source_web


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SECRET_KEY"] = os.getenv("MSH_FLASK_SECRET", "msh-dev")
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
        "CAPABILITY_ONBOARDING_BENCHMARK_DATABASE",
        os.getenv(
            "MSH_FEDERATION_BENCHMARK_DATABASE",
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
    app.config.setdefault(
        "CAPABILITY_ONBOARDING_INSPECTION_TTL_SECONDS",
        int(os.getenv("MSH_INSPECTION_TTL_SECONDS", "900")),
    )

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
    # Read-only and capability-onboarding surfaces are registered before the
    # legacy setup/runtime gates. CFI-4 adds benchmark evidence only and leaves
    # contribution, provider, storage, job, and role-first authority unchanged.
    app.register_blueprint(docs_web)
    app.register_blueprint(federation_web)
    app.register_blueprint(capability_benchmark_web)
    app.register_blueprint(capability_inspection_web)
    app.register_blueprint(capability_onboarding_web)
    app.register_blueprint(server_setup_web)
    app.register_blueprint(web)
    app.register_blueprint(source_web)
    app.register_blueprint(operator_strategy_web)
    app.register_blueprint(operator_support_web)
    app.register_blueprint(ai_web)
    app.register_blueprint(provider_federation_web)
    return app


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
            f"[orchestrator] browser setup needs attention before runtime starts: {exc}",
            flush=True,
        )

    if (
        os.getenv("MSH_SKIP_ORCHESTRATION", "0") != "1"
        and setup is not None
        and runtime_should_start(setup)
    ):
        runtime_manager = get_runtime_manager()
        if runtime_manager.requires_startup_choice():
            print(
                "[orchestrator] startup mode selection required at /startup "
                "before runtime processing begins",
                flush=True,
            )
        else:
            print(
                "[orchestrator] webapp-first startup: Flask available immediately, "
                "runtime starts in background",
                flush=True,
            )
            start_runtime_background()
    elif (
        setup is None
        or not getattr(setup, "configured", False)
        or not getattr(setup, "user_setup_complete", False)
    ):
        print(
            "[orchestrator] first-time browser setup required at /startup; "
            "runtime will remain idle",
            flush=True,
        )
    else:
        print(
            "[orchestrator] orchestration disabled by environment or browser setup; "
            "runtime manager will remain idle",
            flush=True,
        )

    print(f"[orchestrator] starting Flask app on http://{host}:{port}", flush=True)
    app.run(host=host, port=port, debug=debug, threaded=True)
