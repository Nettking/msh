"""Flask application factory and webapp-first runtime startup entrypoint."""

from __future__ import annotations

import os

from flask import Flask

from catalog.common.artifact_refresh import register_artifact_catalog_refresh
from catalog.orchestrator.pipeline import get_runtime_manager, start_runtime_background

from .routes import web
from .services.catalog_service import ArtifactCatalog


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SECRET_KEY"] = os.getenv("MSH_FLASK_SECRET", "msh-dev")
    catalog = ArtifactCatalog()
    app.config["ARTIFACT_CATALOG"] = catalog

    @app.context_processor
    def inject_catalog_freshness() -> dict[str, object]:
        return {"artifact_catalog_freshness": catalog.freshness()}

    register_artifact_catalog_refresh(lambda reason: catalog.start_background_rescan_if_idle(reason=reason))
    catalog.start_background_rescan_if_idle(reason="startup")
    get_runtime_manager().mark_app_started()
    app.register_blueprint(web)
    return app


if __name__ == "__main__":
    app = create_app()
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"

    if os.getenv("MSH_SKIP_ORCHESTRATION", "0") != "1":
        runtime_manager = get_runtime_manager()
        if runtime_manager.requires_startup_choice():
            print("[orchestrator] startup mode selection required at /startup before runtime processing begins", flush=True)
        else:
            print("[orchestrator] webapp-first startup: Flask available immediately, runtime starts in background", flush=True)
            start_runtime_background()
    else:
        print("[orchestrator] orchestration skipped; runtime manager will remain idle", flush=True)

    print(f"[orchestrator] starting Flask app on http://{host}:{port}", flush=True)
    app.run(host=host, port=port, debug=debug, threaded=True)
