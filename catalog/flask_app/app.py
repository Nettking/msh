from __future__ import annotations

import os

from flask import Flask

from catalog.orchestrator.pipeline import get_runtime_manager, start_runtime_background

from .routes import web
from .services.catalog_service import ArtifactCatalog


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SECRET_KEY"] = os.getenv("MSH_FLASK_SECRET", "msh-dev")
    app.config["ARTIFACT_CATALOG"] = ArtifactCatalog()
    get_runtime_manager().mark_app_started()
    app.register_blueprint(web)
    return app


if __name__ == "__main__":
    app = create_app()
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"

    if os.getenv("MSH_SKIP_ORCHESTRATION", "0") != "1":
        print("[orchestrator] webapp-first startup: Flask available immediately, runtime starts in background", flush=True)
        start_runtime_background()
    else:
        print("[orchestrator] orchestration skipped; runtime manager will remain idle", flush=True)

    print(f"[orchestrator] starting Flask app on http://{host}:{port}", flush=True)
    app.run(host=host, port=port, debug=debug)
