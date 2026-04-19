from __future__ import annotations

import os

from flask import Flask

from catalog.orchestrator.pipeline import run_orchestration

from .routes import web
from .services.catalog_service import ArtifactCatalog


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SECRET_KEY"] = os.getenv("MSH_FLASK_SECRET", "msh-dev")
    app.config["ARTIFACT_CATALOG"] = ArtifactCatalog()
    app.register_blueprint(web)
    return app


if __name__ == "__main__":
    if os.getenv("MSH_SKIP_ORCHESTRATION", "0") != "1":
        print("[orchestrator] pre-start orchestration enabled (default coupling)", flush=True)
        orchestration_result = run_orchestration()
        print(
            f"[orchestrator] Flask startup handoff (session={orchestration_result.session_id}, "
            f"failed_scripts={len(orchestration_result.failed_scripts)})",
            flush=True,
        )

    app = create_app()
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    print(f"[orchestrator] starting Flask app on http://{host}:{port}", flush=True)
    app.run(host=host, port=port, debug=debug)
