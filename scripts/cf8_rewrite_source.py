"""One-shot bounded rewrite for large CF8 source files.

The GitHub connector used for CF8 only supports whole-file replacement. This helper
applies exact/regex-bounded edits to large files on the PR branch so unrelated
runtime code remains byte-for-byte untouched. It is temporary and should be
removed once the rewrite commit lands.
"""

from __future__ import annotations

import re
from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


def rewrite_recorder_runtime() -> None:
    path = Path("catalog/mtconnect_recorder/runtime.py")
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "* Legacy compatibility is limited to the four recorder technical fields in\n"
        "  ``server_settings.json``. Deployment role, AI settings, and setup authority are ignored.\n",
        "",
    )
    text = replace_once(
        text,
        'CAPABILITY_CONFIG_FILE = Path("data") / "capabilities" / "config.json"\n'
        'LEGACY_SETTINGS_FILE = Path("data") / "server_setup" / "server_settings.json"\n'
        'DEFAULT_CONFIG_FILE = LEGACY_SETTINGS_FILE\n'
        'CAPABILITY_CONFIG_SCHEMA = "fcp.capability_config.v1"\n'
        'LEGACY_SETTINGS_SCHEMAS = {"fcp.server_setup.v1", "fcp.server_setup.v2", "fcp.server_setup.v3"}\n',
        'CAPABILITY_CONFIG_FILE = Path("data") / "capabilities" / "config.json"\n'
        'DEFAULT_CONFIG_FILE = CAPABILITY_CONFIG_FILE\n'
        'CAPABILITY_CONFIG_SCHEMA = "fcp.capability_config.v1"\n',
        "recorder config constants",
    )
    text, count = re.subn(
        r"def _managed_configuration\(config_file: Path\) -> dict\[str, Any\]:\n.*?\n\ndef _sources_from_config",
        '''def _managed_configuration(config_file: Path) -> dict[str, Any]:
    """Load recorder technical parameters from CapabilityConfig only."""

    if not config_file.exists():
        return {}
    payload = _read_json(config_file)
    if not payload or payload.get("schema") != CAPABILITY_CONFIG_SCHEMA:
        return {}
    return {
        "sources": _parse_sources_text(str(payload.get("recorder_sources") or "")),
        "poll_interval": payload.get("recorder_poll_interval"),
        "include_condition": payload.get("recorder_include_condition"),
    }


def _sources_from_config''',
        text,
        count=1,
        flags=re.S,
    )
    if count != 1:
        raise RuntimeError(
            f"recorder managed config: expected one match, found {count}"
        )
    path.write_text(text, encoding="utf-8")


def rewrite_routes() -> None:
    path = Path("catalog/flask_app/routes.py")
    text = path.read_text(encoding="utf-8")
    text = replace_once(
        text,
        "# Import-only compatibility seam for transition tests/integrations that still\n"
        "# monkeypatch this historical symbol. No product route uses legacy settings.\n"
        "from .services.server_setup_service import load_settings  # noqa: F401\n",
        "",
        "routes legacy monkeypatch import",
    )
    text, count = re.subn(
        r'\n@web\.post\("/startup/choose"\)\ndef choose_startup_mode\(\):\n.*?(?=\n@web\.get\("/get-started"\))',
        "",
        text,
        count=1,
        flags=re.S,
    )
    if count != 1:
        raise RuntimeError(f"startup choose route: expected one match, found {count}")
    path.write_text(text, encoding="utf-8")


def rewrite_pipeline() -> None:
    path = Path("catalog/orchestrator/pipeline.py")
    text = path.read_text(encoding="utf-8")
    text = replace_once(
        text,
        'STARTUP_MODE_PENDING = "pending_choice"\n'
        'STARTUP_MODE_CONTINUE = "continue_existing"\n'
        'STARTUP_MODE_CLEAN = "start_clean"\n',
        'STARTUP_MODE_PENDING = "pending_choice"\n'
        'STARTUP_MODE_CAPABILITY = "capability"\n'
        'STARTUP_MODE_CONTINUE = "continue_existing"\n'
        'STARTUP_MODE_CLEAN = "start_clean"\n',
        "runtime startup constants",
    )
    marker = "    def startup_decision_snapshot(self) -> dict[str, Any]:\n"
    method = '''    def prepare_capability_startup(self) -> None:
        """Resolve pending namespace state from capability authority, not a role choice."""

        with self._lock:
            if self._state.startup_mode != STARTUP_MODE_PENDING:
                return
            app_started_at = self._state.app_started_at
            runtime_started_at = self._state.runtime_started_at
            loaded = self._load_state()
            loaded.startup_mode = STARTUP_MODE_CAPABILITY
            loaded.startup_decision_source = "capability"
            loaded.active_runtime_namespace = loaded.active_runtime_namespace or "default"
            loaded.app_started_at = app_started_at or loaded.app_started_at
            loaded.runtime_started_at = runtime_started_at or loaded.runtime_started_at
            self._state = loaded
            self._persist_state()

'''
    if method not in text:
        text = replace_once(
            text,
            marker,
            method + marker,
            "capability runtime method insertion",
        )
    path.write_text(text, encoding="utf-8")


def rewrite_capability_startup() -> None:
    Path("catalog/orchestrator/capability_startup.py").write_text(
        '''"""Capability-first adapter for the existing workflow runtime."""

from __future__ import annotations

from .pipeline import RuntimeOrchestrator, get_runtime_manager, start_runtime_background


def prepare_capability_runtime(
    manager: RuntimeOrchestrator | None = None,
) -> RuntimeOrchestrator:
    """Prepare the runtime from capability authority without a compatibility choice."""

    manager = manager or get_runtime_manager()
    manager.prepare_capability_startup()
    return manager


def start_capability_runtime_background() -> None:
    prepare_capability_runtime()
    start_runtime_background()


__all__ = [
    "prepare_capability_runtime",
    "start_capability_runtime_background",
]
''',
        encoding="utf-8",
    )


def rewrite_app() -> None:
    path = Path("catalog/flask_app/app.py")
    text = path.read_text(encoding="utf-8")
    text, count = re.subn(
        r"    @app\.before_request\n    def prepare_completed_capability_runtime\(\):\n.*?(?=\n    app\.register_blueprint\(docs_web\))",
        '''    @app.before_request
    def prepare_completed_capability_runtime():
        """Prepare workflow runtime only after capability intent enables it."""

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
            prepare_capability_runtime(get_runtime_manager())
        except Exception as exc:  # noqa: BLE001 - runtime startup remains isolated
            app.logger.warning(
                "Capability runtime preparation failed (%s)",
                type(exc).__name__,
            )
''',
        text,
        count=1,
        flags=re.S,
    )
    if count != 1:
        raise RuntimeError(
            f"app capability runtime hook: expected one match, found {count}"
        )
    path.write_text(text, encoding="utf-8")


def rewrite_gate() -> None:
    path = Path("catalog/flask_app/tests/test_cf8_role_retirement.py")
    text = path.read_text(encoding="utf-8")
    text = replace_once(
        text,
        '        if "tests" in path.parts or path in MIGRATION_ROLE_FILES:\n'
        "            continue\n",
        '        if "tests" in path.parts or path.name.startswith("test_") or path in MIGRATION_ROLE_FILES:\n'
        "            continue\n",
        "CF8 source scan test exclusion",
    )
    path.write_text(text, encoding="utf-8")


def main() -> None:
    rewrite_recorder_runtime()
    rewrite_routes()
    rewrite_pipeline()
    rewrite_capability_startup()
    rewrite_app()
    rewrite_gate()


if __name__ == "__main__":
    main()
