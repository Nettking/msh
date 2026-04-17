"""Minimal backward-compatible runner utility shim.

New code should import from focused modules under ``catalog.runner`` directly.
This file intentionally contains no legacy runner implementation logic; it only
re-exports a small compatibility-safe surface during the transition away from
``menu_utils``.
"""

from catalog.runner.data_filtering import discover_available_dates, ensure_session_filtered_data
from catalog.runner.script_catalog import ScriptOption, discover_runnable_scripts, repo_root
from catalog.runner.script_exec import copy_repo_catalog_into_workspace, execute_script_for_session
from catalog.runner.session_store import (
    WORKFLOW_SCRIPT_ORDER,
    WORKFLOW_STEPS,
    initialize_session_metadata,
    list_sessions,
    normalize_session_metadata,
    workflow_step_status,
    write_session_metadata,
)
from catalog.runner.ui import print_numbered_menu, prompt_menu_choice

__all__ = [
    "ScriptOption",
    "WORKFLOW_SCRIPT_ORDER",
    "WORKFLOW_STEPS",
    "copy_repo_catalog_into_workspace",
    "discover_available_dates",
    "discover_runnable_scripts",
    "ensure_session_filtered_data",
    "execute_script_for_session",
    "initialize_session_metadata",
    "list_sessions",
    "normalize_session_metadata",
    "print_numbered_menu",
    "prompt_menu_choice",
    "repo_root",
    "workflow_step_status",
    "write_session_metadata",
]
