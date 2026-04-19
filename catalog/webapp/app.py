"""Main Streamlit digital twin workspace orchestrator for playback, analyses, and exploration."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

if not __package__:
    repo_root = Path(__file__).resolve().parents[2]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)

from catalog.webapp.analysis_registry import configured_scan_dirs
from catalog.webapp.analysis_views import render_analyses_browser, render_machine_view, render_system_status
from catalog.webapp.data_sources import load_data_from_path, load_registry_frame, resolve_bootstrap_source
from catalog.webapp.exploration import render_dataset_views, render_exploration_mode
from catalog.webapp.playback import render_playback_mode, validate_playback_export_schema
from catalog.webapp.runtime import scan_registry


def main():
    """Run the Streamlit digital twin workspace with tab-level delegation."""
    st.set_page_config(page_title="MSH Digital Twin Workspace", layout="wide")
    st.title("MSH Digital Twin Workspace")
    st.caption("Always-on web workspace for system status, analyses, playback, and exploration.")

    scan_nonce = st.session_state.get("scan_nonce", 0)
    with st.sidebar:
        st.header("Runtime")
        if st.button("Rescan now"):
            st.session_state.scan_nonce = scan_nonce + 1
            scan_nonce = st.session_state.scan_nonce

        auto_refresh = st.checkbox("Auto-refresh", value=True)
        refresh_sec = st.slider("Refresh every (seconds)", min_value=10, max_value=300, value=45, step=5)

        st.header("Manual/secondary data source")
        uploaded = st.file_uploader("Upload CSV/Parquet/JSONL/JSON", type=["csv", "parquet", "pq", "jsonl", "json"])
        default_source, source_hint = resolve_bootstrap_source()
        local_path = st.text_input("or local path", value=default_source)
        if source_hint:
            st.caption(f"Prefilled source: {source_hint}")

    scan_dirs = tuple(configured_scan_dirs())
    scan_started = time.time()
    refresh_bucket = int(time.time() // refresh_sec) if auto_refresh else 0
    artifacts, warnings = scan_registry(scan_dirs, scan_nonce, refresh_bucket)

    tabs = st.tabs(["System status", "Overview", "Analyses", "Machine view", "Playback", "Exploration"])

    with tabs[0]:
        render_system_status(artifacts, warnings, scan_dirs, scan_started)

    selected_artifact = None
    with tabs[1]:
        st.subheader("Overview")
        if artifacts:
            overview_df = pd.DataFrame(artifacts)
            st.dataframe(
                overview_df[["analysis_name", "kind", "status", "row_count", "machine_count", "day_count", "modified_at"]],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No datasets indexed yet.")

    with tabs[2]:
        selected_artifact = render_analyses_browser(artifacts)
        if selected_artifact and selected_artifact["status"] == "ready":
            try:
                analysis_df = load_registry_frame(selected_artifact["path"])
                render_dataset_views(analysis_df, f"Analysis output: {selected_artifact['file_name']}")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Failed to render analysis dataset: {exc}")

    with tabs[3]:
        machine_source = selected_artifact["path"] if selected_artifact and selected_artifact["status"] == "ready" else None
        if machine_source:
            try:
                machine_df = load_registry_frame(machine_source)
                render_machine_view(machine_df)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Failed to render machine view from selected analysis: {exc}")
        else:
            st.info("Select a ready analysis in the Analyses tab to drive machine-level view.")

    with tabs[4]:
        st.subheader("Playback")
        playback_sources = [a for a in artifacts if a["playback_compatible"] and a["status"] == "ready"]
        playback_path = ""
        if playback_sources:
            label_map = {f"{a['file_name']} ({a['analysis_name']})": a["path"] for a in playback_sources}
            selected = st.selectbox("Playback dataset from scan", options=list(label_map.keys()))
            playback_path = label_map[selected]
        else:
            st.info("No playback-compatible artifacts discovered. You can still use manual source input from sidebar.")
            playback_path = local_path

        if playback_path:
            try:
                df, raw_source_df, source_columns, _ = load_data_from_path(playback_path)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Unable to load playback source: {exc}")
            else:
                valid_schema, message = validate_playback_export_schema(raw_source_df, source_columns=source_columns)
                if valid_schema and not df.empty:
                    render_playback_mode(df)
                else:
                    st.warning(f"Selected file is not playback-compatible: {message}")

    with tabs[5]:
        render_exploration_mode(uploaded, local_path, artifacts)

    if auto_refresh and not st.session_state.get("is_playing", False):
        time.sleep(float(refresh_sec))
        st.rerun()


if __name__ == "__main__":
    main()
