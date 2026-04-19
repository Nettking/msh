"""Analysis-centric Streamlit views for status, browser, and machine summaries."""

from __future__ import annotations

import time

import altair as alt
import pandas as pd
import streamlit as st


def render_system_status(artifacts: list[dict], warnings: list[str], scan_dirs: tuple[str, ...], scan_started: float):
    """Render system-level indexing status and recent read errors."""
    st.subheader("System status")
    playback = [a for a in artifacts if a["playback_compatible"]]
    errors = [a for a in artifacts if a["status"] != "ready"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Scan roots", len(scan_dirs))
    c2.metric("Indexed artifacts", len(artifacts))
    c3.metric("Playback-compatible", len(playback))
    c4.metric("Read errors", len(errors))

    st.caption(f"Last scan completed in {time.time() - scan_started:.2f}s")
    st.caption("Configured scan roots: " + ", ".join(scan_dirs))
    if warnings:
        for warning in warnings[:10]:
            st.warning(warning)

    if errors:
        err_df = pd.DataFrame(errors)[["analysis_name", "path", "load_error", "modified_at"]]
        st.dataframe(err_df, use_container_width=True, hide_index=True)


def render_analyses_browser(artifacts: list[dict]):
    """Render analysis listing and return the selected artifact record."""
    st.subheader("Analyses")
    if not artifacts:
        st.info("No tabular artifacts found in configured scan directories.")
        return None

    frame = pd.DataFrame(artifacts)
    st.dataframe(
        frame[["analysis_name", "file_name", "kind", "status", "modified_at", "path", "supported_views"]],
        use_container_width=True,
        hide_index=True,
    )

    options = [f"{row['analysis_name']} :: {row['file_name']}" for _, row in frame.iterrows()]
    selected_label = st.selectbox("Inspect analysis output", options=options)
    selected = frame.iloc[options.index(selected_label)].to_dict()

    st.markdown(f"**Description:** {selected['description']}")
    st.markdown(f"**Path:** `{selected['path']}`")
    st.markdown(f"**Status:** `{selected['status']}` | **Views:** `{', '.join(selected['supported_views'])}`")
    return selected


def render_machine_view(df: pd.DataFrame):
    """Render machine-centric aggregate views from the selected analysis dataset."""
    if "machine_id" not in df.columns:
        st.info("No machine_id column in selected dataset.")
        return

    machine_counts = (
        df.assign(machine_id=df["machine_id"].astype("string"))
        .groupby("machine_id", dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
    )
    st.subheader("Rows by machine")
    st.dataframe(machine_counts, use_container_width=True, hide_index=True)

    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], errors="coerce")
        tdf = df.assign(_ts=ts).dropna(subset=["_ts"])
        if not tdf.empty:
            tdf["day"] = tdf["_ts"].dt.date.astype(str)
            machine_day = (
                tdf.groupby(["day", "machine_id"], dropna=False)
                .size()
                .reset_index(name="rows")
                .sort_values(["day", "rows"], ascending=[True, False])
            )
            st.subheader("Machine/day trends")
            st.altair_chart(
                alt.Chart(machine_day)
                .mark_line(point=True)
                .encode(x="day:T", y="rows:Q", color="machine_id:N", tooltip=["day", "machine_id", "rows"])
                .properties(height=350)
                .interactive(),
                use_container_width=True,
            )
