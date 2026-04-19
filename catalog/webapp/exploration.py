"""Generic dataset exploration views for analysis and ad-hoc tabular inspection."""

from __future__ import annotations

from pathlib import Path

import altair as alt
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from catalog.webapp.data_sources import load_registry_frame, load_table_from_path, load_table_from_upload

DEFAULT_HIST_COLOR = "#334155"
MAX_EXPLORATION_PLOT_ROWS = 15000


def render_dataset_views(df: pd.DataFrame, title: str):
    """Render summary/table/chart inspection views for a selected dataset."""
    st.subheader(title)
    st.caption(f"Rows: {len(df):,} | Columns: {len(df.columns):,}")
    if df.empty:
        st.warning("Dataset is empty.")
        return

    st.dataframe(df.head(400), use_container_width=True)
    numeric_candidates = [col for col in df.columns if pd.to_numeric(df[col], errors="coerce").notna().any()]
    parsed_ts = pd.to_datetime(df["timestamp"], errors="coerce") if "timestamp" in df.columns else None

    if numeric_candidates:
        st.markdown("**Graph view**")
        left, right = st.columns([2, 1])
        selected_numeric = left.multiselect(
            "Numeric columns",
            options=numeric_candidates,
            default=numeric_candidates[: min(3, len(numeric_candidates))],
            key=f"num_{title}",
        )
        chart_type = right.radio("Chart", ["line", "scatter", "histogram", "bar(counts)"], key=f"ctype_{title}")

        plot_df = df.copy().reset_index(drop=True)
        plot_df["_row"] = plot_df.index
        if parsed_ts is not None and parsed_ts.notna().any():
            plot_df["_ts"] = parsed_ts
        if len(plot_df) > MAX_EXPLORATION_PLOT_ROWS:
            plot_df = plot_df.iloc[:MAX_EXPLORATION_PLOT_ROWS].copy()

        if chart_type == "histogram" and selected_numeric:
            col = st.selectbox("Histogram column", selected_numeric, key=f"hist_{title}")
            fig, ax = plt.subplots(figsize=(9, 3.2))
            ax.hist(pd.to_numeric(plot_df[col], errors="coerce").dropna(), bins=30, color=DEFAULT_HIST_COLOR, alpha=0.85)
            ax.set_title(f"Distribution of {col}")
            st.pyplot(fig, use_container_width=True)
        elif chart_type == "bar(counts)":
            category_cols = [c for c in df.columns if df[c].astype("string").nunique(dropna=True) <= 30]
            if not category_cols:
                st.info("No low-cardinality categorical columns for count bars.")
            else:
                cat = st.selectbox("Category column", category_cols, key=f"bar_{title}")
                counts = df[cat].astype("string").fillna("unknown").value_counts().reset_index()
                counts.columns = [cat, "count"]
                st.altair_chart(
                    alt.Chart(counts).mark_bar().encode(x=alt.X(f"{cat}:N", sort="-y"), y="count:Q", tooltip=[cat, "count"]).properties(height=320),
                    use_container_width=True,
                )
        elif selected_numeric:
            long_df = plot_df.melt(
                id_vars=["_ts"] if "_ts" in plot_df.columns else ["_row"],
                value_vars=selected_numeric,
                var_name="metric",
                value_name="value",
            )
            long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
            long_df = long_df.dropna(subset=["value"])
            if not long_df.empty:
                x_encoding = "_ts:T" if "_ts" in long_df.columns else "_row:Q"
                mark = alt.Chart(long_df).mark_line() if chart_type == "line" else alt.Chart(long_df).mark_circle(size=35)
                st.altair_chart(
                    mark.encode(
                        x=alt.X(x_encoding, title="Time" if "_ts" in long_df.columns else "Row"),
                        y="value:Q",
                        color="metric:N",
                        tooltip=["metric", "value"],
                    ).properties(height=360).interactive(),
                    use_container_width=True,
                )


def render_exploration_mode(uploaded, local_path: str, artifacts: list[dict]):
    """Render the exploration tab, supporting discovered and manual sources."""
    st.subheader("Exploration")
    st.caption("Generic table/CSV exploration remains available for arbitrary datasets.")

    discovered_paths = [a["path"] for a in artifacts]
    selected_discovered = st.selectbox("Discovered dataset (optional)", options=["(none)"] + discovered_paths)

    raw_source_df = None
    if selected_discovered != "(none)":
        try:
            raw_source_df = load_registry_frame(selected_discovered)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Unable to load selected discovered dataset: {exc}")
            return
    elif uploaded is not None:
        suffix = Path(uploaded.name).suffix or ".csv"
        raw_source_df = load_table_from_upload(uploaded.getvalue(), suffix)
    elif local_path.strip():
        raw_source_df = load_table_from_path(local_path.strip())

    if raw_source_df is None:
        st.info("Select a discovered dataset, upload a file, or provide a local path.")
        return

    render_dataset_views(raw_source_df, "Exploration dataset")
