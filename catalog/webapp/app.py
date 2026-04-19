"""Main Streamlit digital twin workspace for playback, analyses, and data exploration."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import tempfile
from pathlib import Path

import altair as alt
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

if not __package__:
    repo_root = Path(__file__).resolve().parents[2]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)

from catalog.common.timeline_exports import build_state_interval_export, load_timeline_export_with_schema_info
from catalog.webapp.analysis_registry import configured_scan_dirs, load_artifact_frame, scan_artifacts

STATE_COLORS = {
    "active": "#16a34a",
    "dense_idle": "#f59e0b",
    "idle": "#94a3b8",
    "intervention_candidate": "#ef4444",
    "stopped": "#7c3aed",
}
DEFAULT_HIST_COLOR = "#334155"
MAX_EXPLORATION_PLOT_ROWS = 15000

SIGNALS = ["Srpm", "Sload", "Sovr", "Fovr", "Frapidovr"]
DEFAULT_EXPORT_CANDIDATES = ["timeline_rows.csv", "timeline_rows.parquet", "timeline_rows.jsonl", "timeline_rows.json"]
REQUIRED_PLAYBACK_COLUMNS = {"timestamp", "machine_id", "state"}


@st.cache_data(show_spinner=False)
def _load_data_from_path(path: str) -> tuple[pd.DataFrame, pd.DataFrame, set[str], set[str]]:
    return load_timeline_export_with_schema_info(path)


@st.cache_data(show_spinner=False)
def _load_data_from_upload(content: bytes, suffix: str) -> tuple[pd.DataFrame, pd.DataFrame, set[str], set[str]]:
    with tempfile.NamedTemporaryFile(prefix="timeline_upload_", suffix=suffix, delete=False) as handle:
        handle.write(content)
        tmp_path = Path(handle.name)
    try:
        return load_timeline_export_with_schema_info(tmp_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


@st.cache_data(show_spinner=False)
def _scan_registry(scan_dirs: tuple[str, ...], scan_nonce: int, refresh_bucket: int) -> tuple[list[dict], list[str]]:
    _ = refresh_bucket
    return scan_artifacts(list(scan_dirs))


@st.cache_data(show_spinner=False)
def _load_registry_frame(path: str) -> pd.DataFrame:
    return load_artifact_frame(path)


def _has_non_empty_values(series: pd.Series) -> bool:
    cleaned = series.astype("string").str.strip()
    return cleaned.replace("", pd.NA).notna().any()


def _validate_playback_export_schema(
    raw_source_df: pd.DataFrame,
    *,
    source_columns: set[str],
) -> tuple[bool, str]:
    missing = sorted(REQUIRED_PLAYBACK_COLUMNS.difference(source_columns))
    if missing:
        return False, f"Missing required source columns: {', '.join(missing)}"

    if not pd.to_datetime(raw_source_df["timestamp"], errors="coerce").notna().any():
        return False, "'timestamp' has no parseable values."

    if not _has_non_empty_values(raw_source_df["machine_id"]):
        return False, "'machine_id' has no non-empty values."

    if not _has_non_empty_values(raw_source_df["state"]):
        return False, "'state' has no non-empty values."

    return True, ""


def _read_table(path: str | Path) -> pd.DataFrame:
    source = Path(path)
    suffix = source.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(source)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(source)
    if suffix == ".jsonl":
        rows: list[dict] = []
        with source.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return pd.DataFrame(rows)
    if suffix == ".json":
        return pd.read_json(source)
    raise ValueError(f"Unsupported file extension: {suffix}")


@st.cache_data(show_spinner=False)
def _load_table_from_path(path: str) -> pd.DataFrame:
    return _read_table(path)


@st.cache_data(show_spinner=False)
def _load_table_from_upload(content: bytes, suffix: str) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(prefix="table_upload_", suffix=suffix, delete=False) as handle:
        handle.write(content)
        tmp_path = Path(handle.name)
    try:
        return _read_table(tmp_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _filter_machine_day(df: pd.DataFrame, machine_id: str, day) -> pd.DataFrame:
    rows = df[(df["machine_id"] == str(machine_id)) & (df["date"] == day)].copy()
    return rows.sort_values("timestamp").reset_index(drop=True)


def _plot_state_timeline(intervals: pd.DataFrame, current_ts: pd.Timestamp):
    fig, ax = plt.subplots(figsize=(11, 2.8))

    if intervals.empty:
        ax.text(0.5, 0.5, "No intervals available", ha="center", va="center")
        ax.axis("off")
        return fig

    state_intervals = intervals[intervals["state"] != "stopped"]
    stopped_intervals = intervals[intervals["state"] == "stopped"]

    for _, row in state_intervals.iterrows():
        start = mdates.date2num(pd.to_datetime(row["start"]))
        end = mdates.date2num(pd.to_datetime(row["end"]))
        width = max(end - start, 1 / 86400)
        color = STATE_COLORS.get(row["state"], "#64748b")
        ax.broken_barh([(start, width)], (0, 7), facecolors=color, alpha=0.9)

    for _, row in stopped_intervals.iterrows():
        start = mdates.date2num(pd.to_datetime(row["start"]))
        end = mdates.date2num(pd.to_datetime(row["end"]))
        width = max(end - start, 1 / 86400)
        ax.broken_barh([(start, width)], (7.3, 2.2), facecolors=STATE_COLORS["stopped"], alpha=0.9)

    ax.axvline(mdates.date2num(current_ts), color="black", linestyle="--", linewidth=1.5, label="playback")
    ax.set_yticks([3.5, 8.4])
    ax.set_yticklabels(["inferred state", "stopped"])
    ax.set_xlabel("Time")
    ax.set_title("State timeline")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.set_ylim(0, 10)
    ax.legend(loc="upper right")
    fig.autofmt_xdate()
    fig.tight_layout()
    return fig


def _plot_signals(rows: pd.DataFrame, current_ts: pd.Timestamp):
    available = [col for col in SIGNALS if col in rows.columns and rows[col].notna().any()]
    if not available:
        fig, ax = plt.subplots(figsize=(11, 3.2))
        ax.text(0.5, 0.5, "No numeric signal columns available", ha="center", va="center")
        ax.axis("off")
        return fig

    fig, axes = plt.subplots(len(available), 1, figsize=(11, 2.2 * len(available)), sharex=True)
    if len(available) == 1:
        axes = [axes]

    for ax, col in zip(axes, available):
        ax.plot(rows["timestamp"], rows[col], linewidth=1.0, color="#0f172a")
        ax.axvline(current_ts, color="#dc2626", linestyle="--", linewidth=1.0)
        candidates = rows[rows["intervention_candidate"]]
        if not candidates.empty:
            ax.scatter(candidates["timestamp"], candidates[col], color="#ef4444", s=14, alpha=0.8)
        ax.set_ylabel(col)
        ax.grid(True, alpha=0.2)

    axes[-1].set_xlabel("Time")
    fig.tight_layout()
    return fig


def _playback_controls(n_rows: int):
    if "frame_index" not in st.session_state:
        st.session_state.frame_index = 0
    if "is_playing" not in st.session_state:
        st.session_state.is_playing = False
    if "playback_mode" not in st.session_state:
        st.session_state.playback_mode = "row"

    col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, 2, 2])

    if col1.button("⏮ Step -", use_container_width=True):
        st.session_state.frame_index = max(st.session_state.frame_index - 1, 0)
        st.session_state.is_playing = False
    if col2.button("▶ Play", use_container_width=True):
        st.session_state.is_playing = True
    if col3.button("⏸ Pause", use_container_width=True):
        st.session_state.is_playing = False
    if col4.button("Step + ⏭", use_container_width=True):
        st.session_state.frame_index = min(st.session_state.frame_index + 1, n_rows - 1)
        st.session_state.is_playing = False

    speed = col5.select_slider("Speed", options=[0.25, 0.5, 1.0, 2.0, 4.0], value=1.0)
    st.session_state.playback_mode = col6.selectbox(
        "Mode",
        options=["row", "time"],
        format_func=lambda x: "Row-based" if x == "row" else "Time-based",
        index=0 if st.session_state.playback_mode == "row" else 1,
    )

    st.session_state.frame_index = st.slider(
        "Playback position",
        min_value=0,
        max_value=max(n_rows - 1, 0),
        value=min(st.session_state.frame_index, max(n_rows - 1, 0)),
    )

    return speed, st.session_state.playback_mode


def _parse_bootstrap_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--session-export-dir", default="")
    parser.add_argument("--source-path", default="")
    return parser.parse_known_args(sys.argv[1:])[0]


def _resolve_session_export_file(path: str) -> str:
    if not path:
        return ""
    root = Path(path).expanduser()
    if not root.exists() or not root.is_dir():
        return ""
    for file_name in DEFAULT_EXPORT_CANDIDATES:
        candidate = root / file_name
        if candidate.exists():
            return str(candidate)
    return ""


def _resolve_bootstrap_source() -> tuple[str, str]:
    args = _parse_bootstrap_args()
    if args.source_path:
        return args.source_path, "command-line source"

    from_session_dir = _resolve_session_export_file(args.session_export_dir)
    if from_session_dir:
        return from_session_dir, f"session export ({args.session_export_dir})"

    env_source = os.getenv("MSH_PLAYBACK_SOURCE_PATH", "").strip()
    if env_source:
        return env_source, "MSH_PLAYBACK_SOURCE_PATH"

    env_export_dir = os.getenv("MSH_PLAYBACK_EXPORT_DIR", "").strip()
    from_env_export = _resolve_session_export_file(env_export_dir)
    if from_env_export:
        return from_env_export, f"MSH_PLAYBACK_EXPORT_DIR ({env_export_dir})"

    return "", ""


def _render_playback_mode(df: pd.DataFrame):
    machines = sorted(df["machine_id"].dropna().astype(str).unique().tolist())
    if not machines:
        st.warning("Playback export looks valid, but it contains no machine IDs.")
        return

    c1, c2 = st.columns([1, 1])
    selected_machine = c1.selectbox("Machine", options=machines)
    machine_days = sorted(df[df["machine_id"] == selected_machine]["date"].dropna().unique().tolist())
    selected_day = c2.selectbox("Day", options=machine_days)

    rows = _filter_machine_day(df, selected_machine, selected_day)
    if rows.empty:
        st.warning("Playback export is valid, but the selected machine/day has no rows.")
        return

    speed, playback_mode = _playback_controls(len(rows))

    idx = min(st.session_state.frame_index, len(rows) - 1)
    current = rows.iloc[idx]
    current_ts = pd.to_datetime(current["timestamp"])
    intervals = build_state_interval_export(rows)

    timeline_col, panel_col = st.columns([2.6, 1.4], gap="large")
    with timeline_col:
        st.pyplot(_plot_state_timeline(intervals, current_ts), use_container_width=True)
        st.pyplot(_plot_signals(rows, current_ts), use_container_width=True)

    with panel_col:
        st.subheader("Current playback point")
        st.markdown(f"**state:** `{str(current.get('state', 'unknown'))}`")
        st.write("timestamp", current_ts)
        st.write("machine_id", str(current.get("machine_id", "")))
        st.write("event_score", float(current.get("event_score", 0) or 0))
        st.write("fired_rules", str(current.get("fired_rules", "")) or "-")
        candidate_count = int(rows["intervention_candidate"].sum())
        stopped_count = int(rows["stopped"].sum()) if "stopped" in rows.columns else 0
        st.metric("Intervention-candidate points", candidate_count)
        st.metric("Stopped points", stopped_count)

    if st.session_state.is_playing:
        next_index = min(idx + 1, len(rows) - 1)
        st.session_state.frame_index = next_index
        if next_index >= len(rows) - 1:
            st.session_state.is_playing = False
        else:
            if playback_mode == "time":
                current_time = pd.to_datetime(rows.iloc[idx]["timestamp"])
                next_time = pd.to_datetime(rows.iloc[next_index]["timestamp"])
                frame_gap_sec = max((next_time - current_time).total_seconds(), 0.0)
                wait_sec = max(0.02, min(frame_gap_sec / float(speed), 1.2))
            else:
                wait_sec = max(0.08, 0.35 / float(speed))
            time.sleep(wait_sec)
            st.rerun()


def _render_machine_view(df: pd.DataFrame):
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


def _render_dataset_views(df: pd.DataFrame, title: str):
    st.subheader(title)
    st.caption(f"Rows: {len(df):,} | Columns: {len(df.columns):,}")

    if df.empty:
        st.warning("Dataset is empty.")
        return

    st.dataframe(df.head(400), use_container_width=True)

    numeric_candidates = [col for col in df.columns if pd.to_numeric(df[col], errors="coerce").notna().any()]
    parsed_ts = None
    if "timestamp" in df.columns:
        parsed_ts = pd.to_datetime(df["timestamp"], errors="coerce")

    if numeric_candidates:
        st.markdown("**Graph view**")
        left, right = st.columns([2, 1])
        selected_numeric = left.multiselect(
            "Numeric columns", options=numeric_candidates, default=numeric_candidates[: min(3, len(numeric_candidates))], key=f"num_{title}"
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
            long_df = plot_df.melt(id_vars=["_ts"] if "_ts" in plot_df.columns else ["_row"], value_vars=selected_numeric, var_name="metric", value_name="value")
            long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
            long_df = long_df.dropna(subset=["value"])
            if not long_df.empty:
                x_encoding = "_ts:T" if "_ts" in long_df.columns else "_row:Q"
                mark = alt.Chart(long_df).mark_line() if chart_type == "line" else alt.Chart(long_df).mark_circle(size=35)
                st.altair_chart(
                    mark.encode(x=alt.X(x_encoding, title="Time" if "_ts" in long_df.columns else "Row"), y="value:Q", color="metric:N", tooltip=["metric", "value"]).properties(height=360).interactive(),
                    use_container_width=True,
                )


def _render_system_status(artifacts: list[dict], warnings: list[str], scan_dirs: tuple[str, ...], scan_started: float):
    st.subheader("System status")
    ready = [a for a in artifacts if a["status"] == "ready"]
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


def _render_analyses_browser(artifacts: list[dict]):
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


def _render_exploration_mode(uploaded, local_path: str, artifacts: list[dict]):
    st.subheader("Exploration")
    st.caption("Generic table/CSV exploration remains available for arbitrary datasets.")

    discovered_paths = [a["path"] for a in artifacts]
    selected_discovered = st.selectbox("Discovered dataset (optional)", options=["(none)"] + discovered_paths)

    raw_source_df = None
    if selected_discovered != "(none)":
        try:
            raw_source_df = _load_registry_frame(selected_discovered)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Unable to load selected discovered dataset: {exc}")
            return
    elif uploaded is not None:
        suffix = Path(uploaded.name).suffix or ".csv"
        raw_source_df = _load_table_from_upload(uploaded.getvalue(), suffix)
    elif local_path.strip():
        raw_source_df = _load_table_from_path(local_path.strip())

    if raw_source_df is None:
        st.info("Select a discovered dataset, upload a file, or provide a local path.")
        return

    _render_dataset_views(raw_source_df, "Exploration dataset")


def main():
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
        default_source, source_hint = _resolve_bootstrap_source()
        local_path = st.text_input("or local path", value=default_source)
        if source_hint:
            st.caption(f"Prefilled source: {source_hint}")

    scan_dirs = tuple(configured_scan_dirs())
    scan_started = time.time()
    refresh_bucket = int(time.time() // refresh_sec) if auto_refresh else 0
    artifacts, warnings = _scan_registry(scan_dirs, scan_nonce, refresh_bucket)

    tabs = st.tabs(["System status", "Overview", "Analyses", "Machine view", "Playback", "Exploration"])

    with tabs[0]:
        _render_system_status(artifacts, warnings, scan_dirs, scan_started)

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
        selected_artifact = _render_analyses_browser(artifacts)
        if selected_artifact and selected_artifact["status"] == "ready":
            try:
                analysis_df = _load_registry_frame(selected_artifact["path"])
                _render_dataset_views(analysis_df, f"Analysis output: {selected_artifact['file_name']}")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Failed to render analysis dataset: {exc}")

    with tabs[3]:
        machine_source = selected_artifact["path"] if selected_artifact and selected_artifact["status"] == "ready" else None
        if machine_source:
            try:
                machine_df = _load_registry_frame(machine_source)
                _render_machine_view(machine_df)
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
                df, raw_source_df, source_columns, _ = _load_data_from_path(playback_path)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Unable to load playback source: {exc}")
            else:
                valid_schema, message = _validate_playback_export_schema(raw_source_df, source_columns=source_columns)
                if valid_schema and not df.empty:
                    _render_playback_mode(df)
                else:
                    st.warning(f"Selected file is not playback-compatible: {message}")

    with tabs[5]:
        _render_exploration_mode(uploaded, local_path, artifacts)

    if auto_refresh and not st.session_state.get("is_playing", False):
        time.sleep(float(refresh_sec))
        st.rerun()


if __name__ == "__main__":
    main()
