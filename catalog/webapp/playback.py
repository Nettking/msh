"""Playback validation, filtering, plotting, and playback-mode UI rendering."""

from __future__ import annotations

import time

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from catalog.common.timeline_exports import build_state_interval_export

STATE_COLORS = {
    "active": "#16a34a",
    "dense_idle": "#f59e0b",
    "idle": "#94a3b8",
    "intervention_candidate": "#ef4444",
    "stopped": "#7c3aed",
}
SIGNALS = ["Srpm", "Sload", "Sovr", "Fovr", "Frapidovr"]
REQUIRED_PLAYBACK_COLUMNS = {"timestamp", "machine_id", "state"}


def _has_non_empty_values(series: pd.Series) -> bool:
    cleaned = series.astype("string").str.strip()
    return cleaned.replace("", pd.NA).notna().any()


def validate_playback_export_schema(raw_source_df: pd.DataFrame, *, source_columns: set[str]) -> tuple[bool, str]:
    """Validate whether a source dataframe can drive playback mode."""
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


def filter_machine_day(df: pd.DataFrame, machine_id: str, day) -> pd.DataFrame:
    """Filter timeline rows by machine/day for playback."""
    rows = df[(df["machine_id"] == str(machine_id)) & (df["date"] == day)].copy()
    return rows.sort_values("timestamp").reset_index(drop=True)


def plot_state_timeline(intervals: pd.DataFrame, current_ts: pd.Timestamp):
    """Render state + stopped interval timeline plot."""
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
        ax.broken_barh([(start, width)], (0, 7), facecolors=STATE_COLORS.get(row["state"], "#64748b"), alpha=0.9)

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


def plot_signals(rows: pd.DataFrame, current_ts: pd.Timestamp):
    """Render selected signal lines with current playback marker."""
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


def playback_controls(n_rows: int):
    """Render playback transport controls and return speed/mode selections."""
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


def render_playback_mode(df: pd.DataFrame):
    """Render complete playback UI for validated timeline rows."""
    machines = sorted(df["machine_id"].dropna().astype(str).unique().tolist())
    if not machines:
        st.warning("Playback export looks valid, but it contains no machine IDs.")
        return

    c1, c2 = st.columns([1, 1])
    selected_machine = c1.selectbox("Machine", options=machines)
    machine_days = sorted(df[df["machine_id"] == selected_machine]["date"].dropna().unique().tolist())
    selected_day = c2.selectbox("Day", options=machine_days)

    rows = filter_machine_day(df, selected_machine, selected_day)
    if rows.empty:
        st.warning("Playback export is valid, but the selected machine/day has no rows.")
        return

    speed, playback_mode = playback_controls(len(rows))
    idx = min(st.session_state.frame_index, len(rows) - 1)
    current = rows.iloc[idx]
    current_ts = pd.to_datetime(current["timestamp"])
    intervals = build_state_interval_export(rows)

    timeline_col, panel_col = st.columns([2.6, 1.4], gap="large")
    with timeline_col:
        st.pyplot(plot_state_timeline(intervals, current_ts), use_container_width=True)
        st.pyplot(plot_signals(rows, current_ts), use_container_width=True)

    with panel_col:
        st.subheader("Current playback point")
        st.markdown(f"**state:** `{str(current.get('state', 'unknown'))}`")
        st.write("timestamp", current_ts)
        st.write("machine_id", str(current.get("machine_id", "")))
        st.write("event_score", float(current.get("event_score", 0) or 0))
        st.write("fired_rules", str(current.get("fired_rules", "")) or "-")
        st.metric("Intervention-candidate points", int(rows["intervention_candidate"].sum()))
        st.metric("Stopped points", int(rows["stopped"].sum()) if "stopped" in rows.columns else 0)

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
