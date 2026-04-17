"""
Interactive Streamlit playback tool for JSONL telemetry data.

This app loads telemetry records from JSONL files, sorts them by timestamp,
and provides a simple playback interface for stepping through the data over
time. Users can:

- select a machine (if a machine column exists)
- choose numeric variables to display
- play/pause the playback
- reset playback to the beginning
- view the current record and plots up to the current index

The app is intended as an exploratory visualization utility rather than a
real-time simulator. Playback advances one row at a time at a user-selected
interval.

Notes:
- Data is loaded from top-level JSONL files in ``data/``.
- Data is cached with ``st.cache_data`` to avoid repeated full reloads.
- Timestamps are parsed with ``datetime.fromisoformat`` and invalid rows are
  skipped with a warning.
- The current index slider is display-only in this version.
"""

import json
import os
import time
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

# Folder containing input JSONL files.
DATA_DIR = "data"

# Default playback interval in seconds between row advances.
REFRESH_INTERVAL_SEC = 0.5


@st.cache_data
def load_data():
    """
    Load and cache telemetry data from JSONL files.

    The loader scans top-level ``*.jsonl`` files in ``data/``, parses each line
    as JSON, converts the ``timestamp`` field to ``datetime``, and returns a
    single timestamp-sorted DataFrame.

    Returns
    -------
    pandas.DataFrame
        Combined telemetry data sorted by timestamp.

    Behavior
    --------
    - Blank lines are skipped.
    - Rows with parsing errors are skipped and reported as Streamlit warnings.
    - Data is cached to reduce repeated I/O on reruns.
    """
    records = []

    for filename in sorted(os.listdir(DATA_DIR)):
        if filename.endswith(".jsonl"):
            filepath = os.path.join(DATA_DIR, filename)
            with open(filepath, "r") as f:
                for line in f:
                    if line.strip():
                        try:
                            entry = json.loads(line)
                            entry["timestamp"] = datetime.fromisoformat(entry["timestamp"])
                            records.append(entry)
                        except Exception as e:
                            st.warning(f"Parse error in {filename}: {e}")

    df = pd.DataFrame(records)
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


df = load_data()
if df.empty:
    st.error("No data loaded.")
    st.stop()

# Session state controls playback behavior across reruns.
if "index" not in st.session_state:
    st.session_state.index = 0

if "playing" not in st.session_state:
    st.session_state.playing = False

if "last_run" not in st.session_state:
    st.session_state.last_run = time.time()

st.title("MTConnect Data Simulator with Playback")

# If machine labels are available, filter the dataset to one machine.
if "machine" in df.columns:
    machines = df["machine"].dropna().unique()
    selected_machine = st.selectbox("Select machine", machines)
    df = df[df["machine"] == selected_machine]

# Show only numeric variables and exclude sequence-like index counters.
variables = [col for col in df.select_dtypes(include="number").columns if col != "sequence"]
selected_vars = st.multiselect(
    "Select variables to show",
    variables,
    default=variables[:1],
)

# Playback controls.
cols = st.columns([1, 2, 2])

with cols[0]:
    if st.button("⏯ Play / Pause"):
        st.session_state.playing = not st.session_state.playing

with cols[1]:
    speed = st.slider(
        "Speed (sec per step)",
        0.1,
        2.0,
        REFRESH_INTERVAL_SEC,
        0.1,
    )

with cols[2]:
    st.button(
        "⏹ Reset",
        on_click=lambda: st.session_state.update({"index": 0, "playing": False}),
    )

max_index = len(df) - 1

# Display-only playback position indicator.
st.slider(
    "Current index",
    0,
    max_index,
    value=st.session_state.index,
    disabled=True,
)

current = df.iloc[st.session_state.index]
st.subheader(f"Time: {current['timestamp']}")
st.json(current[selected_vars].to_dict())

# Plot each selected variable from the beginning up to the current playback index.
for var in selected_vars:
    fig, ax = plt.subplots(figsize=(8, 2))
    ax.plot(
        df["timestamp"][: st.session_state.index + 1],
        df[var][: st.session_state.index + 1],
        label=var,
    )
    ax.scatter([current["timestamp"]], [current[var]], color="red")
    ax.set_ylabel(var)
    ax.set_xlabel("Time")
    ax.set_title(var)
    ax.grid(True)
    st.pyplot(fig)

# Advance playback by one row when enough time has elapsed.
if st.session_state.playing:
    now = time.time()
    if now - st.session_state.last_run >= speed:
        st.session_state.last_run = now
        st.session_state.index += 1

        if st.session_state.index >= max_index:
            st.session_state.index = max_index
            st.session_state.playing = False

    st.rerun()