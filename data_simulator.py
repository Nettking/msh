import os
import json
import time
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from datetime import datetime

# === CONFIG ===
DATA_DIR = "data"
REFRESH_INTERVAL_SEC = 0.5  # Default speed

# === Load and cache data ===
@st.cache_data
def load_data():
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

# === Session state setup ===
if "index" not in st.session_state:
    st.session_state.index = 0
if "playing" not in st.session_state:
    st.session_state.playing = False
if "last_run" not in st.session_state:
    st.session_state.last_run = time.time()

# === Controls ===
st.title("MTConnect Data Simulator with Playback")

if "machine" in df.columns:
    machines = df["machine"].dropna().unique()
    selected_machine = st.selectbox("Select machine", machines)
    df = df[df["machine"] == selected_machine]

variables = [col for col in df.select_dtypes(include="number").columns if col != "sequence"]
selected_vars = st.multiselect("Select variables to show", variables, default=variables[:1])

# === Playback controls ===
cols = st.columns([1, 2, 2])
with cols[0]:
    if st.button("⏯ Play / Pause"):
        st.session_state.playing = not st.session_state.playing
with cols[1]:
    speed = st.slider("Speed (sec per step)", 0.1, 2.0, REFRESH_INTERVAL_SEC, 0.1)
with cols[2]:
    st.button("⏹ Reset", on_click=lambda: st.session_state.update({"index": 0, "playing": False}))

# === Current index display ===
max_index = len(df) - 1
st.slider("Current index", 0, max_index, value=st.session_state.index, disabled=True)

current = df.iloc[st.session_state.index]
st.subheader(f"Time: {current['timestamp']}")
st.json(current[selected_vars].to_dict())

# === Plot selected variables ===
for var in selected_vars:
    fig, ax = plt.subplots(figsize=(8, 2))
    ax.plot(df["timestamp"][:st.session_state.index+1], df[var][:st.session_state.index+1], label=var)
    ax.scatter([current["timestamp"]], [current[var]], color="red")
    ax.set_ylabel(var)
    ax.set_xlabel("Time")
    ax.set_title(var)
    ax.grid(True)
    st.pyplot(fig)

# === Playback loop ===
if st.session_state.playing:
    now = time.time()
    if now - st.session_state.last_run >= speed:
        st.session_state.last_run = now
        st.session_state.index += 1
        if st.session_state.index >= max_index:
            st.session_state.index = max_index
            st.session_state.playing = False
    st.rerun()
