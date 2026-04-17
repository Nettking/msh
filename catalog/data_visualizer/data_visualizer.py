"""Infer machine states and generate per-day timeline plots.

This script focuses on I/O, diagnostics, and visualization while relying on
shared DT-foundation helpers for loading, telemetry preparation, state/situation
inference, and interval/candidate derivation.
"""

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Patch

from catalog.common.data_loading import iter_jsonl_files, load_jsonl_dataframe
from catalog.common.state_inference import (
    StateInferenceConfig,
    extract_intervention_candidates,
    infer_states_for_machine,
    rows_to_state_intervals,
)
from catalog.common.telemetry_prep import prepare_machine_telemetry_dataframe

FOLDER = Path("./data")
OUTPUT_DIR = Path("./timeline_images")
CANDIDATE_CSV = Path("./candidate_events.csv")

MACHINE_ID_CANDIDATES = [
    "machine",
    "machine_id",
    "machineId",
    "Machine",
    "MachineId",
    "machine_name",
    "resource",
    "device",
]

RPM_COL = "Srpm"
LOAD_COL = "Sload"
OVR_COL = "Sovr"
EXEC_COL = "execution"
MODE_COL = "mode"
PROG_COL = "program"
TIME_COL = "timestamp"
FOVR_COL = "Fovr"
FRAPIDOVR_COL = "Frapidovr"

MERGE_GAP_SEC = 30.0
FIG_WIDTH = 18
ROW_HEIGHT = 0.8
SAVE_FIGURES = True
SHOW_FIGURES = False

INFERENCE_CONFIG = StateInferenceConfig(
    dense_dt_sec=5.0,
    rpm_active_threshold=100.0,
    load_active_threshold=1.0,
    rpm_rate_q=0.95,
    load_rate_q=0.95,
    ovr_rate_q=0.95,
    fovr_rate_q=0.95,
    frapidovr_rate_q=0.95,
    ovr_drop_threshold=-10.0,
    fovr_drop_threshold=-10.0,
    rpm_collapse_ratio=0.5,
    load_collapse_ratio=0.5,
    merge_gap_sec=MERGE_GAP_SEC,
)


def load_prepared_frames() -> list[pd.DataFrame]:
    """Load telemetry files and return prepared dataframes."""
    files = list(iter_jsonl_files(FOLDER, recursive=False))
    if not files:
        raise FileNotFoundError(f"No files found in {FOLDER!r} matching '*.jsonl'")

    frames: list[pd.DataFrame] = []
    for path in files:
        raw_df = load_jsonl_dataframe(
            path,
            on_malformed_json=lambda msg: print(f"[WARNING] {msg}"),
        )
        prepared = prepare_machine_telemetry_dataframe(
            raw_df,
            source_name=path.name,
            time_col=TIME_COL,
            machine_candidates=MACHINE_ID_CANDIDATES,
            numeric_cols=[RPM_COL, LOAD_COL, OVR_COL, FOVR_COL, FRAPIDOVR_COL, "Stemp"],
            context_cols=[EXEC_COL, MODE_COL, PROG_COL],
            target_machine_col="machine_id",
            source_col_name="source_file",
            date_col="date",
        )
        if prepared is not None and not prepared.empty:
            frames.append(prepared)

    return frames


def plot_day_timeline(interval_df_day, output_path=None, show=False):
    if interval_df_day.empty:
        return

    state_colors = {
        "idle": "lightgray",
        "dense_idle": "orange",
        "active": "tab:blue",
        "intervention_candidate": "tab:red",
    }

    machines = sorted(interval_df_day["machine_id"].dropna().unique().tolist())
    fig_height = max(4, ROW_HEIGHT * len(machines) + 1.5)

    fig, ax = plt.subplots(figsize=(FIG_WIDTH, fig_height))
    y_positions = {m: i for i, m in enumerate(machines)}

    for _, row in interval_df_day.iterrows():
        y = y_positions[row["machine_id"]]
        start = row["start"]
        end = row["end"]
        color = state_colors.get(row["state"], "black")

        if start == end:
            end = start + pd.Timedelta(seconds=1)

        ax.barh(
            y=y,
            width=(end - start).total_seconds() / 86400.0,
            left=mdates.date2num(start),
            height=0.6,
            color=color,
            edgecolor="none",
        )

    ax.set_yticks(list(y_positions.values()))
    ax.set_yticklabels(list(y_positions.keys()))
    ax.set_xlabel("Time")
    ax.set_ylabel("Machine")

    day_label = str(interval_df_day["date"].iloc[0])
    ax.set_title(f"Machine timelines for {day_label}")

    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()

    legend_handles = [
        Patch(color=state_colors["idle"], label="Idle"),
        Patch(color=state_colors["dense_idle"], label="Dense idle"),
        Patch(color=state_colors["active"], label="Active"),
        Patch(color=state_colors["intervention_candidate"], label="Intervention candidate"),
    ]
    ax.legend(handles=legend_handles, loc="upper right")

    plt.tight_layout()

    if output_path is not None:
        plt.savefig(output_path, dpi=200, bbox_inches="tight")
        print(f"Saved: {output_path}")

    if show:
        plt.show()
    else:
        plt.close(fig)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_frames = load_prepared_frames()
    if not all_frames:
        raise ValueError("No usable data found.")

    data = pd.concat(all_frames, ignore_index=True)
    data = data.sort_values(["date", "machine_id", TIME_COL]).reset_index(drop=True)

    state_frames = []
    interval_frames = []
    candidate_frames = []

    print("\n=== TRIGGER DIAGNOSTICS BY DAY AND MACHINE ===")
    for (day_value, machine_id), g in data.groupby(["date", "machine_id"], sort=True):
        gs = infer_states_for_machine(
            g,
            config=INFERENCE_CONFIG,
            time_col=TIME_COL,
            rpm_col=RPM_COL,
            load_col=LOAD_COL,
            ovr_col=OVR_COL,
            exec_col=EXEC_COL,
            mode_col=MODE_COL,
            prog_col=PROG_COL,
            fovr_col=FOVR_COL,
            frapidovr_col=FRAPIDOVR_COL,
        )
        state_frames.append(gs)
        interval_frames.append(rows_to_state_intervals(gs, merge_gap_sec=INFERENCE_CONFIG.merge_gap_sec))
        candidate_frames.append(extract_intervention_candidates(gs, time_col=TIME_COL))

        print(f"\n[{day_value}] machine={machine_id}")
        print("Trigger counts:")
        for k, v in gs.attrs.get("trigger_counts", {}).items():
            if v > 0:
                print(f"  {k}: {v}")

        print(f"  active rows: {int(gs['active'].sum())}")
        print(f"  intervention_candidate rows: {int(gs['intervention_candidate'].sum())}")

        thresholds = gs.attrs.get("thresholds", {})
        non_nan_thresholds = {k: v for k, v in thresholds.items() if pd.notna(v)}
        if non_nan_thresholds:
            print("Thresholds:")
            for k, v in non_nan_thresholds.items():
                print(f"  {k}: {v:.6f}")

    _ = pd.concat(state_frames, ignore_index=True)
    interval_df = pd.concat(interval_frames, ignore_index=True)

    candidate_df = pd.concat(candidate_frames, ignore_index=True) if candidate_frames else pd.DataFrame()
    if not candidate_df.empty:
        candidate_df = candidate_df.sort_values(["date", "machine_id", "timestamp"]).reset_index(drop=True)
        candidate_df.to_csv(CANDIDATE_CSV, index=False)
        print(f"\nSaved candidate events to: {CANDIDATE_CSV.resolve()}")
    else:
        print("\nNo candidate events found.")
        pd.DataFrame(columns=["date", "machine_id", "timestamp", "state", "event_score", "fired_rules"]).to_csv(
            CANDIDATE_CSV,
            index=False,
        )
        print(f"Saved empty candidate file to: {CANDIDATE_CSV.resolve()}")

    interval_df = interval_df[
        ~(
            (interval_df["state"] == "idle")
            & (interval_df["duration_sec"] == 0)
            & (interval_df["n_points"] == 1)
        )
    ].copy()

    print("\n=== MACHINE SUMMARY BY DAY ===")
    summary = (
        interval_df.groupby(["date", "machine_id", "state"])
        .agg(n_intervals=("state", "size"), total_duration_sec=("duration_sec", "sum"))
        .reset_index()
        .sort_values(["date", "machine_id", "state"])
    )
    print(summary.to_string(index=False))

    if not candidate_df.empty:
        print("\n=== FIRST CANDIDATE ROWS ===")
        print(candidate_df.head(50).to_string(index=False))

    unique_days = sorted(interval_df["date"].dropna().unique().tolist())
    for day_value in unique_days:
        day_intervals = interval_df[interval_df["date"] == day_value].copy()
        if day_intervals.empty:
            continue

        filename = f"timeline_{day_value}.png"
        output_path = OUTPUT_DIR / filename
        plot_day_timeline(day_intervals, output_path=output_path if SAVE_FIGURES else None, show=SHOW_FIGURES)

    print(f"\nDone. Images are in: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
