"""
Train per-machine baseline models to predict an upcoming stop event from telemetry.

This script loads JSONL telemetry files, separates the data by machine, resamples
each machine stream to a fixed rate, constructs lag-based features, derives a
binary future-stop label, and trains one RandomForest classifier per machine.

Pipeline
--------
1. Load and combine top-level JSONL files from ``data/``
2. Parse timestamps and sort rows chronologically
3. Split the dataset by machine
4. Resample each machine stream to a fixed frequency
5. Create lagged telemetry features
6. Derive a binary label indicating whether a stop occurs in a future window
7. Train and evaluate one model per machine
8. Save model artifacts and summary metrics

Outputs
-------
Under ``ml_results/<machine>/``:
- ``<machine>_stop_predictor.pkl`` (optional)
- ``feature_importance.png``
- ``confusion_matrix.txt``
- ``classification_report.csv``

Under ``ml_results/``:
- ``summary.csv`` with one row per trained machine

Important
---------
This is a baseline predictive pipeline, not a validated production model.

In particular:
- the stop label is heuristic
- the prediction horizon is defined in future samples, not absolute time
- the train/test split is random rather than time-aware
- forward filling and zero-filling may affect interpretation
- feature importance from RandomForest should not be treated as causal evidence
"""

import json
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split

from catalog.common.telemetry_prep import prepare_timestamp_column, to_numeric

# Directory containing input JSONL telemetry files.
DATA_DIR = Path("data")

# Output directory for trained models and evaluation artifacts.
OUTPUT_DIR = Path("ml_results")

# Whether to save the trained RandomForest model to disk.
SAVE_MODEL = True

# Lag steps (in resampled rows) used to construct lagged telemetry features.
LAG_STEPS = [1, 5, 10]

# Number of future resampled rows used to define the prediction target.
# Note: this is a sample horizon, not an absolute time horizon unless the
# resampling rate is fixed and stable.
FUTURE_WINDOW = 60

# Resampling frequency in Hz. A value of 1 means 1-second bins.
DOWNSAMPLE_HZ = 1


def load_all_data(data_dir):
    """
    Load and combine all top-level JSONL files into one time-indexed DataFrame.

    Parameters
    ----------
    data_dir : pathlib.Path
        Directory containing ``*.jsonl`` telemetry files.

    Returns
    -------
    pandas.DataFrame
        Combined telemetry rows with parsed timestamps set as the index.

    Raises
    ------
    ValueError
        If no ``timestamp`` column is present after loading.

    Notes
    -----
    Malformed JSON lines are skipped. The combined dataset is sorted by time
    before indexing.
    """
    records = []

    for f in sorted(data_dir.glob("*.jsonl")):
        with open(f, "r") as fh:
            for line in fh:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    df = pd.DataFrame(records)
    if "timestamp" not in df.columns:
        raise ValueError("timestamp column missing")

    df = prepare_timestamp_column(df, time_col="timestamp", drop_invalid=True, sort=True)
    df = df.set_index("timestamp")
    return df


def preprocess(df):
    """
    Resample one machine's telemetry and construct model-ready features.

    This step:
    - resamples the machine stream to a fixed frequency
    - forward-fills missing values and fills remaining nulls with zero
    - selects candidate numeric telemetry features
    - creates lagged versions of those features
    - derives a binary future-stop label

    Parameters
    ----------
    df : pandas.DataFrame
        One machine's telemetry, indexed by timestamp.

    Returns
    -------
    tuple[pandas.DataFrame, list[str]]
        ``(processed_df, numeric_features)`` where:
        - ``processed_df`` contains lagged features and the target label
        - ``numeric_features`` lists the base telemetry signals that were used

    Raises
    ------
    ValueError
        If no usable numeric telemetry features are found.

    Notes
    -----
    The target label ``future_stop`` is derived heuristically:
    - if ``execution`` exists, rows where execution is STOPPED or READY are
      treated as stopped
    - otherwise, the first available spindle-speed column is used as a fallback

    This label is shifted by ``FUTURE_WINDOW`` rows, so the prediction horizon
    depends on the resampling frequency.
    """
    df = df.resample(f"{int(1 / DOWNSAMPLE_HZ)}s").mean(numeric_only=True)
    df = df.ffill().fillna(0)

    candidate_features = [
        "Srpm",
        "S2rpm",
        "Fact",
        "Xload",
        "Yload",
        "Zload",
        "Sload",
        "Fovr",
        "Sovr",
        "Frapidovr",
        "auto_time",
        "cut_time",
    ]
    numeric_features = [c for c in candidate_features if c in df.columns]
    if not numeric_features:
        raise ValueError("No numeric telemetry features found in this dataset.")

    for col in numeric_features:
        df[col] = to_numeric(df[col]).fillna(0)

    # Create lagged features so the model can use recent history.
    for col in numeric_features:
        for lag in LAG_STEPS:
            df[f"{col}_lag{lag}"] = df[col].shift(lag)

    if "execution" in df.columns:
        df["is_stopped"] = df["execution"].isin(["STOPPED", "READY"]).astype(int)
    else:
        spindle_cols = [c for c in ["Srpm", "S2rpm"] if c in df.columns]
        if spindle_cols:
            df["is_stopped"] = (df[spindle_cols[0]] == 0).astype(int)
        else:
            print("Warning: no execution or spindle speed data; assuming no stops.")
            df["is_stopped"] = 0

    df["future_stop"] = df["is_stopped"].shift(-FUTURE_WINDOW)

    # Drop context columns and rows that cannot support lagged prediction.
    df = df.drop(columns=["is_stopped", "execution", "mode"], errors="ignore")
    lag_cols = [f"{numeric_features[0]}_lag{max(LAG_STEPS)}"]
    df = df.dropna(subset=lag_cols + ["future_stop"])

    return df, numeric_features


def train_model(df, numeric_features, machine_name):
    """
    Train and evaluate one RandomForest stop-prediction model for a machine.

    Parameters
    ----------
    df : pandas.DataFrame
        Preprocessed machine data containing lagged features and ``future_stop``.
    numeric_features : list[str]
        Base telemetry feature names used to identify lagged feature columns.
    machine_name : str
        Machine identifier used for output naming.

    Returns
    -------
    dict | None
        Summary metrics for the trained model, or None if training is skipped
        because the target contains only one class.

    Notes
    -----
    The train/test split is random and stratified. This is suitable for a simple
    baseline, but it does not preserve temporal ordering and may therefore
    overestimate performance for time-series prediction tasks.
    """
    X = df[[c for c in df.columns if c.startswith(tuple(numeric_features)) and "lag" in c]]
    y = df["future_stop"].astype(int)

    # Skip training if the target contains only one class.
    if y.nunique() < 2:
        print(f"Skipping {machine_name}: only one class present.")
        return None

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = RandomForestClassifier(
        n_estimators=200,
        n_jobs=-1,
        random_state=42,
        class_weight="balanced",
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    report = classification_report(y_test, y_pred, digits=3, output_dict=True)
    print(f"\n=== {machine_name} ===")
    print(classification_report(y_test, y_pred, digits=3))

    imp = pd.Series(model.feature_importances_, index=X.columns).sort_values(ascending=False)
    out_dir = OUTPUT_DIR / machine_name
    out_dir.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(8, 5))
    imp.head(15).plot(kind="barh")
    plt.title(f"{machine_name} – Top 15 Feature Importances")
    plt.tight_layout()
    plt.savefig(out_dir / "feature_importance.png", dpi=300)
    plt.close()

    if SAVE_MODEL:
        joblib.dump(model, out_dir / f"{machine_name}_stop_predictor.pkl")

    cm = confusion_matrix(y_test, y_pred)
    np.savetxt(out_dir / "confusion_matrix.txt", cm, fmt="%d")

    pd.DataFrame(report).to_csv(out_dir / "classification_report.csv")

    return {
        "machine": machine_name,
        "accuracy": report["accuracy"],
        "precision": report["1"]["precision"],
        "recall": report["1"]["recall"],
        "f1": report["1"]["f1-score"],
        "n_samples": len(df),
    }


def main():
    """
    Run the full per-machine stop-prediction pipeline.
    """
    print("Loading data...")
    df = load_all_data(DATA_DIR)

    if "machine" not in df.columns:
        raise ValueError("Missing 'machine' column – cannot separate by machine.")

    machine_stats = []

    for machine_name, mdf in df.groupby("machine"):
        print(f"\n--- Analyzing {machine_name} ---")
        try:
            processed, feats = preprocess(mdf)
            stats = train_model(processed, feats, machine_name)
            if stats:
                machine_stats.append(stats)
        except Exception as e:
            print(f"Error processing {machine_name}: {e}")

    if machine_stats:
        summary = pd.DataFrame(machine_stats)
        OUTPUT_DIR.mkdir(exist_ok=True)
        summary.to_csv(OUTPUT_DIR / "summary.csv", index=False)
        print("\nSummary written to", OUTPUT_DIR / "summary.csv")
        print(summary)
    else:
        print("No machine models were trained.")


if __name__ == "__main__":
    main()
