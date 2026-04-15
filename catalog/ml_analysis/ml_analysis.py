import json
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder
import joblib
import matplotlib.pyplot as plt

# ------------------------------------------------------------
# SETTINGS
# ------------------------------------------------------------
DATA_DIR = Path("data")
OUTPUT_DIR = Path("ml_results")
SAVE_MODEL = True
LAG_STEPS = [1, 5, 10]
FUTURE_WINDOW = 60      # predict stop within next 60 samples
DOWNSAMPLE_HZ = 1       # aggregate to 1 Hz

# ------------------------------------------------------------
# LOAD AND COMBINE JSONL FILES
# ------------------------------------------------------------
def load_all_data(data_dir):
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
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    df = df.set_index("timestamp")
    return df

# ------------------------------------------------------------
# PREPROCESS ONE MACHINE'S DATA
# ------------------------------------------------------------
def preprocess(df):
    df = df.resample(f"{int(1/DOWNSAMPLE_HZ)}s").mean(numeric_only=True)
    df = df.ffill().fillna(0)

    candidate_features = [
        "Srpm","S2rpm","Fact","Xload","Yload","Zload","Sload",
        "Fovr","Sovr","Frapidovr","auto_time","cut_time"
    ]
    numeric_features = [c for c in candidate_features if c in df.columns]
    if not numeric_features:
        raise ValueError("No numeric telemetry features found in this dataset.")

    for col in numeric_features:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Create lag features
    for col in numeric_features:
        for lag in LAG_STEPS:
            df[f"{col}_lag{lag}"] = df[col].shift(lag)

    # Label creation
    if "execution" in df.columns:
        df["is_stopped"] = df["execution"].isin(["STOPPED","READY"]).astype(int)
    else:
        spindle_cols = [c for c in ["Srpm","S2rpm"] if c in df.columns]
        if spindle_cols:
            df["is_stopped"] = (df[spindle_cols[0]] == 0).astype(int)
        else:
            print("Warning: no execution or spindle speed data; assuming no stops.")
            df["is_stopped"] = 0

    df["future_stop"] = df["is_stopped"].shift(-FUTURE_WINDOW)

    # Clean up
    df = df.drop(columns=["is_stopped","execution","mode"], errors="ignore")
    lag_cols = [f"{numeric_features[0]}_lag{max(LAG_STEPS)}"]
    df = df.dropna(subset=lag_cols + ["future_stop"])
    return df, numeric_features

# ------------------------------------------------------------
# TRAIN MODEL FOR ONE MACHINE
# ------------------------------------------------------------
def train_model(df, numeric_features, machine_name):
    X = df[[c for c in df.columns if c.startswith(tuple(numeric_features)) and "lag" in c]]
    y = df["future_stop"].astype(int)

    # skip if no variation
    if y.nunique() < 2:
        print(f"Skipping {machine_name}: only one class present.")
        return None

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=200,
        n_jobs=-1,
        random_state=42,
        class_weight="balanced"
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    report = classification_report(y_test, y_pred, digits=3, output_dict=True)
    print(f"\n=== {machine_name} ===")
    print(classification_report(y_test, y_pred, digits=3))

    # Feature importance
    imp = pd.Series(model.feature_importances_, index=X.columns).sort_values(ascending=False)
    out_dir = OUTPUT_DIR / machine_name
    out_dir.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(8,5))
    imp.head(15).plot(kind="barh")
    plt.title(f"{machine_name} – Top 15 Feature Importances")
    plt.tight_layout()
    plt.savefig(out_dir / "feature_importance.png", dpi=300)
    plt.close()

    if SAVE_MODEL:
        joblib.dump(model, out_dir / f"{machine_name}_stop_predictor.pkl")

    # confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    np.savetxt(out_dir / "confusion_matrix.txt", cm, fmt="%d")

    # save report
    pd.DataFrame(report).to_csv(out_dir / "classification_report.csv")

    return {
        "machine": machine_name,
        "accuracy": report["accuracy"],
        "precision": report["1"]["precision"],
        "recall": report["1"]["recall"],
        "f1": report["1"]["f1-score"],
        "n_samples": len(df)
    }

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
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
