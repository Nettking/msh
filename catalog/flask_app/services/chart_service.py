from __future__ import annotations

import numpy as np
import pandas as pd


def numeric_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if pd.to_numeric(df[col], errors="coerce").notna().any():
            cols.append(col)
    return cols


def category_columns(df: pd.DataFrame, max_unique: int = 40) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if df[col].astype("string").nunique(dropna=True) <= max_unique:
            cols.append(col)
    return cols


def line_or_scatter_data(df: pd.DataFrame, y_cols: list[str], *, mode: str = "line", limit: int = 5000) -> dict:
    rows = df.copy().head(limit)
    ts = pd.to_datetime(rows["timestamp"], errors="coerce") if "timestamp" in rows.columns else None

    if mode == "scatter":
        if ts is not None and ts.notna().any():
            ts_ms = ts.astype("int64", copy=False) // 10**6
            fallback = pd.Series(range(len(rows)), index=rows.index, dtype="int64")
            x_values = ts_ms.where(ts.notna(), fallback)
        else:
            x_values = pd.Series(range(len(rows)), index=rows.index, dtype="int64")
        datasets = []
        for col in y_cols:
            y_series = pd.to_numeric(rows[col], errors="coerce")
            points = []
            for x, y in zip(x_values.tolist(), y_series.tolist()):
                if pd.isna(y):
                    continue
                points.append({"x": float(x), "y": float(y)})
            datasets.append({"label": col, "data": points})
        return {"datasets": datasets, "x_is_time": ts is not None and ts.notna().any()}

    labels = ts.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("").tolist() if ts is not None and ts.notna().any() else [str(i) for i in range(len(rows))]
    datasets = []
    for col in y_cols:
        series = pd.to_numeric(rows[col], errors="coerce")
        datasets.append({"label": col, "data": [None if pd.isna(v) else float(v) for v in series.tolist()]})
    return {"labels": labels, "datasets": datasets, "x_is_time": False}


def histogram_data(df: pd.DataFrame, col: str, bins: int = 20) -> dict:
    values = pd.to_numeric(df[col], errors="coerce").dropna()
    if values.empty:
        return {"labels": [], "counts": []}
    counts, edges = np.histogram(values.to_numpy(), bins=bins)
    labels = [f"{edges[i]:.2f}..{edges[i+1]:.2f}" for i in range(len(edges) - 1)]
    return {"labels": labels, "counts": counts.tolist()}


def category_counts(df: pd.DataFrame, col: str, limit: int = 30) -> dict:
    counts = df[col].astype("string").fillna("unknown").value_counts().head(limit)
    return {"labels": counts.index.tolist(), "counts": [int(v) for v in counts.values.tolist()]}


def machine_day_trend(df: pd.DataFrame) -> dict:
    if "machine_id" not in df.columns or "timestamp" not in df.columns:
        return {"labels": [], "series": []}
    tdf = df.copy()
    tdf["timestamp"] = pd.to_datetime(tdf["timestamp"], errors="coerce")
    tdf = tdf.dropna(subset=["timestamp"])
    if tdf.empty:
        return {"labels": [], "series": []}
    tdf["day"] = tdf["timestamp"].dt.date.astype(str)

    grouped = tdf.groupby(["day", "machine_id"]).size().reset_index(name="rows")
    days = sorted(grouped["day"].unique().tolist())
    machine_ids = sorted(grouped["machine_id"].astype(str).unique().tolist())

    series = []
    for machine in machine_ids:
        mapping = grouped[grouped["machine_id"].astype(str) == machine].set_index("day")["rows"].to_dict()
        series.append({"label": machine, "data": [int(mapping.get(day, 0)) for day in days]})
    return {"labels": days, "series": series}
