from __future__ import annotations

import numpy as np
import pandas as pd


def _is_date_only(value: str) -> bool:
    return len(value) == 10 and value[4:5] == "-" and value[7:8] == "-"


def _windowed_frame(
    df: pd.DataFrame,
    *,
    window_start: str | None,
    window_end: str | None,
    window_preset: str | None,
) -> tuple[pd.DataFrame, pd.Series | None]:
    if "timestamp" not in df.columns:
        return df.copy(), None
    rows = df.copy()
    ts = pd.to_datetime(rows["timestamp"], errors="coerce", utc=True)
    valid_ts = ts.dropna()
    if valid_ts.empty:
        return rows, ts

    end = pd.to_datetime(window_end, errors="coerce", utc=True) if window_end else valid_ts.max()
    end_is_day_boundary = bool(window_end and _is_date_only(window_end))
    if end_is_day_boundary and end is not pd.NaT:
        end = end + pd.Timedelta(days=1)
    start = pd.to_datetime(window_start, errors="coerce", utc=True) if window_start else None
    if start is None and window_preset and window_preset != "full":
        deltas = {
            "1d": pd.Timedelta(days=1),
            "6h": pd.Timedelta(hours=6),
            "1h": pd.Timedelta(hours=1),
            "15m": pd.Timedelta(minutes=15),
            "5m": pd.Timedelta(minutes=5),
        }
        delta = deltas.get(window_preset)
        if delta is not None and end is not pd.NaT:
            start = end - delta

    if end is pd.NaT:
        end = valid_ts.max()
    if start is not None and start is not pd.NaT and start > end:
        start, end = end, start

    mask = ts.notna()
    if start is not None and start is not pd.NaT:
        mask &= ts >= start
    if end is not None and end is not pd.NaT:
        if end_is_day_boundary:
            mask &= ts < end
        else:
            mask &= ts <= end
    return rows.loc[mask].copy(), ts.loc[mask].copy()


def _aggregate_time_frame(rows: pd.DataFrame, *, aggregation: str) -> tuple[pd.DataFrame, pd.Series | None]:
    if "timestamp" not in rows.columns:
        return rows, None
    ts = pd.to_datetime(rows["timestamp"], errors="coerce", utc=True)
    rows = rows.assign(_timestamp=ts).dropna(subset=["_timestamp"])
    if rows.empty:
        return rows, rows.get("_timestamp")

    if aggregation == "day":
        rows["_bucket"] = rows["_timestamp"].dt.floor("D")
    elif aggregation == "hour":
        rows["_bucket"] = rows["_timestamp"].dt.floor("H")
    elif aggregation == "minute":
        rows["_bucket"] = rows["_timestamp"].dt.floor("min")
    else:
        rows["_bucket"] = rows["_timestamp"]
    return rows, rows["_bucket"]


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


def line_or_scatter_data(
    df: pd.DataFrame,
    y_cols: list[str],
    *,
    mode: str = "line",
    limit: int = 5000,
    window_start: str | None = None,
    window_end: str | None = None,
    window_preset: str | None = None,
    aggregation: str = "auto",
) -> dict:
    rows, ts = _windowed_frame(df, window_start=window_start, window_end=window_end, window_preset=window_preset)
    if len(rows) > limit:
        rows = rows.head(limit)
        ts = ts.head(limit) if ts is not None else None

    if aggregation == "auto":
        if len(rows) > 3000:
            aggregation = "hour"
        elif len(rows) > 1200:
            aggregation = "minute"
        else:
            aggregation = "raw"

    rows, bucket = _aggregate_time_frame(rows, aggregation=aggregation)
    if bucket is not None and aggregation in {"day", "hour", "minute"}:
        grouping = rows.assign(_bucket=bucket)
        agg_payload = {col: "mean" for col in y_cols if col in grouping.columns}
        if agg_payload:
            rows = grouping.groupby("_bucket", as_index=False).agg(agg_payload).rename(columns={"_bucket": "timestamp"})
            ts = pd.to_datetime(rows["timestamp"], errors="coerce", utc=True)
        else:
            ts = pd.to_datetime(rows["timestamp"], errors="coerce", utc=True) if "timestamp" in rows.columns else None

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
        return {"datasets": datasets, "x_is_time": bool(ts is not None and ts.notna().any())}

    labels = ts.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("").tolist() if ts is not None and ts.notna().any() else [str(i) for i in range(len(rows))]
    datasets = []
    for col in y_cols:
        series = pd.to_numeric(rows[col], errors="coerce")
        datasets.append({"label": col, "data": [None if pd.isna(v) else float(v) for v in series.tolist()]})
    return {"labels": labels, "datasets": datasets, "x_is_time": bool(ts is not None and ts.notna().any())}


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
