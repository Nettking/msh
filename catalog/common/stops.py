"""Shared helpers for stop-row detection and stop-interval grouping.

These utilities intentionally stay small and explicit so scripts can keep their
own stop semantics (state lists, numeric features, and grouping gaps) while
reusing repeated dataframe logic.
"""

from __future__ import annotations

import pandas as pd

from catalog.common.telemetry_prep import to_numeric


def prepare_stop_numeric_columns(df: pd.DataFrame, numeric_cols: list[str]) -> tuple[pd.DataFrame, list[str]]:
    """Coerce available stop-related numeric columns to numeric dtype.

    Returns a copied dataframe and the subset of ``numeric_cols`` present in the
    dataframe.
    """
    prepared = df.copy()
    available_cols = [col for col in numeric_cols if col in prepared.columns]
    for col in available_cols:
        prepared[col] = to_numeric(prepared[col])
    return prepared, available_cols


def find_stop_rows(
    df: pd.DataFrame,
    *,
    stopped_states: list[str],
    numeric_cols: list[str],
    min_zero_count: int | None = None,
    execution_col: str = "execution",
) -> tuple[pd.DataFrame, list[str]]:
    """Return stop-like rows after coercing stop numeric columns.

    This helper first calls :func:`prepare_stop_numeric_columns` and evaluates
    stop conditions on that prepared copy (the input dataframe is not mutated).
    A row is considered stop-like when ``execution_col`` is in
    ``stopped_states`` and at least ``min_zero_count`` of the available numeric
    columns are equal to zero. If ``min_zero_count`` is ``None``, the threshold
    defaults to ``max(1, len(available_cols) // 2)``.
    """
    prepared, available_cols = prepare_stop_numeric_columns(df, numeric_cols)
    if not available_cols or execution_col not in prepared.columns:
        return prepared.iloc[0:0].copy(), available_cols

    zero_threshold = min_zero_count
    if zero_threshold is None:
        zero_threshold = max(1, len(available_cols) // 2)

    stopped_mask = (
        prepared[execution_col].isin(stopped_states)
        & ((prepared[available_cols] == 0).sum(axis=1) >= zero_threshold)
    )
    return prepared.loc[stopped_mask].copy(), available_cols


def group_stop_rows(
    df: pd.DataFrame,
    *,
    max_gap_seconds: float,
    time_col: str = "timestamp",
    machine_col: str = "machine",
    default_machine: str = "UNKNOWN",
) -> pd.DataFrame:
    """Group nearby stop rows into machine-specific stop intervals."""
    if df.empty:
        return pd.DataFrame(columns=[machine_col, "start", "end", "duration_s"])

    grouped_rows: list[list[object]] = []
    working = df.sort_values(time_col).copy()
    working[machine_col] = working.get(machine_col, default_machine)

    for machine, machine_df in working.groupby(machine_col):
        start = None
        last = None

        for t in machine_df[time_col]:
            if start is None:
                start, last = t, t
                continue

            gap = (t - last).total_seconds()
            if gap <= max_gap_seconds:
                last = t
                continue

            grouped_rows.append([machine, start, last, (last - start).total_seconds()])
            start, last = t, t

        if start is not None:
            grouped_rows.append([machine, start, last, (last - start).total_seconds()])

    return pd.DataFrame(grouped_rows, columns=[machine_col, "start", "end", "duration_s"])
