"""Shared lightweight DataFrame preparation helpers for telemetry scripts.

This module centralizes low-level, repeated preprocessing steps that are common
across exploratory telemetry analyses, such as normalizing ``UNAVAILABLE``
markers, coercing columns to numeric, and preparing sortable timestamp columns.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd


def replace_unavailable(series: pd.Series) -> pd.Series:
    """Replace string sentinel ``"UNAVAILABLE"`` values with ``NaN``.

    Replacement is applied only for object- or string-like series.
    """
    if series.dtype == object or str(series.dtype).startswith("string"):
        return series.replace("UNAVAILABLE", np.nan)
    return series


def to_numeric(series: pd.Series) -> pd.Series:
    """Coerce a telemetry series to numeric after UNAVAILABLE normalization."""
    return pd.to_numeric(replace_unavailable(series), errors="coerce")


def prepare_timestamp_column(
    df: pd.DataFrame,
    *,
    time_col: str = "timestamp",
    drop_invalid: bool = True,
    sort: bool = True,
    reset_index: bool = False,
) -> pd.DataFrame:
    """Parse one timestamp column and optionally drop invalid/sort rows.

    Returns a copied dataframe with ``time_col`` converted via
    ``pandas.to_datetime(errors='coerce')``.
    """
    prepared = df.copy()
    prepared[time_col] = pd.to_datetime(prepared[time_col], errors="coerce")

    if drop_invalid:
        prepared = prepared[prepared[time_col].notna()].copy()

    if sort:
        prepared = prepared.sort_values(time_col)

    if reset_index:
        prepared = prepared.reset_index(drop=True)

    return prepared


def find_machine_column(
    df: pd.DataFrame,
    candidates: Sequence[str],
) -> str | None:
    """Return the first present machine-identifying column from candidates."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def add_machine_id_column(
    df: pd.DataFrame,
    *,
    source_col: str,
    target_col: str = "machine_id",
) -> pd.DataFrame:
    """Add a normalized machine-id column from a source machine column."""
    prepared = df.copy()
    prepared[target_col] = prepared[source_col].astype(str)
    return prepared


def add_date_column(
    df: pd.DataFrame,
    *,
    time_col: str = "timestamp",
    target_col: str = "date",
) -> pd.DataFrame:
    """Add a calendar-date column derived from a datetime timestamp column."""
    prepared = df.copy()
    prepared[target_col] = prepared[time_col].dt.date
    return prepared
