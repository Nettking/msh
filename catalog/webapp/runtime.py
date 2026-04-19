"""Runtime helpers for registry scanning and refresh-aware caching."""

from __future__ import annotations

import streamlit as st

from catalog.webapp.analysis_registry import scan_artifacts


@st.cache_data(show_spinner=False)
def scan_registry(scan_dirs: tuple[str, ...], scan_nonce: int, refresh_bucket: int) -> tuple[list[dict], list[str]]:
    """Scan configured directories and return indexed artifacts plus warnings.

    The cached key intentionally includes both manual `scan_nonce` and timed
    `refresh_bucket` so periodic reruns perform real rescans.
    """
    _ = scan_nonce, refresh_bucket
    return scan_artifacts(list(scan_dirs))
