"""Legacy import surface for analysis registry helpers.

Primary implementation now lives in `catalog.common.artifact_registry` so both
Flask and Streamlit can share framework-neutral data logic.
"""

from catalog.common.artifact_registry import (  # noqa: F401
    REQUIRED_PLAYBACK_COLUMNS,
    SUPPORTED_SUFFIXES,
    configured_scan_dirs,
    read_raw_table as load_artifact_frame,
    scan_artifacts,
)
