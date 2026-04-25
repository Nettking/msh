from __future__ import annotations

import pandas as pd

from catalog.flask_app.routes import _serialize_playback_timestamp


def test_serialize_playback_timestamp_preserves_millisecond_precision() -> None:
    serialized = _serialize_playback_timestamp(
        pd.Series(
            [
                "2026-03-01T10:00:00.000Z",
                "2026-03-01T10:00:00.200Z",
                "2026-03-01T10:00:00.400Z",
            ]
        )
    )

    assert serialized.tolist() == [
        "2026-03-01T10:00:00.000Z",
        "2026-03-01T10:00:00.200Z",
        "2026-03-01T10:00:00.400Z",
    ]
