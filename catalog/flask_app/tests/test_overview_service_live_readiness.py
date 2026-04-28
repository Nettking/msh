from __future__ import annotations

from catalog.flask_app.services.overview_service import _view_readiness


def test_live_readiness_message_mentions_inference_and_candidate_events() -> None:
    readiness = _view_readiness(runtime_state={}, visible=[], session_context={"session": None, "session_id": "", "source": "none"})
    live = next(item for item in readiness if item["view"] == "/live")

    assert "inferred" in live["message"].lower()
    assert "candidate" in live["message"].lower()
