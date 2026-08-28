from __future__ import annotations

import hashlib
import json
from pathlib import Path

from catalog.federation.software_trial import BRANCHES_RESULT_SCHEMA
from catalog.flask_app.services.federation_update_handoff import HostUpdateHandoff


def test_schema_valid_but_unusable_branch_result_is_still_retired(
    tmp_path: Path, monkeypatch
) -> None:
    """A caller retires its own answered result even when the payload is unusable."""

    tmp_path.mkdir(parents=True, exist_ok=True)
    handoff = HostUpdateHandoff(tmp_path, timeout=1.0, poll_interval=0.02)
    original = handoff._write_request
    written: list[Path] = []

    def answer_with_unusable_result(value):
        original(value)
        request_id = str(value["request_id"])
        digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
        path = tmp_path / f"branches-result-{digest}.json"
        path.write_text(
            json.dumps(
                {
                    "schema": BRANCHES_RESULT_SCHEMA,
                    "request_id": request_id,
                    "repository": "not-the-approved-repository",
                    "branches": [{"name": "main", "commit": "0" * 40}],
                }
            ),
            encoding="utf-8",
        )
        written.append(path)

    monkeypatch.setattr(handoff, "_write_request", answer_with_unusable_result)

    assert handoff.approved_branches() == ()
    assert len(written) == 1
    assert not written[0].exists(), (
        "the caller's schema-valid but unusable branch result was left behind"
    )
