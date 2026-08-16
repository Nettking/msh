from __future__ import annotations

import pytest

import connect_recorder


def test_recorder_connect_requires_signed_pairing_code(monkeypatch) -> None:
    called = []
    monkeypatch.setattr(connect_recorder.start_recorder, "main", lambda argv: called.append(argv) or 0)

    with pytest.raises(SystemExit):
        connect_recorder.main(["not-a-code"])

    assert called == []


def test_recorder_connect_is_always_federation_required(monkeypatch) -> None:
    called = []
    monkeypatch.setattr(connect_recorder.start_recorder, "main", lambda argv: called.append(argv) or 0)

    result = connect_recorder.main(["FCP1-one-use"])

    assert result == 0
    assert len(called) == 1
    forwarded = called[0]
    assert forwarded[:3] == ["--federation-key", "FCP1-one-use", "--require-federation"]
    assert "--require-federation" in forwarded
    assert "--federation-key" in forwarded


def test_recorder_connect_forwards_data_sharing_and_scan_options(monkeypatch) -> None:
    called = []
    monkeypatch.setattr(connect_recorder.start_recorder, "main", lambda argv: called.append(argv) or 0)

    result = connect_recorder.main(
        [
            "FCP1-one-use",
            "--require-data-sharing",
            "--scan-cidr",
            "192.168.10.0/24",
            "--device-name",
            "Mazak recorder",
            "--storage-group",
            "machine-data",
            "--once",
        ]
    )

    assert result == 0
    forwarded = called[0]
    assert "--require-federation" in forwarded
    assert "--require-data-sharing" in forwarded
    assert forwarded[forwarded.index("--scan-cidr") + 1] == "192.168.10.0/24"
    assert forwarded[forwarded.index("--device-name") + 1] == "Mazak recorder"
    assert forwarded[forwarded.index("--storage-group") + 1] == "machine-data"
    assert "--once" in forwarded
