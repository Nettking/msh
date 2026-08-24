from __future__ import annotations

from catalog.federation import tailnet_join_responder as responder


def test_linux_process_identity_never_falls_back_to_second_resolution_ps(
    monkeypatch,
) -> None:
    ps_calls: list[int] = []
    monkeypatch.setattr(responder.os, "name", "posix")
    monkeypatch.setattr(responder.sys, "platform", "linux")
    monkeypatch.setattr(responder, "_proc_process_start_token", lambda pid: None)
    monkeypatch.setattr(
        responder,
        "_ps_process_start_token",
        lambda pid: ps_calls.append(pid) or "posix:Mon Aug 24 15:00:00 2026",
    )

    assert responder.process_start_token(4242) is None
    assert ps_calls == []


def test_linux_missing_strong_identity_cannot_authorize_a_signal(monkeypatch) -> None:
    signalled: list[tuple[int, int]] = []
    closed: list[int] = []
    monkeypatch.setattr(responder.os, "name", "posix")
    monkeypatch.setattr(responder.sys, "platform", "linux")
    monkeypatch.setattr(responder, "_proc_process_start_token", lambda pid: None)
    monkeypatch.setattr(
        responder,
        "_ps_process_start_token",
        lambda pid: "posix:Mon Aug 24 15:00:00 2026",
    )
    monkeypatch.setattr(
        responder.os,
        "pidfd_open",
        lambda pid, flags=0: 91,
        raising=False,
    )
    monkeypatch.setattr(
        responder.signal,
        "pidfd_send_signal",
        lambda descriptor, sig: signalled.append((descriptor, sig)),
        raising=False,
    )
    monkeypatch.setattr(responder.os, "close", lambda descriptor: closed.append(descriptor))

    assert not responder.terminate_process_if_same_instance(
        4242, "posix:Mon Aug 24 15:00:00 2026"
    )
    assert signalled == []
    assert closed == [91]
