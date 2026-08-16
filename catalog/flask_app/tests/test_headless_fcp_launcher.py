from __future__ import annotations

from argparse import Namespace

import headless_fcp


def _env(**overrides: str) -> dict[str, str]:
    value = {
        "FCP_BUILD_COMMIT": "a" * 40,
        "FCP_WEB_BIND": "100.64.0.10",
        "FCP_RELAY_BIND": "100.64.0.10",
        "FCP_WEB_PORT": "5000",
        "FCP_RELAY_PORT": "8765",
        "FCP_DATA_DIR": "/tmp/fcp-data",
    }
    value.update(overrides)
    return value


def test_reachable_relay_url_prefers_explicit_value() -> None:
    assert (
        headless_fcp._reachable_relay_url(
            _env(FCP_RELAY_BIND="127.0.0.1"),
            "wss://fcp.example.test:8765",
        )
        == "wss://fcp.example.test:8765"
    )


def test_reachable_relay_url_refuses_loopback() -> None:
    assert (
        headless_fcp._reachable_relay_url(
            _env(FCP_RELAY_BIND="127.0.0.1"),
            None,
        )
        is None
    )


def test_reachable_relay_url_uses_private_or_vpn_bind() -> None:
    assert (
        headless_fcp._reachable_relay_url(
            _env(FCP_RELAY_BIND="100.64.0.10"),
            None,
        )
        == "ws://100.64.0.10:8765"
    )


def test_reachable_relay_url_brackets_ipv6() -> None:
    assert (
        headless_fcp._reachable_relay_url(
            _env(FCP_RELAY_BIND="fd7a:115c:a1e0::1"),
            None,
        )
        == "ws://[fd7a:115c:a1e0::1]:8765"
    )


def test_start_command_creates_federation_and_mints_code(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []
    env = _env()
    monkeypatch.setattr(headless_fcp, "_prepare_env", lambda **_kwargs: env)
    monkeypatch.setattr(headless_fcp, "_start_runtime", lambda _env, *, fresh: calls.append(("runtime", str(fresh))))
    monkeypatch.setattr(headless_fcp, "_inside_flask", lambda _env, *args, **_kwargs: calls.append(tuple(args)))
    monkeypatch.setattr(headless_fcp, "_print_runtime", lambda _env: None)
    monkeypatch.setattr(headless_fcp, "_reachable_relay_url", lambda _env, _explicit: "ws://100.64.0.10:8765")

    result = headless_fcp._start_command(
        Namespace(fresh=True, bind=None, relay_url=None)
    )

    assert result == 0
    assert ("create",) in calls
    assert (
        "pairing-code",
        "--relay-url",
        "ws://100.64.0.10:8765",
    ) in calls


def test_connect_with_code_joins_but_never_creates(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []
    env = _env()
    monkeypatch.setattr(headless_fcp, "_prepare_env", lambda **_kwargs: env)
    monkeypatch.setattr(headless_fcp, "_start_runtime", lambda _env, *, fresh: calls.append(("runtime", str(fresh))))
    monkeypatch.setattr(headless_fcp, "_inside_flask", lambda _env, *args, **_kwargs: calls.append(tuple(args)))
    monkeypatch.setattr(headless_fcp, "_print_runtime", lambda _env: None)

    result = headless_fcp._connect_command(
        Namespace(
            pairing_code="FCP1-one-use",
            fresh=True,
            bind=None,
            relay_url=None,
        )
    )

    assert result == 0
    assert ("join", "FCP1-one-use") in calls
    assert ("create",) not in calls


def test_connect_without_code_only_mints_code(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []
    env = _env()
    monkeypatch.setattr(headless_fcp, "_prepare_env", lambda **_kwargs: env)
    monkeypatch.setattr(headless_fcp, "_start_runtime", lambda _env, *, fresh: calls.append(("runtime", str(fresh))))
    monkeypatch.setattr(headless_fcp, "_inside_flask", lambda _env, *args, **_kwargs: calls.append(tuple(args)))
    monkeypatch.setattr(headless_fcp, "_reachable_relay_url", lambda _env, _explicit: "ws://100.64.0.10:8765")

    result = headless_fcp._connect_command(
        Namespace(
            pairing_code=None,
            fresh=False,
            bind=None,
            relay_url=None,
        )
    )

    assert result == 0
    assert (
        "pairing-code",
        "--relay-url",
        "ws://100.64.0.10:8765",
    ) in calls
    assert not any(call and call[0] in {"create", "join"} for call in calls)
