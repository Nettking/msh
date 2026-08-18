"""A grant the host obtained is redeemed once, silently, through normal pairing."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from flask import Flask

from catalog.federation.tailnet_join_bridge import load_grant, store_grant
from catalog.flask_app import capability_startup_transition_routes as transition_routes

GRANT = "FCP1-test-grant-not-real"


def _install(monkeypatch, tmp_path: Path, *, remote=None, redeemed=None, fail=False):
    grant_file = tmp_path / "auto_join_grant.json"
    monkeypatch.setattr(transition_routes, "grant_path", lambda: grant_file)

    def redeem(code):
        if fail:
            raise RuntimeError("relay unreachable")
        if redeemed is not None:
            redeemed.append(code)

    monkeypatch.setattr(
        transition_routes,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(
            redeem_pairing_code=redeem,
            remote_store=SimpleNamespace(load=lambda: remote),
        ),
    )
    return grant_file


def _run(app: Flask) -> None:
    with app.app_context():
        transition_routes._redeem_pending_auto_join_grant()


def test_a_stored_grant_is_redeemed_through_the_normal_pairing_flow(
    monkeypatch, tmp_path: Path
) -> None:
    redeemed: list[str] = []
    grant_file = _install(monkeypatch, tmp_path, redeemed=redeemed)
    store_grant(grant_file, GRANT)

    _run(Flask(__name__))

    assert redeemed == [GRANT]
    # One use only: the grant must be gone whatever happens next.
    assert load_grant(grant_file) == ""


def test_a_grant_is_never_replayed_after_a_restart(
    monkeypatch, tmp_path: Path
) -> None:
    redeemed: list[str] = []
    grant_file = _install(monkeypatch, tmp_path, redeemed=redeemed)
    store_grant(grant_file, GRANT)

    app = Flask(__name__)
    _run(app)
    _run(app)
    _run(app)

    assert redeemed == [GRANT]


def test_a_failed_redemption_clears_the_grant_and_keeps_onboarding_alive(
    monkeypatch, tmp_path: Path
) -> None:
    grant_file = _install(monkeypatch, tmp_path, fail=True)
    store_grant(grant_file, GRANT)

    _run(Flask(__name__))

    assert load_grant(grant_file) == ""


def test_an_already_enrolled_device_discards_a_stale_grant(
    monkeypatch, tmp_path: Path
) -> None:
    redeemed: list[str] = []
    grant_file = _install(
        monkeypatch,
        tmp_path,
        remote=object(),
        redeemed=redeemed,
    )
    store_grant(grant_file, GRANT)

    _run(Flask(__name__))

    assert redeemed == []
    assert load_grant(grant_file) == ""


def test_no_grant_is_a_silent_no_op(monkeypatch, tmp_path: Path) -> None:
    redeemed: list[str] = []
    _install(monkeypatch, tmp_path, redeemed=redeemed)

    _run(Flask(__name__))

    assert redeemed == []
