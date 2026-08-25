from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.federation import host_resources


class _FakeWindowsPath:
    def __init__(self, *, anchor: str, device_id: int) -> None:
        self.anchor = anchor
        self._device_id = device_id

    def stat(self) -> SimpleNamespace:
        return SimpleNamespace(st_dev=self._device_id)


def test_windows_identity_coalesces_aliases_by_backing_volume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as patch:
        patch.setattr(host_resources.os, "name", "nt")
        mounted = host_resources._resource_identity(
            _FakeWindowsPath(anchor="C:\\mounted\\", device_id=77)  # type: ignore[arg-type]
        )
        drive = host_resources._resource_identity(
            _FakeWindowsPath(anchor="D:\\", device_id=77)  # type: ignore[arg-type]
        )
        other = host_resources._resource_identity(
            _FakeWindowsPath(anchor="E:\\", device_id=88)  # type: ignore[arg-type]
        )

    assert mounted == "volume:77"
    assert drive == mounted
    assert other == "volume:88"


def test_windows_identity_has_anchor_fallback_when_device_id_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as patch:
        patch.setattr(host_resources.os, "name", "nt")
        result = host_resources._resource_identity(
            _FakeWindowsPath(anchor="C:\\", device_id=0)  # type: ignore[arg-type]
        )

    assert result == "volume-anchor:c:\\"
