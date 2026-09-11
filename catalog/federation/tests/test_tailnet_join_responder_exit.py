"""Real owned-process regressions for responder replacement (physical D06/#462)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.federation_host_runner import load_host_module

responder = load_host_module("tailnet_join_responder")


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux SIGTERM delay")
@pytest.mark.parametrize(("exit_timeout", "expected_exit"), [(5.0, 0), (0.05, 1)])
def test_main_waits_for_previous_process_to_release_its_listener(
    tmp_path: Path, monkeypatch, exit_timeout, expected_exit
) -> None:
    monkeypatch.setattr(responder, "PROCESS_EXIT_TIMEOUT_SECONDS", exit_timeout)
    child_code = """
import signal, socket, sys, time
listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen()
def stop(signum, frame):
    time.sleep(0.2)
    listener.close()
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
print(listener.getsockname()[1], flush=True)
while True:
    time.sleep(1)
"""
    child = subprocess.Popen(
        [sys.executable, "-u", "-c", child_code],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        port = int(child.stdout.readline())
        token = responder.process_start_token(child.pid)
        assert token is not None
        pid_file = tmp_path / "responder.pid"
        pid_file.write_text(
            json.dumps(
                {
                    "schema": responder.PROCESS_RECORD_SCHEMA,
                    "pid": child.pid,
                    "start_token": token,
                }
            ),
            encoding="utf-8",
        )
        reached_live_server = []

        def finish_after_real_bind(server, **kwargs):
            reached_live_server.append(server.server_address)
            assert child.poll() == 0, (
                "old process must have finished, not just signalled"
            )
            raise KeyboardInterrupt

        monkeypatch.setattr(responder._Server, "serve_forever", finish_after_real_bind)
        assert (
            responder.main(
                [
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--secret-file",
                    str(tmp_path / "secret"),
                    "--pid-file",
                    str(pid_file),
                ]
            )
            == expected_exit
        )
        if expected_exit == 0:
            assert reached_live_server == [("127.0.0.1", port)]
            assert json.loads(pid_file.read_text())["pid"] != child.pid
        else:
            assert reached_live_server == []
            assert json.loads(pid_file.read_text())["pid"] == child.pid
            assert child.poll() is None
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
        if child.stdout is not None:
            child.stdout.close()


@pytest.mark.parametrize("wait_result", [0, 0x102, 0xFFFFFFFF])
def test_windows_exit_wait_uses_the_verified_handle(monkeypatch, wait_result) -> None:
    import ctypes

    calls = []

    def open_process(access, inherit, pid):
        calls.append(("open", access, inherit, pid))
        return 73

    def terminate(handle, code):
        calls.append(("terminate", handle, code))
        return True

    def wait(handle, timeout):
        calls.append(("wait", handle, timeout))
        return wait_result

    def close(handle):
        calls.append(("close", handle))
        return True

    kernel = SimpleNamespace(
        OpenProcess=open_process,
        TerminateProcess=terminate,
        WaitForSingleObject=wait,
        CloseHandle=close,
    )
    monkeypatch.setattr(ctypes, "WinDLL", lambda *args, **kwargs: kernel, raising=False)
    monkeypatch.setattr(responder, "_windows_start_token_from_handle", lambda h: "same")
    if wait_result == 0:
        assert responder._terminate_windows_process_if_same_instance(
            42, "same", wait_for_exit=True
        )
    else:
        with pytest.raises(responder.ResponderReplacementError):
            responder._terminate_windows_process_if_same_instance(
                42, "same", wait_for_exit=True
            )
    assert calls == [
        ("open", 0x00100000 | 0x1000 | 0x0001, False, 42),
        ("terminate", 73, 1),
        ("wait", 73, 5000),
        ("close", 73),
    ]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux pidfd poll")
@pytest.mark.parametrize("event", [None, "readable", "reaped", "invalid", "error"])
def test_linux_wait_requires_exit_readiness_within_the_bound(
    monkeypatch, event
) -> None:
    import select

    calls = []

    class Poller:
        def register(self, fd, mask):
            calls.append((fd, mask))

        def poll(self, timeout):
            calls.append(timeout)
            masks = {
                "readable": select.POLLIN,
                "reaped": select.POLLHUP,
                "invalid": select.POLLNVAL,
                "error": select.POLLERR,
            }
            return [] if event is None else [(91, masks[event])]

    monkeypatch.setattr(select, "poll", Poller)
    assert responder._wait_linux_process_exit(91) == (event in {"readable", "reaped"})
    assert calls == [(91, select.POLLIN), 5000]


def test_unconfirmed_exit_refuses_replacement_before_bind_or_pid_write(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    pid_file = tmp_path / "responder.pid"
    pid_file.write_text("previous process record", encoding="utf-8")

    def unconfirmed(path):
        raise responder.ResponderReplacementError(
            "previous responder exit was not confirmed"
        )

    monkeypatch.setattr(responder, "stop_previous_instance", unconfirmed)
    monkeypatch.setattr(
        responder, "build_server", lambda **kw: pytest.fail("must not bind")
    )
    monkeypatch.setattr(
        responder, "write_pid_file", lambda p: pytest.fail("must not write")
    )
    assert (
        responder.main(
            [
                "--bind",
                "127.0.0.1",
                "--port",
                "5151",
                "--secret-file",
                str(tmp_path / "secret"),
                "--pid-file",
                str(pid_file),
            ]
        )
        == 1
    )
    assert pid_file.read_text() == "previous process record"
    output = capsys.readouterr().err
    assert "replacement refused" in output
    assert "listening on" not in output
