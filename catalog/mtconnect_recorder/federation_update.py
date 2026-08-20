"""Federation software-update participation for the native standalone recorder.

"Check for updates -> Update all devices" reaches every Federation member through
the authoritative session event log. The standalone recorder was the one member
that could not act on those events: its only host updater rebuilt a Compose
service it does not run.

This module closes that gap without adding any Federation surface. It reuses the
recorder's *existing* authenticated relay connection -- the same client that
already carries capture publication and recorder control -- to replay
leader-authorized update commands and report results. There is no second
Federation node, no second relay client, and no second reader.

Two cooperating pieces run inside the recorder process:

``RecorderFederationUpdateWorker``
    Replays update commands and bridges them to the local native host agent
    through the same bounded file handoff the Compose agents use.
``RecorderUpdateActivationWatcher``
    Watches for the host agent's activation request and, only when it names this
    exact process and an update this recorder is genuinely waiting for, ends
    capture the same way Ctrl+C would.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

from catalog.flask_app.services.federation_active_leader_runtime import (
    ActiveLeaderFederationUpdateEventProcessor,
)
from catalog.flask_app.services.federation_update_handoff import HostUpdateHandoff

from .native_identity import process_nonce, supervisor_session
from .native_update import (
    NativeRecorderUpdatePaths,
    evaluate_activation,
    pending_federation_updates,
    read_json,
)
from .native_update_agent import NativeRecorderUpdateAgent

#: The repository this process is running from. Resolved locally from this
#: module's own location -- never from a peer, an argument, or an environment
#: value a peer could reach.
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

#: The activation file is polled rather than watched so the recorder never
#: depends on a platform file-notification API. One second is far below every
#: activation window and costs one small stat per second.
DEFAULT_POLL_SECONDS = 1.0

#: One bounded host operation. The worker can block for at most this long, so a
#: shutdown limit above it is enough for an in-flight pass to finish normally.
HANDOFF_TIMEOUT_SECONDS = 10.0

#: The explicit limit shutdown waits for an in-flight bounded host operation.
#: Exceeding it is reported rather than ignored: the relay client must never be
#: closed underneath a host operation that is still running.
SHUTDOWN_TIMEOUT_SECONDS = 20.0


class RecorderFederationUpdateWorker:
    """Replay leader-authorized update commands on the recorder's own client."""

    def __init__(
        self,
        node: Any,
        *,
        data_directory: Path | str,
        poll_seconds: float = 5.0,
        processor: Any | None = None,
        handoff: Any | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        self.node = node
        self.paths = NativeRecorderUpdatePaths(data_directory)
        self.poll_seconds = float(poll_seconds)
        self.handoff = handoff or HostUpdateHandoff(
            self.paths.directory,
            timeout=HANDOFF_TIMEOUT_SECONDS,
        )
        self.processor = processor or ActiveLeaderFederationUpdateEventProcessor(
            node.service,
            self.handoff,
            self.paths.federation_state_file,
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        if self.node.remote_store.load() is None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="fcp-recorder-federation-update",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout: float = SHUTDOWN_TIMEOUT_SECONDS) -> bool:
        """Stop within an explicit limit and report whether the worker finished.

        A bounded host operation may be in flight. Returning ``False`` tells the
        caller the relay client is about to be closed underneath it, which is a
        reportable failure rather than something to discover later as corruption.
        """

        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is None:
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def process_once(self) -> bool:
        """Replay one bounded pass. Returns whether a context was available."""

        context = self.node.service.authorized_context()
        if context is None:
            return False
        self.processor.process(context)
        return True

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.process_once()
            except Exception:  # noqa: BLE001 - transport failures retry safely
                # Update participation must never be able to end capture. Every
                # failure simply waits and replays again from durable state.
                self._stop.wait(self.poll_seconds)
                continue
            self._stop.wait(self.poll_seconds)


class RecorderHostUpdateAgentWorker:
    """Run the host update agent inside the recorder process.

    The agent could be a separate process, and originally was. It should not be:
    the recorder already knows its data directory and its repository, so a
    separate process would need both handed to it -- and the only way to compute
    the data directory outside the recorder is to run the launcher's own
    resolution in yet another interpreter, which puts an extra Python
    invocation on the supported startup path for no benefit.

    Running it here keeps startup to exactly one recorder process, makes the
    agent single-instance by construction, and leaves the supervisor with only
    the two steps that genuinely cannot happen inside the recorder: the
    fast-forward after it exits, and the relaunch.
    """

    def __init__(
        self,
        *,
        data_directory: Path | str,
        poll_seconds: float = 2.0,
        environment: Any = None,
        agent: Any = None,
        repository_root: Path | str = REPOSITORY_ROOT,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        self.poll_seconds = float(poll_seconds)
        self.supervisor_session = supervisor_session(environment)
        self.agent = agent
        if self.agent is None and self.supervisor_session is not None:
            self.agent = NativeRecorderUpdateAgent(
                repository_root=repository_root,
                data_directory=data_directory,
                supervisor_session=self.supervisor_session,
            )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def supervised(self) -> bool:
        return self.agent is not None

    def start(self) -> None:
        if not self.supervised or (
            self._thread is not None and self._thread.is_alive()
        ):
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="fcp-recorder-host-update-agent",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout: float = SHUTDOWN_TIMEOUT_SECONDS) -> bool:
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is None:
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def poll_once(self) -> bool:
        return bool(self.agent is not None and self.agent.poll_once())

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.poll_once()
            except Exception:  # noqa: BLE001 - a host failure must not end capture
                self._stop.wait(self.poll_seconds)
                continue
            self._stop.wait(self.poll_seconds)


class RecorderUpdateActivationWatcher:
    """End capture only for a correlated, unexpired, genuinely pending update.

    The watcher is deliberately the *second* independent check. The host agent
    already proved the update is applicable; this side proves the stop request
    is for this exact process and for Federation work this recorder is really
    waiting on. Anything else is ignored, so a marker left behind by an earlier
    process can never restart a new unrelated recorder.
    """

    def __init__(
        self,
        *,
        data_directory: Path | str,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        environment: Any = None,
        pid: int | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        self.paths = NativeRecorderUpdatePaths(data_directory)
        self.poll_seconds = float(poll_seconds)
        self.process_nonce = process_nonce(environment)
        self.supervisor_session = supervisor_session(environment)
        self.pid = int(os.getpid() if pid is None else pid)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._activated = threading.Event()
        self.activated_request_id: str | None = None
        self.activated_target_commit: str | None = None

    @property
    def supervised(self) -> bool:
        return self.process_nonce is not None and self.supervisor_session is not None

    @property
    def activated(self) -> bool:
        return self._activated.is_set()

    def start(self) -> None:
        if not self.supervised or (
            self._thread is not None and self._thread.is_alive()
        ):
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="fcp-recorder-update-activation",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout: float = 3.0) -> bool:
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is None:
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def evaluate_once(self) -> Any:
        value = read_json(self.paths.activation_file)
        if value is None:
            return None
        return evaluate_activation(
            value,
            pid=self.pid,
            process_nonce=self.process_nonce,
            supervisor_session=self.supervisor_session,
            pending_host_request_ids=pending_federation_updates(
                self.paths.federation_state_file
            ),
        )

    def poll_once(self) -> bool:
        """Return whether an approved update activation stopped capture."""

        if not self.supervised or self._activated.is_set():
            return self._activated.is_set()
        decision = self.evaluate_once()
        if decision is None or not decision.accepted:
            return False
        # Import lazily: the recorder runtime freezes its configuration from the
        # environment at import time, and the launcher sets that environment
        # only inside main().
        from .runtime import request_external_stop

        if not request_external_stop():
            # Capture is still starting. Latching now would mean an approved
            # update that never actually stopped anything, so retry instead.
            return False
        self.activated_request_id = decision.request_id
        self.activated_target_commit = decision.target_commit
        self._activated.set()
        return True

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.poll_once():
                    return
            except Exception:  # noqa: BLE001 - a bad marker must never stop capture
                self._stop.wait(self.poll_seconds)
                continue
            self._stop.wait(self.poll_seconds)


def approved_update_restart(watcher: RecorderUpdateActivationWatcher | None) -> bool:
    """Decide whether this exit is an approved native update restart.

    Both halves are required. The watcher must have accepted a correlated
    activation *and* the capture runtime must record that the activation is what
    ended it. An operator's Ctrl+C always wins, so a stop the operator asked for
    can never be turned into an automatic relaunch.
    """

    if watcher is None or not watcher.activated:
        return False
    from .runtime import EXTERNAL_STOP_REASON, last_stop_reason

    return last_stop_reason() == EXTERNAL_STOP_REASON


__all__ = [
    "DEFAULT_POLL_SECONDS",
    "HANDOFF_TIMEOUT_SECONDS",
    "REPOSITORY_ROOT",
    "SHUTDOWN_TIMEOUT_SECONDS",
    "RecorderFederationUpdateWorker",
    "RecorderHostUpdateAgentWorker",
    "RecorderUpdateActivationWatcher",
    "approved_update_restart",
]
