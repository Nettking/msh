"""One CI-only reconnect observation; never changes product callables or clocks.

Install only in a separately identified diagnostic commit. The product checkout,
demo driver, workers, assertions, requests and timeouts remain at PRODUCT_SHA.
No raw traceback lines, exception messages, locals or private state are exported.
"""
from __future__ import annotations

import argparse
import atexit
from datetime import datetime, timezone
import dis
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import time

PRODUCT_SHA = "5d5d7e8f215717cf9b7012d3b357eaf03a1400c9"
ORIGINAL_MERGE = "e9a23147795b73eea7bb73f3f0b4e416e3e3b6fd"
BRANCH = "refs/heads/codex/icse-reconnect-observation-106760391543"
ORIGINAL = {"run_id": 35732223068, "attempt": 1, "job_id": 106760391543,
            "runner": "Beast", "product_head": PRODUCT_SHA,
            "tested_merge": ORIGINAL_MERGE}
CHANGED = {".github/workflows/nitro-artifact-smoke.yml",
           "scripts/ci_icse_reconnect_diagnostic.py"}
FILES = ("demo/icse/network/run.py", "demo/icse/network/worker.py",
         "catalog/node/client.py", "catalog/federation/control_plane_status.py",
         "requirements.txt", "constraints-phase2.txt")
MAX_BYTES = 2 * 1024 * 1024
MAX_EVENTS = 1000
LABELS = ("voter-a", "voter-b", "voter-c", "reviewer")
EXCEPTIONS = {"TimeoutError", "CancelledError", "RuntimeError", "OSError",
              "ConnectionError", "ConnectionResetError", "ConnectionRefusedError",
              "ConnectionAbortedError", "ConnectionClosed", "ConnectionClosedError",
              "ConnectionClosedOK", "RelayRemoteError", "NodeStateError",
              "FederationOperationError", "FederationValidationError", "ValueError",
              "TypeError", "KeyError", "AssertionError", "WorkerCommandFailure",
              "QuorumUnavailable", "InvalidStatus", "InvalidHandshake"}
# Exact source line checkpoints; prepare refuses source whose expected statements
# do not match. Line callbacks report observation, never substitute execution.
PHASE_LINES = {
    196: ("enter", "websocket_open", "websocket = await connect("),
    209: ("transition", "authentication_challenge_wait", "challenge_raw = await asyncio.wait_for("),
    212: ("exit", "authentication_challenge_wait", "challenge = RelayEnvelope.from_json(challenge_raw)"),
    223: ("enter", "enrollment_send", "await websocket.send("),
    233: ("exit", "enrollment_send", "signed = authentication_message("),
    239: ("enter", "authentication_response_send", "await websocket.send("),
    251: ("exit", "authentication_response_send", "authenticated = False"),
    254: ("enter", "authentication_response_wait", "response_raw = await asyncio.wait_for("),
    257: ("exit", "authentication_response_wait", "response = RelayEnvelope.from_json(response_raw)"),
    285: ("enter", "authentication_failure_socket_close", "await websocket.close()"),
    303: ("exit", "authentication_failure_socket_close", "raise"),
    305: ("milestone", "authentication_complete", "self._websocket = websocket"),
    321: ("enter", "coordinator_status", "coordinator_status = await self.coordinator_status()"),
    322: ("transition", "session_reconcile", "self._reconcile_sessions(coordinator_status)"),
    323: ("exit", "session_reconcile", "for session in self.state.joined_sessions():"),
    324: ("enter", "session_replay", "await self.request_replay(session.session_id)"),
    325: ("exit", "session_replay", "for capability in self.state.advertised_capabilities():"),
    327: ("enter", "capability_reannounce", "await self.announce_capability(capability)"),
    343: ("enter", "post_auth_cancel_teardown", "await self.disconnect(error_code=\"initial-replay-cancelled\")"),
    355: ("enter", "post_auth_failure_teardown", "await self.disconnect(error_code=\"initial-replay-failed\")"),
}
WATCHED = {"connect", "disconnect", "coordinator_status", "_coordinator_status_snapshot",
           "_reconcile_sessions", "request_replay", "_request_replay_pass",
           "coordinator_replay_page", "announce_capability", "request"}
ORIGINAL_PACKAGES = """Flask-Login-0.6.3 Flask-Principal-0.4.0 Flask-SQLAlchemy-3.1.1 Flask-Security-Too-5.8.2 Flask-WTF-1.3.0 Markdown-3.10.3 Werkzeug-3.1.8 argon2-cffi-25.1.0 argon2-cffi-bindings-26.1.0 blinker-1.9.0 certifi-2026.7.22 cffi-2.1.1 charset_normalizer-3.5.1 click-8.5.0 cloudpickle-3.1.2 contourpy-1.4.0 cryptography-43.0.3 cycler-0.12.1 dnspython-2.8.0 duckdb-1.5.5 email-validator-2.3.0 flask-3.1.3 fonttools-4.65.0 greenlet-3.5.6 idna-3.20 itsdangerous-2.2.0 jinja2-3.1.6 joblib-1.6.0 kiwisolver-1.5.1 libpass-1.9.3 markupsafe-3.0.3 matplotlib-3.11.2 narwhals-2.26.0 numpy-2.5.3 packaging-26.3 pandas-3.0.6 pillow-12.3.0 psycopg-3.2.9 psycopg-binary-3.2.9 pyarrow-25.0.1 pycparser-3.0 pyparsing-3.3.3 python-dateutil-2.9.0.post0 requests-2.34.2 scikit-learn-1.9.1 scipy-1.18.1 six-1.17.0 sqlalchemy-2.0.54 threadpoolctl-3.7.0 typing-extensions-4.16.0 tzdata-2026.4 urllib3-2.8.0 websockets-15.0.1 wtforms-3.2.2 pip-26.2.1 ruff-0.16.8"""


def utc():
    return datetime.now(timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args],
                                   text=True, stderr=subprocess.PIPE).strip()


def require(value, code):
    if not value:
        raise RuntimeError(code)


def plain_path(path):
    for ancestor in (path, *path.parents):
        if not ancestor.exists():
            continue
        meta = ancestor.lstat()
        require(not stat.S_ISLNK(meta.st_mode) and not (
            getattr(meta, "st_file_attributes", 0) & 0x400), "linked-path")


def write_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def identity(meta):
    return tuple(getattr(meta, key) for key in
                 ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns"))


def read_stable(path):
    plain_path(path)
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_size <= MAX_BYTES,
            "file-kind-or-size-limit")
    with path.open("rb") as stream:
        require(identity(os.fstat(stream.fileno())) == identity(before), "file-open-race")
        data = stream.read(MAX_BYTES + 1)
        after = os.fstat(stream.fileno())
    require(identity(before) == identity(after) == identity(path.lstat())
            and len(data) == before.st_size, "file-read-race")
    return data


def environment(product, output, *, verify_product=True):
    env = os.environ
    require(env.get("GITHUB_REPOSITORY") == "Nettking/msh"
            and env.get("GITHUB_ACTOR") == "Nettking"
            and env.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and env.get("GITHUB_REF") == BRANCH
            and env.get("GITHUB_RUN_ATTEMPT") == "1"
            and env.get("RUNNER_NAME") == "Beast"
            and env.get("RUNNER_OS") == "Windows", "workflow-boundary")
    run = env["GITHUB_RUN_ID"]
    require(run.isdigit(), "run-id")
    workspace = Path(env["GITHUB_WORKSPACE"]).resolve()
    require(product == workspace / "product", "product-location")
    require(output == Path(env["RUNNER_TEMP"]).resolve() /
            f"fcp-icse-reconnect-{run}-1-Windows", "output-location")
    plain_path(product)
    plain_path(output)
    require(git(workspace, "rev-parse", "HEAD") == env["GITHUB_SHA"], "diagnostic-head")
    require(git(workspace, "rev-parse", "HEAD^") == PRODUCT_SHA, "diagnostic-parent")
    require(set(git(workspace, "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD").splitlines())
            == CHANGED, "diagnostic-change-scope")
    require(not git(workspace, "status", "--porcelain", "--untracked-files=no"), "diagnostic-tracked-dirty")
    if verify_product:
        require(git(product, "rev-parse", "HEAD") == PRODUCT_SHA, "product-head")
        require(not git(product, "status", "--porcelain", "--untracked-files=all"), "product-dirty")
    return {"run_id": run, "attempt": 1, "source_sha": env["GITHUB_SHA"],
            "runner": env["RUNNER_NAME"], "product_sha": PRODUCT_SHA}


def packages():
    from importlib import metadata
    normalize = lambda value: re.sub(r"[-_.]+", "-", value).lower()
    original = {normalize(name): version for name, version in
                (item.rsplit("-", 1) for item in ORIGINAL_PACKAGES.split())}
    installed = {}
    for distribution in metadata.distributions():
        name, version = distribution.metadata.get("Name", ""), distribution.version
        if re.fullmatch(r"[A-Za-z0-9_.+-]{1,120}", name) and re.fullmatch(r"[A-Za-z0-9_.+!-]{1,120}", version):
            installed[normalize(name)] = version
    differences = {name: {"original": value, "observed": installed.get(name)}
                   for name, value in original.items() if installed.get(name) != value}
    observed_python = ".".join(str(value) for value in sys.version_info[:3])
    return {"python_original": "3.12.10", "python_observed": observed_python,
            "python_differs": observed_python != "3.12.10",
            "original_native_log_sha256": "0cb5b0028db905bfce79cb3448f43f6ced0799101ae952f25e49ddf1c8f3c341",
            "observed_distributions": installed, "original_logged_distributions": original,
            "version_differences": differences,
            "limit": "Original pip commands retained; the log is not a complete original environment lock."}


def retention_helpers():
    # Existing trusted helper supplies the unchanged capacity floor and Windows
    # per-run private ACL. Importing it performs no archive/network operation.
    workspace = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    sys.path.insert(0, str(workspace))
    try:
        from scripts.artifact_archive import require_local_space
        from scripts.artifact_archive_ci import private_directory
        return require_local_space, private_directory
    finally:
        sys.path.pop(0)


def prepare(product, output):
    binding = environment(product, output)
    # Exclusive output creation is the per-run one-shot marker; no exist_ok/reset.
    require_local_space, private_directory = retention_helpers()
    admission = require_local_space(output, 40 * 1024 * 1024)
    private_directory(output)
    for name in ("observer", "phases", "retained"):
        (output / name).mkdir(mode=0o700)
    client_lines = (product / "catalog/node/client.py").read_text(encoding="utf-8").splitlines()
    for number, (_, _, expected) in PHASE_LINES.items():
        require(client_lines[number - 1].strip() == expected, "phase-source-mismatch")
    python_paths = git(product, "ls-files", "*.py").splitlines()
    require(python_paths and all(not path.startswith("/") and ".." not in Path(path).parts
                                 for path in python_paths), "tracked-paths")
    metadata = {"schema": "fcp.icse-reconnect-observation.v1", "prepared_at": utc(),
                "original": ORIGINAL, "diagnostic": binding,
                "product_files_sha256": {name: digest((product / name).read_bytes()) for name in FILES},
                "tracked_python_files": python_paths, "dependencies": packages(),
                "local_capacity_admission": admission,
                "observation_scope": "Only voter-c; initial and successor connect commands; no replacement of product callables.",
                "limits": {"phase_bytes": MAX_BYTES, "phase_events": MAX_EVENTS,
                           "stderr_bytes_per_file": MAX_BYTES, "traceback_frames_per_exception": 64},
                "product_requirements_unchanged": True, "ordinary_ci_qualification": False,
                "physical_acceptance": "NOT_EVALUATED"}
    write_json(output / "prepare.json", metadata)
    observer = output / "observer"
    shutil.copyfile(Path(__file__), observer / "icse_reconnect_observer.py")
    (observer / "sitecustomize.py").write_text(
        "from icse_reconnect_observer import install_observer\ninstall_observer()\n", encoding="utf-8")
    write_json(output / "observer-manifest.json", {
        name: digest((observer / name).read_bytes())
        for name in ("sitecustomize.py", "icse_reconnect_observer.py")})
    print(json.dumps({"prepared": True, "diagnostic": binding, "product_sha": PRODUCT_SHA,
                      "dependency_differences": metadata["dependencies"]["version_differences"]}, sort_keys=True))


def safe_class(cls):
    return cls.__name__ if cls.__name__ in EXCEPTIONS else "unlisted-exception"


def safe_frame(filename, line, function, product, tracked):
    path = filename.replace("\\", "/")
    prefix = product.as_posix().rstrip("/") + "/"
    relative = path[len(prefix):] if path.casefold().startswith(prefix.casefold()) else None
    if relative not in tracked:
        relative = None
        for family in ("asyncio", "websockets"):
            token = "/" + family + "/"
            if token in path:
                suffix = path.rsplit(token, 1)[1]
                if re.fullmatch(r"[A-Za-z0-9_/]+\.py", suffix) and ".." not in suffix:
                    relative = family + "/" + suffix
                    break
        if relative is None and path.rsplit("/", 1)[-1] in {"ssl.py", "socket.py", "contextlib.py"}:
            relative = "stdlib/" + path.rsplit("/", 1)[-1]
    safe_function = (function if relative and len(function) <= 120
                     and re.fullmatch(r"[A-Za-z_<>][A-Za-z0-9_.<>]*", function) else "withheld")
    return {"file": relative or "unlisted-module", "line": int(line), "function": safe_function}


def exception_chain(error, product, tracked):
    chain, seen = [], set()
    while error is not None and id(error) not in seen and len(chain) < 8:
        seen.add(id(error))
        frames, trace = [], error.__traceback__
        while trace is not None and len(frames) < 64:
            code = trace.tb_frame.f_code
            frames.append(safe_frame(code.co_filename, trace.tb_lineno, code.co_name, product, tracked))
            trace = trace.tb_next
        cause = error.__cause__
        context = error.__context__ if cause is None and not error.__suppress_context__ else None
        chain.append({"exception_class": safe_class(type(error)), "frames": frames,
                      "frames_truncated": trace is not None,
                      "next_link": "cause" if cause is not None else "context" if context is not None else None})
        error = cause if cause is not None else context
    return {"exceptions_outer_first": chain, "chain_truncated": error is not None}


def memory_state(worker, client):
    # Direct instance dictionaries only. Do not call .state/.status/.ready,
    # inspect DBs, acquire product locks, or serialize arbitrary values.
    worker_values = object.__getattribute__(worker, "__dict__") if worker is not None else {}
    if client is None:
        client = worker_values.get("client")
    values = object.__getattribute__(client, "__dict__") if client is not None else {}
    result = {"client_present": client is not None,
              "websocket_present": values.get("_websocket") is not None,
              "receiver_task_present": values.get("_receiver_task") is not None,
              "heartbeat_task_present": values.get("_heartbeat_task") is not None}
    for name in ("_pending", "_replay_tasks", "_gap_replay_tasks", "_pending_message_responses"):
        value = values.get(name)
        if type(value) is dict:
            result[name.removeprefix("_") + "_count"] = len(value)
    runtime = worker_values.get("runtime")
    if runtime is not None:
        node = object.__getattribute__(runtime, "__dict__").get("node")
        if node is not None:
            role = object.__getattribute__(node, "__dict__").get("role")
            if role in {"LEADER", "FOLLOWER", "CANDIDATE"}:
                result["in_memory_replica_role"] = role
    return result


class Observer:
    def __init__(self, product, output, metadata):
        self.product, self.output = product, output
        self.tracked = set(metadata["tracked_python_files"])
        self.frames, self.command_id, self.sequence = {}, 0, 0
        self.worker = None
        self.bytes_written = 0
        self.disabled = False
        self.codes = {}
        self.start = time.perf_counter_ns()
        self.writer = (output / "phases" / f"voter-c-{os.getpid()}.jsonl").open("x", encoding="utf-8", buffering=1)
        self.emit("observer_installed", diagnostic_sha=metadata["diagnostic"]["source_sha"], product_sha=PRODUCT_SHA)

    def emit(self, event, **fields):
        if self.disabled:
            return
        record = {"sequence": self.sequence, "at": utc(), "elapsed_ns": time.perf_counter_ns() - self.start,
                  "pid": os.getpid(), "worker": "voter-c",
                  "connect_ordinal": self.command_id if any(item["name"] == "command" for item in self.frames.values()) else None,
                  "event": event, **fields}
        encoded = json.dumps(record, sort_keys=True) + "\n"
        if self.sequence >= MAX_EVENTS or self.bytes_written + len(encoded.encode("utf-8")) > MAX_BYTES - 1024:
            self.writer.write(json.dumps({"event": "observer_limit_reached", "at": utc(), "worker": "voter-c"}) + "\n")
            self.writer.flush()
            self.disabled = True
            sys.settrace(None)
            return
        self.writer.write(encoded)
        self.writer.flush()
        self.sequence += 1
        self.bytes_written += len(encoded.encode("utf-8"))

    def close(self):
        try:
            self.emit("observer_process_exit")
            self.writer.close()
        except Exception:
            pass

    def trace(self, frame, event, argument):
        try:
            return self._trace(frame, event, argument)
        except Exception:
            # Observation failure must not replace or swallow a product result.
            try:
                self.emit("observer_internal_error")
            except Exception:
                pass
            self.disabled = True
            sys.settrace(None)
            return None

    def _trace(self, frame, event, argument):
        code = frame.f_code
        selected = self.codes.get(code)
        if selected is None:
            filename = code.co_filename.replace("\\", "/").casefold()
            base = self.product.as_posix().casefold().rstrip("/") + "/"
            relative = filename[len(base):] if filename.startswith(base) else ""
            selected = ((relative == "demo/icse/network/worker.py" and code.co_name == "command")
                        or (relative == "catalog/node/client.py" and code.co_name in WATCHED)
                        or (relative == "catalog/federation/control_plane_status.py" and code.co_name == "status_document"))
            self.codes[code] = selected
        if not selected:
            return None
        name, fid = code.co_name, id(frame)
        if name == "status_document":
            frame.f_trace_lines = False
            if event == "return" and type(argument) is dict:
                safe = {}
                for key in ("consensus_term", "commit_index", "last_applied", "fencing_epoch"):
                    value = argument.get(key)
                    if type(value) is int:
                        safe[key] = value
                if argument.get("role") in {"LEADER", "FOLLOWER", "CANDIDATE"}:
                    safe["role"] = argument["role"]
                if type(argument.get("ready")) is bool:
                    safe["ready"] = argument["ready"]
                self.emit("existing_product_status_return", state=safe)
            return self.trace
        if name == "command":
            value = frame.f_locals.get("value")
            if type(value) is not dict or value.get("operation") != "connect":
                return None
        if event == "call" and fid not in self.frames:
            if name == "command":
                self.command_id += 1
                self.worker = frame.f_locals.get("self")
            # An unrelated asyncio task may run while connect is suspended.
            # Only actual tracked call ancestry admits a NEW client frame.
            # Existing frames remain tracked when their coroutine resumes.
            if name != "command":
                ancestor = frame.f_back
                while ancestor is not None and id(ancestor) not in self.frames:
                    ancestor = ancestor.f_back
                if ancestor is None:
                    return None
            self.frames[fid] = {"name": name, "phase": None, "exception": None,
                                "started_ns": time.perf_counter_ns()}
            message_type = frame.f_locals.get("message_type") if name == "request" else None
            self.emit("call_enter", function=name,
                      request_kind=message_type if message_type in {"status.get", "event.replay", "capability.announce"} else None,
                      frame=safe_frame(code.co_filename, frame.f_lineno, name, self.product, self.tracked),
                      state=memory_state(self.worker, frame.f_locals.get("self") if name != "command" else None))
        if fid not in self.frames:
            return None
        info = self.frames[fid]
        frame.f_trace_lines = name == "connect"
        if event == "line" and name == "connect" and frame.f_lineno in PHASE_LINES:
            # Loop-back lines prove a preceding awaited iteration returned.
            if ((frame.f_lineno == 323 and info["phase"] == "session_replay")
                    or (frame.f_lineno == 325 and info["phase"] == "capability_reannounce")):
                self.emit("phase_exit", phase=info["phase"], function=name, line=frame.f_lineno)
                info["phase"] = None
            kind, phase, _ = PHASE_LINES[frame.f_lineno]
            if kind == "transition" and info["phase"] is not None:
                self.emit("phase_exit", phase=info["phase"], function=name, line=frame.f_lineno)
            if kind in {"enter", "transition"}:
                if info["phase"] != phase:
                    info["phase"] = phase
                    self.emit("phase_enter", phase=phase, function=name, line=frame.f_lineno,
                              state=memory_state(self.worker, frame.f_locals.get("self")))
            elif kind == "exit" and info["phase"] == phase:
                self.emit("phase_exit", phase=phase, function=name, line=frame.f_lineno)
                info["phase"] = None
            elif kind == "milestone":
                self.emit("milestone", phase=phase, function=name, line=frame.f_lineno)
        elif event == "exception":
            cls, error, _ = argument
            if cls not in {StopIteration, StopAsyncIteration, GeneratorExit}:
                info["exception"] = safe_class(cls)
                self.emit("exception", function=name, phase=info["phase"], line=frame.f_lineno,
                          exception_class=safe_class(cls), timeout=issubclass(cls, TimeoutError),
                          traceback=exception_chain(error, self.product, self.tracked),
                          state=memory_state(self.worker, frame.f_locals.get("self") if name != "command" else None))
        elif event == "return":
            operation = dis.opname[code.co_code[frame.f_lasti]] if frame.f_lasti >= 0 else ""
            if operation in {"YIELD_VALUE", "YIELD_FROM", "SEND"}:
                return self.trace
            normal = operation in {"RETURN_VALUE", "RETURN_CONST"}
            if normal and info["phase"] is not None:
                self.emit("phase_exit", phase=info["phase"], function=name, line=frame.f_lineno)
            self.emit("call_exit", function=name, phase=info["phase"], completed_normally=normal,
                      last_observed_exception=info["exception"], duration_ns=time.perf_counter_ns() - info["started_ns"],
                      state=memory_state(self.worker, frame.f_locals.get("self") if name != "command" else None))
            del self.frames[fid]
        return self.trace


def install_observer():
    # sitecustomize executes in driver and children. Only the exact new voter-c
    # worker installs a callback; no import of product modules is introduced.
    if "--config" not in sys.argv:
        return
    try:
        output = Path(os.environ["FCP_ICSE_DIAGNOSTIC_ROOT"]).resolve()
        product = Path(os.environ["FCP_ICSE_PRODUCT_ROOT"]).resolve()
        config = Path(sys.argv[sys.argv.index("--config") + 1]).resolve()
        if config != output / "network/private-state/voter-c/worker.json":
            return
        metadata = json.loads(read_stable(output / "prepare.json"))
        require(metadata["diagnostic"]["source_sha"] == os.environ["FCP_ICSE_DIAGNOSTIC_SHA"]
                and metadata["diagnostic"]["product_sha"] == PRODUCT_SHA
                and metadata["diagnostic"]["attempt"] == 1, "observer-binding")
        require(sys.gettrace() is None, "existing-tracer")
        observer = Observer(product, output, metadata)
        atexit.register(observer.close)
        sys.settrace(observer.trace)
    except Exception:
        # Static safe marker only; never sitecustomize's default raw traceback.
        print("ICSE_DIAGNOSTIC_OBSERVER_UNAVAILABLE", file=sys.stderr, flush=True)


def sanitized_stderr(raw, product, tracked):
    records = []
    for line in raw.decode("utf-8", errors="replace").splitlines():
        if line == "Traceback (most recent call last):":
            records.append({"kind": "traceback_start"})
        elif line == "The above exception was the direct cause of the following exception:":
            records.append({"kind": "chain", "link": "cause"})
        elif line == "During handling of the above exception, another exception occurred:":
            records.append({"kind": "chain", "link": "context"})
        else:
            match = re.fullmatch(r'\s*File "([^"]+)", line ([0-9]+), in ([A-Za-z_<>][A-Za-z0-9_.<>]*)', line)
            if match:
                records.append({"kind": "frame", **safe_frame(match[1], match[2], match[3], product, tracked)})
            else:
                match = re.match(r"^(?:[A-Za-z_][A-Za-z0-9_]*\.)*([A-Za-z_][A-Za-z0-9_]*)(?::|$)", line)
                if match and match[1] in EXCEPTIONS:
                    records.append({"kind": "exception", "class": match[1]})
        if len(records) > 1000:
            return {"status": "sanitized_trace_limit", "records": records[:1000], "truncated": True}
    return {"status": "sanitized", "records": records, "truncated": False}


def collect(product, output):
    binding = environment(product, output, verify_product=False)
    metadata = json.loads(read_stable(output / "prepare.json"))
    require(metadata["diagnostic"] == binding, "collect-binding")
    # A post-execution source violation is evidence, not a reason to discard the
    # failure trace. Preserve it and fail the diagnostic collection verdict.
    try:
        source_unchanged = (git(product, "rev-parse", "HEAD") == PRODUCT_SHA
            and not git(product, "status", "--porcelain", "--untracked-files=all")
            and all(digest((product / name).read_bytes()) == expected
                    for name, expected in metadata["product_files_sha256"].items()))
    except Exception:
        source_unchanged = False
    retained = output / "retained"
    require(retained.is_dir() and not any(retained.iterdir()), "retention-output-not-empty")
    observation = {"schema": "fcp.icse-reconnect-observation-result.v1", "collected_at": utc(),
                   "original": ORIGINAL, "diagnostic": binding, "product_source_unchanged": source_unchanged,
                   "network_step_outcome": os.environ.get("FCP_ICSE_NETWORK_OUTCOME")
                       if os.environ.get("FCP_ICSE_NETWORK_OUTCOME") in {"success", "failure", "skipped", "cancelled"} else "unavailable",
                   "components_step_outcome": os.environ.get("FCP_ICSE_COMPONENTS_OUTCOME")
                       if os.environ.get("FCP_ICSE_COMPONENTS_OUTCOME") in {"success", "failure", "skipped", "cancelled"} else "unavailable",
                   "ordinary_icse_result": "NOT_REPLACED", "physical_acceptance": "NOT_EVALUATED",
                   "files": {}, "limitations": [
                       "Tracing adds timing overhead; successful execution is non-reproduction, not a fix or environmental proof.",
                       "Only voter-c has phase tracing; other workers retain sanitized failure traceback metadata only.",
                       "New client frames require tracked call ancestry. Detached asyncio task phases are not independently attributed; propagated exception chains are still captured at the awaiting connect path.",
                       "State snapshots use existing product status returns or direct in-memory presence/count/role fields; no extra RPC or database read.",
                       "Raw stderr is also copied to a restricted local evidence directory; private configs, keys and databases are not copied. No raw stderr/private state is uploaded."]}
    write_json(retained / "prepare.json", metadata)
    write_json(retained / "observer-manifest.json", json.loads(read_stable(output / "observer-manifest.json")))
    (retained / "public").mkdir()
    for name in ("summary.json", "events.jsonl", "operator-report.html"):
        target = output / "network/public" / name
        try:
            data = read_stable(target)
            (retained / "public" / name).write_bytes(data)
            observation["files"]["public/" + name] = {"status": "retained", "bytes": len(data), "sha256": digest(data)}
        except FileNotFoundError:
            observation["files"]["public/" + name] = {"status": "missing"}
        except Exception:
            observation["files"]["public/" + name] = {"status": "refused"}
    tracked = set(metadata["tracked_python_files"])
    stderr_records = {}
    paths = [(label, output / "network/private-state" / label / "worker.stderr.log") for label in LABELS]
    paths.append(("driver", output / "network/private-state/driver-failure.log"))
    for label, path in paths:
        try:
            data = read_stable(path)
            stderr_records[label] = {"original_bytes": len(data), "original_sha256": digest(data),
                                     **sanitized_stderr(data, product, tracked)}
        except FileNotFoundError:
            stderr_records[label] = {"status": "not_present"}
        except Exception:
            stderr_records[label] = {"status": "read_refused"}
    write_json(retained / "sanitized-tracebacks.json", stderr_records)
    (retained / "phases").mkdir()
    observation["phase_observer_installed"] = False
    observation["phase_observer_complete"] = False
    observation["phase_read_errors"] = []
    phase_events = []
    try:
        # At most four files inspected; malformed/partial optional phase output
        # must never prevent fixed original stderr preservation below.
        from itertools import islice
        phase_paths = list(islice((output / "phases").iterdir(), 5))
        if len(phase_paths) > 4:
            observation["phase_read_errors"].append("unexpected-file-count")
        for path in phase_paths[:4]:
            if not re.fullmatch(r"voter-c-[0-9]+\.jsonl", path.name):
                observation["phase_read_errors"].append("unexpected-file-name")
                continue
            try:
                data = read_stable(path)
                events = []
                for line in data.decode("utf-8").splitlines():
                    if not line.strip():
                        continue
                    try:
                        value = json.loads(line)
                    except ValueError:
                        observation["phase_read_errors"].append("partial-or-malformed-phase-line")
                        break
                    if not isinstance(value, dict) or len(events) >= MAX_EVENTS + 1:
                        observation["phase_read_errors"].append("invalid-phase-record-or-limit")
                        break
                    events.append(value)
                phase_events.extend(events)
                # A valid prefix is derived evidence with its original byte
                # hash retained; the original phase log is never overwritten.
                encoded = ("".join(json.dumps(value, sort_keys=True) + "\n" for value in events)).encode("utf-8")
                (retained / "phases" / path.name).write_bytes(encoded)
                observation["files"]["phases/" + path.name] = {"status": "sanitized-json-records",
                    "original_bytes": len(data), "original_sha256": digest(data),
                    "bytes": len(encoded), "sha256": digest(encoded), "records": len(events)}
            except Exception:
                observation["phase_read_errors"].append("phase-file-read-refused")
    except Exception:
        observation["phase_read_errors"].append("phase-directory-unavailable")
    kinds = {event.get("event") for event in phase_events}
    observation["phase_observer_installed"] = "observer_installed" in kinds
    observation["observer_process_exit_marker"] = "observer_process_exit" in kinds
    successor_events = [event for event in phase_events if event.get("connect_ordinal") == 2]
    command_entries = [event for event in successor_events
                       if event.get("event") == "call_enter" and event.get("function") == "command"]
    command_exits = [event for event in successor_events
                     if event.get("event") == "call_exit" and event.get("function") == "command"]
    observation["successor_connect_entry_observed"] = len(command_entries) == 1
    observation["successor_connect_exit_observed"] = len(command_exits) == 1
    observation["successor_connect_completed_normally"] = (command_exits[0].get("completed_normally")
                                                           if len(command_exits) == 1 else None)
    observation["successor_phase_boundaries"] = sorted({event["phase"] for event in successor_events
        if event.get("event") == "phase_enter" and isinstance(event.get("phase"), str)})
    observation["successor_command_exception_classes"] = sorted({event["exception_class"] for event in successor_events
        if event.get("event") == "exception" and event.get("function") == "command"
        and isinstance(event.get("exception_class"), str)})
    observation["successor_timeout_observed"] = any(event.get("timeout") is True for event in successor_events)
    # The unchanged driver deliberately terminates non-successor voters before
    # the minority assertion. atexit is optional; completed ordinal-2 coverage is
    # the relevant observation boundary, independent of network PASS or FAIL.
    observation["phase_observer_complete"] = (observation["phase_observer_installed"]
        and observation["successor_connect_entry_observed"]
        and observation["successor_connect_exit_observed"]
        and not observation["phase_read_errors"]
        and not kinds.intersection({"observer_limit_reached", "observer_internal_error"}))
    observation["connect_ordinals_seen"] = sorted({event["connect_ordinal"] for event in phase_events
                                                   if type(event.get("connect_ordinal")) is int and event["connect_ordinal"] > 0})
    write_json(retained / "observation.json", observation)
    # Retain before Nitro transport, outside Actions' RUNNER_TEMP cleanup. Only
    # fixed stderr files, never keys/configs/SQLite, enter a private local folder.
    # The uploaded retained/ directory still contains sanitized data only.
    local_receipt = retain_locally(output, retained, paths, binding)
    write_json(retained / "local-retention.json", local_receipt)
    print(json.dumps({"collected": True, "diagnostic": binding,
                      "phase_observer_installed": observation["phase_observer_installed"],
                      "phase_observer_complete": observation["phase_observer_complete"],
                      "successor_connect_completed_normally": observation["successor_connect_completed_normally"],
                      "successor_phase_boundaries": observation["successor_phase_boundaries"],
                      "connect_ordinals_seen": observation["connect_ordinals_seen"]}, sort_keys=True))
    return 0 if source_unchanged and observation["phase_observer_complete"] else 1


def retain_locally(output, retained, stderr_paths, binding):
    require_local_space, private_directory = retention_helpers()
    parent = Path(os.environ["RUNNER_TEMP"]).resolve().parent / "fcp-icse-diagnostic-evidence"
    plain_path(parent)
    admission = require_local_space(parent, 40 * 1024 * 1024)
    if not parent.exists():
        private_directory(parent)
    destination = parent / f"{binding['run_id']}-1-{binding['source_sha'][:12]}"
    private_directory(destination)
    (destination / "sanitized").mkdir(mode=0o700)
    (destination / "private-stderr-do-not-upload").mkdir(mode=0o700)
    inventory = []
    fixed = ["prepare.json", "observer-manifest.json", "sanitized-tracebacks.json", "observation.json",
             "public/summary.json", "public/events.jsonl", "public/operator-report.html"]
    fixed += ["phases/" + item.name for item in (retained / "phases").iterdir()]
    for name in fixed:
        source = retained / name
        if not source.exists():
            continue
        data = read_stable(source)
        target = destination / "sanitized" / name
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with target.open("xb") as stream:
            stream.write(data)
        inventory.append({"scope": "sanitized", "file": name, "bytes": len(data), "sha256": digest(data)})
    for label, source in stderr_paths:
        try:
            data = read_stable(source)
            target = destination / "private-stderr-do-not-upload" / f"{label}.stderr.log"
            with target.open("xb") as stream:
                stream.write(data)
            inventory.append({"scope": "private-local-only", "file": f"{label}.stderr.log",
                              "bytes": len(data), "sha256": digest(data)})
        except FileNotFoundError:
            inventory.append({"scope": "private-local-only", "file": f"{label}.stderr.log", "status": "not-present"})
        except Exception:
            inventory.append({"scope": "private-local-only", "file": f"{label}.stderr.log", "status": "read-refused"})
    receipt = {"schema": "fcp.icse-diagnostic-local-retention.v1", "preserved_at": utc(),
               "diagnostic": binding, "local_directory": str(destination), "files": inventory,
               "local_capacity_admission": admission, "archive_complete": False,
               "private_stderr_uploaded": False, "private_configs_keys_databases_copied": False,
               "qualification_status": "NOT_EVALUATED"}
    write_json(destination / "LOCAL_RETENTION.json", receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("prepare", "collect"))
    parser.add_argument("--product-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    try:
        product, output = args.product_root.resolve(), args.output_root.resolve()
        if args.operation == "prepare":
            prepare(product, output)
            return 0
        return collect(product, output)
    except Exception as error:
        # Fixed type only. No filesystem error text, raw traceback or values.
        print(json.dumps({"diagnostic_operation": args.operation, "status": "REFUSED_OR_UNAVAILABLE",
                          "error_class": safe_class(type(error))}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
