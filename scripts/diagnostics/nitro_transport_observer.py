"""One-shot production transport observation; no tests, recovery or SSH changes.

Use only in the reviewed workflow_dispatch diagnostic on the existing Beast
Windows runner. Raw OpenSSH diagnostics stay private outside RUNNER_TEMP;
portable events contain only explicitly selected stages and metadata.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

from scripts import artifact_archive as archive
from scripts import artifact_archive_ci as ci


BASE_SHA = "2722db78b6621fa27f6f533e49692a219de2e65c"
ENDPOINT = "fcp-archive@nitro.tail4ccd2b.ts.net"
LOCAL_EXTRA_BOUND = 32 * 1024**2  # Four tiny exchanges plus private debug/metadata.
STAGES = (
    ("connection_established", "Connection established."),
    ("ssh_remote_banner", "Remote protocol version "),
    ("kexinit_sent", "SSH2_MSG_KEXINIT sent"),
    ("kexinit_received", "SSH2_MSG_KEXINIT received"),
    ("host_key_verified", "is known and matches the "),
    ("public_key_offered", "Offering public key:"),
    ("public_key_accepted", "Server accepts key:"),
    ("authentication_succeeded", "Authenticated to "),
    ("session_channel_created", "new session [client-session]"),
    ("session_channel_opened", "open confirm rwindow"),
    ("session_entered", "Entering interactive session."),
    ("forced_protocol_requested", "Sending command: fcp-artifact-v1"),
    ("exec_requested", "request exec confirm 1"),
    ("exec_accepted", "exec request accepted on channel"),
    ("remote_stderr_observed", "rcvd ext data"),
    ("remote_eof", "rcvd eof"),
    ("exit_status", "Exit status "),
    ("connect_timeout", "Connection timed out"),
    ("server_timeout", "Timeout, server "),
    ("connection_closed", "Connection closed"),
    ("connection_reset", "Connection reset"),
    ("authentication_rejected", "Permission denied"),
    ("host_key_rejected", "Host key verification failed"),
)


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def source_text(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def main() -> int:
    env = os.environ
    event = json.loads(Path(env["GITHUB_EVENT_PATH"]).read_bytes())
    ci.trusted_event(env, event)
    if (
        env["GITHUB_EVENT_NAME"] != "workflow_dispatch"
        or env.get("GITHUB_ACTOR") != "Nettking"
        or env.get("RUNNER_NAME") != "Beast"
        or env.get("RUNNER_OS") != "Windows"
        or env.get("FCP_ARCHIVE_HOST") != ENDPOINT
    ):
        raise ValueError("Diagnostic must use reviewed owner dispatch on Beast Windows")
    for name in ("scripts/artifact_archive.py", "scripts/artifact_archive_ci.py"):
        if source_text("rev-parse", f"HEAD:{name}") != source_text("rev-parse", f"{BASE_SHA}:{name}"):
            raise ValueError("Archive implementation differs from the declared base source")
    tested = source_text("rev-parse", "HEAD")
    parent = Path(env["RUNNER_TEMP"]).absolute().parent / "fcp-archive-diagnostics"
    if any(path.is_symlink() for path in (parent, *parent.parents)):
        raise ValueError("Diagnostic directory must not traverse symlinks")
    admission = archive.require_local_space(parent, LOCAL_EXTRA_BOUND)
    if not parent.exists():
        ci.private_directory(parent)
    root = parent / (
        f"{env['GITHUB_RUN_ID']}-{env['GITHUB_RUN_ATTEMPT']}-{uuid.uuid4().hex}"
    )
    ci.private_directory(root)
    start = time.monotonic()
    event_lock = threading.Lock()
    events = root / "events.jsonl"

    def emit(kind: str, **fields: object) -> None:
        row = dict(
            utc=utc(), monotonic_seconds=time.monotonic(),
            elapsed_seconds=round(time.monotonic() - start, 6), kind=kind, **fields,
        )
        with event_lock, events.open("ab") as stream:
            stream.write(archive.canonical(row))
            stream.flush()
            os.fsync(stream.fileno())
            # The identical allowlisted event must remain retrievable from
            # the native job log even when Nitro and direct runner access fail.
            print(archive.canonical(row).decode().rstrip(), flush=True)

    def save(name: str, value: object) -> None:
        with (root / name).open("xb") as stream:
            stream.write(archive.canonical(value))
            stream.flush()
            os.fsync(stream.fileno())

    identity = {
        "diagnostic_only": True, "archive_complete": False,
        "qualification_status": "NOT_EVALUATED", "started_at": utc(),
        "tested_sha": tested, "base_source": BASE_SHA,
        "repo": env["GITHUB_REPOSITORY"], "run_id": env["GITHUB_RUN_ID"],
        "run_attempt": int(env["GITHUB_RUN_ATTEMPT"]), "job": env["GITHUB_JOB"],
        "runner": env["RUNNER_NAME"], "workflow_ref": env["GITHUB_WORKFLOW_REF"],
        "workflow_sha": env["GITHUB_WORKFLOW_SHA"],
        "wrapper_sha256": archive.sha_file(__file__),
        "archive_tool_sha256": archive.sha_file(archive.__file__),
        "ci_adapter_sha256": archive.sha_file(ci.__file__),
        "local_capacity_admission": admission,
        "clock_note": "SSH stage UTC records observation time, not remote event time",
        "raw_debug_policy": "Private local files; do not upload raw logs automatically",
    }
    save("identity.json", identity)
    emit("diagnostic_started", endpoint=ENDPOINT)
    print(f"Private diagnostic evidence retained outside runner cleanup: {root}", flush=True)
    with Path(env["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as stream:
        stream.write(f"Nitro transport diagnostic STARTED; incomplete until result.json exists.\nLocal evidence: `{root}`\n")

    original_ssh = archive.ssh_command
    original_exchange = archive.exchange
    exchange_number = 0

    def observed_exchange(config, header, output, payload=None, receive=None):
        nonlocal exchange_number
        exchange_number += 1
        number = exchange_number
        operation = header.get("operation")
        if operation not in ("list", "put", "get") or config["host"] != ENDPOINT:
            raise ValueError("Unexpected diagnostic exchange")
        debug = root / f"ssh-{number:02d}-{operation}.private.log"
        debug.touch(exist_ok=False)
        done = threading.Event()
        seen = set()
        offset = 0
        pending = b""

        def stages() -> None:
            nonlocal offset, pending
            while True:
                with debug.open("rb") as stream:
                    stream.seek(offset)
                    block = stream.read(65536)
                    offset += len(block)
                lines = (pending + block).split(b"\n")
                pending = lines.pop()
                for raw in lines:
                    line = raw.decode("utf-8", errors="replace")
                    for stage, marker in STAGES:
                        if marker in line and stage not in seen:
                            seen.add(stage)
                            data = {"exchange": number, "operation": operation, "stage": stage}
                            if stage == "exit_status":
                                code = re.search(r"Exit status (\d+)", line)
                                if code:
                                    data["exit_code"] = int(code.group(1))
                            emit("ssh_stage_observed", **data)
                if done.is_set() and not block:
                    break
                done.wait(0.5)

        watcher = threading.Thread(target=stages, daemon=True)

        def observed_ssh(current):
            command = original_ssh(current)
            # Only OpenSSH logging changes. Key, pin, destination and all
            # original authentication, keepalive and transfer deadlines remain.
            return [command[0], "-vv", "-E", str(debug), *command[1:]]

        emit("exchange_enter", exchange=number, operation=operation)
        began = time.monotonic()
        archive.ssh_command = observed_ssh
        watcher.start()
        outcome = "failure"
        try:
            result = original_exchange(config, header, output, payload, receive)
            outcome = "success"
            return result
        except Exception as exc:
            emit("exchange_error", exchange=number, operation=operation, exception_type=type(exc).__name__)
            raise
        finally:
            archive.ssh_command = original_ssh
            done.set()
            watcher.join(timeout=2)
            emit(
                "exchange_exit", exchange=number, operation=operation, outcome=outcome,
                local_duration_seconds=round(time.monotonic() - began, 6),
                debug_bytes=debug.stat().st_size, observed_stages=sorted(seen),
                stage_observer_complete=not watcher.is_alive(),
            )

    def preflight() -> None:
        hostname = ENDPOINT.split("@", 1)[1]
        tailscale = shutil.which("tailscale")
        if tailscale is None:
            candidate = Path(env.get("ProgramFiles", "C:/Program Files")) / "Tailscale/tailscale.exe"
            tailscale = str(candidate) if candidate.is_file() else None
        if tailscale:
            try:
                result = subprocess.run([tailscale, "status", "--json"], capture_output=True, timeout=10, check=False)
                if result.returncode == 0:
                    status = json.loads(result.stdout)
                    peers = [peer for peer in status.get("Peer", {}).values() if str(peer.get("DNSName", "")).rstrip(".") == hostname]
                    selected = [{key: peer.get(key) for key in ("DNSName", "TailscaleIPs", "Online", "Active", "Relay", "CurAddr", "LastSeen", "LastHandshake", "RxBytes", "TxBytes")} for peer in peers]
                    emit("tailscale_status", backend_state=status.get("BackendState"), nitro=selected)
                else:
                    emit("tailscale_status", returncode=result.returncode)
            except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
                emit("tailscale_status", exception_type=type(exc).__name__)
        else:
            emit("tailscale_status", available=False)
        # A subprocess gives DNS its own small observation bound; no change to
        # the archive client's actual resolver/SSH behavior.
        code = "import json,socket,sys; print(json.dumps(sorted(set(x[4][0] for x in socket.getaddrinfo(sys.argv[1],22,type=socket.SOCK_STREAM)))))"
        began = time.monotonic()
        try:
            result = subprocess.run([sys.executable, "-B", "-c", code, hostname], capture_output=True, timeout=10, check=False)
            if result.returncode:
                emit("dns_observation", returncode=result.returncode)
                return
            addresses = json.loads(result.stdout)
            emit("dns_observation", addresses=addresses, local_duration_seconds=round(time.monotonic()-began, 6))
        except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
            emit("dns_observation", exception_type=type(exc).__name__)
            return
        # One bounded TCP/banner observation; it neither authenticates nor
        # claims forced-command success. No SSH pin decision uses this banner.
        began = time.monotonic()
        try:
            with socket.create_connection((addresses[0], 22), timeout=10) as connection:
                emit("tcp_connected", address=addresses[0], local_duration_seconds=round(time.monotonic()-began, 6))
                connection.settimeout(10)
                banner = connection.recv(512)
                emit("ssh_banner_observation", ssh_identification_seen=banner.startswith(b"SSH-"), bytes_received=len(banner), local_duration_seconds=round(time.monotonic()-began, 6))
        except (OSError, IndexError) as exc:
            emit("tcp_banner_observation", exception_type=type(exc).__name__, errno=getattr(exc, "errno", None), local_duration_seconds=round(time.monotonic()-began, 6))

    result = "failure"
    operation = "preflight"
    archive.exchange = observed_exchange
    try:
        preflight()
        probe_file = root / "transport-synthetic.txt"
        probe_file.write_bytes(b"Synthetic archive transport diagnostic only\n")
        artifact_name = "nitro-post-auth-diagnostic-Beast-Windows"
        env.update({
            "ARCHIVE_NAME": artifact_name, "ARCHIVE_MATRIX": json.dumps({"os": "Windows", "runner": "Beast", "purpose": "post-auth-transport-diagnostic"}),
            "ARCHIVE_JOB_STATUS": "success", "ARCHIVE_MERGE_MULTIPLE": "true",
        })
        for operation in ("probe", "upload", "download"):
            archive.require_local_space(root, LOCAL_EXTRA_BOUND)
            env["ARCHIVE_OPERATION"] = operation
            env["ARCHIVE_PATH"] = str(root / "downloaded") if operation == "download" else str(probe_file)
            env["ARCHIVE_PATTERN"] = artifact_name
            emit("adapter_operation_enter", operation=operation)
            ci.main()
            emit("adapter_operation_success", operation=operation)
        restored = root / "downloaded" / probe_file.name
        if restored.read_bytes() != probe_file.read_bytes():
            raise ValueError("Synthetic archive retrieval content mismatch")
        emit("retrieval_verified", bytes=restored.stat().st_size, sha256=archive.sha_file(restored))
        result = "success"
        return 0
    except Exception as exc:
        emit("diagnostic_error", operation=operation, exception_type=type(exc).__name__)
        if operation == "upload":
            try:
                retained = ci.preserve_failed_inputs(env)
                emit("original_failure_inputs_retained", pending_path=str(retained))
            except Exception as retention_error:
                emit("additional_failure_retention_error", exception_type=type(retention_error).__name__)
        print(f"Nitro transport diagnostic INCOMPLETE at {operation}; original local inputs and private diagnostics retained: {root}", flush=True)
        return 1
    finally:
        archive.exchange = original_exchange
        archive.ssh_command = original_ssh
        emit("diagnostic_finished", outcome=result, final_operation=operation)
        save("result.json", {"outcome": result, "finished_at": utc(), "final_operation": operation, "exchanges": exchange_number, "qualification_status": "NOT_EVALUATED", "evidence_path": str(root)})
        with Path(env["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as stream:
            stream.write(f"Nitro transport diagnostic {result.upper()}; no automatic retry.\nPrivate evidence: `{root}`\n")


if __name__ == "__main__":
    raise SystemExit(main())
