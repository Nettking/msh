"""Single operator job: real isolated Windows product observations, not v1 PASS."""
from __future__ import annotations

import argparse
import base64
import ctypes
import datetime as dt
import hashlib
import html
import http.cookiejar
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from ctypes import wintypes

CANDIDATE = "d4d29b0c1890c63ce3e7b43415a9068a03e302cc"
RESERVE = 12_893_316_031


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    root = Path(parser.parse_args().root).resolve()
    if platform.system() != "Windows" or root.parent != Path(r"C:\fcp-cf7-physical"):
        raise RuntimeError("Explicit isolated actual Windows root required")
    evidence = root / "evidence"
    source, runtime = root / "source", root / "native-state"
    receipts, processes = [], []
    result = {"schema": "fcp.cf7.native-windows-observations.v1", "observed_commit": CANDIDATE,
              "github_run_id": os.environ["GITHUB_RUN_ID"], "started_utc": utc(),
              "physical_browser_model_cnc_claims": False, "full_compose_launcher_observed": False,
              "physical_scenario_pass_claimed": False, "deterministic_gates_separate": True}

    def redact(value):
        value = str(value)
        for private in (str(root), str(Path.home()), os.environ.get("COMPUTERNAME", "")):
            if private:
                value = value.replace(private, "<redacted-local>")
        value = re.sub(r"(?:https?|wss?)://[^\s<>\"']+", "<redacted-endpoint>", value)
        value = re.sub(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)", "<redacted-address>", value)
        value = re.sub(r"(?i)[a-z]:[\\/][^\r\n\t]+", "<redacted-path>", value)
        return value[-100_000:]

    def run(args, *, cwd=source, env=None, timeout=180, name=None):
        started = utc()
        cp = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True,
                            encoding="utf-8", errors="replace", timeout=timeout, stdin=subprocess.DEVNULL)
        receipts.append({"name": name or str(args[1:]), "started_utc": started, "completed_utc": utc(),
                         "returncode": cp.returncode, "output": redact(cp.stdout + cp.stderr)})
        if cp.returncode:
            raise RuntimeError(f"Required local boundary failed: {name or args[1:]}; exit {cp.returncode}")
        return cp.stdout

    def cli(*args):
        raw = run([str(python), "-B", "-m", *args], cwd=runtime, env=env, timeout=90,
                  name="supported native CLI " + " ".join(args))
        return json.loads(next(line for line in reversed(raw.splitlines()) if line.startswith("{")))

    def creation_filetime(process):
        created, exited, kernel, user = (wintypes.FILETIME() for _ in range(4))
        get_times = ctypes.WinDLL("kernel32", use_last_error=True).GetProcessTimes
        get_times.argtypes = [wintypes.HANDLE, *([ctypes.POINTER(wintypes.FILETIME)] * 4)]
        if not get_times(wintypes.HANDLE(int(process._handle)), *map(ctypes.byref, (created, exited, kernel, user))):
            raise ctypes.WinError(ctypes.get_last_error())
        return str((created.dwHighDateTime << 32) | created.dwLowDateTime)

    def start_service():
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        child_env = dict(env, FLASK_RUN_HOST="127.0.0.1", FLASK_RUN_PORT=str(port))
        log = (root / (f"native-private-{len(processes)}.log")).open("wb")
        child = subprocess.Popen([str(python), "-B", "-m", "catalog.flask_app.app"],
                                 cwd=runtime, env=child_env, stdin=subprocess.DEVNULL,
                                 stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
        processes.append((child, log))
        owner = {"pid": child.pid, "creation_filetime": creation_filetime(child), "started_utc": utc()}
        (root / "process-owner.json").write_text(json.dumps(owner), encoding="utf-8")
        result.setdefault("service_processes", []).append(owner)
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise RuntimeError("Owned native Flask process exited before readiness")
            try:
                response = opener.open(f"http://127.0.0.1:{port}/admin/users/bootstrap", timeout=3)
                body = response.read(1_048_577)
                if len(body) > 1_048_576:
                    raise RuntimeError("Response exceeds evidence bound")
                result.setdefault("actual_http_observations", []).append({"utc": utc(), "route": "first-user bootstrap",
                        "http_status": response.status, "body_sha256": hashlib.sha256(body).hexdigest(), "loopback_owned_process": True})
                return child, opener, port, body.decode("utf-8")
            except (OSError, urllib.error.URLError):
                time.sleep(1)
        raise RuntimeError("Owned native Flask process did not become ready within 90 seconds")

    def stop_service(child):
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)

    try:
        if source.exists() or runtime.exists():
            raise RuntimeError("Fresh source/state root already exists")
        source.mkdir(); runtime.mkdir()
        token = os.environ.pop("CF7_READ_TOKEN")
        git_env = dict(os.environ, GIT_TERMINAL_PROMPT="0", GCM_INTERACTIVE="Never", GIT_CONFIG_COUNT="1",
                       GIT_CONFIG_KEY_0="http.https://github.com/.extraheader",
                       GIT_CONFIG_VALUE_0="AUTHORIZATION: basic " + base64.b64encode(("x-access-token:"+token).encode()).decode())
        run(["git", "init"], env=git_env, name="fresh git init")
        run(["git", "remote", "add", "origin", "https://github.com/Nettking/msh.git"], env=git_env, name="configured source origin")
        run(["git", "fetch", "--depth=1", "origin", CANDIDATE], env=git_env, name="fetch exact candidate", timeout=180)
        run(["git", "checkout", "--detach", "FETCH_HEAD"], env=git_env, name="checkout exact candidate")
        del token, git_env
        actual = run(["git", "rev-parse", "HEAD"], name="actual source SHA").strip()
        clean = run(["git", "status", "--porcelain"], name="fresh clean source").strip() == ""
        if actual != CANDIDATE or not clean:
            raise RuntimeError("Exact clean candidate checkout not proven")
        result["fresh_checkout"] = {"actual_commit": actual, "repository_clean": clean, "created_in_this_job": True}
        run([sys.executable, "-m", "venv", str(root / "python-env")], cwd=root, name="isolated Windows environment")
        python = root / "python-env" / "Scripts" / "python.exe"
        run([str(python), "-m", "pip", "install", "--disable-pip-version-check", "--no-cache-dir", "-r", "requirements.txt", "-c", "constraints-phase2.txt", "pytest", "ruff==0.16.10"], name="candidate-constrained local dependencies", timeout=480)
        run([str(python), "-m", "pytest", "-o", "addopts=", "-q", "catalog/federation/tests/cf7_acceptance"], name="documented fresh checkout deterministic CF7 gate", timeout=480)
        run([str(python), "-m", "pytest", "-o", "addopts=", "-q", *["catalog/flask_app/tests/"+name for name in (
            "test_capability_onboarding_route.py", "test_capability_inspection_route.py", "test_capability_benchmark_route.py",
            "test_capability_contribution_route.py", "test_capability_startup_transition_route.py", "test_federation_overview_route.py")]], name="documented fresh checkout deterministic route gates", timeout=360)
        run(["docker", "compose", "config", "--quiet"], name="documented Compose configuration gate only")
        if run(["git", "status", "--porcelain"], name="clean source immediately before physical observation").strip():
            raise RuntimeError("Candidate checkout changed during documented gates")
        if shutil.disk_usage(root).free < RESERVE:
            raise RuntimeError("Host reserve no longer sufficient before native startup")
        result["fresh_state_empty_before_product"] = not any(runtime.iterdir())
        env = {k: v for k, v in os.environ.items() if not k.startswith(("FCP_", "FLASK_", "OLLAMA_", "GIT_CONFIG_"))}
        env.update(PYTHONPATH=str(source), FCP_BUILD_COMMIT=CANDIDATE, FCP_DEVICE_NAME="CF7 isolated Windows device",
                   FCP_FEDERATION_COORDINATOR_DATABASE=str(runtime / "data" / "federation" / "control.sqlite3"), FCP_SCAN_DIRS="data,results")
        before = cli("catalog.flask_app.services.headless_federation_cli", "--json", "status")
        if before["state"] != "identity-missing":
            raise RuntimeError("Existing identity found in supposedly fresh state")
        result["fresh_native_status"] = before
        child, opener, port, body = start_service()
        csrf = re.search(r'name="csrf_token"\s+value="([^"]+)"', body)
        if not csrf:
            raise RuntimeError("Normal first-human bootstrap CSRF form absent")
        password = secrets.token_urlsafe(32)
        form = urllib.parse.urlencode({"csrf_token": html.unescape(csrf.group(1)), "email": "cf7-isolated@example.org",
                                      "password": password, "password_confirm": password}).encode()
        response = opener.open(urllib.request.Request(f"http://127.0.0.1:{port}/admin/users/bootstrap", data=form), timeout=20)
        response.read(1_048_576)
        result["normal_http_first_admin_bootstrap"] = {"utc": utc(), "http_status": response.status, "csrf_enabled": True,
                    "secrets_logged_or_uploaded": False, "account_scope": "this isolated local state only"}
        del password, csrf, form, body
        created = cli("catalog.flask_app.services.headless_federation_cli", "--json", "create")
        status = cli("catalog.flask_app.services.headless_federation_cli", "--json", "status")
        inspection = cli("catalog.flask_app.services.headless_capability_cli", "--json", "inspect")
        if created["state"] != "created" or status["state"] != "connected" or inspection["state"] != "complete":
            raise RuntimeError("Actual create/status/inspection did not complete")
        result.update(local_federation_create=created, local_federation_status=status, actual_native_inspection=inspection)
        stop_service(child)
        child, _, _, _ = start_service()
        restarted = cli("catalog.flask_app.services.headless_federation_cli", "--json", "status")
        repeated = cli("catalog.flask_app.services.headless_federation_cli", "--json", "create")
        for key in ("node_id", "federation_id", "session_id"):
            if restarted[key] != status[key] or repeated[key] != status[key]:
                raise RuntimeError("Actual restarted identity or local binding changed")
        if repeated["state"] != "already-connected":
            raise RuntimeError("Existing local binding was not reused")
        result.update(restart_status=restarted, idempotent_create=repeated, native_observations_complete=True)
        result["remaining_unobserved"] = ["full Compose/product launcher", "contribution reconciliation with persisted enabled intent", "real browser interaction", "mobile browser", "model/accelerator benchmark", "multi-host trust/transport", "MTConnect control"]
    except Exception as exc:
        result.update(native_observations_complete=False, error_type=type(exc).__name__, error=redact(str(exc)))
        returncode = 2
    else:
        returncode = 0
    finally:
        for child, log in processes:
            stop_service(child); log.close()
        result.update(completed_utc=utc(), owned_processes_closed=all(p.poll() is not None for p, _ in processes),
                      windows_free_bytes_after=shutil.disk_usage(root).free, command_observations=receipts)
        evidence.mkdir(exist_ok=True)
        def redact_tree(value):
            if isinstance(value, dict):
                return {key: redact_tree(item) for key, item in value.items()}
            if isinstance(value, list):
                return [redact_tree(item) for item in value]
            return redact(value) if isinstance(value, str) else value
        raw = json.dumps(redact_tree(result), indent=2).encode()
        (evidence / "NATIVE_OBSERVATIONS.json").write_bytes(raw)
        (evidence / "NATIVE_OBSERVATIONS.sha256").write_text(hashlib.sha256(raw).hexdigest()+"  NATIVE_OBSERVATIONS.json\n", encoding="utf-8")
        print(json.dumps({"candidate": CANDIDATE, "native_observations_complete": result.get("native_observations_complete"),
                          "physical_scenario_pass_claimed": False, "evidence_sha256": hashlib.sha256(raw).hexdigest(), "error": result.get("error")}))
    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
