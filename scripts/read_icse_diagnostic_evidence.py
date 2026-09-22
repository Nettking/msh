"""File-only recovery of one completed diagnostic; no product/test execution."""
from datetime import datetime, timezone
import hashlib
from itertools import islice
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

ORIGINAL_SHA = "17d3cc1360763b8d35805130199ed3a24f82ac94"
PRODUCT_SHA = "5d5d7e8f215717cf9b7012d3b357eaf03a1400c9"
SOURCE = Path("/mnt/c/actions-runner/_work/_temp/fcp-icse-reconnect-35749009563-1-Windows")
BRANCH = "refs/heads/codex/icse-read-retained-35749009563"
LIMIT = 2 * 1024 * 1024
EARLIEST = datetime(2026, 9, 22, 15, 40, tzinfo=timezone.utc).timestamp()
LATEST = datetime(2026, 9, 22, 15, 45, tzinfo=timezone.utc).timestamp()
result = {"schema": "fcp.icse-same-execution-recovery.v1",
          "observed_at": datetime.now(timezone.utc).isoformat(),
          "original_diagnostic_run_id": 35749009563, "original_attempt": 1,
          "original_job_id": 106817918738, "original_diagnostic_sha": ORIGINAL_SHA,
          "product_sha": PRODUCT_SHA, "original_runner": "Beast",
          "test_execution": False, "raw_stderr_exported": False,
          "source_root": str(SOURCE), "files": {}, "observation": "NOT_STARTED"}


def require(value, code):
    if not value:
        raise RuntimeError(code)


def git(*args):
    return subprocess.check_output(["git", *args], text=True, stderr=subprocess.PIPE).strip()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def metadata(value):
    return {key: getattr(value, key) for key in
            ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")}


def cross_identity(value):
    # Compare file identity/content metadata across APIs; ctime is compared only
    # path-to-path and handle-to-handle, never path-to-CRT-handle on Windows.
    return (value.st_dev, value.st_ino, stat.S_IFMT(value.st_mode),
            value.st_size, value.st_mtime_ns)


def open_directory(path, *, parent=None):
    return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                   dir_fd=parent)


def read_file(root_fd, relative):
    parts = relative.split("/")
    require(all(part not in {"", ".", ".."} for part in parts), "invalid-fixed-path")
    descriptors = []
    before = None
    item = {"status": "NOT_READ"}
    result["files"][relative] = item
    try:
        directory = root_fd
        for part in parts[:-1]:
            directory = open_directory(part, parent=directory)
            descriptors.append(directory)
        before = os.stat(parts[-1], dir_fd=directory, follow_symlinks=False)
        item["path_before"] = metadata(before)
        require(stat.S_ISREG(before.st_mode), "nonregular-file")
        require(before.st_size <= LIMIT, "size-limit")
        require(EARLIEST <= before.st_mtime <= LATEST, "outside-original-execution-interval")
        descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(descriptor, "rb") as stream:
            opened = os.fstat(stream.fileno())
            item["handle_before"] = metadata(opened)
            require(cross_identity(opened) == cross_identity(before), "open-identity-mismatch")
            data = stream.read(LIMIT + 1)
            after = os.fstat(stream.fileno())
        named_after = os.stat(parts[-1], dir_fd=directory, follow_symlinks=False)
        item["handle_after"] = metadata(after)
        item["path_after"] = metadata(named_after)
        require(metadata(before) == metadata(named_after)
                and metadata(opened) == metadata(after)
                and cross_identity(after) == cross_identity(named_after)
                and len(data) == before.st_size, "changed-during-read")
        item.update(status="STABLE_READ", bytes=len(data), sha256=sha(data))
        return data
    except FileNotFoundError:
        item["status"] = "ABSENT" if before is None else "DISAPPEARED_DURING_READ"
    except PermissionError:
        item["status"] = "PERMISSION_DENIED"
    except RuntimeError as error:
        # These are this reader's fixed literal guard codes, not OS/log text.
        item.update(status="REFUSED", guard_code=error.args[0])
    except OSError:
        item["status"] = "OS_READ_REFUSED"
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
    return None


def write(name, value):
    with (output / name).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def safe_phase(value):
    """Copy only observer's closed field vocabulary; no raw arbitrary values."""
    words = {"event", "worker", "function", "phase", "request_kind",
             "exception_class", "last_observed_exception", "file", "next_link",
             "in_memory_replica_role", "role"}
    numbers = {"sequence", "elapsed_ns", "pid", "connect_ordinal", "line", "duration_ns",
               "pending_count", "replay_tasks_count", "gap_replay_tasks_count",
               "pending_message_responses_count", "consensus_term", "commit_index",
               "last_applied", "fencing_epoch"}
    booleans = {"completed_normally", "timeout", "frames_truncated", "chain_truncated",
                "client_present", "websocket_present", "receiver_task_present",
                "heartbeat_task_present", "ready"}
    def clean(item, depth=0):
        require(depth <= 12 and isinstance(item, dict), "phase-shape")
        safe = {}
        for key, data in item.items():
            if key in words:
                require(data is None or (isinstance(data, str) and len(data) <= 180
                        and re.fullmatch(r"[A-Za-z0-9_./<>-]+", data)), "phase-token")
                safe[key] = data
            elif key in numbers:
                require(data is None or type(data) is int, "phase-integer")
                safe[key] = data
            elif key in booleans:
                require(type(data) is bool, "phase-boolean")
                safe[key] = data
            elif key == "at":
                require(isinstance(data, str) and re.fullmatch(r"[0-9T:.+Z-]{20,40}", data), "phase-time")
                safe[key] = data
            elif key in {"diagnostic_sha", "product_sha"}:
                require(data == (ORIGINAL_SHA if key == "diagnostic_sha" else PRODUCT_SHA), "phase-source")
                safe[key] = data
            elif key in {"state", "frame", "traceback"}:
                safe[key] = clean(data, depth + 1)
            elif key in {"frames", "exceptions_outer_first"}:
                require(isinstance(data, list) and len(data) <= 64, "phase-list")
                safe[key] = [clean(child, depth + 1) for child in data]
            else:
                raise RuntimeError("unexpected-phase-field")
        return safe
    return clean(value)


def sanitized_stderr(raw, tracked):
    records = []
    classes = {"TimeoutError", "CancelledError", "RuntimeError", "OSError", "ValueError",
               "TypeError", "KeyError", "AssertionError", "ConnectionError", "ConnectionClosedError",
               "ConnectionClosedOK", "RelayRemoteError", "NodeStateError", "WorkerCommandFailure"}
    prefix = "c:/actions-runner/_work/msh/msh/product/"
    for line in raw.decode("utf-8", errors="replace").splitlines():
        if line == "Traceback (most recent call last):":
            records.append({"kind": "traceback_start"})
        elif line == "The above exception was the direct cause of the following exception:":
            records.append({"kind": "chain", "link": "cause"})
        elif line == "During handling of the above exception, another exception occurred:":
            records.append({"kind": "chain", "link": "context"})
        match = re.fullmatch(r'\s*File "([^"]+)", line ([0-9]+), in ([A-Za-z_<>][A-Za-z0-9_.<>]*)', line)
        if match:
            path = match[1].replace("\\", "/")
            name = path[len(prefix):] if path.casefold().startswith(prefix) else None
            if name not in tracked:
                name = None
                for family in ("asyncio", "websockets"):
                    token = "/" + family + "/"
                    if token in path:
                        suffix = path.rsplit(token, 1)[1]
                        if re.fullmatch(r"[A-Za-z0-9_/]+\.py", suffix) and ".." not in suffix:
                            name = family + "/" + suffix
            records.append({"kind": "frame", "file": name or "unlisted-module", "line": int(match[2]),
                            "function": match[3] if name and len(match[3]) <= 120 else "withheld"})
        match = re.match(r"^(?:[A-Za-z_][A-Za-z0-9_]*\.)*([A-Za-z_][A-Za-z0-9_]*)(?::|$)", line)
        if match and match[1] in classes:
            records.append({"kind": "exception", "class": match[1]})
        if len(records) >= 1000:
            return {"status": "SANITIZED_TRUNCATED", "ordered_records": records[:1000]}
    return {"status": "SANITIZED", "ordered_records": records}


def recover(root_fd):
    raw = read_file(root_fd, "prepare.json")
    if raw is None:
        result["observation"] = "PREPARE_UNAVAILABLE_NO_CONTENT_RECOVERY"
        return
    prepared = json.loads(raw)
    expected = {"run_id": "35749009563", "attempt": 1, "runner": "Beast",
                "source_sha": ORIGINAL_SHA, "product_sha": PRODUCT_SHA}
    require(prepared.get("diagnostic") == expected, "original-preparation-binding")
    result["original_preparation_binding_verified"] = True
    (output / "prepare.json").write_bytes(raw)
    tracked = set(prepared["tracked_python_files"])
    require(all(isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_./-]+\.py", name)
                and not name.startswith("/") and ".." not in name for name in tracked), "tracked-paths")
    manifest_raw = read_file(root_fd, "observer-manifest.json")
    if manifest_raw is not None:
        manifest = json.loads(manifest_raw)
        require(set(manifest) == {"sitecustomize.py", "icse_reconnect_observer.py"}
                and all(re.fullmatch(r"[0-9a-f]{64}", value) for value in manifest.values()), "observer-manifest-shape")
        (output / "observer-manifest.json").write_bytes(manifest_raw)
    summary = read_file(root_fd, "network/public/summary.json")
    if summary is not None:
        public = json.loads(summary)
        require(public.get("schema") == "fcp.icse-network-demo.v1"
                and public.get("source_sha") == PRODUCT_SHA
                and public.get("physical_acceptance") == "NOT_EVALUATED", "public-source-binding")
        (output / "public").mkdir()
        (output / "public/summary.json").write_bytes(summary)
        for name in ("events.jsonl", "operator-report.html"):
            data = read_file(root_fd, "network/public/" + name)
            if data is not None:
                (output / "public" / name).write_bytes(data)
    stderr = {}
    for label in ("voter-a", "voter-b", "voter-c", "reviewer", "driver"):
        relative = (f"network/private-state/{label}/worker.stderr.log" if label != "driver"
                    else "network/private-state/driver-failure.log")
        data = read_file(root_fd, relative)
        if data is not None:
            stderr[label] = sanitized_stderr(data, tracked)
            stderr[label]["observer_unavailable_marker"] = b"ICSE_DIAGNOSTIC_OBSERVER_UNAVAILABLE" in data
        else:
            stderr[label] = {"status": result["files"][relative]["status"]}
    write("sanitized-tracebacks.json", stderr)
    phase_fd = None
    phase_records = []
    try:
        phase_fd = open_directory("phases", parent=root_fd)
        with os.scandir(phase_fd) as entries:
            names = [entry.name for entry in islice(entries, 5)]
        result["phase_inventory_limit_exceeded"] = len(names) > 4
        for name in names[:4]:
            if not re.fullmatch(r"voter-c-[0-9]+\.jsonl", name):
                result["unrecognized_phase_name_present"] = True
                continue
            data = read_file(root_fd, "phases/" + name)
            if data is None:
                continue
            records = []
            for line in data.decode("utf-8").splitlines():
                if not line.strip():
                    continue
                if len(records) >= 1001:
                    result["phase_record_limit_exceeded"] = True
                    break
                try:
                    records.append(safe_phase(json.loads(line)))
                except (ValueError, RuntimeError, TypeError):
                    result["phase_partial_or_refused_record"] = True
                    break
            phase_records.append({"source_file": name, "original_sha256": sha(data),
                                  "derived_sanitized_records": records})
    except FileNotFoundError:
        result["phase_directory"] = "ABSENT"
    except PermissionError:
        result["phase_directory"] = "PERMISSION_DENIED"
    except OSError:
        result["phase_directory"] = "OS_READ_REFUSED"
    finally:
        if phase_fd is not None:
            os.close(phase_fd)
    write("phase-observations.json", phase_records)
    result["observation"] = "SAME_EXECUTION_FILES_RECOVERED_SUBJECT_TO_REVIEW"


output = None
descriptors = []
try:
    require(os.environ.get("GITHUB_REPOSITORY") == "Nettking/msh"
            and os.environ.get("GITHUB_ACTOR") == "Nettking"
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and os.environ.get("GITHUB_REF") == BRANCH
            and os.environ.get("GITHUB_RUN_ATTEMPT") == "1"
            and os.environ.get("RUNNER_NAME") == "Beast-Linux-WSL"
            and os.environ.get("RUNNER_OS") == "Linux", "reader-boundary")
    result["reader_run_id"] = os.environ["GITHUB_RUN_ID"]
    result["reader_attempt"] = 1
    result["reader_source"] = git("rev-parse", "HEAD")
    require(result["reader_source"] == os.environ["GITHUB_SHA"]
            and git("rev-parse", "HEAD^") == PRODUCT_SHA, "reader-source")
    require(set(git("diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD").splitlines())
            == {".github/workflows/nitro-artifact-smoke.yml", "scripts/read_icse_diagnostic_evidence.py"}, "reader-scope")
    new_output = Path(os.environ["RUNNER_TEMP"]) / ("icse-recovery-35749009563-" + os.environ["GITHUB_RUN_ID"])
    new_output.mkdir(mode=0o700, exist_ok=False)
    output = new_output
    try:
        directory = open_directory("/")
        descriptors.append(directory)
        for component in SOURCE.parts[1:]:
            directory = open_directory(component, parent=directory)
            descriptors.append(directory)
        recover(directory)
    except FileNotFoundError:
        result["observation"] = "ORIGINAL_EXECUTION_ROOT_ABSENT"
    except PermissionError:
        result["observation"] = "ORIGINAL_EXECUTION_ROOT_PERMISSION_DENIED"
except RuntimeError as error:
    result.update(observation="RECOVERY_REFUSED", guard_code=error.args[0])
except Exception:
    result["observation"] = "RECOVERY_UNAVAILABLE"
finally:
    for descriptor in reversed(descriptors):
        os.close(descriptor)
if output is None:
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(1)
write("observation.json", result)
print(json.dumps(result, sort_keys=True))
try:
    # Existing trusted local retention only; no Nitro/network call here.
    sys.path.insert(0, os.environ["GITHUB_WORKSPACE"])
    from scripts.artifact_archive_ci import preserve_failed_inputs
    pending = preserve_failed_inputs(dict(os.environ, ARCHIVE_PATH=str(output),
                                         ARCHIVE_NAME="same-execution-recovery-35749009563"))
    print(json.dumps({"local_retention": "PRESERVED_NOT_ARCHIVE_PROOF", "path": str(pending)}, sort_keys=True))
except Exception:
    print('{"local_retention":"UNCONFIRMED"}')
    raise SystemExit(1)
if result["observation"] not in {"SAME_EXECUTION_FILES_RECOVERED_SUBJECT_TO_REVIEW", "ORIGINAL_EXECUTION_ROOT_ABSENT", "PREPARE_UNAVAILABLE_NO_CONTENT_RECOVERY"}:
    raise SystemExit(1)
