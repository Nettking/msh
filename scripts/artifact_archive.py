"""Private SSH evidence archive. Standard library only; no external-storage fallback.

Server: forced SSH command, Linux. Client: Python 3.12+, Windows/Linux OpenSSH.
Packages are uncompressed ZIPs; manifests describe the actual files and checkout.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import uuid
import zipfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

SCHEMA = "fcp.ssh-artifact.v1"
REPO = "Nettking/msh"
MAX_BYTES = 2 * 1024**3
MAX_HEADER = 8 * 1024**2
RESERVE_BYTES = 200 * 1024**3  # Existing physical acceptance margin stays available.
CI_ONLY_RUNNERS = frozenset({"Beast", "Beast-Linux-WSL"})


def canonical(value):
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode()


def sha_file(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def require_local_space(path, extra_bytes):
    if type(extra_bytes) is not int or extra_bytes < 0:
        raise ValueError("Invalid local space requirement")
    ci_only = (
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("GITHUB_REPOSITORY") == REPO
        and os.environ.get("RUNNER_NAME") in CI_ONLY_RUNNERS
    )
    # Beast is explicitly CI-only, not a physical acceptance host or archive.
    # Keep its existing CI refusal at <=12 GiB, including the pending write.
    reserve = (12 if ci_only else 66) * 1024**3
    path = Path(path).absolute()
    while not path.exists():
        path = path.parent
    volumes = [path]
    release = Path("/proc/sys/kernel/osrelease")
    if release.is_file() and "microsoft" in release.read_text().lower():
        if not Path("/mnt/c").is_dir():
            raise OSError("WSL physical host-volume capacity is unavailable")
        # Also preserve the physical Windows volume, not just the virtual disk.
        volumes.append(Path("/mnt/c"))
    observations = []
    for volume in volumes:
        free = shutil.disk_usage(volume).free
        required = reserve + extra_bytes
        if free < required or (ci_only and free == required):
            raise OSError(
                f"Local evidence operation would breach the free-space reserve: "
                f"volume={volume}, free_bytes={free}, required_bytes={required}"
            )
        observations.append(
            {
                "volume": str(volume),
                "free_bytes": free,
                "reserve_bytes": reserve,
                "extra_bytes_bound": extra_bytes,
                "role": "ci-only" if ci_only else "acceptance-capacity-preserved",
            }
        )
    return observations


def component(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,159}", value
    ):
        raise ValueError("Invalid archive path component")
    return value


def safe_name(name):
    p = PurePosixPath(name)
    if (
        not name
        or p.is_absolute()
        or any(x in ("", ".", "..") for x in name.split("/"))
        or "\\" in name
        or ":" in name
        or any(ord(x) < 32 for x in name)
    ):
        raise ValueError("Unsafe package member")
    for part in p.parts:
        if part.endswith((" ", ".")) or re.fullmatch(
            r"(con|prn|aux|nul|com[1-9]|lpt[1-9])", part.split(".")[0], re.IGNORECASE
        ):
            raise ValueError("Package member is not portable to Windows")
    return p


def reference(manifest):
    if manifest["schema"] != SCHEMA or manifest["repo"] != REPO:
        raise ValueError("Wrong schema or repository")
    sha = manifest["tested_sha"]
    if not re.fullmatch("[0-9a-f]{40}", sha):
        raise ValueError("Missing exact tested SHA")
    attempt = manifest["run_attempt"]
    if type(attempt) is not int or attempt < 1:
        raise ValueError("Invalid attempt")
    matrix_hash = hashlib.sha256(canonical(manifest["matrix"])).hexdigest()[:16]
    return "/".join(
        (
            REPO,
            sha,
            component(manifest["run_id"]),
            str(attempt),
            component(manifest["job"]) + "-" + matrix_hash,
            component(manifest["artifact"]),
        )
    )


def verified_zip(path, manifest):
    expected = manifest["files"]
    if not expected or len(expected) > 100000:
        raise ValueError("Empty or excessive file inventory")
    seen = set()
    total = 0
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        if len(infos) != len(expected):
            raise ValueError("File inventory mismatch")
        for info, item in zip(infos, expected, strict=True):
            safe_name(info.filename)
            key = info.filename.casefold()
            if key in seen or info.filename != item["path"] or info.is_dir():
                raise ValueError("Duplicate or mismatched member")
            seen.add(key)
            if info.compress_type != zipfile.ZIP_STORED or stat.S_ISLNK(
                info.external_attr >> 16
            ):
                raise ValueError("Only uncompressed regular files are accepted")
            total += info.file_size
            if total > MAX_BYTES or info.file_size != item["size"]:
                raise ValueError("Size inventory mismatch")
            with archive.open(info) as stream:
                actual = hashlib.file_digest(stream, "sha256").hexdigest()
            if actual != item["sha256"]:
                raise ValueError("File checksum mismatch")


def read_header(stream):
    line = stream.readline(MAX_HEADER + 1)
    if len(line) > MAX_HEADER or not line.endswith(b"\n"):
        raise ValueError("Invalid protocol header")
    return json.loads(line)


@contextmanager
def server_lock(root):
    import fcntl

    with (root / ".publish.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def package_path(root, ref):
    parts = ref.split("/")
    if len(parts) != 7 or "/".join(parts[:2]) != REPO:
        raise ValueError("Invalid archive reference")
    for part in parts:
        component(part)
    path = root.joinpath(*parts)
    if not path.resolve().is_relative_to(root.resolve()) or any(
        p.is_symlink() for p in (path, *path.parents)
    ):
        raise ValueError("Archive path escapes root")
    return path


def serve(root, reserve=RESERVE_BYTES):
    os.umask(0o077)
    root = Path(root).resolve(strict=True)
    req = read_header(sys.stdin.buffer)
    op = req["operation"]
    if op == "put":
        manifest = req["manifest"]
        ref = reference(manifest)
        size = req["zip_size"]
        if type(size) is not int or not 0 < size <= MAX_BYTES:
            raise ValueError("Package exceeds archive bound")
        stage = root / ".incoming" / uuid.uuid4().hex
        with server_lock(root):
            if shutil.disk_usage(root).free < reserve + size + MAX_HEADER:
                raise OSError(
                    "Archive incomplete: free-space reserve would be breached"
                )
            stage.mkdir(parents=True, mode=0o700)
            with (stage / "bundle.zip").open("xb") as out:
                # Reserve real blocks before accepting concurrent transfers.
                os.posix_fallocate(out.fileno(), 0, size)
        try:
            with (stage / "bundle.zip").open("r+b") as out:
                left = size
                while left:
                    chunk = sys.stdin.buffer.read(min(left, 1024**2))
                    if not chunk:
                        raise ValueError("Truncated transfer; not published")
                    out.write(chunk)
                    left -= len(chunk)
                out.flush()
                os.fsync(out.fileno())
            if sha_file(stage / "bundle.zip") != req["zip_sha256"]:
                raise ValueError("Transfer checksum mismatch")
            verified_zip(stage / "bundle.zip", manifest)
            raw = canonical(manifest)
            (stage / "manifest.json").write_bytes(raw)
            receipt = {
                "reference": ref,
                "manifest_sha256": hashlib.sha256(raw).hexdigest(),
                "zip_sha256": req["zip_sha256"],
                "zip_size": size,
                "complete": True,
            }
            (stage / "COMPLETE.json").write_bytes(canonical(receipt))
            for file in stage.iterdir():
                with file.open("rb") as stream:
                    os.fsync(stream.fileno())
                file.chmod(0o400)
            stage_fd = os.open(stage, os.O_RDONLY)
            try:
                os.fsync(stage_fd)
            finally:
                os.close(stage_fd)
            destination = package_path(root, ref)
            with server_lock(root):
                if destination.exists():
                    old = json.loads((destination / "COMPLETE.json").read_bytes())
                    if old != receipt:
                        raise FileExistsError(
                            "Immutable reference already contains different evidence"
                        )
                    if sha_file(destination / "bundle.zip") != receipt["zip_sha256"]:
                        raise ValueError("Previously completed package is corrupt")
                else:
                    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    stage.rename(destination)
                    destination.chmod(0o500)
                    fd = os.open(destination.parent, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
            sys.stdout.buffer.write(canonical(receipt))
        finally:
            if stage.exists():
                stage.chmod(0o700)
                shutil.rmtree(
                    stage
                )  # Only this invocation's uncommitted staging directory.
    elif op == "get":
        path = package_path(root, req["reference"])
        receipt = json.loads((path / "COMPLETE.json").read_bytes())
        manifest = json.loads((path / "manifest.json").read_bytes())
        if (
            reference(manifest) != req["reference"]
            or hashlib.sha256(canonical(manifest)).hexdigest()
            != receipt["manifest_sha256"]
            or sha_file(path / "bundle.zip") != receipt["zip_sha256"]
        ):
            raise ValueError("Stored package verification failed")
        sys.stdout.buffer.write(canonical({"receipt": receipt, "manifest": manifest}))
        with (path / "bundle.zip").open("rb") as stream:
            shutil.copyfileobj(stream, sys.stdout.buffer, 1024**2)
    elif op == "list":
        if req["repo"] != REPO or not re.fullmatch("[0-9a-f]{40}", req["tested_sha"]):
            raise ValueError("Invalid source identity")
        base = root / REPO / req["tested_sha"] / component(req["run_id"])
        rows = []
        for p in sorted(base.glob("*/*/*/COMPLETE.json")):
            if len(rows) >= 10000:
                raise ValueError("Inventory bound exceeded")
            row = json.loads(p.read_bytes())
            if int(row["reference"].split("/")[4]) <= req["run_attempt"]:
                rows.append(row)
        sys.stdout.buffer.write(canonical(rows))
    else:
        raise ValueError("Unsupported archive operation")


def select_files(patterns):
    roots, files = [], set()
    for pattern in patterns:
        found = glob.glob(pattern, recursive=True)
        if not found:
            raise FileNotFoundError("Required evidence input missing: " + pattern)
        for name in found:
            path = Path(name).absolute()
            if path.is_symlink():
                raise ValueError("Evidence symlinks are not supported")
            roots.append(path if path.is_dir() else path.parent)
            for file in path.rglob("*") if path.is_dir() else (path,):
                if file.is_symlink():
                    raise ValueError("Evidence symlinks are not supported")
                if file.is_file():
                    files.add(file)
    if not files:
        raise FileNotFoundError("No evidence files; archive incomplete")
    common = Path(os.path.commonpath(roots))
    return [(p, p.relative_to(common).as_posix()) for p in sorted(files)]


def build_package(metadata, patterns, spool):
    spool = Path(spool)
    spool.mkdir(parents=True, exist_ok=False)
    files = select_files(patterns)
    total = sum(path.stat().st_size for path, _ in files)
    if total > MAX_BYTES - MAX_HEADER:
        raise ValueError(
            "Evidence exceeds the bounded package size; originals retained"
        )
    admission = require_local_space(spool, total + MAX_HEADER)
    manifest = dict(
        metadata,
        schema=SCHEMA,
        archived_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        tool_sha256=sha_file(__file__),
        local_capacity_admission=admission,
        files=[],
    )
    reference(manifest)
    with zipfile.ZipFile(
        spool / "bundle.zip", "x", compression=zipfile.ZIP_STORED
    ) as archive:
        for path, name in files:
            safe_name(name)
            before = path.stat()
            sha = sha_file(path)
            archive.write(path, name)
            after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (
                after.st_size,
                after.st_mtime_ns,
            ):
                raise ValueError(
                    "Evidence changed during packaging; retry only from stable original files"
                )
            manifest["files"].append(
                {"path": name, "size": after.st_size, "sha256": sha}
            )
    verified_zip(spool / "bundle.zip", manifest)
    (spool / "manifest.json").write_bytes(canonical(manifest))
    return spool


def ssh_executable():
    executable = shutil.which("ssh")
    if executable:
        return executable
    if sys.platform == "win32":
        candidates = [
            Path(os.environ.get("SystemRoot", "C:/Windows"))
            / "System32/OpenSSH/ssh.exe"
        ]
        git = shutil.which("git")
        if git:
            git_path = Path(git).resolve()
            candidates.append(git_path.parent.parent / "usr/bin/ssh.exe")
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)
    raise FileNotFoundError(
        "OpenSSH client unavailable: checked PATH, Windows OpenSSH and existing Git for Windows. "
        "No client installation or storage fallback attempted."
    )


def ssh_command(config):
    # No shell construction; no environment-selected SSH command or host-key bypass.
    host = config["host"]
    if not re.fullmatch(r"[A-Za-z0-9_.@-]+", host) or host.startswith("-"):
        raise ValueError("Invalid SSH destination")
    return [
        ssh_executable(),
        "-F",
        "none",
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=2",
        "-o",
        "UserKnownHostsFile=" + Path(config["known_hosts"]).absolute().as_posix(),
        "-i",
        Path(config["identity_file"]).absolute().as_posix(),
        host,
        "fcp-artifact-v1",
    ]


def exchange(config, header, output, payload=None, receive=None):
    with Path(output).open("xb") as out, tempfile.TemporaryFile() as err:
        proc = subprocess.Popen(
            ssh_command(config),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE if receive else out,
            stderr=err,
        )
        timer = threading.Timer(600, proc.kill)
        timer.start()
        try:
            try:
                proc.stdin.write(canonical(header))
                if payload:
                    with Path(payload).open("rb") as data:
                        shutil.copyfileobj(data, proc.stdin, 1024**2)
                proc.stdin.close()
            except BrokenPipeError:
                pass
            if receive:
                receive(proc.stdout, out)
            code = proc.wait(timeout=610)
        finally:
            timer.cancel()
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            if proc.stdout is not None:
                proc.stdout.close()
        if code:
            err.seek(0)
            raise RuntimeError(
                "Archive incomplete; originals and local package retained. SSH: "
                + err.read(3000).decode(errors="replace")
            )


def upload(config, package):
    package = Path(package)
    manifest = json.loads((package / "manifest.json").read_bytes())
    verified_zip(package / "bundle.zip", manifest)
    header = {
        "operation": "put",
        "manifest": manifest,
        "zip_size": (package / "bundle.zip").stat().st_size,
        "zip_sha256": sha_file(package / "bundle.zip"),
    }
    response = package / ("transfer-" + uuid.uuid4().hex + ".json")
    exchange(config, header, response, package / "bundle.zip")
    receipt = json.loads(response.read_bytes())
    if receipt != {
        "reference": reference(manifest),
        "manifest_sha256": hashlib.sha256(canonical(manifest)).hexdigest(),
        "zip_sha256": header["zip_sha256"],
        "zip_size": header["zip_size"],
        "complete": True,
    }:
        raise ValueError("Archive acknowledgment identity/checksum mismatch")
    return receipt


def fetch(config, receipt, output):
    output = Path(output)
    size = receipt["zip_size"]
    if type(size) is not int or not 0 < size <= MAX_BYTES:
        raise ValueError("Invalid download size")
    require_local_space(output, size + MAX_HEADER)
    output.mkdir(parents=True, exist_ok=False)
    received = []

    def receive(stream, dest):
        response = read_header(stream)
        if response["receipt"] != receipt:
            raise ValueError("Requested immutable receipt does not match archive")
        manifest = response["manifest"]
        if (
            reference(manifest) != receipt["reference"]
            or hashlib.sha256(canonical(manifest)).hexdigest()
            != receipt["manifest_sha256"]
        ):
            raise ValueError("Downloaded manifest identity mismatch")
        left = size
        while left:
            chunk = stream.read(min(left, 1024**2))
            if not chunk:
                raise ValueError("Truncated download; partial package retained")
            dest.write(chunk)
            left -= len(chunk)
        if stream.read(1):
            raise ValueError("Download exceeds declared size")
        dest.flush()
        os.fsync(dest.fileno())
        received.append(manifest)

    exchange(
        config,
        {"operation": "get", "reference": receipt["reference"]},
        output / "bundle.zip",
        receive=receive,
    )
    manifest = received[0]
    if (output / "bundle.zip").stat().st_size != receipt["zip_size"] or sha_file(
        output / "bundle.zip"
    ) != receipt["zip_sha256"]:
        raise ValueError("Downloaded package checksum mismatch")
    verified_zip(output / "bundle.zip", manifest)
    (output / "manifest.json").write_bytes(canonical(manifest))
    (output / "receipt.json").write_bytes(canonical(receipt))
    return manifest


def extract(package, destination):
    package, destination = Path(package), Path(destination)
    manifest = json.loads((package / "manifest.json").read_bytes())
    verified_zip(package / "bundle.zip", manifest)
    require_local_space(destination, sum(row["size"] for row in manifest["files"]))
    destination.mkdir(parents=True, exist_ok=True)
    for row in manifest["files"]:
        target = destination / str(safe_name(row["path"]))
        if target.exists() or not target.resolve().is_relative_to(
            destination.resolve()
        ):
            raise FileExistsError("Refusing extraction overwrite/escape")
    with zipfile.ZipFile(package / "bundle.zip") as archive:
        for row in manifest["files"]:
            target = destination / row["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(row["path"]) as src, target.open("xb") as dst:
                shutil.copyfileobj(src, dst, 1024**2)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="operation", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--root", required=True)
    s = sub.add_parser("pack")
    s.add_argument("--metadata", required=True)
    s.add_argument("--output", required=True)
    s.add_argument("paths", nargs="+")
    s = sub.add_parser("upload")
    s.add_argument("--config", required=True)
    s.add_argument("--package", required=True)
    s = sub.add_parser("get")
    s.add_argument("--config", required=True)
    s.add_argument("--receipt", required=True)
    s.add_argument("--output", required=True)
    s = sub.add_parser("extract")
    s.add_argument("--package", required=True)
    s.add_argument("--output", required=True)
    args = p.parse_args()
    if args.operation == "serve":
        serve(args.root)
    elif args.operation == "pack":
        print(
            build_package(
                json.loads(Path(args.metadata).read_bytes()), args.paths, args.output
            )
        )
    elif args.operation == "upload":
        print(
            json.dumps(upload(json.loads(Path(args.config).read_bytes()), args.package))
        )
    elif args.operation == "get":
        fetch(
            json.loads(Path(args.config).read_bytes()),
            json.loads(Path(args.receipt).read_bytes()),
            args.output,
        )
        print("Verified immutable archive package: " + args.output)
    else:
        extract(args.package, args.output)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 -- protocol boundary always fails closed
        print("ARTIFACT_ARCHIVE_INCOMPLETE: " + str(exc), file=sys.stderr)
        sys.exit(1)
