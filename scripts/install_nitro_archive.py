"""Administrator-only, bounded Nitro setup; does not touch runners or Recorder.

sudo python3 scripts/install_nitro_archive.py --public-key /path/archive.pub
No password, SSH daemon change, network listener, or scheduled cleanup is created.
"""

import argparse
import grp
import hashlib
import os
import pwd
import shutil
import subprocess
from pathlib import Path


def install(public_key):
    if os.geteuid() != 0:
        raise PermissionError(
            "Dedicated archive account setup requires administrator access"
        )
    key = Path(public_key).read_text().strip()
    if not key.startswith("ssh-ed25519 ") or "\n" in key:
        raise ValueError("Expected one Ed25519 public key")
    root = Path("/srv/fcp-artifacts")
    home = Path("/var/lib/fcp-archive")
    if shutil.disk_usage("/srv").free < 200 * 1024**3:
        raise OSError("Archive setup would not retain the 200 GiB free-space reserve")
    try:
        user = pwd.getpwnam("fcp-archive")
    except KeyError:
        subprocess.run(
            [
                "useradd",
                "--system",
                "--user-group",
                "--create-home",
                "--home-dir",
                str(home),
                "--shell",
                "/bin/sh",
                "fcp-archive",
            ],
            check=True,
        )
        user = pwd.getpwnam("fcp-archive")
    if user.pw_dir != str(home) or user.pw_uid == 0:
        raise ValueError("Existing account is not the dedicated archive account")
    if set(os.getgrouplist(user.pw_name, user.pw_gid)) != {user.pw_gid}:
        raise ValueError("Archive account must not belong to privileged/shared groups")
    if grp.getgrgid(user.pw_gid).gr_name != "fcp-archive":
        raise ValueError("Archive account requires its own dedicated primary group")
    for p in (root, home, home / ".ssh"):
        if p.is_symlink():
            raise ValueError("Archive setup paths must not be symlinks")
    root.mkdir(mode=0o700, exist_ok=True)
    if root.stat().st_uid not in (0, user.pw_uid):
        raise ValueError("Existing archive directory belongs to a different account")
    os.chown(root, user.pw_uid, user.pw_gid)
    root.chmod(0o700)
    home.mkdir(mode=0o755, exist_ok=True)
    os.chown(home, 0, 0)
    home.chmod(0o755)
    (home / ".ssh").mkdir(mode=0o755, exist_ok=True)
    os.chown(home / ".ssh", 0, 0)
    (home / ".ssh").chmod(0o755)
    source = Path(__file__).with_name("artifact_archive.py").read_bytes()
    sha = hashlib.sha256(source).hexdigest()
    server = Path("/usr/local/lib/fcp-artifacts") / sha / "artifact_archive.py"
    server.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    if server.exists():
        if server.read_bytes() != source:
            raise ValueError("Installed server digest collision/mismatch")
    else:
        with server.open("xb") as out:
            out.write(source)
    os.chown(server, 0, 0)
    server.chmod(0o444)
    authorization = home / ".ssh/authorized_keys"
    line = f'restrict,command="/usr/bin/python3 -B {server} serve --root {root}" {key}'
    if authorization.exists() and authorization.read_text().strip() != line:
        raise ValueError(
            "Existing archive authorization differs; review an explicit rotation"
        )
    authorization.write_text(line + "\n")
    os.chown(authorization, 0, 0)
    authorization.chmod(0o444)
    subprocess.run(
        ["runuser", "-u", "martin", "--", "test", "!", "-r", str(root)], check=True
    )
    print(f"Archive installed: {root}; account fcp-archive; receiver SHA256 {sha}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-key", required=True)
    install(parser.parse_args().public_key)
