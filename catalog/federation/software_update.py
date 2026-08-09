"""Safe, narrowly-scoped MSH Git update primitives.

This module intentionally contains no transport or shell-command facility.  A
peer may name an approved commit, but repository, remote, branch and executable
choices remain local constants.
"""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

APPROVED_REPOSITORY = "Nettking/msh"
APPROVED_BRANCH = "main"
OID_RE = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class UpdateInspection:
    state: str
    current_commit: str | None = None
    target_commit: str | None = None
    code: str | None = None
    message: str | None = None
    running_commit: str | None = None
    request_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class GitUpdateAdapter:
    """Inspect and fast-forward one trusted local checkout without a shell."""

    def __init__(
        self,
        repository_root: Path,
        *,
        remote: str = "origin",
        branch: str = APPROVED_BRANCH,
        timeout: float = 20.0,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.root = Path(repository_root).resolve()
        self.remote = remote
        self.branch = branch
        self.timeout = min(max(float(timeout), 1.0), 60.0)
        self._runner = runner

    def _git(self, *args: str) -> subprocess.CompletedProcess[str]:
        try:
            return self._runner(
                ["git", *args],
                cwd=self.root,
                shell=False,
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("git_unavailable") from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("git_timeout") from exc

    @staticmethod
    def _approved_remote(value: str) -> bool:
        # Credentials, query strings and fragments are never accepted. Support
        # canonical HTTPS and Git's common SSH/scp forms only.
        value = value.strip().removesuffix("/").removesuffix(".git")
        if value.startswith("git@github.com:"):
            return (
                value[len("git@github.com:") :].casefold()
                == APPROVED_REPOSITORY.casefold()
            )
        parsed = urlsplit(value)
        return (
            parsed.scheme in {"https", "ssh"}
            and parsed.hostname == "github.com"
            and parsed.username in {None, "git"}
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
            and parsed.path.strip("/").casefold()
            == APPROVED_REPOSITORY.casefold()
        )

    def _failure(
        self,
        code: str,
        current: str | None = None,
        target: str | None = None,
    ) -> UpdateInspection:
        messages = {
            "git_unavailable": "Git is not available on this device.",
            "git_timeout": "The approved Git operation timed out.",
            "unsupported_checkout": "This is not a supported MSH Git checkout.",
            "unapproved_remote": (
                "The checkout is not connected to the approved MSH source."
            ),
            "remote_unavailable": "The approved Git source could not be reached.",
            "dirty": "Local changes must be reviewed before updating.",
            "detached_head": "The checkout is not on the supported main branch.",
            "target_unavailable": (
                "The requested commit is unavailable from approved main."
            ),
            "update_failed": "The safe fast-forward update did not complete.",
        }
        if code == "dirty":
            state = "dirty"
        elif code in {"unsupported_checkout", "unapproved_remote", "detached_head"}:
            state = "unsupported_checkout"
        else:
            state = "error"
        return UpdateInspection(state, current, target, code, messages[code])

    def inspect(
        self,
        *,
        target: str | None = None,
        fetch: bool = True,
    ) -> UpdateInspection:
        try:
            inside = self._git("rev-parse", "--show-toplevel")
            if inside.returncode or Path(inside.stdout.strip()).resolve() != self.root:
                return self._failure("unsupported_checkout")
            remote = self._git("remote", "get-url", self.remote)
            if remote.returncode or not self._approved_remote(remote.stdout):
                return self._failure("unapproved_remote")
            branch = self._git("symbolic-ref", "--quiet", "--short", "HEAD")
            if branch.returncode or branch.stdout.strip() != self.branch:
                return self._failure("detached_head")
            current_result = self._git("rev-parse", "--verify", "HEAD^{commit}")
            if current_result.returncode:
                return self._failure("unsupported_checkout")
            current = current_result.stdout.strip().lower()
            status = self._git(
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            )
            if status.returncode or status.stdout:
                return self._failure("dirty", current)
            if fetch:
                fetched = self._git("fetch", "--no-tags", self.remote, self.branch)
                if fetched.returncode:
                    return self._failure("remote_unavailable", current)
            ref = (
                "FETCH_HEAD"
                if fetch
                else f"refs/remotes/{self.remote}/{self.branch}"
            )
            candidate = target.lower() if target else self._resolve(ref)
            if candidate is None or not OID_RE.fullmatch(candidate):
                return self._failure("target_unavailable", current, target)
            approved_tip = self._resolve(ref)
            if approved_tip is None or not self._ancestor(candidate, approved_tip):
                return self._failure("target_unavailable", current, candidate)
            if current == candidate:
                return UpdateInspection("up_to_date", current, candidate)
            if self._ancestor(current, candidate):
                return UpdateInspection("update_available", current, candidate)
            if self._ancestor(candidate, current):
                return UpdateInspection(
                    "ahead",
                    current,
                    candidate,
                    "ahead",
                    "This checkout is ahead of approved main.",
                )
            return UpdateInspection(
                "diverged",
                current,
                candidate,
                "diverged",
                "This checkout has diverged from approved main.",
            )
        except RuntimeError as exc:
            return self._failure(str(exc))

    def _resolve(self, ref: str) -> str | None:
        result = self._git("rev-parse", "--verify", f"{ref}^{{commit}}")
        value = result.stdout.strip().lower()
        return value if result.returncode == 0 and OID_RE.fullmatch(value) else None

    def _ancestor(self, older: str, newer: str) -> bool:
        return self._git("merge-base", "--is-ancestor", older, newer).returncode == 0

    def apply(
        self,
        target: str,
        *,
        request_id: str | None = None,
    ) -> UpdateInspection:
        if not OID_RE.fullmatch(target):
            return self._failure("target_unavailable", target=target)
        inspection = self.inspect(target=target, fetch=True)
        if inspection.state != "update_available":
            return UpdateInspection(
                inspection.state,
                inspection.current_commit,
                inspection.target_commit,
                inspection.code,
                inspection.message,
                inspection.running_commit,
                request_id,
            )
        # No checkout/reset/clean/stash: merge --ff-only is the sole mutation.
        applied = self._git("merge", "--ff-only", target)
        if applied.returncode:
            result = self._failure(
                "update_failed",
                inspection.current_commit,
                target,
            )
            return UpdateInspection(
                result.state,
                result.current_commit,
                result.target_commit,
                result.code,
                result.message,
                request_id=request_id,
            )
        proven = self._resolve("HEAD")
        if proven != target:
            result = self._failure("update_failed", proven, target)
            return UpdateInspection(
                result.state,
                result.current_commit,
                result.target_commit,
                result.code,
                result.message,
                request_id=request_id,
            )
        return UpdateInspection(
            "source_updated_restart_required",
            proven,
            target,
            "runtime_not_updated",
            (
                "The source checkout was fast-forwarded. The running MSH "
                "installation was not rebuilt, reinstalled, or restarted."
            ),
            request_id=request_id,
        )

    def latest_result(self) -> UpdateInspection | None:
        # This primitive has no runtime activation/result channel. Production
        # uses HostUpdateHandoff instead; the method only keeps the adapter shape
        # explicit for isolated tests and deliberately returns no runtime proof.
        return None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
