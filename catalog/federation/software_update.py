"""Safe, narrowly-scoped FCP Git update primitives.

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

#: A branch name this module is willing to hand to Git as an argument.
#:
#: Names are only ever read back from the approved remote, so this is not the
#: primary defence -- it is the one that holds if a later caller ever passes a
#: name in from somewhere else. It refuses anything Git itself would reject and
#: anything that could be read as an option, a path escape or a ref revision.
BRANCH_RE = re.compile(
    r"^(?![./-])(?!.*\.\.)(?!.*//)(?!.*@\{)(?!.*\.lock(?:/|$))"
    r"[A-Za-z0-9][A-Za-z0-9._/-]{0,180}(?<![./])$"
)

#: Bound on how many branches the dropdown will ever carry.
MAX_APPROVED_BRANCHES = 200


@dataclass(frozen=True)
class ApprovedBranch:
    """One branch published by the approved repository, with its exact tip."""

    name: str
    commit: str

    @property
    def is_approved_branch(self) -> bool:
        return self.name == APPROVED_BRANCH

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "commit": self.commit,
            "is_default": self.is_approved_branch,
        }


@dataclass(frozen=True)
class UpdateInspection:
    state: str
    current_commit: str | None = None
    target_commit: str | None = None
    code: str | None = None
    message: str | None = None
    running_commit: str | None = None
    request_id: str | None = None
    #: Bounded branch-trial summary, present only on a device that has run one.
    #: Additive and optional: every existing producer and reader of this record
    #: keeps working unchanged, and a device that has never left approved main
    #: reports ``None`` exactly as before.
    trial: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def with_trial(self, trial: dict[str, object] | None) -> UpdateInspection:
        return UpdateInspection(
            self.state,
            self.current_commit,
            self.target_commit,
            self.code,
            self.message,
            self.running_commit,
            self.request_id,
            trial,
        )


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
            "unsupported_checkout": "This is not a supported FCP Git checkout.",
            "unapproved_remote": (
                "The checkout is not connected to the approved FCP source."
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

    def checkout_baseline(self) -> tuple[UpdateInspection | None, str | None]:
        """Validate the checkout itself and return its exact ``HEAD`` commit.

        Supported checkout, approved remote, approved branch, clean tree -- the
        four properties every mutation in this module depends on, in the order
        they have always been checked. Extracted so a caller that is *not*
        fast-forwarding (a branch trial pinning its fallback) can require the
        same checkout guarantees without also requiring an update target.
        """

        inside = self._git("rev-parse", "--show-toplevel")
        if inside.returncode or Path(inside.stdout.strip()).resolve() != self.root:
            return self._failure("unsupported_checkout"), None
        remote = self._git("remote", "get-url", self.remote)
        if remote.returncode or not self._approved_remote(remote.stdout):
            return self._failure("unapproved_remote"), None
        branch = self._git("symbolic-ref", "--quiet", "--short", "HEAD")
        if branch.returncode or branch.stdout.strip() != self.branch:
            return self._failure("detached_head"), None
        current_result = self._git("rev-parse", "--verify", "HEAD^{commit}")
        if current_result.returncode:
            return self._failure("unsupported_checkout"), None
        current = current_result.stdout.strip().lower()
        status = self._git(
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        )
        if status.returncode or status.stdout:
            return self._failure("dirty", current), current
        return None, current

    def inspect(
        self,
        *,
        target: str | None = None,
        fetch: bool = True,
    ) -> UpdateInspection:
        try:
            failure, baseline = self.checkout_baseline()
            if failure is not None:
                return failure
            current = str(baseline)
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

    # ---- approved branch discovery ---------------------------------------
    #
    # Reading the approved remote's branch list is the only new Git capability
    # this module gained for branch trials, and it is read-only. It runs behind
    # the same approved-remote check every other operation runs behind, so a
    # checkout wired to some other repository can never publish a branch list.

    def _remote_is_approved(self) -> bool:
        remote = self._git("remote", "get-url", self.remote)
        return remote.returncode == 0 and self._approved_remote(remote.stdout)

    def approved_branches(self) -> tuple[ApprovedBranch, ...]:
        """List branches published by the approved repository, tips included.

        Raises ``RuntimeError`` with a bounded code rather than returning a
        partial list: an empty dropdown is a much better failure than one that
        silently lost the branch an operator was looking for.
        """

        if not self._remote_is_approved():
            raise RuntimeError("unapproved_remote")
        listed = self._git("ls-remote", "--heads", "--", self.remote)
        if listed.returncode:
            raise RuntimeError("remote_unavailable")
        branches: dict[str, str] = {}
        for line in listed.stdout.splitlines():
            commit, _, reference = line.strip().partition("\t")
            commit = commit.strip().lower()
            name = reference.strip().removeprefix("refs/heads/")
            if reference.strip() == name or not OID_RE.fullmatch(commit):
                continue
            if not BRANCH_RE.fullmatch(name):
                # A name this module would refuse to hand to Git is not offered
                # as a choice either, so the dropdown and the executor agree.
                continue
            branches[name] = commit
        ordered = sorted(
            branches.items(),
            # Approved main first; everything else alphabetically.
            key=lambda item: (item[0] != APPROVED_BRANCH, item[0]),
        )
        return tuple(
            ApprovedBranch(name, commit)
            for name, commit in ordered[:MAX_APPROVED_BRANCHES]
        )

    def fetch_branch(self, branch: str) -> str | None:
        """Fetch one approved-remote branch and return its exact tip commit.

        The fetch is what makes the trial commit locally available; the tip is
        what a caller checks the requested commit against. Returns ``None``
        when the branch cannot be resolved from the approved remote at all.
        """

        if not BRANCH_RE.fullmatch(branch) or not self._remote_is_approved():
            return None
        fetched = self._git("fetch", "--no-tags", "--", self.remote, branch)
        if fetched.returncode:
            return None
        return self._resolve("FETCH_HEAD")

    def contains_commit(self, branch_tip: str, commit: str) -> bool:
        """Confirm an exact commit is contained in an approved branch tip."""

        return (
            OID_RE.fullmatch(branch_tip) is not None
            and OID_RE.fullmatch(commit) is not None
            and self._ancestor(commit, branch_tip)
        )

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
                "The source checkout was fast-forwarded. The running FCP "
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
