"""Explicit local bindings between the P01-P12 harness and a deployment.

The acceptance harness checkout and the product checkout/runtime are allowed to
be different.  This module is the deliberately small, operator-authored bridge
between them.  It never discovers containers or data roots heuristically: a
binding must name the exact Compose project/configuration or native recorder
status/data surfaces and must pin the target candidate SHA.

Binding files are local control-plane input.  Callers must only expose the
redacted summary returned by :func:`public_summary` in portable evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

BINDING_SCHEMA: Final = "fcp.v1.physical-runtime-binding.v1"
HOST_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,47}")
SHA_RE: Final = re.compile(r"[0-9a-f]{40}")


class RuntimeBindingError(ValueError):
    """A local runtime binding is malformed or cannot be proven."""


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeBindingError(f"binding field {field} must be non-empty text")
    return value.strip()


def _sha(value: object, field: str) -> str:
    result = _text(value, field).lower()
    if SHA_RE.fullmatch(result) is None:
        raise RuntimeBindingError(f"binding field {field} must be a 40-character SHA")
    return result


def _path(value: object, field: str) -> Path:
    result = Path(_text(value, field)).expanduser()
    if not result.is_absolute():
        raise RuntimeBindingError(f"binding field {field} must be an absolute path")
    return result


def _paths(value: object, field: str) -> tuple[Path, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise RuntimeBindingError(f"binding field {field} must be a non-empty list")
    result = tuple(_path(item, field) for item in value)
    if not result:
        raise RuntimeBindingError(f"binding field {field} must be a non-empty list")
    return result


@dataclass(frozen=True)
class RuntimeBinding:
    host_id: str
    target_candidate_sha: str
    acceptance_harness_sha: str
    harness_checkout: Path
    kind: str
    compose_project: str | None = None
    compose_working_directory: Path | None = None
    compose_config_files: tuple[Path, ...] = ()
    data_root: Path | None = None
    results_root: Path | None = None
    recorder_status_file: Path | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> RuntimeBinding:
        if value.get("schema") != BINDING_SCHEMA:
            raise RuntimeBindingError("runtime binding has an invalid schema")
        host_id = _text(value.get("host_id"), "host_id")
        if HOST_RE.fullmatch(host_id) is None:
            raise RuntimeBindingError("binding host_id is not a safe alias")
        kind = _text(value.get("runtime_kind"), "runtime_kind").casefold()
        if kind not in {"compose", "native-recorder"}:
            raise RuntimeBindingError("runtime_kind must be compose or native-recorder")
        runtime = value.get("runtime")
        if not isinstance(runtime, Mapping):
            raise RuntimeBindingError("binding runtime must be an object")
        binding = cls(
            host_id=host_id,
            target_candidate_sha=_sha(value.get("target_candidate_sha"), "target_candidate_sha"),
            acceptance_harness_sha=_sha(
                value.get("acceptance_harness_sha"), "acceptance_harness_sha"
            ),
            harness_checkout=_path(value.get("harness_checkout"), "harness_checkout"),
            kind=kind,
            compose_project=(
                _text(runtime.get("project"), "runtime.project") if kind == "compose" else None
            ),
            compose_working_directory=(
                _path(runtime.get("working_directory"), "runtime.working_directory")
                if kind == "compose"
                else None
            ),
            compose_config_files=(
                _paths(runtime.get("config_files"), "runtime.config_files")
                if kind == "compose"
                else ()
            ),
            data_root=(
                _path(runtime.get("data_root"), "runtime.data_root")
                if runtime.get("data_root") is not None
                else None
            ),
            results_root=(
                _path(runtime.get("results_root"), "runtime.results_root")
                if runtime.get("results_root") is not None
                else None
            ),
            recorder_status_file=(
                _path(runtime.get("recorder_status_file"), "runtime.recorder_status_file")
                if runtime.get("recorder_status_file") is not None
                else None
            ),
        )
        if kind == "native-recorder" and binding.recorder_status_file is None:
            raise RuntimeBindingError(
                "native-recorder binding must name recorder_status_file"
            )
        return binding

    def verify_harness_checkout(self) -> None:
        """Prove the external harness is the exact declared SHA, and clean.

        Identity alone is not enough.  ``rev-parse HEAD`` still reports the
        declared commit when the worktree carries uncommitted edits, so a
        harness proven only by HEAD can execute probe code that exists in no
        commit while the evidence claims a reviewed SHA.  Both halves are
        therefore required and both fail closed: an unreadable checkout is
        refused exactly like a mismatched or dirty one.
        """

        code, output = _git(self.harness_checkout, "rev-parse", "HEAD")
        if code != 0 or output.strip().casefold() != self.acceptance_harness_sha:
            raise RuntimeBindingError(
                "acceptance harness checkout does not match acceptance_harness_sha"
            )
        code, output = _git(self.harness_checkout, "status", "--porcelain")
        if code != 0:
            raise RuntimeBindingError(
                "acceptance harness checkout cleanliness could not be proven"
            )
        if output.strip():
            raise RuntimeBindingError(
                "acceptance harness checkout has uncommitted changes"
            )

    def compose_prefix(self) -> list[str]:
        if self.kind != "compose":
            raise RuntimeBindingError("Compose command requested for native binding")
        assert self.compose_project is not None
        assert self.compose_working_directory is not None
        files: list[str] = []
        for path in self.compose_config_files:
            files.extend(["--file", str(path)])
        return [
            "docker",
            "compose",
            "--project-name",
            self.compose_project,
            "--project-directory",
            str(self.compose_working_directory),
            *files,
        ]


def _git(cwd: Path, *args: str) -> tuple[int, str]:
    """Run git in ``cwd`` and return its exit code and standard output.

    Standard error is deliberately not folded into the returned text.  The
    cleanliness gate below compares the output of ``status --porcelain``
    against the empty string, and a git advisory printed to stderr on an
    otherwise clean tree would masquerade as a modified file.
    """

    try:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.SubprocessError):
        return 126, ""
    return result.returncode, result.stdout or ""


def load(path: Path, *, host_id: str, target_candidate_sha: str) -> RuntimeBinding:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeBindingError(f"cannot read runtime binding: {path}") from exc
    if not isinstance(document, Mapping):
        raise RuntimeBindingError("runtime binding must be an object")
    binding = RuntimeBinding.from_mapping(document)
    if binding.host_id != host_id:
        raise RuntimeBindingError("runtime binding host does not match requested host")
    if binding.target_candidate_sha != target_candidate_sha.casefold():
        raise RuntimeBindingError("runtime binding targets a different candidate")
    binding.verify_harness_checkout()
    return binding


def _digest(value: Path | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8", errors="replace")).hexdigest()[:16]


def public_summary(binding: RuntimeBinding) -> dict[str, object]:
    """Return binding evidence without publishing private paths or endpoints."""

    result: dict[str, object] = {
        "explicit": True,
        "runtime_kind": binding.kind,
        "target_candidate_sha": binding.target_candidate_sha,
        "acceptance_harness_sha": binding.acceptance_harness_sha,
        "harness_checkout_digest": _digest(binding.harness_checkout),
    }
    if binding.kind == "compose":
        result.update(
            {
                "compose_project_alias": hashlib.sha256(
                    str(binding.compose_project).encode("utf-8")
                ).hexdigest()[:12],
                "compose_working_directory_digest": _digest(
                    binding.compose_working_directory
                ),
                "compose_config_digests": [
                    _digest(path) for path in binding.compose_config_files
                ],
            }
        )
    result.update(
        {
            "data_root_digest": _digest(binding.data_root),
            "results_root_digest": _digest(binding.results_root),
            "recorder_status_file_digest": _digest(binding.recorder_status_file),
        }
    )
    return result
