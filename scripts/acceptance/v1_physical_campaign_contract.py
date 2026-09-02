"""Checked-in evidence contract for the Federation v1 P01-P12 campaign."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final


@dataclass(frozen=True)
class ScenarioSpec:
    title: str
    assertions: dict[str, str]
    allow_na: frozenset[str] = field(default_factory=frozenset)
    assertion_os: dict[str, str] = field(default_factory=dict)
    minimum_elapsed_seconds: int = 0
    minimum_samples: int = 0


def _a(*items: tuple[str, str]) -> dict[str, str]:
    return dict(items)


SCENARIOS: Final[dict[str, ScenarioSpec]] = {
    "P01": ScenarioSpec(
        "Repeated Federation update growth and failure cleanup",
        _a(
            ("windows-resource-baseline", "Windows/Beast baseline covers host, Docker, data/results, logs and model storage."),
            ("posix-resource-baseline", "POSIX baseline covers host, Docker, data/results, logs, model storage and inode state."),
            ("windows-three-activations", "At least three supported Windows activations cover unchanged/distinct commits where meaningful."),
            ("posix-three-activations", "At least three supported POSIX activations cover unchanged/distinct commits where meaningful."),
            ("windows-failed-build-cleanup", "A failed/interrupted Windows build leaves bounded cache/resource state or safe pressure behavior."),
            ("posix-failed-build-cleanup", "A failed/interrupted POSIX build leaves bounded cache/resource state or safe pressure behavior."),
            ("windows-runtime-state", "Windows proves exact running commit and retained FCP state after activation."),
            ("posix-runtime-state", "POSIX proves exact running commit and retained FCP state after activation."),
            ("windows-growth-bounded", "Windows does not return to the historical multi-gigabyte-per-activation growth slope."),
            ("posix-growth-bounded", "POSIX does not show unbounded per-activation growth."),
        ),
        assertion_os={
            "windows-resource-baseline": "windows",
            "windows-three-activations": "windows",
            "windows-failed-build-cleanup": "windows",
            "windows-runtime-state": "windows",
            "windows-growth-bounded": "windows",
            "posix-resource-baseline": "posix",
            "posix-three-activations": "posix",
            "posix-failed-build-cleanup": "posix",
            "posix-runtime-state": "posix",
            "posix-growth-bounded": "posix",
        },
    ),
    "P02": ScenarioSpec(
        "Independent backing-resource exhaustion",
        _a(
            ("checkout-docker-pressure", "Checkout/build/Docker pressure refuses optional work safely."),
            ("data-pressure", "Data-volume pressure is surfaced without corrupting committed state."),
            ("results-pressure", "Results pressure refuses optional work before the emergency floor."),
            ("model-pressure", "Model/provider pressure refuses large optional writes safely."),
            ("inode-pressure", "Inode/file-capacity pressure is handled where exposed by the filesystem."),
            ("core-remains-usable", "The old healthy core remains usable while optional work is refused."),
        ),
        allow_na=frozenset({"inode-pressure"}),
    ),
    "P03": ScenarioSpec(
        "All supported start/update entry points and concurrency",
        _a(
            ("start-cmd", "start.cmd exercises the supported Windows start contract."),
            ("start-sh", "start.sh exercises the supported POSIX start contract."),
            ("start-tailscale-cmd", "start-tailscale.cmd exercises the supported Windows/tailnet path."),
            ("update-cmd-disposition", "update.cmd follows its final approved/retired disposition without bypassing update policy."),
            ("windows-concurrent-launchers", "Concurrent Windows launchers serialize host mutation safely."),
            ("posix-concurrent-launchers", "Concurrent POSIX launchers serialize host mutation safely."),
            ("launcher-vs-update", "Launcher versus pending/active update serializes source/build mutation."),
            ("identity-and-isolation", "Source/image/runtime identity, bounded build state, and model/network failure isolation are proven."),
        ),
        assertion_os={
            "start-cmd": "windows",
            "start-sh": "posix",
            "start-tailscale-cmd": "windows",
            "update-cmd-disposition": "windows",
            "windows-concurrent-launchers": "windows",
            "posix-concurrent-launchers": "posix",
        },
    ),
    "P04": ScenarioSpec(
        "Recorder finite transaction and disk pressure",
        _a(
            ("concurrent-sources", "Recorder exercises up to eight simultaneous sources or the supported configured maximum."),
            ("maximum-ingress", "Maximum accepted response, observation and sequence-span bounds are exercised."),
            ("aggregate-admission", "Aggregate admission leaves completion room for raw/manifest/observation/JSONL/checkpoint/status/journal writes."),
            ("pressure-state-ladder", "WARNING, PRESSURE and CRITICAL behavior is observed within the emergency reserve."),
            ("safe-pause", "Critical pressure pauses capture without deleting primary evidence."),
            ("recovery-continuity", "Restored capacity resumes sequence/checkpoint continuity without --fresh."),
        ),
    ),
    "P05": ScenarioSpec(
        "Service and failure injection",
        _a(
            ("flask-crash", "Flask crash is bounded and visible."),
            ("relay-crash", "Relay process crash is bounded and visible."),
            ("relay-stale-db-failure", "Relay stale-sweep database failure does not leave a running-but-dead service."),
            ("managed-recorder-crash", "Managed recorder crash is supervised without unrelated loss."),
            ("native-recorder-crash", "Native recorder child crash follows bounded supervision semantics."),
            ("publication-db-failure", "Publication database failure is surfaced/retried without losing durable work."),
            ("analysis-db-failure", "Analysis scheduler database failure is surfaced at the required-thread boundary."),
            ("poison-recorder-archive", "A poison recorder archive is isolated from later eligible work."),
            ("slow-trickle-response", "A slow-trickle MTConnect response is terminated by the finite deadline."),
            ("oversized-ingress", "Oversized MTConnect response/observation input is refused within finite bounds."),
            ("huge-sequence-gap", "Huge sequence discontinuity is handled without proportional allocation."),
            ("malformed-timestamp-path", "Malicious/malformed timestamp path components cannot escape recorder roots."),
            ("event-storm", "Repeated discontinuity evidence is deduplicated/rate-bounded."),
            ("ollama-absence", "Ollama/model absence degrades capability without removing core availability."),
            ("stale-responder-pid", "Stale responder PID reuse cannot target an unrelated process."),
            ("global-invariant", "Logs remain bounded, health visible, and no unrelated authority/data is lost."),
        ),
    ),
    "P06": ScenarioSpec(
        "Native recorder supervision contract",
        _a(
            ("unexpected-child-restart", "One unexpected child crash restarts with bounded backoff."),
            ("crash-loop-fence", "Repeated deterministic crashes reach the declared crash-loop fence."),
            ("operator-stop", "Operator Ctrl+C/stop does not restart the child."),
            ("update-trial-semantics", "Approved update/trial replacement semantics remain unchanged."),
            ("checkpoint-continuity", "Recorder checkpoint continuity survives supervised restart."),
            ("service-manager-boundary", "Supervisor crash/reboot behavior matches the declared external service-manager boundary."),
        ),
        allow_na=frozenset({"service-manager-boundary"}),
        assertion_os={
            "unexpected-child-restart": "windows",
            "crash-loop-fence": "windows",
            "operator-stop": "windows",
            "update-trial-semantics": "windows",
            "checkpoint-continuity": "windows",
            "service-manager-boundary": "windows",
        },
    ),
    "P07": ScenarioSpec(
        "Long Federation outage with aged corpus",
        _a(
            ("aged-corpus", "A non-trivial historical corpus/outbox exists before disconnecting Federation."),
            ("capture-continues", "Local capture/checkpoints continue during control-plane outage."),
            ("backlog-durable", "Publication backlog remains durable during outage."),
            ("latency-bounded", "Source polling and publication reconciliation latency remain bounded."),
            ("workers-alive", "No required worker silently dies during outage."),
            ("catchup-progress", "Reconnect catch-up makes continuous measurable forward progress."),
            ("poison-isolation", "A poison item cannot block later eligible work during catch-up."),
            ("duplicate-suppression", "Duplicate suppression remains correct after outage/reconnect."),
            ("restart-progress", "Restart during backlog does not lose durable progress."),
        ),
        minimum_elapsed_seconds=3600,
        minimum_samples=2,
    ),
    "P08": ScenarioSpec(
        "Logical storage exhaustion under host pressure",
        _a(
            ("allocation-floor", "A real storage authority reaches allocation/floor under the stricter host-pressure contract."),
            ("committed-reads", "Existing committed reads and control surfaces remain available at refusal."),
            ("no-partial-commit", "No partial storage commit becomes visible."),
            ("recovery-no-repair", "Restored capacity recovers without manual database/filesystem repair."),
        ),
    ),
    "P09": ScenarioSpec(
        "Durable-write crash windows, control-plane loss and clock skew",
        _a(
            ("raw-publication", "Interruption around raw temp/final publication recovers without false commit."),
            ("raw-manifest", "Interruption around raw manifest publication recovers safely."),
            ("observation-archive", "Interruption around observation archive publication recovers safely."),
            ("compat-jsonl", "Interruption around compatibility JSONL publication recovers safely."),
            ("checkpoint-status", "Interruption around checkpoint/status replacement preserves continuity."),
            ("outbox-transaction", "Interruption around outbox transaction preserves durable idempotency/progress."),
            ("analysis-slice", "Interruption around deterministic analysis slice cannot promote a truncated artifact."),
            ("upload-transitions", "Upload staging/database/publication crash windows reconcile without partial exposure."),
            ("sqlite-wal", "Coordinator/relay SQLite/WAL interruption preserves authoritative recovery."),
            ("coordinator-disappearance", "Coordinator disappearance cannot fabricate continuity or self-promote authority."),
            ("positive-clock-skew", "Bounded positive clock offset around storage lease expiry fails safely."),
            ("negative-clock-skew", "Bounded negative clock offset around storage lease expiry fails safely."),
            ("authority-projection", "No crash/loss/skew case yields silent partial authority projection."),
        ),
    ),
    "P10": ScenarioSpec(
        "Model installation through every product path",
        _a(
            ("normal-startup", "Required-model absence/pressure is exercised through normal startup."),
            ("federation-update", "Required-model absence/pressure is exercised through Federation update."),
            ("browser-install", "Required-model absence/pressure is exercised through browser-triggered installation."),
            ("provider-profile-install", "Required-model absence/pressure is exercised through provider/profile installation."),
            ("failure-isolation", "Model failure does not remove workbench/Federation/recorder/control availability."),
            ("emergency-floor", "No model pull crosses the host emergency resource floor."),
        ),
    ),
    "P11": ScenarioSpec(
        "Exact-candidate backup and restore",
        _a(
            ("external-destination", "Backup destination is independent and preflighted before quiescence."),
            ("helper-prestaged", "Any helper needed after quiescence is available before services stop."),
            ("writers-fenced", "Compose and relevant host writers are fenced and quiescence is proven."),
            ("update-refused", "Concurrent update/host mutation is refused while backup owns the host mutation boundary."),
            ("failed-copy-safe", "A deliberate copy/verify failure leaves FCP stopped and primary state intact."),
            ("successful-backup", "A successful exact-candidate backup completes with manifest/source identity."),
            ("sqlite-integrity", "Every applicable restored SQLite database passes integrity/quick-check."),
            ("isolated-restore", "Restore is performed in isolation into clean destination resources."),
            ("identity-continuity", "Same-installation device/Federation/auth/recorder continuity is verified where applicable."),
            ("core-no-model-download", "Core recovery succeeds without requiring model download."),
            ("replacement-identity", "Replacement-member recovery uses a new identity rather than cloning member authority."),
            ("windows-dpapi", "Windows DPAPI identity boundary is exercised where applicable."),
        ),
        allow_na=frozenset({"windows-dpapi"}),
    ),
    "P12": ScenarioSpec(
        "Aged-history 24-hour soak plus accelerated ceiling tests",
        _a(
            ("aged-history", "The soak begins with meaningful history rather than an empty installation."),
            ("storage-series", "Free bytes and inode/file counts are sampled for relevant backing resources."),
            ("docker-series", "Docker root/VHDX, cache, image and log growth is sampled."),
            ("recorder-series", "Recorder corpus growth and per-poll recovery time are sampled."),
            ("publication-series", "Publication reconciliation duration/progress and outbox backlog are sampled."),
            ("history-series", "Session/member/job/attempt/command/grant/artifact/provider/update history and DB sizes are sampled."),
            ("orphan-series", "Orphan staging/workspaces and required-thread/process/restart health are sampled."),
            ("cpu-ram-series", "CPU/RAM observations are sufficient to identify concrete leaks or OOM isolation defects."),
            ("accelerated-ceilings", "Accelerated tests cross the known authority/history page ceilings."),
            ("no-unexplained-growth", "The completed soak has no unexplained growth, restart amplification, or backlog slope."),
        ),
        minimum_elapsed_seconds=24 * 60 * 60,
        minimum_samples=2,
    ),
}
