"""Checked-in automation classification for every Federation v1 P01-P12 assertion.

Each assertion in ``v1_physical_campaign_contract`` belongs to exactly one lane:

``automated-probe``
    A checked-in probe proves the assertion end to end on the target host.

``operator-fault-injection``
    PREPARE and VERIFY are automated; the fault itself stays an explicit,
    deliberate operator action between them. The harness never performs the
    fault, and preparation output can never close the assertion.

``human-observation``
    The proof genuinely requires a person, a browser/device, an external service
    manager boundary, or a platform surface the harness must not simulate.

This module is data, not behaviour: the runner reads it, the report reads it, and
the tests assert that it stays exactly aligned with the evidence contract. If a
new assertion is added to the contract without a lane here, import fails.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from scripts.acceptance.v1_physical_campaign_contract import SCENARIOS
from scripts.acceptance.v1_physical_probes import PROBES

AUTOMATED: Final = "automated-probe"
FAULT_INJECTION: Final = "operator-fault-injection"
HUMAN: Final = "human-observation"
CLASSIFICATIONS: Final = frozenset({AUTOMATED, FAULT_INJECTION, HUMAN})


@dataclass(frozen=True)
class ProbeBinding:
    """One checked-in probe with the exact options this assertion needs.

    ``options`` are fixed by the contract. ``required`` names options only the
    operator can supply -- a restored destination, a completed activation count.
    The runner refuses the assertion until they are given, so a probe can never
    quietly answer about the wrong thing (the live data directory instead of the
    restored copy, say) because an argument was left off.
    """

    probe_id: str
    options: tuple[tuple[str, str], ...] = ()
    required: tuple[str, ...] = ()

    def option_map(self) -> dict[str, str]:
        return dict(self.options)


def _p(probe_id: str, *, require: tuple[str, ...] = (), **options: str) -> ProbeBinding:
    return ProbeBinding(probe_id, tuple(sorted(options.items())), tuple(sorted(require)))


@dataclass(frozen=True)
class AssertionPlan:
    classification: str
    probes: tuple[ProbeBinding, ...] = ()
    prepare: tuple[ProbeBinding, ...] = ()
    verify: tuple[ProbeBinding, ...] = ()
    operator_action: str = ""
    reviewed_helper: str = ""
    rationale: str = ""

    @property
    def automated(self) -> bool:
        return self.classification == AUTOMATED

    @property
    def needs_operator_action(self) -> bool:
        return self.classification == FAULT_INJECTION

    def all_bindings(self) -> tuple[ProbeBinding, ...]:
        return tuple(
            binding
            for group in (self.probes, self.prepare, self.verify)
            for binding in group
        )

    def all_probe_ids(self) -> tuple[str, ...]:
        return tuple(binding.probe_id for binding in self.all_bindings())

    def required_options(self) -> frozenset[str]:
        return frozenset(
            name for binding in self.all_bindings() for name in binding.required
        )


def _auto(*probes: ProbeBinding) -> AssertionPlan:
    return AssertionPlan(AUTOMATED, probes=probes)


def _fault(
    *,
    prepare: tuple[ProbeBinding, ...],
    verify: tuple[ProbeBinding, ...],
    action: str,
    helper: str = "",
) -> AssertionPlan:
    return AssertionPlan(
        FAULT_INJECTION,
        prepare=prepare,
        verify=verify,
        operator_action=action,
        reviewed_helper=helper,
    )


def _human(rationale: str) -> AssertionPlan:
    return AssertionPlan(HUMAN, rationale=rationale)


_HEALTH = _p("service-health")
_CORE = _p("core-availability")
_RESOURCES = _p("host-resource-baseline")
_DOCKER = _p("docker-state")
_RUNTIME = _p("running-commit-identity")
_BACKLOG = _p("publication-backlog")
_IDEMPOTENCY = _p("publication-idempotency")
_INTEGRITY = _p("sqlite-integrity")
_RECORDER = _p("recorder-continuity")
_LIMITS = _p("recorder-limits")
_CHECKOUT = _p("checkout-identity")


PLANS: Final[dict[str, dict[str, AssertionPlan]]] = {
    "P01": {
        "windows-resource-baseline": _auto(_RESOURCES, _DOCKER),
        "posix-resource-baseline": _auto(_RESOURCES, _DOCKER, _p("inode-capacity")),
        "windows-three-activations": _fault(
            prepare=(_RESOURCES, _DOCKER, _RUNTIME),
            verify=(_p("activation-growth", require=("activations",)), _RUNTIME),
            action=(
                "Perform three supported Federation activations on this host, "
                "covering unchanged and distinct commits where meaningful, and "
                "record one campaign resource sample after each activation."
            ),
        ),
        "posix-three-activations": _fault(
            prepare=(_RESOURCES, _DOCKER, _RUNTIME),
            verify=(_p("activation-growth", require=("activations",)), _RUNTIME),
            action=(
                "Perform three supported Federation activations on this host, "
                "covering unchanged and distinct commits where meaningful, and "
                "record one campaign resource sample after each activation."
            ),
        ),
        "windows-failed-build-cleanup": _fault(
            prepare=(_RESOURCES, _DOCKER),
            verify=(_DOCKER, _RESOURCES, _CHECKOUT),
            action=(
                "Interrupt or fail one Federation build on this host, then let "
                "the product settle before verifying."
            ),
        ),
        "posix-failed-build-cleanup": _fault(
            prepare=(_RESOURCES, _DOCKER),
            verify=(_DOCKER, _RESOURCES, _CHECKOUT),
            action=(
                "Interrupt or fail one Federation build on this host, then let "
                "the product settle before verifying."
            ),
        ),
        "windows-runtime-state": _auto(_CHECKOUT, _RUNTIME, _HEALTH),
        "posix-runtime-state": _auto(_CHECKOUT, _RUNTIME, _HEALTH),
        "windows-growth-bounded": _auto(_p("growth-analysis")),
        "posix-growth-bounded": _auto(_p("growth-analysis")),
    },
    "P02": {
        "checkout-docker-pressure": _fault(
            prepare=(_RESOURCES, _DOCKER),
            verify=(_p("safe-storage-refusal"), _CORE),
            action=(
                "Bring the checkout/build/Docker backing resource to real "
                "PRESSURE with an external filler, then attempt optional work."
            ),
        ),
        "data-pressure": _fault(
            prepare=(_RESOURCES,),
            verify=(_p("safe-storage-refusal"), _INTEGRITY, _CORE),
            action=(
                "Bring the data volume to real PRESSURE with an external filler, "
                "then attempt an optional data write."
            ),
        ),
        "results-pressure": _fault(
            prepare=(_RESOURCES,),
            verify=(_p("safe-storage-refusal"), _CORE),
            action=(
                "Bring the results resource to real PRESSURE with an external "
                "filler, then attempt optional results work."
            ),
        ),
        "model-pressure": _fault(
            prepare=(_RESOURCES, _DOCKER),
            verify=(_p("model-pull-floor"), _CORE),
            action=(
                "Bring the model/provider backing resource to real PRESSURE, "
                "then request a model installation through the product."
            ),
        ),
        "inode-pressure": _fault(
            prepare=(_p("inode-capacity"),),
            verify=(_p("safe-storage-refusal"), _CORE),
            action=(
                "Exhaust inode/file capacity on a filesystem that exposes it, "
                "then attempt optional work. Record not-applicable only when no "
                "measured filesystem exposes inode accounting."
            ),
        ),
        "core-remains-usable": _auto(_p("safe-storage-refusal"), _CORE),
    },
    "P03": {
        "start-cmd": _fault(
            prepare=(_CHECKOUT, _DOCKER),
            verify=(_RUNTIME, _HEALTH),
            action="Run start.cmd on this Windows host and let it complete.",
        ),
        "start-sh": _fault(
            prepare=(_CHECKOUT, _DOCKER),
            verify=(_RUNTIME, _HEALTH),
            action="Run start.sh on this POSIX host and let it complete.",
        ),
        "start-tailscale-cmd": _fault(
            prepare=(_CHECKOUT, _DOCKER),
            verify=(_RUNTIME, _HEALTH),
            action=(
                "Run start-tailscale.cmd on this Windows host over the tailnet "
                "path and let it complete."
            ),
        ),
        "update-cmd-disposition": _auto(
            _p("update-entrypoint-disposition"),
            _p("update-status"),
        ),
        "windows-concurrent-launchers": _auto(_p("host-mutation-serialization")),
        "posix-concurrent-launchers": _auto(_p("host-mutation-serialization")),
        "launcher-vs-update": _auto(
            _p("host-mutation-serialization"),
            _p("update-status"),
        ),
        "identity-and-isolation": _fault(
            prepare=(_RUNTIME, _DOCKER, _HEALTH),
            verify=(_CORE, _RUNTIME),
            action=(
                "Make the language-model provider and its network path "
                "unreachable, then exercise the workbench and Federation "
                "surfaces before verifying."
            ),
        ),
    },
    "P04": {
        "concurrent-sources": _fault(
            prepare=(_LIMITS, _RECORDER),
            verify=(_RECORDER, _HEALTH),
            action=(
                "Drive the recorder with eight simultaneous sources, or the "
                "supported configured maximum, and keep them running."
            ),
        ),
        "maximum-ingress": _fault(
            prepare=(_LIMITS,),
            verify=(_RECORDER, _p("bounded-logs-and-health")),
            action=(
                "Serve maximum accepted response, observation and sequence-span "
                "input to the recorder from a test agent."
            ),
        ),
        "aggregate-admission": _fault(
            prepare=(_LIMITS, _RESOURCES),
            verify=(_p("safe-storage-refusal"), _RECORDER),
            action=(
                "Drive concurrent recorder transactions while the backing "
                "resource is near its declared reserve."
            ),
        ),
        "pressure-state-ladder": _fault(
            prepare=(_RESOURCES,),
            verify=(_p("safe-storage-refusal"), _RECORDER, _HEALTH),
            action=(
                "Walk the recorder resource down through WARNING, PRESSURE and "
                "CRITICAL with an external filler, observing each state."
            ),
        ),
        "safe-pause": _fault(
            prepare=(_RECORDER,),
            verify=(_RECORDER, _p("corpus-size", subject="recorder")),
            action=(
                "Hold the recorder at CRITICAL pressure until capture pauses, "
                "without deleting any primary evidence."
            ),
        ),
        "recovery-continuity": _fault(
            prepare=(_RECORDER,),
            verify=(_RECORDER, _HEALTH),
            action=(
                "Restore capacity and let the recorder resume without --fresh."
            ),
        ),
    },
    "P05": {
        "flask-crash": _fault(
            prepare=(_HEALTH,),
            verify=(_HEALTH, _CORE),
            action="Kill the Flask process and let supervision recover it.",
        ),
        "relay-crash": _fault(
            prepare=(_HEALTH,),
            verify=(_HEALTH, _CORE),
            action="Kill the relay process and let supervision recover it.",
        ),
        "relay-stale-db-failure": _fault(
            prepare=(_HEALTH, _INTEGRITY),
            verify=(_HEALTH, _INTEGRITY),
            action=(
                "Make the relay stale-sweep database fail (revoke access or "
                "move it aside) while the relay is running."
            ),
        ),
        "managed-recorder-crash": _fault(
            prepare=(_HEALTH, _RECORDER),
            verify=(_HEALTH, _RECORDER),
            action="Kill the managed recorder process and let it be supervised.",
        ),
        "native-recorder-crash": _fault(
            prepare=(_HEALTH, _RECORDER),
            verify=(_HEALTH, _RECORDER),
            action="Kill the native recorder child process.",
        ),
        "publication-db-failure": _fault(
            prepare=(_BACKLOG, _INTEGRITY),
            verify=(_BACKLOG, _INTEGRITY, _IDEMPOTENCY),
            action="Make the publication database unavailable while work is queued.",
        ),
        "analysis-db-failure": _fault(
            prepare=(_HEALTH, _INTEGRITY),
            verify=(_HEALTH, _INTEGRITY),
            action=(
                "Make the analysis scheduler database fail at the required-thread "
                "boundary."
            ),
        ),
        "poison-recorder-archive": _fault(
            prepare=(_BACKLOG,),
            verify=(_BACKLOG, _HEALTH),
            action=(
                "Introduce one poison recorder archive, then queue later eligible "
                "work behind it."
            ),
        ),
        "slow-trickle-response": _fault(
            prepare=(_LIMITS,),
            verify=(_HEALTH, _p("bounded-logs-and-health")),
            action="Serve a slow-trickle MTConnect response from a test agent.",
        ),
        "oversized-ingress": _fault(
            prepare=(_LIMITS,),
            verify=(_HEALTH, _p("bounded-logs-and-health")),
            action=(
                "Serve oversized MTConnect response/observation input from a "
                "test agent."
            ),
        ),
        "huge-sequence-gap": _fault(
            prepare=(_LIMITS, _RECORDER),
            verify=(_RECORDER, _HEALTH),
            action="Serve a huge sequence discontinuity from a test agent.",
        ),
        "malformed-timestamp-path": _auto(_p("recorder-path-containment")),
        "event-storm": _fault(
            prepare=(_p("bounded-logs-and-health"),),
            verify=(_p("bounded-logs-and-health"), _HEALTH),
            action=(
                "Drive repeated discontinuity events from a test agent until the "
                "deduplication/rate bound is observable."
            ),
        ),
        "ollama-absence": _fault(
            prepare=(_HEALTH,),
            verify=(_CORE, _HEALTH),
            action="Stop or remove the Ollama/model service on this host.",
        ),
        "stale-responder-pid": _fault(
            prepare=(_HEALTH,),
            verify=(_HEALTH, _CORE),
            action=(
                "Leave a stale responder PID record behind and let an unrelated "
                "process occupy that PID."
            ),
        ),
        "global-invariant": _auto(_p("bounded-logs-and-health"), _CORE, _INTEGRITY),
    },
    "P06": {
        "unexpected-child-restart": _fault(
            prepare=(_HEALTH, _RECORDER),
            verify=(_HEALTH, _RECORDER),
            action="Crash the supervised native recorder child once.",
        ),
        "crash-loop-fence": _fault(
            prepare=(_HEALTH,),
            verify=(_HEALTH,),
            action=(
                "Make the native recorder child fail deterministically until the "
                "declared crash-loop fence is reached."
            ),
        ),
        "operator-stop": _fault(
            prepare=(_HEALTH,),
            verify=(_HEALTH,),
            action="Stop the supervisor with Ctrl+C and leave it stopped.",
        ),
        "update-trial-semantics": _fault(
            prepare=(_RUNTIME, _p("update-status")),
            verify=(_RUNTIME, _p("update-status")),
            action=(
                "Run one approved update/trial replacement through the supported "
                "product path."
            ),
        ),
        "checkpoint-continuity": _fault(
            prepare=(_RECORDER,),
            verify=(_RECORDER,),
            action="Force one supervised restart of the native recorder child.",
        ),
        "service-manager-boundary": _human(
            "Supervisor crash/reboot behaviour is owned by the external Windows "
            "service manager. Observing it requires a real reboot and the "
            "platform's own service surface, which this harness must not "
            "simulate. Record not-applicable when no external service manager is "
            "declared for this host."
        ),
    },
    "P07": {
        "aged-corpus": _auto(_p("corpus-size", subject="recorder"), _BACKLOG),
        "capture-continues": _fault(
            prepare=(_RECORDER, _HEALTH),
            verify=(_RECORDER, _HEALTH),
            action=(
                "Disconnect the Federation control plane and keep it down for the "
                "full timed outage."
            ),
        ),
        "backlog-durable": _fault(
            prepare=(_BACKLOG,),
            verify=(_BACKLOG, _INTEGRITY),
            action="Keep the control plane down while publication work accumulates.",
        ),
        "latency-bounded": _fault(
            prepare=(_BACKLOG, _RECORDER),
            verify=(_BACKLOG, _RECORDER),
            action=(
                "Keep source polling and publication reconciliation running "
                "during the outage."
            ),
        ),
        "workers-alive": _fault(
            prepare=(_HEALTH,),
            verify=(_HEALTH,),
            action="Keep every required worker running for the full outage.",
        ),
        "catchup-progress": _fault(
            prepare=(_BACKLOG,),
            verify=(_BACKLOG, _HEALTH),
            action="Reconnect the control plane and let catch-up run.",
        ),
        "poison-isolation": _fault(
            prepare=(_BACKLOG,),
            verify=(_BACKLOG, _HEALTH),
            action=(
                "Place one poison item ahead of later eligible work before "
                "reconnecting."
            ),
        ),
        "duplicate-suppression": _fault(
            prepare=(_IDEMPOTENCY,),
            verify=(_IDEMPOTENCY, _BACKLOG),
            action="Complete the reconnect catch-up so suppression is exercised.",
        ),
        "restart-progress": _fault(
            prepare=(_BACKLOG,),
            verify=(_BACKLOG, _INTEGRITY, _HEALTH),
            action="Restart the product while the backlog is still draining.",
        ),
    },
    "P08": {
        "allocation-floor": _fault(
            prepare=(_RESOURCES, _BACKLOG),
            verify=(_p("safe-storage-refusal"), _BACKLOG),
            action=(
                "Drive a real storage authority to its allocation floor under "
                "host pressure with an external filler."
            ),
        ),
        "committed-reads": _fault(
            prepare=(_CORE,),
            verify=(_CORE,),
            action="Hold the storage authority at refusal while reads are attempted.",
        ),
        "no-partial-commit": _fault(
            prepare=(_INTEGRITY, _IDEMPOTENCY),
            verify=(_INTEGRITY, _IDEMPOTENCY),
            action="Attempt a storage commit that must be refused at the floor.",
        ),
        "recovery-no-repair": _fault(
            prepare=(_INTEGRITY,),
            verify=(_INTEGRITY, _HEALTH, _CORE),
            action=(
                "Restore capacity and let the product recover without manual "
                "database or filesystem repair."
            ),
        ),
    },
    "P09": {
        "raw-publication": _fault(
            prepare=(_INTEGRITY, _RECORDER),
            verify=(_INTEGRITY, _RECORDER),
            action="Interrupt the product around raw temp/final publication.",
        ),
        "raw-manifest": _fault(
            prepare=(_INTEGRITY, _RECORDER),
            verify=(_INTEGRITY, _RECORDER),
            action="Interrupt the product around raw manifest publication.",
        ),
        "observation-archive": _fault(
            prepare=(_INTEGRITY, _RECORDER),
            verify=(_INTEGRITY, _RECORDER),
            action="Interrupt the product around observation archive publication.",
        ),
        "compat-jsonl": _fault(
            prepare=(_INTEGRITY, _RECORDER),
            verify=(_INTEGRITY, _RECORDER),
            action="Interrupt the product around compatibility JSONL publication.",
        ),
        "checkpoint-status": _fault(
            prepare=(_RECORDER,),
            verify=(_RECORDER,),
            action="Interrupt the product around checkpoint/status replacement.",
        ),
        "outbox-transaction": _fault(
            prepare=(_BACKLOG, _IDEMPOTENCY),
            verify=(_BACKLOG, _IDEMPOTENCY, _INTEGRITY),
            action="Interrupt the product inside an outbox transaction.",
        ),
        "analysis-slice": _fault(
            prepare=(_INTEGRITY,),
            verify=(_INTEGRITY, _CORE),
            action="Interrupt the product inside a deterministic analysis slice.",
        ),
        "upload-transitions": _fault(
            prepare=(_INTEGRITY, _BACKLOG),
            verify=(_INTEGRITY, _BACKLOG),
            action=(
                "Interrupt the product across upload staging, database and "
                "publication transitions."
            ),
        ),
        "sqlite-wal": _fault(
            prepare=(_INTEGRITY,),
            verify=(_INTEGRITY, _HEALTH),
            action="Interrupt the coordinator/relay during SQLite/WAL work.",
        ),
        "coordinator-disappearance": _fault(
            prepare=(_HEALTH, _INTEGRITY),
            verify=(_HEALTH, _INTEGRITY, _IDEMPOTENCY),
            action="Remove the coordinator while members remain running.",
        ),
        "positive-clock-skew": _fault(
            prepare=(_HEALTH, _INTEGRITY),
            verify=(_HEALTH, _INTEGRITY),
            action=(
                "Apply a bounded positive clock offset around storage lease "
                "expiry, then restore the clock."
            ),
        ),
        "negative-clock-skew": _fault(
            prepare=(_HEALTH, _INTEGRITY),
            verify=(_HEALTH, _INTEGRITY),
            action=(
                "Apply a bounded negative clock offset around storage lease "
                "expiry, then restore the clock."
            ),
        ),
        "authority-projection": _fault(
            prepare=(_INTEGRITY, _IDEMPOTENCY, _HEALTH),
            verify=(_INTEGRITY, _IDEMPOTENCY, _HEALTH, _CORE),
            action=(
                "After the crash, loss and skew cases above, leave the "
                "installation settled for a final authority review."
            ),
        ),
    },
    "P10": {
        "normal-startup": _fault(
            prepare=(_RESOURCES, _DOCKER),
            verify=(_CORE, _HEALTH, _p("model-pull-floor")),
            action=(
                "Remove or pressure the required model, then start the product "
                "normally."
            ),
        ),
        "federation-update": _fault(
            prepare=(_RESOURCES, _p("update-status")),
            verify=(_CORE, _HEALTH),
            action=(
                "Remove or pressure the required model, then run a Federation "
                "update through the supported product path."
            ),
        ),
        "browser-install": _human(
            "Browser-triggered model installation is proven by watching a real "
            "desktop/mobile browser drive the install surface. No local process "
            "exit code can stand in for that observation."
        ),
        "provider-profile-install": _fault(
            prepare=(_RESOURCES,),
            verify=(_CORE, _HEALTH),
            action=(
                "Remove or pressure the required model, then install through the "
                "provider/profile path."
            ),
        ),
        "failure-isolation": _fault(
            prepare=(_HEALTH,),
            verify=(_CORE, _HEALTH),
            action="Make the model provider fail while the product is running.",
        ),
        "emergency-floor": _auto(_p("model-pull-floor"), _RESOURCES),
    },
    "P11": {
        "external-destination": _auto(
            _p("backup-preflight", require=("destination",)),
        ),
        "helper-prestaged": _auto(_p("backup-helper-prestage")),
        "writers-fenced": _fault(
            prepare=(_HEALTH, _DOCKER),
            verify=(_DOCKER, _HEALTH),
            action=(
                "Fence Compose and the relevant host writers and prove quiescence."
            ),
            helper="python -m catalog.federation.backup_recovery",
        ),
        "update-refused": _fault(
            prepare=(_p("host-mutation-serialization"), _p("update-status")),
            verify=(_p("update-status"), _CHECKOUT),
            action=(
                "Attempt a concurrent update/host mutation while the backup owns "
                "the host mutation boundary."
            ),
        ),
        "failed-copy-safe": _fault(
            prepare=(_HEALTH, _INTEGRITY),
            verify=(_INTEGRITY, _CHECKOUT),
            action=(
                "Force one copy/verify failure during a backup and leave FCP "
                "stopped."
            ),
            helper="python -m catalog.federation.backup_recovery",
        ),
        "successful-backup": _fault(
            prepare=(
                _p("backup-preflight", require=("destination",)),
                _p("backup-helper-prestage"),
            ),
            verify=(_p("sqlite-integrity", require=("path",)), _CHECKOUT),
            action=(
                "Run one exact-candidate quiesced backup to the preflighted "
                "destination."
            ),
            helper="python -m catalog.federation.backup_recovery",
        ),
        "sqlite-integrity": _auto(_p("sqlite-integrity", require=("path",))),
        "isolated-restore": _fault(
            prepare=(_p("backup-preflight", require=("destination",)),),
            verify=(_p("sqlite-integrity", require=("path",)),),
            action=(
                "Restore into clean, isolated destination resources on a separate "
                "installation."
            ),
            helper="python -m catalog.federation.backup_recovery",
        ),
        "identity-continuity": _fault(
            prepare=(_HEALTH, _RUNTIME),
            verify=(_HEALTH, _RUNTIME, _RECORDER),
            action=(
                "Start the restored same-installation copy and exercise device, "
                "Federation, auth and recorder continuity."
            ),
        ),
        "core-no-model-download": _fault(
            prepare=(_RESOURCES,),
            verify=(_CORE, _HEALTH, _p("model-pull-floor")),
            action=(
                "Bring the restored core up with no model provider reachable and "
                "no model download permitted."
            ),
        ),
        "replacement-identity": _fault(
            prepare=(_HEALTH,),
            verify=(_HEALTH, _RUNTIME),
            action=(
                "Recover a replacement member using a new identity rather than "
                "cloning member authority."
            ),
        ),
        "windows-dpapi": _human(
            "The Windows DPAPI identity boundary is a platform credential "
            "surface. Proving it requires operating the real Windows user/"
            "machine key boundary, which this harness must not exercise. Record "
            "not-applicable where DPAPI is not part of this host's identity."
        ),
    },
    "P12": {
        "aged-history": _auto(_p("corpus-size", subject="history"), _BACKLOG),
        "storage-series": _auto(_p("campaign-series", series="storage")),
        "docker-series": _auto(_p("campaign-series", series="docker")),
        "recorder-series": _auto(_p("campaign-series", series="recorder")),
        "publication-series": _auto(_p("campaign-series", series="publication")),
        "history-series": _auto(_p("campaign-series", series="history")),
        "orphan-series": _auto(_p("campaign-series", series="orphan")),
        "cpu-ram-series": _auto(_p("campaign-series", series="cpu_ram")),
        "accelerated-ceilings": _fault(
            prepare=(_INTEGRITY, _BACKLOG),
            verify=(_INTEGRITY, _BACKLOG, _HEALTH),
            action=(
                "Run the accelerated tests that cross the known authority and "
                "history page ceilings."
            ),
        ),
        "no-unexplained-growth": _auto(_p("growth-analysis")),
    },
}


def _validate() -> None:
    """Refuse to import a classification that has drifted from the contract."""

    if set(PLANS) != set(SCENARIOS):
        raise RuntimeError(
            "automation classification does not cover exactly P01-P12"
        )
    for scenario, spec in SCENARIOS.items():
        plans = PLANS[scenario]
        if set(plans) != set(spec.assertions):
            raise RuntimeError(
                f"{scenario} automation classification does not match its assertions"
            )
        for assertion, plan in plans.items():
            if plan.classification not in CLASSIFICATIONS:
                raise RuntimeError(f"{scenario}/{assertion} has an unknown lane")
            for probe_id in plan.all_probe_ids():
                if probe_id not in PROBES:
                    raise RuntimeError(
                        f"{scenario}/{assertion} refers to unknown probe {probe_id}"
                    )
            if plan.classification == AUTOMATED:
                if not plan.probes or plan.prepare or plan.verify:
                    raise RuntimeError(
                        f"{scenario}/{assertion} must declare only automated probes"
                    )
                if plan.operator_action:
                    raise RuntimeError(
                        f"{scenario}/{assertion} is automated but names an action"
                    )
            elif plan.classification == FAULT_INJECTION:
                if not (plan.prepare and plan.verify and plan.operator_action):
                    raise RuntimeError(
                        f"{scenario}/{assertion} must declare prepare, action and verify"
                    )
                if plan.probes:
                    raise RuntimeError(
                        f"{scenario}/{assertion} must not also declare a direct probe"
                    )
            else:
                if plan.probes or plan.prepare or plan.verify:
                    raise RuntimeError(
                        f"{scenario}/{assertion} is human-only and must declare no probe"
                    )
                if not plan.rationale:
                    raise RuntimeError(
                        f"{scenario}/{assertion} is human-only without a rationale"
                    )
            for binding in plan.all_bindings():
                spec_options = PROBES[binding.probe_id].options
                declared = set(binding.option_map()) | set(binding.required)
                unsupported = sorted(declared - set(spec_options))
                if unsupported:
                    raise RuntimeError(
                        f"{scenario}/{assertion} passes unsupported options to "
                        f"{binding.probe_id}: {', '.join(unsupported)}"
                    )
                overlap = sorted(set(binding.option_map()) & set(binding.required))
                if overlap:
                    raise RuntimeError(
                        f"{scenario}/{assertion} both fixes and requires options "
                        f"on {binding.probe_id}: {', '.join(overlap)}"
                    )


def plan_for(scenario: str, assertion: str) -> AssertionPlan:
    try:
        return PLANS[scenario][assertion]
    except KeyError as exc:
        raise RuntimeError(f"no automation lane for {scenario}/{assertion}") from exc


def classification_summary() -> dict[str, dict[str, object]]:
    """Return the checked-in lane for every P01-P12 assertion."""

    summary: dict[str, dict[str, object]] = {}
    for scenario, plans in PLANS.items():
        spec = SCENARIOS[scenario]
        summary[scenario] = {
            "title": spec.title,
            "assertions": {
                assertion: {
                    "classification": plan.classification,
                    "assertion_text": spec.assertions[assertion],
                    "probes": [binding.probe_id for binding in plan.probes],
                    "prepare": [binding.probe_id for binding in plan.prepare],
                    "verify": [binding.probe_id for binding in plan.verify],
                    "required_options": sorted(plan.required_options()),
                    "operator_action": plan.operator_action,
                    "reviewed_helper": plan.reviewed_helper,
                    "rationale": plan.rationale,
                    "required_os": spec.assertion_os.get(assertion),
                    "allow_not_applicable": assertion in spec.allow_na,
                }
                for assertion, plan in plans.items()
            },
        }
    return summary


def lane_counts() -> dict[str, int]:
    counts = dict.fromkeys(sorted(CLASSIFICATIONS), 0)
    for plans in PLANS.values():
        for plan in plans.values():
            counts[plan.classification] += 1
    return counts


_validate()
