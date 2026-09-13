# Original AQG host: one bounded diagnostic context run

**CANCELLED BY USER, 2026-09-13.** AQG was intentionally powered off because it is unnecessary. Run34714139932 was cancelled before any runner assignment. This historical plan must not be executed, retargeted or treated as a release dependency. The coordination-only workflow is removed; scripts/evidence remain audit history. See diagnostics/AQG-diagnostic-cancelled-by-user.json.


The completed NETTKING seeded contexts (D14 nine cases, D15 two cases) did not
reproduce either AQG failure. Preserve their raw evidence and do not repeat them
on NETTKING. Original AQG inspection403/background error remain unexposed.

At19:20UTC both AQG runners are offline. Existing AQG Linux runner31 retains
`fcp-test-linux`, `fcp-docker-linux`, `aqg7ncc-linux-admission`; it does not have
`fcp-linux-fast`. Existing native admission run34684352823/job103528608892 on
source17ab3a05 already proved its shared-pool tooling, storage and isolated Docker
requirements. See diagnostics/aqg31-existing-admission-proof.json. Do not rerun
admission, restore labels, change accounts or introduce another release runner.

One coordination-only diagnostic job may target its existing test/admission
labels and wait in the queue while offline. It must never enter a candidate.
Verify original runner31 registration, AQG hostname, expected runner workspace
and existing native Python3.12.13 before testing. Checkout unchanged5e6source;
use the source's Python action and constrained pytest9.1.1/randomly4.1.0 pins.
No product deployment, service reset, pool change or qualification dispatch.

Capture a bounded read-only host receipt: identity, kernel/Python, available
memory/swap, CPU count, proc pressure data and paired wall/monotonic clock samples.
Do not read environment secrets, production paths, mounted Recorder data, or
change clock/network/resource settings. Original log has one timestamp reversal
of0.712003seconds between a dependency-step header and pip output (lines168-169).
This could be log emission/order or clock behavior; it proves neither clock drift
nor a test root cause, and is not a new independent blocker. Preserve it only as
a hypothesis in diagnostics/D14-D15-native-timing-access-review.json.

Execute the already reviewed D14 nine-case seed1702 context once, then D15 two-case
context once only if D14 passes, in separate fresh processes with180second outer
bounds and existing unchanged internal guards/deadlines. Forwarding observer
records error codes, CSRF booleans and failed background thread stack without
secrets, altered return values or retries. No full-suite replay. Stop on the first
failure so its evidence can be committed/pushed before further investigation.

Store exact source before/after, actual commands/order, host receipt, logs/JUnit
and observer results in one always-uploaded artifact. If it is still queued,
do not create duplicate jobs or repeatedly poll; resume on runner/job transition.
If it passes, retain non-reproduction and reassess the missing original full-order
or host-load evidence; never turn passing diagnostic repeats into acceptance.
