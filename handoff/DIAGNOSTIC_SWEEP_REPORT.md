# Federation v1 diagnostic sweep report

For current execution, read [QUALIFICATION_COORDINATION.md](QUALIFICATION_COORDINATION.md).
The sweep below is complete and must not be repeated.

**Sweep complete: 2026-09-11, 12:44 UTC.** Candidate remains
`0536f03d67eb277e11573c2188d8e820399627e3`.
**DIAGNOSTIC ONLY — NOT PHYSICAL ACCEPTANCE EVIDENCE.**

Post-sweep progress: D05 was repaired and pushed as draft
[#461](https://github.com/Nettking/msh/pull/461), exact head
`5b826c6806ab1bdb960412ba20ca78192971fb1d`;50 focused tests passed with no skips.
The [current handoff](FEDERATION_V1_DIAGNOSTIC_SWEEP.md) records the continuing
exact-head qualification and source-proof gaps. The report below preserves the
sweep conclusion and repair plan at its completion.

Three independent product defects require the next fix set; one environment
blocker must be resolved at admission. The detailed accumulating table, exact
procedures, state effects and recovery information are in
[FEDERATION_V1_DIAGNOSTIC_SWEEP.md](FEDERATION_V1_DIAGNOSTIC_SWEEP.md).

| Finding | Conclusion | Repair boundary / required regression |
|---|---|---|
| D01 / [#456](https://github.com/Nettking/msh/pull/456) | POSIX readiness follows onboarding into stateful login and fails supported startup. | Keep separate PR. Exercise actual launcher command against direct success/redirect/error/deadline cases, including no login or cross-origin follow. Existing 25 focused results retained. |
| D02 / [#457](https://github.com/Nettking/msh/pull/457) | Ordinary 0.75 s discovery budget rejects valid slower replies; public routing metadata unnecessarily depends on fresh authority. | Keep separate PR containing the coherent discovery boundary and bounded complete scan. Retain slower-valid, unreachable, malformed, ambiguous, budget-exhaustion, no-quorum advertisement, no-quorum/no-peer grant refusal, saved-membership and no-legacy-key regressions. Existing 175 focused cases retained. |
| D04 / [#459](https://github.com/Nettking/msh/issues/459) | Nitro legacy host responder owns tailnet5151, outside N; current legacy checkout5dfbcd1, imported runtime unproven. | Environment admission plan, not product-code PR. Do not disturb legacy process. After D05 and qualification, verify a free dedicated candidate port and consistent host/app configuration, real peer health and grant provenance. |
| D05 / [#460](https://github.com/Nettking/msh/issues/460) | Supported custom host responder port is not propagated into Flask Compose environment; host5152 would advertise5151. | New narrow separate PR. Render actual Compose with unset/default and non-default ports; verify agreement with unchanged port resolver and both supported launchers. Preserve all grant/authority bounds. |

D03 / [#458](https://github.com/Nettking/msh/issues/458) is closed as a diagnostic
false alarm, not an additional product blocker. The initial audit chose60s;
the actual unchanged120s adapter completed in103.55s with100% GPU and correctly
recommended `not-recommended` for latency. Existing model hash unchanged, no
download. Slow cold load and provider1.5GiB cap remain environment observations;
no OOM/restart and no justified deadline or resource-policy change.

## Shared causes and remaining hypotheses

D01 and D02 encounter authority/network latency but violate different boundaries:
direct application readiness versus non-authoritative public discovery. They do
not need a combined patch. H01 (unavailable/slow Beast endpoints and serialized
authority work) is a demonstrated latency contributor, not a separately isolated
grant/quorum defect. Tailnet-online alone does not prove those endpoints healthy.
D04 and D05 interact during admission but are distinct environment and product
configuration issues. No broad authority or retry/deadline rewrite is justified.

H02 clock error did not recur: Nettking reports synchronized/error0 without any
clock mutation. H03 negative-grant probes did not demonstrate a bypass: Nitro
directly returns JSON403/no grant; fresh pre-admin Nettking returns503 under the
checked-in first-admin gate. No authenticated grant was requested or redeemed.
Initial audit JSON/redirect assumptions are documented as limitations, not defects.
Hosted CI billing denials are infrastructure failures; no product tests ran there.

## Coverage and reason to stop this sweep

| Paths | Diagnostic work / remaining dependency |
|---|---|
| P01/P03 startup, Windows/POSIX runtime | Exact core SHA/image/source, fingerprints, services, clocks and RAM inspected. Prior supported Nitro activation/D01 retained. Repeating activation would re-hit D01; no new candidate deployed. |
| Discovery/onboarding/join/authority | D02 retained; Nitro host process/socket ownership isolated as D04; custom-port render isolated as D05. Direct unauthenticated app refusals observed. Valid joining needs owned qualified responder, D02 and Nettking first-admin/admission; no bypass. |
| P02/P04/P08 resource/storage | Actual backing-resource map and unchanged reservation floor checked without writes. Recorder limits finite. Physical pressure would touch shared C: or live control/storage resources; independent backing-resource isolation and active test recorder prerequisites are absent. |
| P05/P06/P09 crash, supervision, recovery | Current workers running without OOM/restart. Usable-core/active-capture baseline and owned fault targets are not established; crashing them now would measure pre-admission state or disturb shared control. Update trials require the next approved candidate. No corruption, process kills, clock changes or legacy daemon replacement. |
| P10 local AI | Existing model identity, real N adapter inference and GPU use inspected; reservation refusal observed without download. Model removal/pressure/install needs explicit isolated provider path and usable-core baseline; not substituted by the diagnostic inference. |
| P11 backup/restore | No independent destination/restored copy or fenced test writers established. Protected corpus cannot be used for exploratory backup faults. |
| P07/P12 and Recorder+AI | Not started. No active accepted capture/enrollment and candidate will change. Real1h/24h remain required later. Same-device Recorder+AI is still outstanding. |

The accessible independent paths have been reasonably exercised. Remaining
meaningful transitions require known repairs/admission or isolation/protected-state
prerequisites. Additional retries would repeat known causes or produce misleading
pre-admission observations. This is the user-specified diagnostic stop condition,
not physical acceptance completion.

## Final state and next action

[Final metadata invariant](diagnostics/sweep-end-metadata.json): Nettking runtime
and clean harness remain N; all owned containers retain the same IDs/images/start
times and run without OOM. Nitro core metadata remains N; its legacy responder is
explicitly excluded. Protected Recorder container/image/start/mount metadata are
exactly unchanged, with no corpus content read or written; production remains
6fb77c and its separate voter remains fba508. Recorder final control index1551.
Tailscale Running; the four retained hosts report online. No topology/identity
changes or CI account changes. Fresh formal P01–P12=0/12, B01–B09=0/9, CF7 not
physically accepted; P07/P12 absent.

**Smallest next fix set:** D01 + D02 + D05 as three narrow PRs, plus D04 owned
responder-port admission after qualification. D03 requires no product repair.
The four-page ICSE paper retains incomplete physical acceptance; no new claim,
capture or video was fabricated.

**NEXT_HIGHEST_VALUE_ACTION:** create an isolated branch from N for D05, add
focused default/custom Compose propagation regression, show it failing on N,
apply the narrow repair, run focused/static checks, push and open a draft PR
linked to460. Keep all physical hosts unchanged. Then reconcile valid exact-head
qualification already obtained for456/457, fill only genuine gaps and qualify
the D05 exact head as required. Merge normally only after all required gates;
fully qualify the actual final merged main once, freeze one new SHA, run checked-in
revalidation, and restart formal acceptance with fresh evidence. No requalification
of already valid N or unchanged exact PR results.
