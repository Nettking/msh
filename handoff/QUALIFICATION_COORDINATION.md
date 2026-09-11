# Federation v1 qualification coordination

Current user direction, September 11 2026: execute on state changes, using this
checkpoint and the completed diagnostic sweep report; do not repeat the sweep.

Sequence: qualify required exact PR heads -> merge456/457/461 as separate fixes
-> qualify actual final merged main once -> resolve/verify D04 through the
documented controlled host procedure -> freeze one AUTHORITATIVE_SHA -> clean
checked-in revalidation -> fresh formal physical acceptance.

| PR | Intended exact head | Phase |
|---|---|---|
|456|1a0c634f47f8a247b6d1d2d1a219f5c12590587d|Qualification incomplete at previous checkpoint; refresh only state delta|
|457|143fe7a9082193114af3d34dc437b85f845849a2|Qualification incomplete at previous checkpoint; exact software-update dispatched|
|461|5b826c6806ab1bdb960412ba20ca78192971fb1d|Draft repair;50 focused tests pass; required qualification pending|

Runtime remains0536f03d67eb277e11573c2188d8e820399627e3. No physical PASS; P07/P12
not started. Protected Recorder data remains out of bounds. The user's latest
D04 instruction authorizes controlled removal/disablement of the stale Nitro
responder when that stage becomes actionable; this supersedes the earlier
dedicated-port proposal. Verify ownership immediately before acting and retain
evidence. D04 is environment work, never folded into a product PR.

Retain valid exact-head PASS, native checkout/log proofs, registry and ICSE
artifacts. Poll only these three PRs and required qualification; inspect logs only
for newly completed/failed jobs or contradictions. No duplicate dispatches. No
merge before exact-head qualification and relevant correctness findings resolve.
No intermediate merged-main qualification or per-PR physical restart.

## September11 13:04UTC — checkpoint branch recovery

Git fetch and targeted ls-remote found the diagnostic branch absent on GitHub.
The three intended repair refs and main were unchanged. The clean local
checkpoint was6483e95fed889b05553e4d2e311d0eedb56722ff, previously pushed and
referenced by the diagnostic artifacts. Restored that exact branch with a normal
non-force push. No source/runtime/host/CI state changed. Cause of remote branch
absence is unknown and is not a product finding.

Next exact action: read only current PR heads/state and required workflow job
state; compare against diagnostics/post-sweep-qualification-checkpoint.json.
Retain new completed evidence, classify failures before changing code, and
dispatch only proven required gaps. Publish each meaningful transition here.
