# Controlled three-host physical acceptance — preparation for bcf5c9ab

Prepared, **not executed**. No physical action has been taken.

```
MERGED_MAIN_SHA: bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4
MERGED_MAIN_SOFTWARE_QUALIFICATION: PASS   (run 34029983129)
PHYSICAL_ACCEPTANCE: NOT RUN
PHYSICAL_ACTION_STARTED: NO
```

Physical acceptance is separately authorized. Nothing below is to run until the
executive owner authorizes a controlled start.

## 1. The coverage gap this campaign must close

**The existing P01-P12 campaign does not exercise recorder capability identity
at all.** Searching every file under `scripts/acceptance/` and
`catalog/federation/tests/cf7_acceptance/` at `bcf5c9ab` for
`capability-identity-conflict`, `recorder-local`, `recorder-{`, multi-recorder
or "two recorders" returns nothing. The contract's recorder scenarios are P04
finite transaction and disk pressure, and P06 native recorder supervision —
crash, checkpoint continuity, poison archives, path confinement. None of them
announce a second recorder or assert a capability ID.

So a green P01-P12 run would say nothing about the defect PR435 fixed. The
campaign therefore has two parts, and the second is the one that makes this
candidate's acceptance meaningful:

- **Part A — the existing P01-P12 contract**, unchanged, on the merged SHA.
- **Part B — recorder capability identity on real hardware**, which currently
  has no physical coverage and must be added.

Part B is not optional and must not be substituted with the in-process suite.
`catalog/mtconnect_recorder/tests/test_multi_recorder_capability_identity.py`
drives a real relay, client, pairing runtime and SQLite coordinator, and it
passes, but it runs inside one host. The defect appeared because two separately
paired physical recorders met in one session.

## 2. Preconditions

### Identity — currently unmet

All three hosts must run the **same exact** `bcf5c9ab`. As last observed all
three are on the pre-merge `6101c86d`:

| Host | Deployed | Required | Role in the campaign |
| --- | --- | --- | --- |
| Nettking | `6101c86d` | `bcf5c9ab` | Federation member, coordinator/relay side |
| Nitro | `6101c86d` | `bcf5c9ab` | Federation member, **recorder 1** (currently inactive) |
| MSH Recorder | `6101c86d` | `bcf5c9ab` | Federation member, **recorder 2**, separately paired |

Verify the deployed identity from the source label **and** the per-component
build label on each host, not from the checkout. A campaign whose hosts do not
all report the identical SHA is void.

### Isolation and safety

- **Do not update the normal development checkouts.** `C:\wsl\msh` is clean at
  `6101c86d` and stays that way. Use separate clean acceptance
  checkouts/worktrees per host.
- **Preserve rollback to `6101c86d`** on every host, and confirm the rollback
  path before starting, not after.
- **Arrowhead remains stopped** on Nettking-Linux unless the operator decides
  otherwise. It was the OOM cause during qualification; if it is restarted for
  realism, that is a deliberate scenario variable to record, not a default.
- Capture fresh initial runtime and Federation state, and the rollback targets,
  before anything starts. `CURRENT_SESSION_ID` is still UNKNOWN in the handoff;
  a fresh authenticated Federation state query is a prerequisite for meaningful
  before/after evidence.
- Nitro's recorder is inactive and is to be activated **only inside** the
  campaign window.

## 3. Required proof matrix

Every row must be evidenced on the real three-host rig at `bcf5c9ab`.

| # | Requirement | Coverage today | How it is proven |
| --- | --- | --- | --- |
| 1 | Three real Federation members | partial (topology) | All three hosts authenticated and visible in one session's membership view |
| 2 | Pairing / enrollment | partial (P03 entry points) | Each host enrolls and redeems its own pairing offer; no code on a command line |
| 3 | Two legitimate recorder nodes coexist | **none** | Nitro and MSH Recorder both accepted in one session, both READY |
| 4 | Distinct `recorder-{node_id}` identities | **none** | Coordinator capability rows show two distinct IDs, each derived from that node's durable Ed25519 node id |
| 5 | No `capability-identity-conflict` for legitimate recorders | **none** | Zero conflict rejections across the campaign for either recorder |
| 6 | Restart-stable recorder identity | **none** | Same capability ID before and after recorder restart, no second row |
| 7 | Independent recorder-control targeting | **none** | recorder-control reaches each recorder by node id; commands do not cross |
| 8 | Legacy `recorder-local` migration and retirement | **none** | A node that genuinely holds `recorder-local` keeps it while it owns it, then the superseded row is announced UNAVAILABLE and is inert |
| 9 | Retired identity is not recreated on replay | **none** | After retirement, reconnect replay does not resurrect `recorder-local` |
| 10 | Stale local-state recovery | **none** | A restored stale backup claiming another node's identity still converges; `connect()` does not strand the node |
| 11 | Capability publication / replay | partial | Capability rows survive reconnect; replay creates no duplicates |
| 12 | Reconnect / convergence | partial (P05) | Deliberate link loss, then convergence to one row per recorder |
| 13 | Recorder restart | partial (P06) | Covered jointly with rows 6 and 11 |
| 14 | Participant restart | partial (P05/P06) | Non-recorder member restart without capability loss |
| 15 | Coordinator restart | partial | Coordinator restart; both recorders reconverge, identities unchanged |
| 16 | No duplicate capability ownership | **none** | At no point are two READY rows held for one recorder |
| 17 | No unauthorized takeover | **none** | Neither recorder ever adopts the other's identity, including from stale state |
| 18 | Logical + physical storage | P04, P08 | Existing scenarios on the merged SHA |
| 19 | Persistence / recovery | P09, P11 | Existing scenarios, including exact-candidate backup/restore |
| 20 | Rolling update / workload drain | P01, P03 | Existing scenarios on the merged SHA |
| 21 | Cleanup / rollback | P01, P11 | Deterministic cleanup, then demonstrated rollback to `6101c86d` |
| 22 | Final healthy Federation state | P12 | Final state captured and asserted healthy |

Eleven of the twenty-two rows have **no** physical coverage today, and they are
exactly the rows PR435 is about.

## 4. Negative control — the campaign's most important step

A campaign that cannot fail the old way has not tested the fix.

The control must reproduce the original condition on the real rig: a second
recorder announcing the **fixed** capability ID `recorder-local` while another
node already owns it, and it must be observed to be rejected with
`capability-identity-conflict`. Then the same node, on the merged code path,
must be observed to succeed on its node-scoped identity.

Requirements for the control:

- It runs against the same live coordinator as the positive case, not a mock.
- The rejection is captured as evidence, with the coordinator's own error code.
- It is bounded and reversible: the injected row must be retired and the session
  left with exactly the two legitimate recorder rows.
- It must not be simulated by editing SQLite, revoking membership, or asserting
  on a schema. Those do not reproduce the defect.

If the negative control cannot be made to fail the old way, the campaign result
is inconclusive regardless of how many other rows are green.

## 5. Evidence and disposition

Record for every scenario: exact SHA on all three hosts, host identity, initial
state, actions, expected versus observed, coordinator capability rows before and
after, and final state. Keep the P07/P12 duration requirements as they stand.

CI, loopback tests and schema-only evidence cannot substitute for any row.

No test, deadline, retry, skip, flaky mark, coverage setting or storage
admission threshold may be changed to obtain a physical pass. If a scenario
fails, stop, classify, and report — do not retry it into green.

## 6. Status

```
PHYSICAL_ACCEPTANCE: NOT RUN — PREPARED ONLY
```

Federation v1 is **not** accepted and no physical PASS is declared. Execution
requires explicit authorization from the executive owner, all three hosts moved
to `bcf5c9ab` under clean acceptance checkouts, and Part B coverage to exist
before the campaign starts.
