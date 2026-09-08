# Federation v1 B01-B09 physical acceptance contract

`scripts.acceptance.b01_b09_physical_contract` is the checked-in supplemental
contract for the cross-host physical cases. It validates operator-supplied,
redacted observation packets; it does not perform restarts, scans, routing
changes, or any other physical action, and it never creates a PASS packet from
missing evidence.

## Evidence boundary

Every packet must carry the exact target candidate SHA, a registered host alias,
an ISO-8601 observation time, and the `physical-observation` verification
record. A PASS additionally requires a SHA-256 evidence digest and an explicit
redaction attestation. Candidate, host, target, timestamp, and provenance are
checked before a packet is accepted. Endpoint, address, and local-path text is
rejected rather than guessed or silently repaired.

The P01-P12 harness remains the installation robustness lane. The B01-B09
catalogue maps each case to the relevant P assertions and CF7 acceptance lanes;
it does not transfer a P or historical result into a physical PASS.

## Operator sequence

1. **PLAN** — review `CASE_CONTRACT`, host roles, the exact qualified candidate,
   the P/CF7 mapping, and the long-duration requirements. Prepare a unique,
   redacted evidence root.
2. **PREPARE** — verify the candidate/runtime provenance on Nettking, Nitro, and
   MSH Recorder. Record the target and expected evidence fields. Confirm that
   the protected MSH Recorder `record data/` directory is outside every
   operation and that Beast is excluded.
3. **ACTION** — perform only the reviewed physical action for the case: dual
   admission, targeted scan, the named recorder/coordinator restart, replay or
   stale-state setup, or the isolated old-SHA negative control. Human operators
   attest what actually happened; the harness does not restart or mutate hosts.
4. **VERIFY** — collect the corresponding runtime/report observations, exact
   candidate provenance, target/actor identities, privacy flags, and timestamps.
   For B03 require one report for the requested target and no cross-target
   execution. For B09 prove isolation and that no contact with the old candidate
   was observed. Validate each packet and then validate the packet set.

## Safety and timing

The only destructive operations are the explicitly reviewed human ACTION steps;
all contract validation and evidence writing are read-only with respect to the
product. Preserve persistent volumes and data. Do not prune Docker, delete
volumes, modify product code, restart Arrowhead, or touch the protected recorder
data directory. MTConnect cases require a physically available agent and a
real observed report; zero-machine scans do not substitute for a discovered
agent. P07 and P12 retain their real wall-clock durations and cannot be
shortened by this contract.

Suggested validation shape (using a local ignored evidence root):

```python
from scripts.acceptance.b01_b09_physical_contract import validate_campaign

summary = validate_campaign(
    packets,
    expected_candidate=QUALIFIED_SHA,
    expected_hosts={"nettking", "nitro", "msh-recorder"},
)
assert summary["complete"] is True
```

The checked-in tests cover wrong and mixed candidates, wrong hosts, duplicates,
missing/cross-target B03 identities, stale evidence, privacy leakage,
fabricated PASS packets, and accidental B09 contact. No test is a physical PASS
claim.
