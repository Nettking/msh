# Demonstration: membership survives one voter loss

The primary story is an authenticated Federation whose independently running
members retain their identities, membership and capability ownership after the
original leader process is lost. The same members reconnect through the elected
successor and repeat a real payload exchange. `network/README.md` contains the
exact commands, archive provenance options and full limitations.

Use the exact released Federation v1 source for the paper and recording. Final
release SHA/tag, completed run evidence and physical acceptance are **PENDING**
until the corresponding release process succeeds. This script is preparation.

## Five-minute flow

Prepare a clean checkout or verified publication ZIP, Python 3.12 and the pinned
runtime environment described in the network guide. Keep output outside source.
Run the full unchanged network command, optionally with `--step-pause 3` for
narration, and retain the current exit code and `public/` evidence. Do not replay
old observations as a live run.

| Time | Show | Explain | Actual observation required |
| --- | --- | --- | --- |
| 0:00–0:30 | Exact source identity and architecture | Four independent processes use production authority and transport on one reviewer machine | Verified Git/archive provenance; three voters and a joining member |
| 0:30–1:15 | Quorum bootstrap and authenticated join | Durable membership follows authorized enrollment, not discovery alone | Same Federation identity; authenticated member join |
| 1:15–2:00 | Capability discovery and owner rejection | Members declare illustrative capabilities under their own identities | Authorized discovery plus rejection of another owner's identity |
| 2:00–2:40 | Synthetic reading payload received by another member | Membership-authorized product relay traffic carries the small public dataset | Received sender/session/full payload matches the request |
| 2:40–3:40 | Leader process termination and surviving quorum | Remaining voters elect through production consensus | Later term, unchanged Federation/creator and retained ownership; explicit reconnect and repeated exchange |
| 3:40–4:20 | Former voter return and minority refusal | Returning authority converges; insufficient quorum cannot authorize new enrollment | Returning follower and refused minority mutation |
| 4:20–5:00 | Offline report, checks and package | Observations and reproducible package identify the same source | All ten checks PASS, successful teardown and current zero exit |

The static `operator-report.html` is an evidence presentation, not a live Flask
screen. Actual recovery waits may exceed the editorial slots; disclose any video
time compression. The run's recorded times remain unchanged.

## What this story demonstrates

Production components run in four separate processes with independent state.
Consensus uses authenticated encrypted TCP; enrollment, discovery and generic
payload delivery use authenticated WebSocket connections on loopback. The
scenario terminates an actual owned leader process and observes automatic voter
election. Clients explicitly reconnect to the successor before repeating their
interaction. The old voter returns from its own retained state.

The synthetic reading capabilities are illustrative owner-scoped declarations.
They are not approved executable providers. The payload exchange does not run a
Recorder, compute job, storage transfer or generic JSONL materialization. The
scenario starts no Flask UI and uses explicit development plaintext WebSockets
on loopback. One machine supplies all process failure domains; this cannot prove
physical machine-loss, browser, hardware, WAN or real-duration acceptance.

## Supporting E1–E4 experiments

The separate Compose command in `ARTIFACT.md` runs four bounded production
component experiments. E1 shows independent contribution intent; E2 shows the
storage pending/authority activation boundary; E3 separates provider identity
from current eligibility; E4 separates selection from leased job ownership.
These support the authority distinctions without pretending they are steps
executed by the network scenario.

E2's three logical member stacks share a coordinator in one process. Its test
clients and synthetic observations do not provide additional independent
network nodes. E3/E4 use supplied times, not elapsed-time failure experiments.

## Evaluation wording

Report the two Linux/Windows network executions and three E1–E4 executions
separately, with the exact source, workflow and archive digests. Physical
P01–P12/B01–B09/CF7 acceptance and strict evidence validation form a separate
evaluation. Fill those results from actual evidence, never from this script or
test names. No result here establishes comparative productivity, performance
superiority, optimal scheduling, general scalability or a general security
theorem. The final paper, video and archive must cite the same accepted release.
