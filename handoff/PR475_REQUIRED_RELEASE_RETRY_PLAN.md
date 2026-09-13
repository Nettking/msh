# PR475 required release failed-jobs retry

Recorded: 2026-09-13T02:29:24.356834+00:00

## Source and remaining gate

PR475 head `5e6f184311019b9982e8544a18f3dc02c1b16e98`; base/main
`b7194820d8f1940ae60b8c9639e09b7f61e65c55`; automatic PR checkout
`a5e743fee24e27bfd8d6d4f57c8efd589c6c3a42`. Head and checkout share tree
`1c671f446fa215c99a6a58a155806de394aa569a`. Local repair checkout clean.
PR473 remains draft at `440123f6bc6dc358eef3d233236bc14f91af60e0`.
No head, workflow, runtime, runner configuration or qualification contract changes.

Release run34707260030 attempt1 is completed/failure. Its sole branch run is
this completed run, so no active same-concurrency qualification will be displaced.
Only two underlying required jobs failed: rotating full suite103589446004
(D14/D15) and Windows journal/artifacts103589446088(D16). Four failed aggregates
are consequences: suite order independence, both release matrices, release verdict.
The ten passing release jobs and all passing companion/retirement/native proofs
remain valid and must not rerun. Original raw logs/JUnit/native receipts are
already retained in diagnostics/pr475-completed-* and pr475-complete-native-artifacts.zip.

## Controlled action

Use GitHub failed-jobs-only rerun for Nettking/msh run34707260030, once.
This resumes a failed required gate on unchanged source with the same rotating
seed/run number1702. It does not recreate or retarget the cancelled optional
AQG observer diagnostic. Expected new execution scope: two test jobs and four
failed dependent aggregates. Verify returned attempt/jobs before any further action.
Native checkout must be read from resulting logs; API head is not a checkout proof.

Existing checked-in scheduler labels remain fcp-test-linux and fcp-test-windows.
Nettking22/Nettking-Linux27/Beast28/Beast-Linux-WSL29 now report offline; previous
01:50 UTC online receipt is preserved as historical. Queueing this required work
allows normal scheduling when qualified capacity returns. No host diagnosis,
service restart, pool/label/account change or AQG power-on dependency is introduced.
Beast Linux lacks fcp-test-linux and must not be relabelled for convenience.
Availability is an observation, not a demonstrated new product/host defect.

## Correctness and interpretation

Exact D13 repair diff reviewed: two files only,26 product and84 test additions.
Unknown POST sends the unchanged404 before bounded declared-body draining with
an absolute existing10s budget. JOIN authorization path remains unchanged. No
new actionable correctness finding was identified in this review; GitHub review
lists are empty, so this is not external approval. Focused native and actual
Windows/Linux discovery-boundary evidence remains recorded; no repeat needed.
D11/D14/D15/D16 original failures and later diagnostic non-reproductions remain
unresolved historical mechanisms. A green retry does not establish their cause,
erase evidence, or authorize candidate freeze/physical PASS by itself. Review
relevant remaining correctness disposition and exact-source gate coverage before merge.
If a new independent confirmed failure appears, immediately persist evidence,
blocker-table entry and GitHub artifact before substantial continuation.

## Follow-up

After dispatch, save attempt/job IDs and a single startup/progress snapshot.
If still queued, leave the next30-minute heartbeat minimal. When assigned,
check near expected completion (original rotating suite about21minutes; checked-in
job timeout30minutes), not at short intervals. For genuinely long1-2hour work,
retain45-60minutes between checks. No duplicate dispatch or passing-job rerun.

Physical runtime9b286f93 unchanged. Protected Recorder data not accessed or
changed. No deployments, Docker reset/prune, physical acceptance claims or P07/P12.
