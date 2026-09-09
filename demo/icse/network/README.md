# Federation membership survives one voter loss

This reviewer demonstration starts **four independent local Python
processes**: three voter/relay nodes and one joining member. It runs the release's
`FederationV1ReleaseRuntime`, `PhysicalReadyReplicatedSessionCoordinator`,
`ProviderAuthorityRelayServer`, and `RelayNodeClient`. Consensus messages travel
over the production authenticated encrypted TCP transport; node enrollment,
membership, discovery and payload delivery use real authenticated WebSocket
connections on loopback. The default production election timings are retained.

The demonstration is intentionally narrower than a complete product deployment.
It does not start Flask, the Recorder, a compute worker or a storage provider.
The parent-to-child JSON pipe protocol is demonstration orchestration, not a new
product API. No product objects, network transports or election results are
mocked. Assertions fail the command; a failed startup is not replaced by an
in-process fixture.

## Reproduce from the exact release

Prerequisites are Git, Python 3.12, and the normal repository runtime dependencies.
Use a clean checkout of the **accepted release SHA and tag** from the release
manifest. No release identity is hardcoded here, and a matching local tag alone
does not certify physical acceptance. The orchestrator refuses a dirty checkout,
a mismatched SHA/tag, an existing output directory, or output inside the checkout.

Linux/macOS shell, from the repository root:

```bash
python3.12 -m venv /tmp/fcp-icse-network-venv
/tmp/fcp-icse-network-venv/bin/python -m pip install -r requirements.txt -c constraints-phase2.txt
/tmp/fcp-icse-network-venv/bin/python -B -m demo.icse.network.run \
  --source-sha "$RELEASE_SHA" --release-tag "$RELEASE_TAG" \
  --output /tmp/fcp-icse-network-run
```

PowerShell, from the repository root:

```powershell
python -m venv "$env:TEMP\fcp-icse-network-venv"
& "$env:TEMP\fcp-icse-network-venv\Scripts\python.exe" -m pip install -r requirements.txt -c constraints-phase2.txt
& "$env:TEMP\fcp-icse-network-venv\Scripts\python.exe" -B -m demo.icse.network.run `
  --source-sha $env:RELEASE_SHA --release-tag $env:RELEASE_TAG `
  --output "$env:TEMP\fcp-icse-network-run"
```

Set `RELEASE_SHA` and `RELEASE_TAG` from the published release manifest; these are
placeholders, not claimed release coordinates. For a maintainer's unreleased
candidate smoke run, supply its exact clean commit and omit `--release-tag`.
The resulting summary then explicitly records `release_tag: null`. Always choose
a fresh output/virtual-environment path if the examples already exist.

### An extracted publication ZIP without Git

Retain the original `fcp-icse-tool-demo-<version>.zip` outside its extracted
`fcp-icse-tool-demo-<version>/source/` directory. Obtain its complete ZIP SHA-256
from a trusted release/deposit channel before running any extracted code; an
unverified digest supplied by the same untrusted download does not establish
authenticity. First verify that download using your operating system's checksum
tool, then extract it into a fresh directory. Keep its recorded directory names.

From the extracted `source/` directory, use an external virtual environment and
output directory. The existing pinned requirements installation above applies.
The parent **must use `-B`** to prevent Python bytecode files from changing the
verified source tree. All demo children also use `-B`.

```bash
/tmp/fcp-icse-network-venv/bin/python -B -m demo.icse.network.run \
  --source-sha "$RELEASE_SHA" \
  --source-archive "$PUBLICATION_ZIP" --archive-sha256 "$TRUSTED_ZIP_SHA256" \
  --release-tag "$ARTIFACT_TAG" --output /tmp/fcp-icse-network-archive-run
```

```powershell
& "$env:TEMP\fcp-icse-network-venv\Scripts\python.exe" -B -m demo.icse.network.run `
  --source-sha $env:RELEASE_SHA `
  --source-archive $env:PUBLICATION_ZIP --archive-sha256 $env:TRUSTED_ZIP_SHA256 `
  --release-tag $env:ARTIFACT_TAG --output "$env:TEMP\fcp-icse-network-archive-run"
```

The paired `--source-archive`/`--archive-sha256` options select explicit archive
mode; there is no fallback from a failed Git check. The driver, every child,
and the final check verify the full ZIP digest, embedded manifest schema and
revision, declared source/artifact prefixes, and the complete extracted source
file set and bytes. Unsafe paths, duplicate entries, case collisions, symlinks,
reparse points, extra source files/directories, and missing or modified files
are rejected. No executable, `__pycache__`, virtual environment or local helper
is ignored. Use a fresh extraction if a prior command changed this tree.

In archive mode, `--release-tag` can check **only the artifact tag** named by the
embedded manifest's `source_ref`, such as `refs/tags/fcp-icse-tool-demo-v0.2.0`.
It cannot verify a separate product tag such as `v1.0.0`. Omit this optional
argument for a candidate artifact whose manifest names a branch or PR ref.
The summary records `artifact_source_ref`, `artifact_tag_checked`, and the
complete `archive_sha256`; it keeps `release_tag: null` and
`release_tag_checked: false`. Product release/tag/physical acceptance mapping
still comes from the separate accepted release manifest.

The demo needs nine free loopback consensus/credential/migration ports and three
additional relay ports; it chooses them at startup. It preserves the runtime's
ordinary host resource admission requirements, so use a host with sufficient
disk and memory headroom. Do not run it concurrently with a resource-sensitive
release gate or a physical acceptance campaign. No Docker or private hostnames
are required. Windows and Linux execution must be validated separately before
claiming support; macOS is an unvalidated reviewer option.

## Demonstration flow

1. Show the four actual process IDs and three separate durable voter stores.
2. Create one Federation through a real authenticated voter quorum.
3. Enroll the fourth identity and join it with one-use enrollment/invitation
   credentials. Credential values never appear in public output.
4. Announce two separately owned illustrative capabilities. Read the authorized
   discovery snapshot through the member's WebSocket connection. Demonstrate
   rejection when that member claims another node as an announcement's owner.
5. Send the checked-in synthetic machine readings from voter B to voter C.
   Compare the received sender, session and full payload to the request.
6. Terminate the actual original leader child process. Let the two surviving
   voters elect automatically; the script never sets their role or term.
7. Show the unchanged Federation/creator identity, retained committed membership
   and capability ownership, increased consensus/leadership term, and the new
   leader's increased **local** fencing epoch.
8. Explicitly reconnect the surviving clients to the elected relay, discover
   the same capability owners, and repeat the authenticated dataset exchange.
9. Restart the old voter from its own retained files. Observe convergence as a
   follower with the successor's committed authority.
10. Stop the current leader's two peers and show its enrollment-token mutation
    refused for lack of quorum. Shut down every remaining owned process.

An optional `--step-pause 3` allows time for narration after each recorded step.
All waits remain finite. Each invocation retains its own files; it neither
searches for unrelated processes nor deletes prior evidence.

## Output and interpretation

`public/summary.json` uses schema **`fcp.icse-network-demo.v1`** and records the
actual source SHA, provenance mode, optional checked tag, real UTC observations, process IDs,
individual assertions, and `STARTED`, `PASS`, or `FAIL`. Startup readiness is
separate from a completed `PASS`. Every failed assertion or failed teardown
produces a nonzero exit. An unexpected child exit or a forced teardown needed
after failed graceful shutdown also fails the run. Deliberate process termination
at a narrated fault step is recorded separately. `public/events.jsonl` retains
incremental observations. Public failures contain bounded error identities;
full diagnostic tracebacks remain in `private-state/`.

`public/operator-report.html` is a static offline rendering of those same recorded
observations. It is labeled as a demonstration report. It is **not** a screenshot
of the product UI. Open it for narration or capture an actual screenshot of a
successful report with its source identity visible. No screenshots or passing
records are checked in as if execution had already occurred.

`private-state/` contains disposable node identities, the random shared transport
secret, local databases, deployment addresses and worker error logs. It is private
run state and must not be uploaded as a publication artifact. Only the `public/`
directory is intended for the package. POSIX permissions restrict generated private
directories; on Windows, choose an output parent accessible only to your account.
State is retained for inspection and is never automatically deleted.

## Claim boundaries and limitations

- `demo.synthetic-readings` and `demo.reading-viewer` are honestly named
  **illustrative capability declarations**. They demonstrate identity and owner
  authorization. They are not production Recorder/analysis capabilities and
  **do not demonstrate approved executable-provider activation**.
- The payload is a deterministic invented dataset. Delivery uses the product's
  membership-authorized generic relay API. There is no analysis result, compute
  scheduling, storage transfer, Recorder sampling or capability-specific dispatch
  in this scenario. The driver verifies content; it does not masquerade as an
  application worker.
- Relay clients explicitly reconnect to the observed successor. This demonstrates
  recovery through the new authority, not transparent client endpoint migration.
- WebSocket traffic is authenticated but plaintext on loopback, using the
  product's explicit local-development option. Cross-host TLS, browser discovery,
  installation/onboarding UI and WAN traversal are outside this local setup.
- Three processes on one machine are distinct nodes, not independent physical
  failure domains. Process termination is not evidence of a machine outage,
  power loss, partition, or physical P01–P12/B01–B09/CF7 acceptance.
- Fencing epochs are per-voter counters, not a globally ordered number by
  themselves. Cross-voter authority ordering is checked through the consensus
  term, with immutable creator provenance checked separately.
- The final minority check uses the product coordinator's administrative token
  method. It establishes refusal to authorize enrollment without quorum, not
  universal availability or correctness of every product operation in a partition.
- This scenario is separate from the E1–E4 component experiments and
  its `fcp.icse-demo-summary.v1` evidence. Passing either one does not substitute
  for the other, for release CI, or for physical acceptance.

## Maintainer validation before publication

Run syntax/lint checks first, then one complete invocation on an otherwise idle
qualified Windows host and one on Linux, each from the exact candidate commit.
Inspect all ten narrated steps, every `checks` entry and
`all_owned_processes_stopped: true`. Confirm a zero exit, exact SHA/tag binding,
and that public output contains no enrollment/invitation or transport secret.
Keep a failing run and classify its actual cause before retrying. Publication
requires both network executions alongside the unchanged E1–E4 requirements.
