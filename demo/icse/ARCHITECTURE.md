# Architecture figure source

These Mermaid diagrams are explanatory source for the paper/demo. They are not
screenshots or validation results. Render and visually review the final figures
against the exact released source before publication. Final release SHA/tag:
**PENDING** until the release process succeeds.

## Primary network demonstration

The main story uses four independent local processes: three voter/relay nodes
and one joining member. The script invokes production runtime/relay/client APIs;
TCP/WebSocket messages cross actual process boundaries. See `network/README.md`
for exact implementation and limitations. Execution validation remains pending
until the corresponding exact-release evidence exists.

```mermaid
flowchart TB
  Driver[Demo driver: private bounded process orchestration] --> A[Voter A / original relay leader]
  Driver --> B[Voter B / relay]
  Driver --> C[Voter C / relay]
  Driver --> Member[Joining member]
  A <-->|authenticated encrypted consensus| B
  B <-->|authenticated encrypted consensus| C
  A <-->|authenticated encrypted consensus| C
  Member -->|authenticated enrollment, join and discovery| A
  B -->|generic relay payload: synthetic readings| C
  Driver --> Evidence[Public summary and event log]
  Evidence --> Report[Static offline operator report]
```

Caption: “Independent local members exchange a synthetic payload through the
production membership-authorized relay. After an actual leader process is
terminated, the survivors elect and clients explicitly reconnect to repeat the
interaction. The driver records observations; it does not implement consensus.”
The B-to-C arrow denotes logical relay delivery, not a direct peer transport.
Capability declarations are illustrative; no executable provider, Recorder,
storage, compute job or Flask UI is started. One-machine process loss is not
physical-host failure.

## Product boundaries

The standalone [Federation overview figure](figures/federation-v1-overview.svg)
is explanatory architecture, not an execution observation. It is included in
both source and artifact metadata with checksums.

```mermaid
flowchart TB
  UI[Flask operator surface and public-safe projections] --> Actions[Bounded onboarding and action services]
  Actions --> Identity[Persistent device identity and trusted membership]
  Actions --> Intent[Inspection, benchmark evidence and contribution intent]
  Actions --> Authority[Membership, provider, storage and job authority]
  Identity --> Transport[Authenticated Federation transport]
  Authority --> Transport
  Intent --> Adapters[Capability-specific adapters]
  Authority --> Adapters
  Adapters --> Recorder[Recorder: local capture and checkpoint-gated publication]
  Adapters --> Model[Language-model provider]
  Adapters --> Compute[Registered compute and leased job ownership]
  Adapters --> Storage[Storage: assignment, fencing and manifests]
  Storage --> Data[Verified local materialization and existing consumers]
  Data --> UI
  Mode[Configured C03: three-voter committed authority and per-host coordinator views] --> Authority
```

Suggested caption: “FCP separates device membership, contribution intent,
capability-specific authority and execution. A device can combine several
contributions. Configured C03 deployments derive authority from a fixed
three-voter command log and materialize local coordinator views.”

The optional C03 mode uses authenticated encrypted peer transport and a separate
private committed credential-snapshot path. Transient connections and browser
sessions are not replicated. With no `FCP_REPLICATED_CONTROL_PLANE_CONFIG`, the
relay wrapper delegates to the established coordinator service. Name the mode
actually evaluated. Control-plane, storage and job authority have distinct
failure/recovery contracts.

Source anchors: `catalog/federation/control_plane_product.py`,
`catalog/federation/control_plane_runtime.py`,
`catalog/relay/replicated_provider_service.py`, and `docs/architecture.md`.

## Supporting E1–E4 component composition

```mermaid
flowchart TB
  Compose[One reviewer Compose service] --> Parent[demo.icse.run]
  Parent --> E1[E1 child: selective contribution]
  Parent --> E2[E2 child: authority boundary]
  Parent --> E3[E3 child: provider eligibility]
  Parent --> E4[E4 child: ownership and lease]
  subgraph Logical[E2: one process]
    Owner[Owner stack] --> Coordinator[Shared production coordinator]
    Compute[Compute stack] --> Coordinator
    Storage[Storage stack] --> Coordinator
  end
  E2 --> Owner
  E2 --> Compute
  E2 --> Storage
  Synthetic[Synthetic hardware, endpoints and configured discovery] -.-> E1
  Synthetic -.-> E2
  E1 --> Summary[Four-scenario summary with supplied source SHA]
  E2 --> Summary
  E3 --> Summary
  E4 --> Summary
  Summary --> Bundle[CI evidence agreement, manifest, checksums and source archive]
```

Suggested caption: “The portable artifact exercises production components in
four child-process scenarios. E2's three logical member stacks share a
coordinator; synthetic adapters stand in for external observations. This is not
a networked three-host deployment or a physical failover experiment.”

Source anchors: `demo/icse/run.py`, `harness.py`, `scenarios.py`,
`runtime_eligibility.py`, `ownership_boundary.py`, and `bundle.py`. The current
artifact demonstrates no live browser, actual compute result or Recorder data.
Keep physical topology and eventual acceptance results in a separately labeled
evaluation figure/table, with exact source and evidence identifiers.
