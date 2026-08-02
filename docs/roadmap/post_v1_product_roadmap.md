# Post-v1 product roadmap

Status: preserved future plan. This roadmap records approved future directions so repository cleanup can remove prototypes and stale planning files without losing intended product work.

This roadmap is not part of the MSH Federation v1.0 release contract unless an item is explicitly promoted into a release plan.

## Planning principles

- Finish and stabilize v1 before expanding federation behavior.
- Prefer small compatible `1.x` product updates before a broader `2.0` trust or protocol change.
- Keep user-facing concepts simpler than internal authority and fencing models.
- Integrate future UI and documentation into the existing Flask-first application.
- Do not run a second Flask application for documentation.
- Do not expose private endpoints or weaken existing authority boundaries to simplify UX.
- Every guided action must still use the same authenticated, revision-fenced, and fail-closed backend authority.

## V1.1 — integrated documentation and guided federation UI

Purpose: make the completed v1 capabilities understandable and operable without requiring users to read implementation plans or know internal protocol terminology.

### Integrated `/docs`

Deliver a read-only documentation reader inside the existing MSH Flask application:

- base route: `/docs`;
- reads canonical Markdown from the repository `docs/` tree;
- nested folder navigation;
- clear document titles and breadcrumbs;
- responsive/mobile navigation;
- fenced code, tables, admonitions, headings, and stable anchors;
- relative links between Markdown documents;
- safe local images and assets;
- path-traversal and symlink escape protection;
- controlled not-found behavior;
- available before runtime startup and, where appropriate, in recorder-only mode;
- no external font or CDN dependency required for basic reading;
- no upload, edit, or arbitrary file browsing capability;
- direct links from relevant UI errors and help panels.

The visual and interaction ideas from `new-stuff/md_viewer/` are preserved as design input, including the hierarchical reading experience. The prototype itself is not the production architecture and may be deleted after these requirements are retained here.

### Canonical documentation information architecture

Proposed public structure:

```text
docs/
  index.md
  getting-started/
  user-guides/
  federation-v1/
  administration/
  troubleshooting/
  reference/
  developer/
  history/
```

Default navigation must prioritize current user tasks. Historical phase records must not appear as the main onboarding path.

### Guided Federation UI

Add a coherent System → Federation area:

```text
Federation
  Overview
  Create or join
  Devices
  Providers
  Storage
  Activity
  Troubleshooting
```

#### Overview

Show:

- whether this device belongs to a federation session;
- current role and ownership state;
- connected and unavailable devices;
- pending approvals;
- storage health and degraded state;
- active AI and compute provider counts;
- one recommended next action.

#### Create or join

Provide guided flows for:

- create a federation;
- generate a time-limited invitation;
- join using an invitation;
- verify the expected owner/session identity;
- recover from expired or invalid invitations;
- explain LAN/VPN/relay requirements in user language.

#### Devices

Show:

- this device versus connected devices;
- friendly name and verified logical identity;
- member state;
- last current health/connectivity observation;
- contributed provider types;
- safe owner actions such as revoke membership;
- advanced identity and revision details only on demand.

#### Providers

Show a task-focused approval inbox:

- pending provider requests;
- provider type and contributing device;
- what approval permits and does not permit;
- approve, suspend, resume where supported, and revoke;
- current availability and expiry;
- reason codes translated into user guidance;
- no credentials, private endpoints, prompts, results, handler paths, or storage authority leakage.

#### Storage

Show:

- current primary and replicas;
- completeness and synchronization status;
- degraded state and why no candidate is eligible;
- last successful replication evidence;
- controlled handover/recovery actions only where safe and already supported;
- advanced terms, leases, fencing, watermarks, and missing ranges behind an expert panel.

#### Activity

Show a bounded, safe timeline of:

- joins and revocations;
- provider requests and decisions;
- health expiry and recovery;
- storage assignment and failover decisions;
- reconnect/reconciliation outcomes;
- no secret or private payload content.

#### Troubleshooting

Use guided diagnosis rather than raw state dumps:

1. identify the failed task;
2. show the specific failed precondition;
3. explain the likely cause;
4. offer one safe next action;
5. link directly to the matching `/docs` section;
6. provide advanced evidence for technical users.

### User-language policy

Prefer:

- This device
- Connected device
- Waiting for approval
- Approved
- Temporarily unavailable
- Suspended
- Access removed
- Storage is synchronized
- Storage needs attention
- Try connection again

Hide by default:

- generation fencing;
- report revision;
- lease generation;
- descriptor fingerprint;
- reconciliation cursor;
- protocol-major mismatch internals.

The internal values remain available in advanced diagnostics and logs.

### V1.1 acceptance

- a new user can create or join a federation using only the UI and user guide;
- a federation owner can understand and decide a provider request;
- a user can identify why a provider or replica is unavailable;
- every error state links to current documentation;
- `/docs` works on desktop and mobile;
- documentation rendering cannot escape the allowed docs root;
- existing v1 regression gates remain green;
- no new authority source is introduced by the UI.

## V1.2 — operational administration and observability

Purpose: make trusted federation easier to operate over time without changing its trust model.

Planned areas:

- persistent safe health dashboard;
- clearer component readiness and dependency state;
- structured operational logs;
- bounded metrics and alert hooks;
- session/provider/storage event history;
- backup status and recovery rehearsal guidance;
- upgrade readiness and compatibility checks;
- operator export of safe diagnostics bundles;
- soak, restart, and controlled failure testing;
- clearer relay/direct-route visibility without exposing private addresses broadly.

## V1.3 — improved local and connected AI experience

Planned areas:

- guided connection testing;
- clearer model/provider readiness;
- safe provider-choice explanation;
- streaming responses where compatible;
- cancellation UX;
- bounded durable request history without storing secret prompts by accident;
- model lifecycle and warm-up status;
- better fallback explanation;
- explicit context/privacy controls.

This update must not turn a language-model provider into a storage or command-execution authority.

## V2.0 candidate — broader network operation

Potential scope:

- operational public relay and rendezvous deployment;
- restrictive NAT and unrelated-network acceptance;
- automatic authenticated route publication and renewal;
- certificate and key rotation workflows;
- multi-route policy;
- stronger production abuse controls;
- upgrade and protocol negotiation across deployed versions.

A V2.0 plan must define migration and compatibility before implementation begins.

## V2.x candidate — advanced scheduling

Potential scope:

- cost and latency policy;
- data locality;
- energy-aware scheduling;
- GPU/accelerator requirements;
- quotas, priorities, fairness, affinity, and preemption;
- durable distributed interactive queues;
- model warm pools and lifecycle orchestration;
- broader streaming support;
- production load and SLO acceptance.

## V2.x candidate — organizations and policy

Potential scope:

- multiple administrators and delegated roles;
- organization/project boundaries;
- policy-based provider and artifact sharing;
- durable audit and compliance views;
- retention and data-governance policy;
- federation-to-federation trust decisions.

## Future research boundary — less-trusted external providers

This remains deliberately late and separately gated:

- sandboxing and isolation;
- signed packages and provenance;
- supply-chain verification;
- resource enforcement;
- reputation and dispute systems;
- billing and marketplace behavior;
- anonymous or public participation;
- execution of externally supplied code.

No current v1 or planned 1.x UI should imply that unknown third-party execution is safe.

## Promotion rule

A roadmap item becomes active work only when:

1. the repository owner explicitly selects it;
2. a bounded implementation plan defines authority and compatibility impact;
3. current v1 tests remain mandatory;
4. the work is separated from unrelated cleanup;
5. exit criteria and deferrals are written before implementation.
