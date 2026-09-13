# PR483 exact-head ICSE gate, September13

The one worker-frame capture is complete and will not repeat. It neither fixes
D18 nor qualifies a release. Existing35/37+3 native PR4755e results and stacked
f104 release16/16/other automatic results remain immutable.

## Concrete source gap and bounded action

PR483 head06b956787ae63215982d5afd7198dead366e05ea has only automatic PR event
runs, whose native checkout is f104038a2b77705adaa547cdd1df7d895bf80a41. The trees
are identical, but the standing exact-head qualification discipline does not
permit relabeling source identities. PR475_EXACT_HEAD_QUALIFICATION_RECONCILIATION.md
records the established exact-head scope; checked-in CF7 acceptance requires
exact-commit matrices and v1_physical_campaign.md requires new candidates to pass
the permanent gates before physical work. No optional diagnostic is a new gate.

Start only the existing workflow_dispatch icse-tool-demo.yml at unchanged
codex/fix-d20-icse-failure-evidence. This is the first native06 execution of its
required ICSE gate, not a retry of original5e or syntheticf104. It runs the existing
Linux/Windows reviewer jobs, isolated Compose reviewer, and dependent publication
bundle with original commands/dependencies/deadlines/runner labels. D20 only
changes failed public evidence retention and redacted command context; its patch
is byte-identical to the already reviewed repair. No product source is changed.

Defer all other new-head dispatches until this short gate is reviewed. Do not
restart the full37-job scope or a passing old-source job. Preserve failed f104
ICSE and original5e attempts; do not use a green result to close unknown causes.
PR483/475 have no submitted GitHub reviews or line comments at this checkpoint;
this is not external approval. D18/D21 remain unresolved correctness observations
for later disposition. No PR is merge-ready based on this partial gate alone.

## Execution discipline

Before dispatch, guard PR48306/base5e, source ref, terminal automatic runs, and
absence of an existing manual06 ICSE run. Persist the accepted/uncertain receipt
immediately. Confirm allocation once, then inspect near expected completion
(about10minutes), with no automatic retry on recurrence. Retain native checkout
and source-bound public/bundle artifacts. If it fails, classify its exact failed
operation first; no speculative repair or full-suite dispatch.

This does not authorize another PR475 attempt3, old D18 capture, AQG action, merge,
source freeze, physical deployment, protected Recorder access, Docker reset/prune,
runner pool/account/label changes or P07/P12.

Next exact API action: POST /actions/workflows/icse-tool-demo.yml/dispatches with
ref codex/fix-d20-icse-failure-evidence after the above guards.
