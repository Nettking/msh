# B01 recorder resource admission delivery

Status: implementation candidate

This delivery closes the recorder aggregate-admission gap left by the existing pressure integration.

The recorder already had finite per-source transaction bounds and pressure-aware pause/recovery behavior, but its guard created a private admission controller per runtime and admitted multi-filesystem requirements sequentially. That violated the B01 invariant established by the other supported writers: supported in-process writers on the same backing resource must share one process-wide reservation ledger, and logical transactions spanning several resources must publish reservations atomically.

This candidate therefore:

- routes the default recorder guard through `PROCESS_RESOURCE_ADMISSION`;
- uses `reserve_many()` for new recorder transactions so same-resource requirements coalesce and cross-resource admission is all-or-nothing;
- keeps the distinct recovery-completion rule, but performs that completion admission atomically across all backing resources using the same shared controller and reservation ledger;
- preserves raw-first/checkpoint-last ordering, source-local pause behavior, bounded recovery, and existing shutdown draining semantics;
- adds targeted regressions for process-wide controller identity, cross-resource refusal without partial accounting, same-resource coalescing, and PRESSURE-vs-CRITICAL completion semantics.

No physical Federation evidence or acceptance state changes with this delivery.
