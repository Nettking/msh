# Upload/background-analysis production readiness

The remediation pass made uploaded JSONL the only durable payload copy. Import
validation is line-streamed and SQLite retains lifecycle/count metadata only;
this removes the previous full payload duplicate in `data_upload_records`.
Import admission is bounded to eight active/pending imports per Flask service.
Interrupted validation and partially completed publication are resumed, and the
publish move is idempotent. Restart recovery drains the durable FIFO queue again
whenever a worker slot is released, so batches beyond immediate worker capacity
cannot remain stranded in `queued`.
Publishing recovery verifies the exact expected file set by size and SHA-256. It resumes
missing atomic moves while the import marker keeps partial output hidden, or
recognizes a fully published directory and only finalizes its durable DB state.

Per-day filtering now consumes each candidate as an iterator. The source is read
once to build/refresh its durable date index and once to emit the selected day.
Previously it was read once for indexing and twice for every selected day while
also retaining every decoded object in a Python list. The deterministic 10,000
record regression measures **3 source opens before / 2 after** and changes peak
filter memory from **O(file records) / O(file bytes)** to **O(largest JSONL
record)**. Unchanged files reuse the index on subsequent days, so they require
only the single filtering scan rather than a full-history discovery rescan.

Analysis jobs now carry their durable job id into runtime execution. Completion
requires the matching `completed_execution_id`; unrelated global runtime starts,
failures, and completions cannot terminalize another job. Runtime reservations
also close the request/start race, persist success/failure independently, and
renew the existing lease throughout long analysis.

These changes remove the identified in-process blockers for a 200-day, 5 Hz
JSONL source: input-size RAM growth, repeated candidate rescans, payload disk
duplication, unbounded import threads, unsafe partial-publish restart, and global
runtime-state job inference. Actual end-to-end wall time remains dependent on the
selected analysis scripts and host storage/CPU; readiness does not grant new
Federation execution authority.
