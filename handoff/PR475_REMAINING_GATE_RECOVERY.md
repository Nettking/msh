# PR475 remaining required-gate recovery

Exact source/PRhead5e6f184311019b9982e8544a18f3dc02c1b16e98; mainb719 and
separate retirement PR473440 unchanged. Before-state receipt:
diagnostics/pr475-remaining-gates-before-recovery.json. All original native
qualification runs are terminal. F85 infrastructure-only retry now passes,
raising verified scope to26/37 required plus3/3 companions. No candidate frozen.

## Minimal recovery scope

| Existing run | Preserve without execution | Recover once |
|---|---|---|
|34734869036 release attempt1|Seven passed jobs, including both full-suite orders, PostgreSQL, Linux shard0, Windows release/capability/journal|Five infrastructure-cancelled jobs, one failed Windows transport/storage job, and three downstream release aggregates not created; nine expected remaining completions|
|34734857086 ICSE attempt1|Windows reviewer entrypoint and reviewer Compose|Failed Linux reviewer entrypoint and skipped dependent publication bundle; two expected remaining completions|

Use GitHub failed-jobs-only retry once per existing run. Preserve successful
results/timestamps even if GitHub assigns cloned job IDs in the new attempt.
Do not dispatch a new whole workflow, rerun any other qualification, change
source/head/commands/dependencies/deadlines or adjust runner pools/accounts/labels.
Before each call verify unchanged exact source, terminal attempt1 and no already
accepted retry. Persist each accepted action immediately before the next mutation.

## Why this continuation is justified and its limits

D19/#481 cancellations have explicit GitHub-internal-error annotations. F85
recovered on unchanged source without rerunning successful Linux. These cancellations
are infrastructure gaps, not product failures; release still needs their execution.
D17/#479 original Windows timeout is fully retained and read-only request lifecycle
review completed. D18/#480 original failure, missing detailed error and one bounded
same-host clean-source non-reproduction are retained. No deterministic product
regression or concrete repair has been established for either. Safe isolated CI
continuation does not require changing the candidate or trusting a failed invariant
from a previous process: each checked-in job creates its own disposable fixtures.

The one failed-job recovery below resumes remaining required qualification on the
unchanged intended head after failure review; it is not another diagnostic sweep,
D18 diagnostic repetition or an attempt to relabel diagnostics as acceptance. It
also does NOT dismiss the D17/D18 observations. They remain unresolved historical
failures until evidence supports a narrower disposition. A green retry alone will
not establish root cause, close issues or freeze a release candidate. No blind
retry loop: recurrence must first update its issue/evidence and trigger focused
mechanism work, not another automatic failed-jobs retry.

Native logs must verify5e again, and aggregation must cover all required identities
without treating cancelled/missing jobs as passes. If GitHub retries an unintended
successful execution, record the discrepancy and stop expanding the scope. Keep
original failed logs/raw artifacts and original successful logs immutable.

## Next checks

Confirm new attempts/job allocation once. Ordinary long qualification polling no
earlier than2026-09-13T06:35:00UTC; if an actual terminal event arrives sooner, act
on that event. Short ICSE may finish sooner, but no repeated progress polling is
needed. Existing30-minute heartbeat remains; intervening unchanged runs stay quiet.
No source freeze/merge/physical deployment, protected Recorder-data access, Docker
reset/prune, AQG dependency or P07/P12. Coordination-only files stay outside candidates.
