# D16 bounded predecessor-context follow-up

The original-host isolated original and observed runs did not reproduce D16.
Preserve controlf96ec892/run34712329572 and its two passes as diagnostic only.
Original native Windows JUnit places D16 at index175, immediately after:

1. `test_actual_release_voters_resume_interrupted_fresh_bootstrap[after-genesis]`
2. `test_actual_release_voters_resume_interrupted_fresh_bootstrap[after-journal-before-seal]`
3. `test_different_voter_completes_exact_witnessed_prefix_after_first_real_chunk`

Source5e6 inspection finds that these predecessor fixtures explicitly close their
owned runtimes. No resource leak or causal order dependency is established.
The D16 error code originates at the initial leader role/identity guard; it is
distinct from failure to obtain quorum later in the same method. Why the initial
leader was no longer authoritative in the failed CI remains unknown.

One bounded same-process replay of these three predecessors followed by the
unchanged D16 test is warranted by the exact original JUnit order. Use the same
Beast runner, checked-in source5e6, Python/dependency pins, temporary fixture
ownership and exclusions as D16_BOUNDED_BEAST_DIAGNOSTIC_PLAN.md. The prior
workflow has completed; its control commit, scripts, logs and artifact are saved.

Add an explicit context mode to the coordination-only driver/workflow. In this
mode run the four tests once in the original order, in one fresh process with a
300-second outer process bound (original test/product deadlines unchanged).
No full journal suite, seed search, loop or retry. On failure preserve and stop.

Extend the forwarding observer to record live thread names at test boundaries
and exceptions escaping the existing D16 lifecycle-round method. Observe only;
forward returns and re-raise unchanged exceptions, with no extra probes or
elections. Snapshot timing may perturb behavior and remains a limitation.
This can distinguish a reproduced lifecycle exception from an unexplained
role change and reveal, but cannot by itself attribute, surviving predecessor
threads. Record only test-owned process metadata, no secrets or host services.

If this sequence passes, preserve the non-reproduction and stop repeating D16
in isolation or short contexts. Shift to D14/D15 order/error evidence, keeping
D16 unresolved unless new causal evidence appears. No candidate/source/runner
change, qualification PASS or physical acceptance inference is permitted.

Evidence: diagnostics/D16-original-native-order-context.json and
diagnostics/D16-original-beast-reviewed.json. Next exact action: implement the
reviewed context mode, statically validate, commit/push, confirm startup once,
then inspect near the expected 5-minute completion window.
