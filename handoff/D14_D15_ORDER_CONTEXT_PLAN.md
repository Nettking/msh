# Bounded D14/D15 order and error diagnosis

Candidate remains clean5e6f184311019b9982e8544a18f3dc02c1b16e98. These are local
NETTKING Ubuntu WSL development observations, not original AQG host evidence,
qualification, or physical acceptance. Original isolated2PASS is preserved.

New context: retain pytest-randomly4.1.0 seed1702 per-test state, but explicitly
disable reordering to replay the retained original JUnit neighbor order.
Run D14 with its eight immediately preceding cases (indices483-491), then D15
with its immediately preceding fresh/restart/expiry case (indices672-673).
Use separate processes to keep the two observations independent; once per group,
180-second outer bounds, unchanged2-second/adapter/authorization deadlines.
Do not replay the full suite or search seeds/retry until green.

Use the existing constrained Python3.12.13 development venv; install only the
same pytest-randomly4.1.0 plugin used by original CI into that venv. No candidate
dependency/source/CI configuration change or deployment. Original tests use
their owned temporary stores, fake recorder runtime and existing test auth
fixture. No production Recorder path, Docker, physical services or runner change.

External forwarding observer: preserve exact original return values/exceptions;
record the actual inspection response code, validation refusal code and only
CSRF presence/equality booleans (never token values). On D14 assertion failure,
capture the background error list and its thread stack function/file/line,
without modifying or unblocking the benchmark. Record test order/timing and
thread metadata. Prior original observer output must not be overwritten.

Source review narrows the ordinary inspection403 path to request validation or
an AuthorizationError from inspection/authorized context; it does not yet prove
CSRF failure. D14 failed before its try/finally cleanup; leaked thread/order
effects remain a hypothesis, not a confirmed additional defect. The observer
must distinguish these possibilities without weakening any guard.

Preserve commands, pinned versions, native source before/after, logs/JUnit and
observer output immediately after completion. A new independent failure requires
its own durable issue/checkpoint before continuing. Passing contexts do not
resolve D14/D15 or justify candidate qualification.
