# One bounded worker-frame capture after stacked qualification settles

The new native f104 ICSE failures now retain public operation/type reports:
Windows voter-a bootstrap QuorumUnavailable, Linux voter-b successor connect
TimeoutError. They do not identify the raising worker source line. The worker
already writes that traceback to its CI-owned private stderr, which is lost
during runner cleanup. D20 works as designed; no product repair is established.

On Linux the public predecessor snapshot proves voter-b was the ready successor
at term2/index11, with voter-c ready and the committed membership/capabilities
intact. Connect starts after that snapshot and fails about26seconds later. Its
operation includes disconnect, handshake, authentication, coordinator status and
replay: the public error does not identify which15second request timed out.
Bootstrap also has multiple quorum-checked proposal/election paths. Do not pick
a root cause from exception type alone, or assign it retroactively to old5e.

Next controlled action, only after release34746641263 is terminal and retained:

1. Use one coordination-only Linux job on the existing qualified
   `[self-hosted, Linux, X64, beast-linux, fcp-linux-fast]` labels; require original
   runner Beast-Linux-WSL/workspace/Python3.12.13. Do not change pools or labels.
2. Check out exactf104038a2b77705adaa547cdd1df7d895bf80a41, clean, and use its
   self-hosted Python action. Install the original ICSE commands unchanged:
   `python -m pip install --upgrade pip`,
   `python -m pip install -r requirements.txt -c constraints-phase2.txt`,
   `python -m pip install ruff`. Retain actual package versions; compare against
   original native installation logs before calling environments matched.
3. Extract the reviewed external capture script from the exact coordination
   commit into a new runner-temp evidence directory; never edit candidate files.
4. Run the original checked-in network command exactly once. Preserve its exit
   status and deadlines. Before cleanup, read only this execution's owned worker
   stderr files and publish bounded tracked source file/line frames and a fixed
   exception-type vocabulary. Never publish exception messages, source-line text,
   raw stderr, private-state, tokens, configs or databases.
5. Upload only the external script, sanitized receipt and original public files.
   No retry if it passes or fails; retain the observation and decide from its
   evidence. This is focused qualification-failure triage, not a restarted sweep,
   a release qualification job, a D18 repair, or physical acceptance.

The existing old5e/constraints-release D18 diagnostic must not be re-run or edited.
This new one-shot captures worker frames missing from the newly demonstrated
failure, uses the actual ICSE dependency commands, and does not seek a green
qualification result. No AQG dependency or protected Recorder access.

Activation authorized after release34746641263 completed and all native evidence was retained. The workflow is now copied byte-for-byte from the reviewed template on the coordination branch. D18-worker-frame-activation-guard.json records unchanged PR/source/main guards; the next checkpoint must record the exact activation commit and resulting run ID. Five offline safety cases remain PASS. One execution only, then evidence review; all PR heads stay fixed.
