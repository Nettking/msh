# PR 435: the stale-backup restore test is flaky, and why

A second, independent defect found while diagnosing the red self-hosted gate.
It is unrelated to the Windows disk shortfall and it will redden the Linux
release jobs on its own.

Status: **caused by PR 435**, in PR 435's own new test. The product behaves
correctly throughout; the test hands it a database that cannot be consistent
and the product refuses it, as designed.

## Symptom

`catalog/mtconnect_recorder/tests/test_multi_recorder_capability_identity.py::test_local_state_restored_from_a_stale_backup_reconverges`
fails intermittently, only inside a long-running process:

```
catalog/node/client.py:790: RelayRemoteError
E  catalog.node.client.RelayRemoteError: relay replay revisions were
   inconsistent (invalid-replay-completion)
```

raised from the third `await nitro.connect()`, the one that follows the
restore of the pre-migration backup.

Observed rates:

| Context | Result |
| --- | --- |
| the single test, 15 separate processes | 15 passed |
| the single test, 30 separate processes under CPU load | 30 passed |
| the whole file, 5 separate processes | 5 x 17 passed |
| **the full suite, 5 runs** | **2 failed, 3 clean** |
| **the scenario 300 times in one process** | **50 failed, 250 passed** |

Isolation hides it; a long process exposes it. That is why the Windows gate
(run 33956840831) saw all 17 tests of this file pass while a full-suite job
does not.

## Root cause

`catalog/node/state.py` opens the node state database in WAL mode:

```python
with self._connect() as database:
    database.execute("PRAGMA journal_mode=WAL")
```

so its committed state lives partly in `node_state.sqlite3` and partly in the
`node_state.sqlite3-wal` sidecar. The test backs the database up, and later
restores it, by copying the main file alone:

```python
await nitro.close()
shutil.copy(database, backup)
...
await nitro.close()
shutil.copy(backup, database)
```

The destination keeps its own `-wal` and `-shm`, so what the next client opens
is a mixture of the two databases rather than either one of them. Instrumenting
the replay apply path shows exactly that mixture:

```
relay replay revisions were inconsistent
(session=session-recorders before=0 last=10 current=10 after=0 has_more=False
 applied=[(1,'DUPLICATE'),(2,'DUPLICATE'),(3,'DUPLICATE'),(4,'DUPLICATE'),
          (5,'DUPLICATE'),(6,'DUPLICATE'),(7,'DUPLICATE'),(8,'DUPLICATE'),
          (9,'DUPLICATE'),(10,'GAP')])
```

The session watermark reads 0, as the backup has it, while the event log
already holds revisions 1..9, as the live database had them. The relay replays
1..10 from watermark 0; nine are refused as duplicates, the tenth is a gap, and
the watermark never moves. `after_revision (0) < last_revision (10)` is then
true and `_request_replay_pass` raises. **The product is right to refuse: that
database really is inconsistent.** Whether the copy tears this way depends on
whether SQLite had checkpointed and removed the WAL at that instant, which is
why the failure is intermittent and why it needs a busy process to appear.

## Fix

Restore the database as a whole. One helper, two call sites, no assertion
weakened, nothing skipped, no product code touched:

```diff
@@ -63,6 +63,30 @@ SESSION_ID = "session-recorders"
 # --- real rig ---------------------------------------------------------------
 
 
+def _copy_node_state(source: Path, destination: Path) -> None:
+    """Copy a node state database as a whole, WAL sidecar included.
+
+    ``node_state.sqlite3`` runs in WAL mode, so its committed state is split
+    between the database file and its ``-wal`` sidecar. Copying the database
+    file alone leaves the destination's own sidecar in place, and the reopened
+    database is then a mixture of the two: the event log can hold revisions the
+    session watermark does not. The node's replay check refuses that -- rightly
+    -- with ``invalid-replay-completion``, so a partial copy tests the refusal
+    rather than the restore this scenario is about. Move the whole set, and drop
+    the destination's stale shared-memory index so it is rebuilt from the copied
+    WAL rather than from the replaced one.
+    """
+
+    for suffix in ("", "-wal"):
+        origin = Path(str(source) + suffix)
+        target = Path(str(destination) + suffix)
+        target.unlink(missing_ok=True)
+        if origin.exists():
+            shutil.copy(origin, target)
+    Path(str(destination) + "-shm").unlink(missing_ok=True)
+
+
+
 @dataclass
 class Recorder:
     """One physical recorder host: its own state directory and identity."""
@@ -748,7 +772,7 @@ def test_local_state_restored_from_a_stale_backup_reconverges(
             database = nitro.state_directory / "node_state.sqlite3"
             backup = tmp_path / "nitro-node-state.backup"
             await nitro.close()
-            shutil.copy(database, backup)
+            _copy_node_state(database, backup)
 
             # Move on: the node converges to the scoped identity.
             nitro.runtime = _runtime(nitro.state_directory, nitro.name)
@@ -763,7 +787,7 @@ def test_local_state_restored_from_a_stale_backup_reconverges(
 
             # Now restore the pre-migration backup over the live state.
             await nitro.close()
-            shutil.copy(backup, database)
+            _copy_node_state(backup, database)
             nitro.runtime = _runtime(nitro.state_directory, nitro.name)
             nitro.node.runtime = nitro.runtime
             await nitro.connect()
```

After the fix, the same scenario run 300 times in one process: **300 passed,
0 failed**, against **50 failed, 250 passed** on the unmodified test. The whole
file still passes (17 tests) and `ruff check` on it is clean.

## Reproduction

```bash
# 1. copy the test file, parametrise the scenario 300 times
#    @pytest.mark.parametrize("iteration", range(300))
#    def test_local_state_restored_from_a_stale_backup_reconverges(
#        tmp_path: Path, iteration: int) -> None:
# 2. run it in one process, off the repository tmp root
python -m pytest -o addopts= -q -p no:randomly \
  --basetemp=/tmp/stress \
  stress/test_stress_stale_backup.py
# unmodified: 50 failed, 250 passed
# with the fix: 300 passed
```

## The first fix was POSIX-only

Copying the database and its `-wal` side by side, after clearing the
destination's stale sidecars, is correct on POSIX and wrong on Windows: a file
cannot be unlinked there while any handle is still open on it. Once Nettking
had the disk and the storage subset ran for real, the gate reported

```
catalog\mtconnect_recorder\tests\test_multi_recorder_capability_identity.py:83:
    in _copy_node_state
    target.unlink(missing_ok=True)
E   PermissionError: [WinError 32] ... node_state.sqlite3
```

with the other 366 tests of that subset passing.

The restore now goes through SQLite's online backup API instead, which reads a
consistent snapshot and writes it through the destination's own connection. No
sidecar is removed by hand, so there is nothing for Windows to refuse, and the
same code is correct on both platforms. The scenario still passes 300 out of
300 repetitions in one process.

## Where the fix landed

The wrapper on `ci/self-hosted-pr435` cannot help here: the failure is in the
candidate's own test, so the fix belongs on the product branch.

* `claude/federation-recorder-capability-id-19tqkk`
  `13967aea9f4561cea64b5427bdf572de823ec577` ->
  `b2a7c6e5fb68bc4d4dbbb57322ca496feb1438a9`
  (`test: restore the whole node state database, not just its main file`) ->
  **`f0434e4a4fd4cf86b3574563151d1e6241b4e06c`**
  (`test: move the node state through SQLite instead of over the filesystem`)
* `ci/self-hosted-pr435` `VALIDATED_SHA` follows that head, so the gate
  validates the candidate that can actually be merged.

No product code changed. The diff is confined to
`catalog/mtconnect_recorder/tests/test_multi_recorder_capability_identity.py`:
one helper and two call sites.
