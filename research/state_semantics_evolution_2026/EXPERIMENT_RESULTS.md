# Executed operational-state evolution results

Measurement timestamp (UTC): `2026-11-09T13:49:54.256014+00:00`. This report is generated from the measured JSON summaries; it makes no claim of physical ground truth.

## Executed scope

Evaluated **24 source windows**, **4,561 original rows**, and **4 pseudonymous machines**. Source archive SHA256: `1daf6ec6a899b12a21302f8f8a1768c1cbcc4d2c6d8785a646f8567d13f282e5` (25,241,600 compressed bytes).

Sampling policy: complete timestamp-group blocks; greedy feature coverage then temporal quantiles. Target: 6 windows per machine and 200 records per block, retaining whole timestamp groups. A final positive-time block can be shorter than the record target.

Selection limitation: purposive balanced sample of first1000-batch verified subset; not representative of complete corpus. The same original source windows feed every scenario and mutant. Windows have strict timestamp separation within each machine; machines can overlap in clock time. Window counts are not independent statistical replications.

The production classifier assesses **active, non-active and unclassified** operation. There are no independent physical labels, so these results cannot establish stable/unstable machining, fault-detection accuracy or actual production downtime.

## Dataset and baseline coverage

JSONL members inventoried: **4,000**. Reader inventory: `{"dictionary_records": 223188, "excluded_single_timestamp_records": 75, "invalid_timestamp_records": 0, "lines": 223188}`. Schema inventory: `{"fcp.mtconnect.snapshot.v2": 223188}`.

Selected blocks contain **11–216 records**. **19/24** windows satisfy the production sufficient-data criterion; **1** windows have zero usable time. Zero usable time gives undefined class coverage, not evidence of downtime or perfect coverage. Short final blocks retained: `55a4889a2041-block-0505` = 11 records; `97c10d578ab2-block-0011` = 78 records.

| Machine hash | Windows | Rows | Active s | Non-active s | Unclassified s | Class coverage | Wall-span coverage | Excluded long-gap s | Excluded short s | Sufficient-data windows |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0237f82ecd8e | 6 | 1223 | 182.385 | 59.343 | 7.363 | 97.044% | 97.044% | 0.000 | 0.000 | 3/6 |
| 55177bb0edce | 6 | 1236 | 169.495 | 540.252 | 0.000 | 100.000% | 53.146% | 625.708 | 0.000 | 5/6 |
| 55a4889a2041 | 6 | 1018 | 43.598 | 406.953 | 22.075 | 95.329% | 82.349% | 74.455 | 0.045 | 5/6 |
| 97c10d578ab2 | 6 | 1084 | 0.000 | 1035.422 | 0.203 | 99.980% | 69.245% | 459.609 | 0.078 | 6/6 |

Class coverage = classified usable time / usable time. Wall-span coverage = classified time / positive elapsed span. These differ from production `continuity`, which divides usable by positive interval counts. Intervals outside [0.1,30] seconds are excluded; zero-length duplicate intervals have no duration. Unclassified usable time is kept distinct from excluded gap time. Final observations are not extrapolated.

Selected-window condition inventory (counts can overlap): active_rows=2398; conflicting_state_timestamp_groups=23; duplicate_timestamp_records=2980; fallback_classified_rows=1186; fallback_rows=1504; long_gaps=20; missing_execution=1374; non_active_rows=1845; row_label_adjacency_changes=25; threshold_sensitive_rows=7; unavailable_execution=130; unclassified_rows=318.

The inventory's **25 row-label adjacency changes** include neighboring raw rows and timestamp ties. They are a sampling feature, not the temporal outcome metric. The baseline measurement contains **21 observable transition boundaries** under the declared final-state-per-timestamp and usable-interval convention.

All requested condition categories have at least one selected historical witness; this does not establish representative sampling.

## Controlled scenario comparisons

Agreement below is pooled over compared original source rows. Duplicated records do not inflate the denominator. Duration deltas are signed sums across windows, so opposite changes can cancel; full per-window measurements and row traces remain in `MEASUREMENTS.json`. Transition counts report removed/added observable boundaries under the declared timestamp-tie and usable-interval convention.

| Scenario | Compared rows | Changed rows | Pooled agreement | Δ active s | Δ non-active s | Δ unclassified s | Transitions removed/added | Windows with duration/transition differences | Verdicts |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| E1 | 4561 | 0 | 100.000% | 0.000 | 0.000 | 0.000 | 0/0 | 0/0 | pass=24 |
| E2 | 4561 | 0 | 100.000% | 0.000 | 0.000 | 0.000 | 0/0 | 0/0 | pass=24 |
| E3 | 4561 | 0 | 100.000% | 0.000 | 0.000 | 0.000 | 0/0 | 0/0 | pass=24 |
| E4 | 4561 | 75 | 98.356% | -18.229 | 18.229 | 0.000 | 0/29 | 4/4 | pass=24 |
| E5_conflict | 4561 | 0 | 100.000% | 4.992 | -6.302 | 1.310 | 10/7 | 4/4 | ambiguous_ties=8; pass=16 |
| E5a | 4561 | 0 | 100.000% | 0.000 | 0.000 | 0.000 | 0/0 | 0/0 | pass=24 |
| E5b | 4561 | 0 | 100.000% | -7.642 | -15.888 | 0.000 | 11/11 | 23/4 | pass=24 |
| E6 | 4561 | 7 | 99.847% | -1.232 | 1.232 | 0.000 | 1/1 | 1/1 | pass=24 |

E1–E3 test declared preservation. E4 permits changes only in masked execution rows. E5a preserves tie order and inserts an adjacent identical duplicate. E5b intentionally removes the usable interval crossed by the injected gap and moves later timestamps. E6 intentionally changes the fallback speed threshold from >100 to >150 rpm; expected changes are checked against the predefined mask. These expected changes are not defects.

The arbitrary timestamp-tie probe has **8 ambiguous trials**. Ambiguity does not imply an observed change in every such trial; the duration/transition window counts above distinguish actual sensitivity. Such trials have no unconditional preservation contract and are excluded from valid-control false-positive denominators. Their numerical differences remain reported in the scenario table and JSON.

- **E1:** 0 changed source-row classifications; 0 unexpected-outcome window trials.
- **E2:** 0 changed source-row classifications; 0 unexpected-outcome window trials.
- **E3:** 0 changed source-row classifications; 0 unexpected-outcome window trials.
- **E6:** 7 changed source-row classifications; 0 unexpected-outcome window trials.

## Injected defects and error accounting

B1 has the same source rows, declared invariants and expected-change masks as all evaluated variants. Activation means the mutant caused an original-row classification difference. Detection means that B1 rejected its preservation contract using rows, durations, transitions or interval counts. A non-activated mutant trial is not an activated miss.

| Defect type | Window trials | Activated | Detected | Activated misses | Not activated | Changed source rows |
| --- | --- | --- | --- | --- | --- | --- |
| omitted_provider_load | 24 | 2 | 2 | 0 | 22 | 8 |
| omitted_rename_adapter | 24 | 6 | 6 | 0 | 18 | 745 |
| unknown_as_non_active | 24 | 4 | 4 | 0 | 20 | 318 |
| wrong_unit_adapter | 24 | 3 | 3 | 0 | 21 | 231 |

Defect-trial totals: **96 trials**, **15 activated**, **15 detected**, **0 activated misses**, **81 not activated**. Type totals: 4 injected types, 4 activated types, 4 detected types, 0 completely missed activated types. These are selected sensitivity witnesses, not an estimate of defect detection in the population of real changes.

Valid-control rejection count: **0/184** (0.000%), excluding 8 ambiguous tie trials and non-activated scenarios. Every rejection must be investigated as an unexpected valid-control outcome; the label does not establish that the harness, rather than the implementation, is correct. Measured unexpected experiment outcomes: **0**.

B0 consists of the unchanged production regression tests. The three research adapter mutants are outside their execution scope and are reported as unexercised. The shared unknown-as-non-active policy can be exercised by B0 using an isolated in-memory classifier mutation; its separate execution evidence appears below when supplied. B1 is conventional regression/differential/metamorphic testing. **B2 was not implemented** because no equal-information B1 limitation justified an additional procedure. Traceability and explicit expectations alone are not a novel testing method.

## Per-window baseline and selection audit

| Window ID | Rows | UTC start / end | Usable s | Unclassified s | Class coverage | Long gaps | Duplicate timestamp rows | Conflicting timestamp groups | Sufficient data | Selection strata |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0237f82ecd8e-block-0000 | 206 | 2026-10-06T12:29:52.877196+00:00 / 2026-10-06T12:30:34.139269+00:00 | 41.262 | 7.363 | 82.155% | 0 | 172 | 2 | False | missing_execution=206;unavailable_execution=0;fallback_classified_rows=163;threshold_sensitive_rows=0 |
| 0237f82ecd8e-block-0011 | 201 | 2026-10-06T12:36:11.554520+00:00 / 2026-10-06T12:36:46.623382+00:00 | 35.069 | 0.000 | 100.000% | 0 | 172 | 0 | False | missing_execution=201;unavailable_execution=0;fallback_classified_rows=201;threshold_sensitive_rows=0 |
| 0237f82ecd8e-block-0022 | 205 | 2026-10-06T12:42:01.572335+00:00 / 2026-10-06T12:42:27.873981+00:00 | 26.302 | 0.000 | 100.000% | 0 | 183 | 0 | False | missing_execution=205;unavailable_execution=0;fallback_classified_rows=205;threshold_sensitive_rows=0 |
| 0237f82ecd8e-block-0032 | 206 | 2026-10-06T12:49:09.112114+00:00 / 2026-10-06T12:49:59.579660+00:00 | 50.468 | 0.000 | 100.000% | 0 | 165 | 4 | True | missing_execution=206;unavailable_execution=0;fallback_classified_rows=206;threshold_sensitive_rows=0 |
| 0237f82ecd8e-block-0042 | 201 | 2026-10-06T12:54:37.175320+00:00 / 2026-10-06T12:55:30.558614+00:00 | 53.383 | 0.000 | 100.000% | 0 | 157 | 6 | True | missing_execution=201;unavailable_execution=0;fallback_classified_rows=201;threshold_sensitive_rows=7 |
| 0237f82ecd8e-block-0043 | 204 | 2026-10-06T12:55:31.791016+00:00 / 2026-10-06T12:56:14.398148+00:00 | 42.607 | 0.000 | 100.000% | 0 | 169 | 6 | True | missing_execution=204;unavailable_execution=0;fallback_classified_rows=204;threshold_sensitive_rows=0 |
| 55177bb0edce-block-0000 | 216 | 2026-10-05T03:55:44.739281+00:00 / 2026-10-05T03:56:39.956570+00:00 | 0.000 | 0.000 | n/a | 1 | 214 | 1 | False | missing_execution=91;unavailable_execution=56;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55177bb0edce-block-0001 | 208 | 2026-10-05T03:56:44.351771+00:00 / 2026-10-05T04:12:34.577798+00:00 | 476.798 | 0.000 | 100.000% | 9 | 121 | 1 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55177bb0edce-block-0106 | 201 | 2026-10-05T05:25:30.286189+00:00 / 2026-10-05T05:26:08.140362+00:00 | 37.854 | 0.000 | 100.000% | 0 | 119 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55177bb0edce-block-0213 | 209 | 2026-10-05T09:39:54.349401+00:00 / 2026-10-05T09:40:21.724561+00:00 | 27.375 | 0.000 | 100.000% | 0 | 145 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55177bb0edce-block-0319 | 200 | 2026-10-05T10:49:13.669012+00:00 / 2026-10-05T10:53:26.456574+00:00 | 155.725 | 0.000 | 100.000% | 2 | 130 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55177bb0edce-block-0426 | 202 | 2026-10-05T11:15:47.688244+00:00 / 2026-10-05T11:15:59.683706+00:00 | 11.995 | 0.000 | 100.000% | 0 | 160 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55a4889a2041-block-0000 | 200 | 2026-10-05T05:04:58.179998+00:00 / 2026-10-05T05:07:13.486922+00:00 | 60.806 | 22.075 | 63.697% | 1 | 165 | 2 | True | missing_execution=28;unavailable_execution=74;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55a4889a2041-block-0101 | 201 | 2026-10-05T08:22:27.324571+00:00 / 2026-10-05T08:25:23.274672+00:00 | 175.950 | 0.000 | 100.000% | 0 | 20 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55a4889a2041-block-0202 | 203 | 2026-10-05T10:44:25.579174+00:00 / 2026-10-05T10:44:50.799269+00:00 | 25.220 | 0.000 | 100.000% | 0 | 133 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55a4889a2041-block-0303 | 201 | 2026-10-05T11:37:24.637441+00:00 / 2026-10-05T11:40:37.960765+00:00 | 193.323 | 0.000 | 100.000% | 0 | 28 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55a4889a2041-block-0404 | 202 | 2026-10-05T12:26:43.193288+00:00 / 2026-10-05T12:26:59.748332+00:00 | 16.555 | 0.000 | 100.000% | 0 | 160 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 55a4889a2041-block-0505 | 11 | 2026-10-05T12:53:13.274237+00:00 / 2026-10-05T12:53:14.045293+00:00 | 0.771 | 0.000 | 100.000% | 0 | 8 | 0 | False | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 97c10d578ab2-block-0000 | 201 | 2026-10-06T06:42:41.156250+00:00 / 2026-10-06T06:47:30.140625+00:00 | 112.031 | 0.203 | 99.819% | 2 | 144 | 1 | True | missing_execution=32;unavailable_execution=0;fallback_classified_rows=6;threshold_sensitive_rows=0 |
| 97c10d578ab2-block-0002 | 202 | 2026-10-06T07:02:20.328125+00:00 / 2026-10-06T07:11:39.328125+00:00 | 276.266 | 0.000 | 100.000% | 5 | 117 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 97c10d578ab2-block-0004 | 202 | 2026-10-06T07:18:37.375000+00:00 / 2026-10-06T07:20:04.296875+00:00 | 86.922 | 0.000 | 100.000% | 0 | 144 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 97c10d578ab2-block-0007 | 201 | 2026-10-06T07:45:41.078125+00:00 / 2026-10-06T07:47:32.468750+00:00 | 111.391 | 0.000 | 100.000% | 0 | 140 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 97c10d578ab2-block-0009 | 200 | 2026-10-06T07:49:22.453125+00:00 / 2026-10-06T07:54:33.562500+00:00 | 311.109 | 0.000 | 100.000% | 0 | 14 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |
| 97c10d578ab2-block-0011 | 78 | 2026-10-06T08:00:08.546875+00:00 / 2026-10-06T08:02:26.453125+00:00 | 137.906 | 0.000 | 100.000% | 0 | 0 | 0 | True | missing_execution=0;unavailable_execution=0;fallback_classified_rows=0;threshold_sensitive_rows=0 |

Each `MEASUREMENTS.json` window index resolves to the corresponding manifest window. A changed-row reference is a zero-based ordinal in that canonical, source-ordered window. The manifest provides source member-content and member-name hashes plus one-based inclusive source-line ranges, making the same input rows reconstructible on Nitro without publishing records or machine names. Per-difference before/after labels, duration deltas, added/removed boundaries and scenario labels are retained in the measured JSON.

## Software, resources and test execution

Experiment source commit: `e6f199be9e5917f5ae5e66f94e17e2fcff6ea2ab`. Tracked working-tree changes at execution: `False`. Test-validation commit: `e6f199be9e5917f5ae5e66f94e17e2fcff6ea2ab`. Exact source SHA256 values are preserved in the measurement and dataset JSON; a later report-only commit does not retroactively change the executed source revision.

Python: `3.12.13 (main, Aug  7 2026, 02:25:39) [Clang 22.1.3 ]`. Platform: `Linux-7.0.0-30-generic-x86_64-with-glibc2.43`. Dependencies: `{"numpy": "2.5.2", "pandas": "3.0.5", "pytest": "9.1.1", "python-dateutil": "2.9.0.post0"}`.

Measured process costs: wall **71.177 s**, CPU **66.636 s**, peak RSS **335364 KiB** (Linux `ru_maxrss`, including dataset selection and evaluation). One-host costs are descriptive; they establish neither capacity qualification nor scalability.

### Executed B0 tests

Exit code: **0**. Wall seconds: **99.732**.

Exact argument vector:

```json
["/home/martin/dt-state-evolution-20261109/.venv312/bin/python", "-m", "pytest", "-o", "addopts=", "-q", "catalog/flask_app/tests/test_digital_twin_service.py", "--basetemp", "results/test-tmp-B0"]
```

Captured stdout:

```text
......                                                                   [100%]
6 passed in 90.42s (0:01:30)
```

Captured stderr:

```text
(empty)
```

### Executed B1 tests

Exit code: **0**. Wall seconds: **7.381**.

Exact argument vector:

```json
["/home/martin/dt-state-evolution-20261109/.venv312/bin/python", "-m", "pytest", "-o", "addopts=", "-q", "research/state_semantics_evolution_2026", "--basetemp", "results/test-tmp-B1"]
```

Captured stdout:

```text
.........................                                                [100%]
25 passed in 6.96s
```

Captured stderr:

```text
(empty)
```

### Executed B0 shared-policy sensitivity probe

Mutant: `unknown_as_non_active`. Source commit: `e6f199be9e5917f5ae5e66f94e17e2fcff6ea2ab`. Exit code: **1**; captured assertion failures: **1**; collection/setup/teardown/other call errors: **0**. The probe reports assertion-based detection: **True**. A nonzero exit caused by environmental errors alone would not count as defect detection.

Research adapters outside B0 execution scope: `wrong_unit_adapter`, `omitted_rename_adapter`, `omitted_provider_load`. These are not activated misses and do not establish an advantage over conventional testing.

Exact probe argument vector:

```json
["C:\\Users\\utlan\\AppData\\Local\\Programs\\Python\\Python312\\python.exe", "-c", "\nimport json\nimport sys\nimport pytest\nfrom catalog.flask_app.services import digital_twin_service as production\n\noriginal = production._classify_rows\ndef unknown_as_non_active(frame):\n    result = original(frame)\n    unknown = result[\"state_class\"].isna()\n    result.loc[unknown, \"state_class\"] = \"non_active\"\n    result.loc[unknown, \"state_label\"] = \"DEFECT: unknown as non-active\"\n    return result\n\nproduction._classify_rows = unknown_as_non_active\n\nclass Evidence:\n    def __init__(self):\n        self.assertion_failures = []\n        self.other_call_failures = []\n        self.setup_teardown_errors = []\n        self.collection_errors = []\n    def pytest_runtest_logreport(self, report):\n        if not report.failed:\n            return\n        crash = getattr(report.longrepr, \"reprcrash\", None)\n        message = getattr(crash, \"message\", str(report.longrepr))\n        item = {\"nodeid\": report.nodeid, \"when\": report.when, \"message\": message}\n        if report.when != \"call\":\n            self.setup_teardown_errors.append(item)\n        elif message.startswith(\"AssertionError\") or message.startswith(\"assert \"):\n            self.assertion_failures.append(item)\n        else:\n            self.other_call_failures.append(item)\n    def pytest_collectreport(self, report):\n        if report.failed:\n            self.collection_errors.append({\"nodeid\": report.nodeid, \"message\": str(report.longrepr)})\n\nevidence = Evidence()\narguments = [\"-o\", \"addopts=\", \"-q\", \"catalog/flask_app/tests/test_digital_twin_service.py\",\n             \"--basetemp\", sys.argv[1]]\nresult = pytest.main(arguments, plugins=[evidence])\nprint(\"DT_B0_PROBE_SUMMARY=\" + json.dumps(vars(evidence), sort_keys=True))\nraise SystemExit(int(result))\n", "C:\\Users\\utlan\\AppData\\Local\\Temp\\dt-evolution-b0-mutant-final-20261109"]
```

Captured probe stdout:

```text
..F...                                                                   [100%]
================================== FAILURES ===================================
____ test_execution_state_precedes_signal_fallback_and_unknown_is_not_idle ____

    def test_execution_state_precedes_signal_fallback_and_unknown_is_not_idle() -> None:
        frame = pd.DataFrame(
            [
                {"execution": "ACTIVE", "Srpm": 0, "Sload": 0},
                {"execution": "UNAVAILABLE", "Srpm": 101, "Sload": 0},
                {"execution": "UNAVAILABLE", "Srpm": 0, "Sload": 0},
                {"execution": "OTHER", "Srpm": 800, "Sload": 8},
                {"execution": "UNAVAILABLE", "Srpm": None, "Sload": None},
            ]
        )

        classified = _classify_rows(frame)

        assert classified["state_class"].tolist()[:3] == ["active", "active", "non_active"]
        assert classified["state_label"].tolist()[:3] == [
            "ACTIVE",
            "ACTIVE (signals inferred)",
            "NON-ACTIVE (signals inferred)",
        ]
>       assert classified["state_class"].isna().tolist() == [
            False,
            False,
            False,
            True,
            True,
        ]
E       assert [False, False... False, False] == [False, False...e, True, True]
E
E         At index 3 diff: False != True
E         Use -v to get more diff

catalog\flask_app\tests\test_digital_twin_service.py:45: AssertionError
=========================== short test summary info ===========================
FAILED catalog/flask_app/tests/test_digital_twin_service.py::test_execution_state_precedes_signal_fallback_and_unknown_is_not_idle
1 failed, 5 passed in 3.12s
DT_B0_PROBE_SUMMARY={"assertion_failures": [{"message": "assert [False, False... False, False] == [False, False...e, True, True]\n  \n  At index 3 diff: False != True\n  Use -v to get more diff", "nodeid": "catalog/flask_app/tests/test_digital_twin_service.py::test_execution_state_precedes_signal_fallback_and_unknown_is_not_idle", "when": "call"}], "collection_errors": [], "other_call_failures": [], "setup_teardown_errors": []}
```

Captured probe stderr:

```text
(empty)
```

### Self-hosted CI evidence

The supplied CI evidence records runner identity, source revision, test output, final conclusion and cleanup. CI executes synthetic/regression tests and does not receive historical telemetry. A successful CI test suite is separate from the Nitro historical replay measurements.

```json
{
  "conclusion": "success",
  "created_at": "2026-11-09T13:46:06Z",
  "head_sha": "e6f199be9e5917f5ae5e66f94e17e2fcff6ea2ab",
  "html_url": "https://github.com/Nettking/msh/actions/runs/42628360908",
  "id": 42628360908,
  "status": "completed",
  "updated_at": "2026-11-09T13:48:41Z",
  "source_commit": "e6f199be9e5917f5ae5e66f94e17e2fcff6ea2ab",
  "run_id": 42628360908,
  "run_url": "https://github.com/Nettking/msh/actions/runs/42628360908",
  "runner_name": "Beast-Linux-WSL",
  "runner_id": 29,
  "labels": [
    "self-hosted",
    "Linux",
    "X64",
    "fcp-linux-fast"
  ],
  "test_stdout": "31 passed in 5.04s",
  "job": {
    "conclusion": "success",
    "id": 129197904840,
    "labels": [
      "self-hosted",
      "Linux",
      "X64",
      "fcp-linux-fast"
    ],
    "runner_id": 29,
    "runner_name": "Beast-Linux-WSL",
    "status": "completed",
    "steps": [
      {
        "conclusion": "success",
        "name": "Set up job",
        "status": "completed"
      },
      {
        "conclusion": "success",
        "name": "Run actions/checkout@v4",
        "status": "completed"
      },
      {
        "conclusion": "success",
        "name": "Run ./.github/actions/self-hosted-python",
        "status": "completed"
      },
      {
        "conclusion": "success",
        "name": "Install focused research dependencies",
        "status": "completed"
      },
      {
        "conclusion": "success",
        "name": "Compile research harness",
        "status": "completed"
      },
      {
        "conclusion": "success",
        "name": "Run synthetic tests against the production classifier",
        "status": "completed"
      },
      {
        "conclusion": "success",
        "name": "Post Run ./.github/actions/self-hosted-python",
        "status": "completed"
      },
      {
        "conclusion": "success",
        "name": "Post Run actions/checkout@v4",
        "status": "completed"
      },
      {
        "conclusion": "success",
        "name": "Complete job",
        "status": "completed"
      }
    ]
  },
  "cleanup_conclusion": "success"
}
```

### Failure history and operational limits

Initial workstation pytest execution encountered permission errors in the default Windows pytest temporary directory; explicit job-local temporary directories were used for verification. The initial remote minimal environment produced 10 passing research tests and six production setup errors because Flask was missing. Installing the full pinned dependency set under Nitro's default Python 3.14 failed on the pinned psycopg dependency. The existing Python 3.12.13 interpreter supported a separate research venv and full requirements; the complete relevant suite then executed with the final outcomes captured above. A local validation-wrapper attempt also produced 28 setup errors because its explicit temporary directory's parent did not exist; creating that parent resolved the wrapper error. These were dependency/environment failures, not evidence of semantic regressions or successful mutant detection. No expected assertion outcome was changed to make tests pass. The final JSON test capture above is the executed evidence for the reported suites; additional initial/final validation artifacts document the broader audit history where supplied. No unavailable test is counted as passed.

Historical evaluation runs in the isolated Nitro research checkout and virtual environment. Source archive and telemetry are read-only. No raw telemetry is transferred to the workstation or GitHub. Production files, services and recorder configuration are outside the experiment changes. See `SCIENTIFIC_ASSESSMENT.md` for the bounded scientific interpretation and literature comparison.
