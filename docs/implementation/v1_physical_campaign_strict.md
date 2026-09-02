# Federation v1 physical campaign — strict timed evidence

Status: **final validation rule for the physical P01–P12 campaign**

The base physical harness remains `scripts/acceptance/v1_physical_campaign.py` and owns initialization, host registration, P01–P12 planning, resource samples, ordinary assertions/commands, privacy sealing and the evidence tree.

For the two elapsed-time scenarios, **P07 and P12**, release evidence must additionally be bound to one concrete timed run. Do not use the base `observe`/`run` commands for P07/P12 assertions. Use the strict layer below so a PASS from an earlier outage/soak or another host cannot be combined with the current timed session.

## Timed flow

Start the session with the base harness and retain the returned `run_id`:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  begin --commit <candidate-sha> --host nitro --scenario P07
```

Samples during P07/P12 already require the active run id:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  sample --commit <candidate-sha> --host nitro --scenario P07 \
  --run-id <run-id> --label during-outage
```

Record every P07/P12 assertion through the strict layer:

```bash
python -m scripts.acceptance.v1_physical_campaign_strict \
  timed-observe --commit <candidate-sha> --host nitro --scenario P07 \
  --run-id <run-id> --assertion capture-continues --status pass \
  --note "capture/checkpoint progression observed during outage"
```

When command exit status is the evidence:

```bash
python -m scripts.acceptance.v1_physical_campaign_strict \
  timed-run --commit <candidate-sha> --host nitro --scenario P07 \
  --run-id <run-id> --assertion workers-alive \
  --label "required worker health probe" -- python <reviewed-probe>
```

Finish with the base harness after the real minimum duration:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  finish --commit <candidate-sha> --host nitro --scenario P07 \
  --run-id <run-id>
```

P07 requires at least one real hour. P12 uses the same flow and requires at least 24 real hours.

## Release status and final validation

Use the strict layer for campaign status and the final release decision:

```bash
python -m scripts.acceptance.v1_physical_campaign_strict \
  status --commit <candidate-sha>
```

After all evidence has been merged and the base privacy seal has been created:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  privacy --commit <candidate-sha>

python -m scripts.acceptance.v1_physical_campaign_strict \
  validate --commit <candidate-sha>
```

For P07/P12, strict validation accepts only a **single** begin/finish run whose assertions, samples and host provenance all belong to that run and fall inside its actual timestamp interval. Assertions from separate runs are not unioned. The elapsed duration is recomputed from begin/finish timestamps. All ordinary candidate, host, OS, N/A and privacy requirements from the base harness remain in force.

A successful strict P01–P12 validation does not replace the separate CF7 physical evidence/real browser review requirement.
