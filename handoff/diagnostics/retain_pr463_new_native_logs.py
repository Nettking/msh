"""Retain only newly completed PR463 job logs from the cached state snapshot."""
import json
import pathlib
import subprocess
import sys

root = pathlib.Path(__file__).resolve().parent
audit = pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
head = '7286f30d30e20c94c13cc9acebb2fd6d61bc1163'
state = json.loads((root / 'pr463-qualification-state.json').read_text())['prs']['463']
if state['head'] != head:
    raise SystemExit('PR head changed: review before retaining qualification')
path = root / 'pr463-native-provenance.json'
baseline = path if path.exists() else root / 'pr463-initial-native-provenance.json'
receipt = json.loads(baseline.read_text())
assert receipt['source_commit'] == head
records = {r['job_id']: r for r in receipt['records']}
retained = {i for i, r in records.items() if 'sha256' in r}
workflows = []
for workflow in state['workflows']:
    jobs = [j for j in workflow['jobs'] if j['status'] == 'completed' and j['id'] not in retained]
    if jobs:
        workflows.append(dict(workflow=workflow['workflow'], run_id=workflow['run_id'], jobs=jobs))
if not workflows:
    print(json.dumps(dict(new_logs=0)))
    raise SystemExit(0)
label = 'pr463-head-7286f30d-delta'
(audit / (label + '-qualification-latest.json')).write_text(
    json.dumps(dict(source_commit=head, workflows=workflows), indent=2) + '\n')
result = subprocess.run([sys.executable, '-B', str(audit / 'retain_qualification_logs.py'), label],
                        capture_output=True, text=True, timeout=240)
if result.returncode:
    raise SystemExit('Native retention failed; inspect private audit output')
new = json.loads((audit / (label + '-native-retention.json')).read_text())
for record in new['records']:
    records[record['job_id']] = record
receipt.update(retained_at=new['retained_at'], records=list(records.values()))
path.write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
print(json.dumps(dict(new_logs=len(new['records']), records=new['records'])))
