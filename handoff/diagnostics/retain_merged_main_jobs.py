"""Retain only new completed final-main logs; reuse the existing audited downloader."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parent
A=Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
state=json.loads((ROOT/'merged-main-qualification-state.json').read_text())
sha=state['source_commit'];label='main-'+sha[:8]
path=ROOT/'merged-main-native-provenance.json'
old=json.loads(path.read_text()) if path.exists() else dict(source_commit=sha,records=[])
assert old['source_commit']==sha
records={r['job_id']:r for r in old['records']}
todo=[]
for workflow in state['workflows']:
    jobs=[j for j in workflow['jobs'] if j['status']=='completed' and (j['id'] not in records or 'sha256' not in records[j['id']])]
    if jobs:todo.append(dict(workflow=workflow['workflow'],run_id=workflow['run_id'],jobs=jobs))
if not todo:
    print(json.dumps(dict(newly_retained=[])))
    raise SystemExit(0)
signature=hashlib.sha256(json.dumps([j['id'] for w in todo for j in w['jobs']]).encode()).hexdigest()[:12]
delta_label=label+'-delta-'+signature
(A/(delta_label+'-qualification-latest.json')).write_text(json.dumps(dict(source_commit=sha,workflows=todo)))
with (A/(delta_label+'-retention.log')).open('wb') as output:
    subprocess.run([sys.executable,'-B',str(A/'retain_qualification_logs.py'),delta_label],cwd=A,stdout=output,stderr=subprocess.STDOUT,timeout=300,check=True)
delta=json.loads((A/(delta_label+'-native-retention.json')).read_text())
for r in delta['records']:records[r['job_id']]=r
path.write_text(json.dumps(dict(source_commit=sha,records=list(records.values()),retained_at=datetime.datetime.now(datetime.timezone.utc).isoformat()),indent=2)+'\n')
print(json.dumps(dict(source_commit=sha,newly_retained=[{k:r.get(k) for k in ['job_id','run_id','conclusion','checkout_matches','checkout_commits','log_http_error']} for r in delta['records']])))
