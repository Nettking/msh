"""Reuse audited log retention on new completed jobs only; preserve prior proofs."""
import concurrent.futures
import datetime
import hashlib
import json
import pathlib
import subprocess
import sys

root=pathlib.Path(__file__).resolve().parent
audit=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
state=json.loads((root/'qualification-state.json').read_text())
path=root/'qualification-native-provenance.json'
known=json.loads(path.read_text()) if path.exists() else dict(prs={})
def retain(item):
    number,pr=item
    sha=pr['head'];label=f'pr{number}-head-{sha[:8]}'
    old=known['prs'].get(number)
    if old is None or old['source_commit']!=sha:
        baseline=audit/(label+'-native-retention.json')
        old=json.loads(baseline.read_text()) if baseline.exists() else dict(source_commit=sha,records=[])
    records={r['job_id']:r for r in old['records']}
    todo=[]
    for workflow in pr['workflows']:
        jobs=[j for j in workflow['jobs'] if j['status']=='completed' and (j['id'] not in records or 'sha256' not in records[j['id']])]
        if jobs:todo.append(dict(workflow=workflow['workflow'],run_id=workflow['run_id'],jobs=jobs))
    if not todo:return number,old,[]
    signature=hashlib.sha256(json.dumps([j['id'] for w in todo for j in w['jobs']]).encode()).hexdigest()[:12]
    delta_label=label+'-delta-'+signature
    (audit/(delta_label+'-qualification-latest.json')).write_text(json.dumps(dict(source_commit=sha,workflows=todo)),encoding='utf-8')
    with (audit/(delta_label+'-retention.log')).open('wb') as output:
        subprocess.run([sys.executable,'-B',str(audit/'retain_qualification_logs.py'),delta_label],cwd=audit,stdout=output,stderr=subprocess.STDOUT,timeout=300,check=True)
    delta=json.loads((audit/(delta_label+'-native-retention.json')).read_text())
    for r in delta['records']:records[r['job_id']]=r
    new=dict(source_commit=sha,records=list(records.values()),retained_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    return number,new,delta['records']

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    futures=[pool.submit(retain,item) for item in state['prs'].items()]
    for future in concurrent.futures.as_completed(futures):
        number,result,delta=future.result()
        known['prs'][number]=result
        path.write_text(json.dumps(known,indent=2)+'\n',encoding='utf-8')
        print(json.dumps(dict(pr=number,newly_retained=[{k:r.get(k) for k in ['job_id','run_id','conclusion','checkout_matches','checkout_commits','log_http_error']} for r in delta])),flush=True)
