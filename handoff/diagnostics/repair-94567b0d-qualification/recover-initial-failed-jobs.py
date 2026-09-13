"""One guarded failed-job recovery for the two current qualification failures."""
import datetime,json,pathlib,sys
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client

root=pathlib.Path(__file__).parent
receipt=root/'failed-job-recovery.json'
if receipt.exists():raise SystemExit('Recovery receipt already exists; inspect live state instead of redispatching.')
source='94567b0d9ac916eec9e4f094d2f6754562b966aa';head='fe21bdc59bc1414fd7c4cdcf1aa4f6d6aa9048fe';main='1492d925d791b9a0ebc7bcee39ce9b3b7477254b'
a=client();pr=a('/pulls/486')
assert pr['state']=='open' and pr['head']['sha']==head and pr['base']['sha']==main and pr['merge_commit_sha']==source
assert a('/git/ref/heads/codex/federation-v1-qualify-94567b0d')['object']['sha']==source
snapshot=json.loads((root.parent/'physical-e6a9b74a/windows-repair-qualification-current.json').read_text())
assert snapshot['source']==source and not snapshot['missing_workflows']
(root/'initial-qualification.json').write_text(json.dumps(snapshot,indent=2)+'\n')
expected={34760641401:{103732755647,103737667744,103737667941,103737980143},34760641469:{103732755738}}
for rid,failed in expected.items():
 run=a(f'/actions/runs/{rid}');jobs=a(f'/actions/runs/{rid}/jobs?per_page=100')['jobs']
 assert run['run_attempt']==1 and run['head_sha']==head and run['conclusion']=='failure'
 assert {j['id'] for j in jobs if j['conclusion']=='failure'}==failed
out={'source':source,'head':head,'base':main,'pr':486,'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'failed jobs and dependent summaries only; preserve every valid green job','classification':'unresolved but non-demonstrated candidate defect','observations':[{'run_id':34760641401,'job_id':103732755647,'runner':'Beast','path':'test_three_machine_deployment.py:318 -> phase_d_client.py:202 -> relay_storage.py:165','failure':'TimeoutError during initial storage ingest before restart; 369 passed, 1 skipped'},{'run_id':34760641469,'job_id':103732755738,'runner':'Beast','path':'demo/icse/network/run.py:400 reviewer join','failure':'WorkerCommandFailure wrapping TimeoutError; independent processes and quorum bootstrap PASS; all owned children stopped'}],'mechanism':'Different request paths. Shared host/timing is possible but not demonstrated; no shared mechanism or deterministic harness defect established. Current Linux equivalents passed. The changed Windows build launcher is not exercised by either failing path.','source_changes':False,'runner_changes':False,'assertion_changes':False,'actions':[]}
def save():receipt.write_text(json.dumps(out,indent=2)+'\n')
save()
for rid in expected:
 action={'run_id':rid,'status':'dispatching'};out['actions'].append(action);save()
 response=a(f'/actions/runs/{rid}/rerun-failed-jobs',{})
 action.update(status='accepted',response=response);save()
print(json.dumps(out))
