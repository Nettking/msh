"""Publish compact source-specific CI provenance, not raw private job logs."""
import datetime
import hashlib
import json
import pathlib

audit=pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
root=pathlib.Path(__file__).resolve().parent
report=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),phase='post-sweep repair qualification in progress',
 runtime_candidate='0536f03d67eb277e11573c2188d8e820399627e3',physical_acceptance=False,prs=[])
for label in ['pr456-head-1a0c634f','pr457-head-143fe7a9','pr461-head-5b826c68']:
    qpath=audit/(label+'-qualification-latest.json')
    npath=audit/(label+'-native-retention.json')
    q=json.loads(qpath.read_text());native=json.loads(npath.read_text())
    assert q['source_commit']==native['source_commit']
    row=dict(label=label,source_sha=q['source_commit'],snapshot_at=q['recorded_at'],
             snapshot_sha256=hashlib.sha256(qpath.read_bytes()).hexdigest(),native_receipt_sha256=hashlib.sha256(npath.read_bytes()).hexdigest(),
             native_records=native['records'],missing_workflows=q['missing_workflows'],
             workflows=[{k:w[k] for k in ['workflow','run_id','event','status','conclusion','jobs']} for w in q['workflows']],
             full_exact_head_qualification='INCOMPLETE; do not infer PASS from API head SHA or focused tests')
    row['exact_successful_native_jobs']=sum(x.get('checkout_matches') is True and x.get('conclusion')=='success' for x in native['records'])
    row['other_checkout_jobs']=sum(x.get('checkout_matches') is False for x in native['records'])
    report['prs'].append(row)
(root/'post-sweep-qualification-checkpoint.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
print(json.dumps({r['label']:{k:r[k] for k in ['exact_successful_native_jobs','other_checkout_jobs','full_exact_head_qualification']} for r in report['prs']}))
