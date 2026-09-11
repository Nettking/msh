"""Verify the metadata invariant and source/ownership at diagnostic stop."""
import datetime
import json
import pathlib
import subprocess

root=pathlib.Path(__file__).resolve().parent
first=json.loads((root/'sweep-recorder-metadata.json').read_text(encoding='utf-8-sig'))
last=json.loads((root/'recorder-sweep-end-invariant.json').read_text(encoding='utf-8-sig'))
assert first['protected_container']==last['protected_container']
n='0536f03d67eb277e11573c2188d8e820399627e3'
report=dict(candidate_sha=n,mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',
 observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),protected_container_metadata_exactly_unchanged=True,
 protected_data_content_inspected=False,protected_recorder_data_untouched=True,local_checkouts=[],containers=[])
for path in ['C:/wsl/fcp-v1-0536f03d-nettking-20260911','C:/wsl/fcp-v1-73c779-nettking-runtime-20260910']:
 sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=path,text=True).strip()
 clean=not subprocess.check_output(['git','status','--porcelain'],cwd=path,text=True).strip()
 assert sha==n and clean
 report['local_checkouts'].append(dict(path=path,sha=sha,clean=clean))
firstlocal=json.loads((root/'sweep-nettking-metadata.json').read_text(encoding='utf-8'))
for old in firstlocal['services']:
 now=json.loads(subprocess.check_output(['docker','inspect',old['container_id']]))[0]
 assert now['Id']==old['container_id'] and now['Image']==old['image_id']
 assert now['State']['Running'] and now['State']['StartedAt']==old['started_at']
 report['containers'].append(dict(service=old['service'],same_id_image_start=True,running=True,oom_killed=now['State']['OOMKilled']))
tailnet=json.loads(subprocess.check_output(['tailscale','status','--json']))
report['tailscale_backend']=tailnet['BackendState']
report['tailnet_hosts']={x['HostName'].casefold():dict(online=x.get('Online'),os=x.get('OS')) for x in [tailnet['Self'],*tailnet['Peer'].values()] if x['HostName'].casefold() in {'nettking','nitro','desktop-n5ki14r','beast','aqg7ncc'}}
report.update(formal_p01_p12='0/12 fresh acceptance; diagnostic observations only',formal_b01_b09='0/9 fresh acceptance',cf7='not physically accepted',p07='not started',p12='not started',state_changed=False)
(root/'sweep-end-metadata.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
print(json.dumps(report))
