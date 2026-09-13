import json,pathlib,subprocess,datetime,hashlib
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
control=root/'.acceptance/runtime-control';sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
status=json.loads((control/'activation-status.json').read_text())
if status['status']!='COMPLETED':
 print(json.dumps(status));raise SystemExit(0)
if status['exit_code']:
 print(json.dumps({'activation':status,'log_tail':(control/'supported-start.private.log').read_text()[-3000:]}));raise SystemExit(1)
def run(args):return subprocess.check_output(args,cwd=root,text=True,timeout=30).strip()
assert run(['git','rev-parse','HEAD'])==sha and not run(['git','status','--porcelain'])
rows=[]
for service in ['flask','relay','recorder','ollama']:
 cid=run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.oneoff=False','--filter','label=com.docker.compose.service='+service]);assert cid and '\n' not in cid
 c=json.loads(run(['docker','inspect',cid]))[0];img=json.loads(run(['docker','image','inspect',c['Image']]))[0]
 build=img['Config'].get('Labels',{}).get('no.fcp.build_commit')
 if service!='ollama':assert build==sha
 else:assert c['Image']=='sha256:b88c73ace3e115f8ec53dc8761ae1c0aabfa675406e3681786b98757ce050f42'
 assert c['State']['Running'] and not c['State']['OOMKilled']
 rows.append({'service':service,'container_id':cid,'image':c['Image'],'build_commit':build,'running':True,'started_at':c['State']['StartedAt']})
r={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':sha,'host':'nitro','supported_activation_count':1,'source_clean':True,'services':rows,'startup_exit':0,'startup_log_sha256':hashlib.sha256((control/'supported-start.private.log').read_bytes()).hexdigest(),'runtime_admitted':True,'physical_pass':False,'P07':'NOT_STARTED','P12':'NOT_STARTED','protected_recorder_data_accessed':False}
(control/'candidate-admission.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps(r))
