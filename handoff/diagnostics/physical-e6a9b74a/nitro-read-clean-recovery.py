import hashlib,json,pathlib,subprocess
h=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
c=h/'.acceptance/runtime-control-clean'
s=c/'activation-status.json'
r={'activation':json.loads(s.read_text()) if s.exists() else 'NOT_STARTED'}
for name in ['startup.private.log','wrapper.private.log']:
 p=c/name
 if p.exists():
  raw=p.read_bytes(); lines=raw.decode(errors='replace').splitlines()
  r[name]={'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
ids=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro'],text=True).split()
r['services']=[]
if ids:
 for x in json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)):
  labels=x['Config']['Labels']; service=labels.get('com.docker.compose.service')
  if service in ['flask','relay','recorder']:
   image=json.loads(subprocess.check_output(['docker','image','inspect',x['Image']],text=True))[0]
   check=subprocess.run(['docker','exec',x['Id'],'python','-c',"import pathlib,sys;sys.exit(int(any(pathlib.Path(p).exists() for p in ['/app/.acceptance','/app/evidence','/app/.env'])))"],capture_output=True,timeout=20)
   r['services'].append({'service':service,'running':x['State']['Running'],'commit':image['Config']['Labels'].get('no.fcp.build_commit'),'image':x['Image'],'harness_inputs_absent':check.returncode==0})
(c/'admission-reviewed.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps(r))
