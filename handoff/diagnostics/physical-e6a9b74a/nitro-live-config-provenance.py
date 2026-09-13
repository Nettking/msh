import json,subprocess,pathlib
cid=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service=flask'],text=True).strip()
c=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
labels=c['Config']['Labels']
files=labels.get('com.docker.compose.project.config_files','')
print(json.dumps({'working_directory':labels.get('com.docker.compose.project.working_dir'),'config_files':files,'image_label':labels.get('com.docker.compose.image')}))
for name in files.split(','):
 p=pathlib.Path(name)
 if p.name.endswith('.json') and 'override' in p.name:
  d=json.loads(p.read_text());print(json.dumps({'file':p.name,'services':{k:{key:v.get(key) for key in ['build','image']} for k,v in d.get('services',{}).items()}}))
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
summary=root/'evidence/school-control/gate-summary.json'
if summary.exists():print(json.dumps({'latest_readiness':json.loads(summary.read_text())}))
