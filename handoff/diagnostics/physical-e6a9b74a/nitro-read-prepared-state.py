import json,pathlib,subprocess
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
r={'candidate':'e6a9b74a1d555609eed6bf40c800e1258f1c9077','gate_summary':json.loads((root/'evidence/school-control/gate-summary.json').read_text()),'control_files':[p.name for p in (root/'.acceptance/runtime-control').iterdir()],'services':[]}
ids=subprocess.check_output(['docker','ps','-aq','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro'],text=True).split()
for cid in ids:
 c=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
 if c['Config']['Labels']['com.docker.compose.service'] in ['flask','relay','recorder']:
  r['services'].append({'id':c['Id'],'service':c['Config']['Labels']['com.docker.compose.service'],'oneoff':c['Config']['Labels'].get('com.docker.compose.oneoff'),'running':c['State']['Running'],'status':c['State']['Status']})
print(json.dumps(r))
