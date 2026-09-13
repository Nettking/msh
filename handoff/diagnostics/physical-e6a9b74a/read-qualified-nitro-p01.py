import hashlib,json,pathlib
c=pathlib.Path('/home/martin/fcp-v1-501b528e-harness-20260913/.acceptance/runtime-control')
p=c/'P01-qualified-status.json'
s=json.loads(p.read_text()) if p.exists() else {'status':'NOT_STARTED'}
out={'host':'nitro','status':s['status'],'completed_activations':len(s.get('completed_activations',[])),'started_at':s.get('started_at'),'finished_at':s.get('finished_at'),'three_activations':s.get('three_activations',{}).get('verdict'),'growth':s.get('growth',{}).get('verdict'),'physical_pass':False}
if s['status']=='STOPPED':out.update(error_type=s.get('error_type'),error=s.get('error'))
log=c/'qualified-wrapper.private.log'
if log.exists():out['log_sha256']=hashlib.sha256(log.read_bytes()).hexdigest()
print(json.dumps(out))
