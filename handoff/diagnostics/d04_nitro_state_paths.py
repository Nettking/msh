"""Inspect only Nitro onboarding path metadata; no secret content or state writes."""
import datetime,json,os,pathlib,stat,subprocess
data=pathlib.Path('/home/martin/fcp-v1-fba508-20260910/campaign/nitro-runtime/data')
paths=[data,data/'federation',data/'federation/onboarding',data/'federation/onboarding/auto_join_secret',data/'federation/onboarding/auto_join_responder.pid']
metadata=[]
for p in paths:
    item={'path':str(p),'lexists':os.path.lexists(p)}
    if item['lexists']:
        s=p.lstat();item.update(uid=s.st_uid,mode=oct(stat.S_IMODE(s.st_mode)),symlink=p.is_symlink(),is_file=p.is_file(),readable=os.access(p,os.R_OK),writable=os.access(p,os.W_OK))
    metadata.append(item)
cid=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service=flask'],text=True).strip()
d=json.loads(subprocess.check_output(['docker','inspect',cid]))[0]
env=dict(x.split('=',1) for x in d['Config']['Env'] if '=' in x)
print(json.dumps({'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'host':'nitro','paths':metadata,'app_custom_secret_path':env.get('FCP_AUTO_JOIN_SECRET_FILE'),'app_custom_pid_path':env.get('FCP_AUTO_JOIN_PID_FILE'),'state_changed':False,'protected_recorder_data_untouched':True},indent=2))
