"""Read-only D06 post-failure process/socket/config metadata; no retry."""
import datetime,hashlib,json,pathlib,subprocess
B=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910');R=B/'source'
def run(args):return subprocess.check_output(args,cwd=R,text=True,timeout=20).strip()
processes=[]
for pid in (1174977,1234730):
 p=pathlib.Path('/proc')/str(pid);item={'pid':pid,'exists':p.exists()}
 if p.exists():
  s=(p/'stat').read_text().rsplit(')',1)[1].split();item.update(state=s[0],start_ticks=s[19],fd_count=len(list((p/'fd').iterdir())))
 processes.append(item)
listeners=[]
for table in ('tcp','tcp6'):
 for line in pathlib.Path('/proc/net',table).read_text().splitlines()[1:]:
  x=line.split()
  if x[3]=='0A' and int(x[1].split(':')[1],16)==5151:listeners.append({'table':table,'inode':x[9]})
secret=pathlib.Path('/home/martin/fcp-v1-fba508-20260910/campaign/nitro-runtime/data/federation/onboarding/auto_join_secret')
services=[]
for cid in run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro']).splitlines():
 c=json.loads(run(['docker','inspect',cid]))[0];services.append({'service':c['Config']['Labels'].get('com.docker.compose.service'),'id':cid,'image':c['Image'],'started_at':c['State']['StartedAt'],'restarts':c['RestartCount']})
print(json.dumps({'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':run(['git','rev-parse','HEAD']),'source_clean':not run(['git','status','--porcelain','--untracked-files=all']),'host':'nitro','processes':processes,'port5151_listeners':listeners,'secret_mtime_utc':datetime.datetime.fromtimestamp(secret.stat().st_mtime,datetime.timezone.utc).isoformat(),'services':services,'native_log_sha256':hashlib.sha256((B/'inputs/responder-9b286f93-native.log').read_bytes()).hexdigest(),'state_changed':False,'protected_recorder_data_untouched':True,'physical_acceptance':False},indent=2))
