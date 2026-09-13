"""Finish the existing P03 launcher proof after its single same-deadline HTTP recovery."""
import datetime,json,os,pathlib,subprocess
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
record=c/'P03-launchers-status.json';status=json.loads(record.read_text());recovery=json.loads((c/'P03-readiness-recovery.json').read_text())
assert status['status']=='STOPPED' and status['error_type']=='TimeoutError' and status['last_launcher_exit_code']==0
assert recovery['candidate']==sha and recovery['configured_http_status']==200 and recovery['http_timeout_seconds_unchanged']==10 and not recovery['launcher_repeated']
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None);env['FCP_BUILD_COMMIT']=sha
py='/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source/.venv/bin/python'
base=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
target=['--commit',sha,'--host','nitro','--scenario','P03','--assertion','start-sh','--prepare-id',status['prepare_id']]
def call(command,label):
 p=subprocess.run(command,cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.private.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; inspect existing proof'
 return json.loads(p.stdout)
call([*base,'action',*target,'--note','Executed the unmodified supported start.sh once. It exited0 and the three core images carry the frozen candidate. The first supplemental HTTP read timed out; one read-only recovery of the existing runtime returned HTTP200 in1.381s with the same10s deadline. Original timeout retained; no launcher/build repeated and no asserted bound changed.'],'P03-start-sh-action')
proof=call([*base,'verify',*target],'P03-start-sh-verify')
status['results']=[{'assertion':'start-sh','verdict':proof.get('verdict'),'prepare_id':status['prepare_id'],'launcher_exit_code':0,'configured_http_status':200,'core_images':recovery['cores'],'log_sha256':status['last_log_sha256']}]
status.pop('error',None);status.pop('error_type',None);status.pop('active_assertion',None)
status.update(status='COMPLETED',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),supplemental_http_observation='unresolved but non-demonstrated candidate defect; one unchanged-deadline read-only recovery passed',original_timeout_retained=True,launcher_repeated=False)
record.write_text(json.dumps(status,indent=2)+'\n');print(json.dumps({'status':status,'recovery':recovery,'original':json.loads((c/'P03-launchers-original-http-timeout.json').read_text())}))
