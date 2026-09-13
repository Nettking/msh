"""Verify the existing Windows fault after a wrapped-error parser mismatch; no build."""
import datetime,hashlib,json,os,pathlib,re,subprocess
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910');c=h/'.acceptance/runtime-control'
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80';py='C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe'
record=c/'P01-build-failure-status.json';original=json.loads(record.read_text())
assert original['status']=='STOPPED' and original['controller_exit_code']==1 and original['missing_dockerfile_error_observed'] and not original['controller_refused_build']
assert original['error']=='Unexpected fault result; do not record PASS'
preserved=c/'P01-build-failure-original-parser-stop.json';assert not preserved.exists(),'Inspect existing verification before any retry'
preserved.write_bytes(record.read_bytes())
log=c/'P01-build-failure.private.log';stdout=c/'P01-build-failure.private.stdout';text=log.read_text(errors='replace')+stdout.read_text(errors='replace')
match=re.search(r'FCP\s+host\s+build\s+refused:\s*core_image_\s*build_failed:1',text)
assert match and '\n' in match.group(),'Only the demonstrated terminal-wrap mismatch may be resumed'
assert not (c/'P01-failed-build-result.txt').exists()
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()));env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None);env['FCP_BUILD_COMMIT']=sha
for root in [h,r]:
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()==sha
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=root,text=True).strip()
images={x['service']:x['image'] for x in json.loads((c/'P01-qualified-status.json').read_text())['completed_activations'][-1]['images']}
fault_start=datetime.datetime.fromisoformat(original['started_at']);cores=[]
for service in ['flask','relay','recorder']:
 cid=subprocess.check_output(['docker','compose','ps','-q',service],cwd=r,env=env,text=True,timeout=30).strip();assert cid and '\n' not in cid
 item=json.loads(subprocess.check_output(['docker','inspect',cid],text=True,timeout=30))[0]
 image=json.loads(subprocess.check_output(['docker','image','inspect',item['Image']],text=True,timeout=30))[0]
 assert item['State']['Running'] and item['RestartCount']==0 and item['Image']==images[service] and image['Config']['Labels']['no.fcp.build_commit']==sha
 assert datetime.datetime.fromisoformat(item['State']['StartedAt'].replace('Z','+00:00'))<fault_start
 cores.append({'service':service,'container':cid,'image':item['Image'],'restart_count':item['RestartCount'],'started_at':item['State']['StartedAt']})
builder=original['owned_builder'];ids=subprocess.check_output(['docker','ps','-aq','--filter','name=buildx_buildkit_'+builder.lower()],text=True,timeout=30).split()
builders=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True,timeout=30)) if ids else []
assert all(not b['State']['Running'] for b in builders),'Owned writer remains active'
runner=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(h/'evidence/v1-physical'),'--runtime-binding',str(c/'runtime-binding.json')]
target=['--commit',sha,'--host','nettking','--scenario','P01','--assertion','windows-failed-build-cleanup','--prepare-id',original['prepare_id']]
def call(command,label):
 p=subprocess.run(command,cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/(label+'.json')).write_text(p.stdout)
 if p.stderr:(c/(label+'.private.stderr')).write_text(p.stderr)
 assert p.returncode==0,label+' refused; inspect retained evidence'
 return json.loads(p.stdout)
call([*runner,'action',*target,'--note','Failed one real Windows build through the frozen native controller with a missing-Dockerfile override outside the clean source. The controller returned 1 and refused core_image_build_failed:1, stopped its writer and published no success marker. All live cores retain the last verified candidate images, have zero restarts, and started before the fault. The local evidence parser initially missed the line-wrapped error; its original STOPPED record and logs are retained. This continuation performs verification only and does not repeat the build.'],'P01-build-failure-action')
proof=call([*runner,'verify',*target],'P01-build-failure-verify')
status=dict(original);status.pop('error',None);status.pop('error_type',None)
status.update(status='COMPLETED',controller_refused_build=True,owned_writer_stopped=True,cores_verified_after_fault=cores,core_continuity_basis='same previously verified candidate images, StartedAt before fault, and RestartCount zero; immediate pre-fault IDs were not saved',log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),stdout_sha256=hashlib.sha256(stdout.read_bytes()).hexdigest(),parser_classification='deterministic local evidence-helper defect: terminal line wrapping',build_repeated=False,original_parser_stop_retained=True,verdict=proof.get('verdict'),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
record.write_text(json.dumps(status,indent=2)+'\n');print(json.dumps(status))
