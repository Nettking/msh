"""Run the identical real Docker fault once through the repaired controller."""
import datetime,hashlib,json,os,pathlib,re,subprocess
fix=pathlib.Path('C:/wsl/fcp-v1-windows-build-exit-status-20260913');r=pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910')
old=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913/.acceptance/runtime-control');c=fix/'.acceptance/physical-build-proof-handle'
assert not c.exists(),'Inspect the retained repair proof';c.mkdir()
sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip()==sha
assert not subprocess.check_output(['git','status','--porcelain'],cwd=r,text=True).strip()
base={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};base.update(json.loads((old/'environment.private.json').read_text()));base.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None)
def cores():
 rows=[]
 for svc in ['flask','relay','recorder']:
  cid=subprocess.check_output(['docker','compose','ps','-q',svc],cwd=r,env=base,text=True).strip();assert cid and '\n' not in cid
  x=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
  assert x['State']['Running'];rows.append((svc,cid,x['Image'],x['RestartCount']))
 return rows
before=cores();env=base.copy();env['COMPOSE_FILE']=base['COMPOSE_FILE']+';'+str(old/'missing-dockerfile.compose.json')
log=c/'missing-dockerfile.private.log';marker=c/'build-result.txt'
controller=fix/'scripts/windows/fcp_host_build.ps1'
started=datetime.datetime.now(datetime.timezone.utc).isoformat()
with log.open('wb') as output:
 p=subprocess.run(['powershell.exe','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(controller),'-RepoRoot',str(r),'-OutputFile',str(marker),'-ExpectedCommit',sha],cwd=r,env=env,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW,timeout=1100)
raw=log.read_bytes();text=raw.decode(errors='replace')
out={'runtime_source':sha,'controller_git_blob':subprocess.check_output(['git','hash-object','--path=scripts/windows/fcp_host_build.ps1',str(controller)],cwd=fix,text=True).strip(),'started_at':started,'finished_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'controller_exit_code':p.returncode,'missing_dockerfile_error_observed':'.__fcp_p01_intentionally_missing_Dockerfile__' in text and 'no such file' in text.lower(),'controller_refused_build':'core_image_build_failed:1' in re.sub(r'\s+','',text),'success_marker_written':marker.exists(),'core_containers_unchanged':cores()==before,'log_sha256':hashlib.sha256(raw).hexdigest(),'physical_campaign_pass':False}
(c/'proof.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out));assert p.returncode!=0 and out['missing_dockerfile_error_observed'] and out['controller_refused_build'] and not marker.exists() and out['core_containers_unchanged']
