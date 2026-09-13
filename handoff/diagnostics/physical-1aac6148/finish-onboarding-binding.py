"""Correct proven operator metadata and verify the existing tailnet action once."""
import datetime,hashlib,json,os,pathlib,subprocess,sys,urllib.request,zipfile
root=pathlib.Path(__file__).parent;h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/onboarding-test';e=h/'evidence/v1-physical'
r=pathlib.Path('C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913');sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
py='C:/wsl/fcp-v1-e6a9b74a-main-20260913/.venv/Scripts/python.exe';receipt=c/'binding-recovery-status.json'
assert not receipt.exists(),'Inspect the one existing recovery; never repeat it'
original=json.loads((c/'bootstrap-status.json').read_text());assert original['phase']=='VERIFY' and original['status']=='STOPPED' and original['tailnet-start']['exit_code']==0
inspection=json.loads((c/'binding-inspection.json').read_text());binding=json.loads((c/'runtime-binding.json').read_text())
assert pathlib.Path(binding['runtime']['working_directory']).resolve()==r.resolve()
config=json.loads((c/'compose.private.json').read_text())
for service in ['flask','relay','recorder']:
 assert pathlib.Path(config['services'][service]['build']['context']).resolve()==r.resolve()
 assert config['services'][service]['build']['args']['FCP_BUILD_COMMIT']==sha
for row in inspection['rows']:
 assert row['project']==binding['runtime']['project'] and row['running'] and row['restart_count']==0
 assert pathlib.Path(row['working_directory']).resolve()==c.resolve()
 assert pathlib.Path(row['config_files']).resolve()==(c/'compose.private.json').resolve()
 if row['service'] in ['flask','relay','recorder']:assert row['commit']==sha
for checkout in [h,r]:
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==sha
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout,text=True).strip()
with (c/'runtime-binding-before-directory-correction.private.json').open('xb') as out:out.write((c/'runtime-binding.json').read_bytes())
binding['runtime']['working_directory']=str(c)
(c/'runtime-binding.json').write_text(json.dumps(binding,indent=2)+'\n')
status={'candidate':sha,'harness_sha':sha,'status':'RUNNING','classification':'deterministic operator-authored acceptance binding defect','change':'Bind the existing Compose project to its actual configuration working directory. Source build context, project, config, data, results, and candidate pins unchanged.','original_failure_retained':True,'launcher_repeated':False,'product_changed':False,'daemon_restarted_by_this_task':False,'physical_pass':False,'protected_data_accessed':False,'AQG_requested':False,'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
def save():receipt.write_text(json.dumps(status,indent=2)+'\n')
save()
try:
 env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()))
 availability=[]
 for name,control in [('original-owned',h/'.acceptance/runtime-control'),('isolated-onboarding',c)]:
  settings=json.loads((control/'environment.private.json').read_text());address=settings.get('FCP_WEB_BIND','127.0.0.1')
  if address in ['0.0.0.0','::']:address='127.0.0.1'
  with urllib.request.urlopen('http://'+address+':'+settings['FCP_WEB_PORT']+'/onboarding',timeout=10) as response:
   assert response.status==200;availability.append({'installation':name,'http_status':response.status})
 status['host_availability']=availability;save()
 args=[py,'-m','scripts.acceptance.v1_physical_runner','--checkout',str(h),'--evidence-root',str(e),'--runtime-binding',str(c/'runtime-binding.json'),'verify','--commit',sha,'--host','nettking','--scenario','P03','--assertion','start-tailscale-cmd','--prepare-id',original['prepare_id']]
 p=subprocess.run(args,cwd=h,env=env,capture_output=True,text=True,timeout=120)
 (c/'tailnet-verify-binding-recovery.json').write_text(p.stdout)
 if p.stderr:(c/'tailnet-verify-binding-recovery.private.stderr').write_text(p.stderr)
 status['verify_exit_code']=p.returncode;save();assert p.returncode==0,'One corrected-binding verification refused; inspect retained result'
 proof=json.loads(p.stdout);assert proof['verdict']=='pass'
 rows=json.loads(subprocess.check_output(['docker','inspect',*[x['id'] for x in inspection['rows']]],text=True,timeout=20))
 assert all(item['State']['Running'] and item['RestartCount']==0 for item in rows)
 sys.path.insert(0,str(h));from scripts.acceptance.v1_physical_campaign import scenario_status
 progress=scenario_status(e,'P03',expected_commit=sha);assert not progress['missing_assertions'] and not progress['failing_assertions']
 status.update(status='COMPLETED',verdict='pass',P03_assertions='8/8 PASS',progress=progress,core_images=[x for x in inspection['rows'] if x['service'] in ['flask','relay','recorder']],finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
 for row in status['core_images']:
  row.pop('working_directory',None);row.pop('config_files',None)
 save()
 for name in ['binding-recovery-status.json','tailnet-verify-binding-recovery.json']:
  with (root/('onboarding-'+name)).open('xb') as out:out.write((c/name).read_bytes())
 archive=root/'P03-complete-evidence.zip'
 with zipfile.ZipFile(archive,'x',zipfile.ZIP_DEFLATED) as z:
  for path in [e/'campaign.json',*sorted((e/'hosts').glob('*.json')),*sorted((e/'observations/P03').glob('*.json'))]:z.writestr(path.relative_to(e).as_posix(),path.read_bytes())
 report={'candidate':sha,'P03_assertions':'8/8 PASS','progress':progress,'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'original_failure_retained':True,'product_changed':False,'physical_pass':False,'protected_data_accessed':False}
 (root/'P03-complete-result.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();raise
