"""One read-only recovery check after successful P03 launch and a timed-out HTTP read."""
import datetime,hashlib,json,os,pathlib,subprocess,time,urllib.request
h=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control';r=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source');record=c/'P03-readiness-recovery.json'
assert not record.exists(),'Inspect the existing bounded HTTP recovery; never loop'
original=json.loads((c/'P03-launchers-status.json').read_text());assert original['status']=='STOPPED' and original['last_launcher_exit_code']==0 and original['error_type']=='TimeoutError'
(c/'P03-launchers-original-http-timeout.json').write_bytes((c/'P03-launchers-status.json').read_bytes())
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()))
out={'candidate':original['candidate'],'host':'nitro','checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'original_timeout_retained':True,'launcher_repeated':False,'http_timeout_seconds_unchanged':10,'cpu_count':os.cpu_count(),'load_average':os.getloadavg(),'cores':[],'physical_pass':False,'protected_data_accessed':False}
for service in ['flask','relay','recorder']:
 cid=subprocess.check_output(['docker','compose','ps','-q',service],cwd=r,env=env,text=True,timeout=25).strip();assert cid and '\n' not in cid
 x=json.loads(subprocess.check_output(['docker','inspect',cid],text=True,timeout=25))[0]
 image=json.loads(subprocess.check_output(['docker','image','inspect',x['Image']],text=True,timeout=25))[0]
 out['cores'].append({'service':service,'container':cid,'image':x['Image'],'running':x['State']['Running'],'restart_count':x['RestartCount'],'started_at':x['State']['StartedAt'],'candidate':image['Config']['Labels'].get('no.fcp.build_commit')})
assert all(x['running'] and x['candidate']==out['candidate'] for x in out['cores'])
bind=env.get('FCP_WEB_BIND','127.0.0.1');address='127.0.0.1' if bind in ['0.0.0.0','::'] else bind
started=time.monotonic()
try:
 with urllib.request.urlopen('http://'+address+':'+env.get('FCP_WEB_PORT','5000')+'/onboarding',timeout=10) as response:out['configured_http_status']=response.status
except Exception as exc:out['http_error_type']=type(exc).__name__
out['http_elapsed_seconds']=round(time.monotonic()-started,3)
record.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
