import pathlib,subprocess,json
h=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source');r=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source');c=h/'.acceptance/runtime-control-clean'
assert not (c/'activation-status.json').exists(),'Inspect the existing single recovery'
script=c/'activate.py'
code='''import pathlib,json,subprocess,os,datetime
h=pathlib.Path(HARNESS);r=pathlib.Path(RUNTIME);c=h/'.acceptance/runtime-control-clean';sha=SHA
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip()==sha
assert not subprocess.check_output(['git','status','--porcelain'],cwd=r,text=True).strip()
assert not any((r/n).exists() for n in ['.acceptance','evidence','.env','.venv'])
values=json.loads((c/'environment.private.json').read_text())
assert not (pathlib.Path(values['FCP_DATA_DIR'])/'federation/update-agent/request.json').exists()
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(values);env.pop('FCP_BUILD_COMMIT',None);env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None)
status={'candidate':sha,'status':'RUNNING','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'purpose':'One clean runtime admission recovery with existing owned builder','physical_pass':False}
(c/'activation-status.json').write_text(json.dumps(status,indent=2)+'\\n')
with (c/'startup.private.log').open('wb') as log:
 p=subprocess.run(['bash','start.sh'],cwd=r,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT)
status.update(status='COMPLETED',exit_code=p.returncode,finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());(c/'activation-status.json').write_text(json.dumps(status,indent=2)+'\\n')
'''
script.write_text('HARNESS='+repr(str(h))+'\nRUNTIME='+repr(str(r))+'\nSHA='+repr('e6a9b74a1d555609eed6bf40c800e1258f1c9077')+'\n'+code)
log=(c/'wrapper.private.log').open('wb');p=subprocess.Popen([str(h/'.venv/bin/python'),str(script)],cwd=r,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
print(json.dumps({'candidate':'e6a9b74a1d555609eed6bf40c800e1258f1c9077','wrapper_pid':p.pid,'status':'DISPATCHED_ONCE','physical_pass':False}))
