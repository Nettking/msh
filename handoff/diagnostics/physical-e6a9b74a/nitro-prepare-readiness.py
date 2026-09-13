import json,pathlib,subprocess,os,platform
assert platform.node().casefold()=='nitro'
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
python=str(root/'.venv/bin/python')
def run(args):return subprocess.check_output(args,text=True,timeout=40).strip()
assert run(['git','-C',str(root),'rev-parse','HEAD'])==sha
assert not run(['git','-C',str(root),'status','--porcelain'])
assert 'No broken requirements' in run([python,'-m','pip','check'])
subprocess.run([python,'-m','scripts.acceptance.cf7_physical_readiness','--checkout',str(root),'--evidence-root','evidence','init','--machine','school-control','--commit',sha,'--operator','Martin'],cwd=root,check=True)
log=(root.parent/'inputs/readiness-gate.log').open('wb')
process=subprocess.Popen([python,'-m','scripts.acceptance.cf7_physical_readiness','--checkout',str(root),'--evidence-root','evidence','gate','--machine','school-control','--commit',sha],cwd=root,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
cid=run(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service=relay']);assert cid and '\n' not in cid
c=json.loads(run(['docker','inspect',cid]))[0]
env=dict(v.split('=',1) for v in c['Config']['Env'] if '=' in v)
config=json.loads(run(['docker','exec',cid,'cat',env['FCP_REPLICATED_CONTROL_PLANE_CONFIG']]))
def shape(v):
 if isinstance(v,dict):return {k:shape(x) for k,x in v.items()}
 if isinstance(v,list):return [shape(x) for x in v]
 return type(v).__name__
print(json.dumps({'candidate':sha,'gate_pid':process.pid,'physical_pass':False,'runtime_changed':False,'control_plane_config_shape':shape(config)}))
