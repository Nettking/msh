import json,pathlib,subprocess,os,platform
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
sha='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
assert platform.node().casefold()=='nitro'
def run(args):return subprocess.check_output(args,text=True,timeout=40).strip()
cid=run(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service=relay']);assert cid and '\n' not in cid
c=json.loads(run(['docker','inspect',cid]))[0];env=dict(v.split('=',1) for v in c['Config']['Env'] if '=' in v)
config=json.loads(run(['docker','exec',cid,'cat',env['FCP_REPLICATED_CONTROL_PLANE_CONFIG']]))
peers={f'configured-peer-{i}':str(p['host'])+':'+str(p['port']) for i,p in enumerate(config['peers']) if p['voter_id']!=config['local_voter_id']}
assert len(peers)==2
environment=os.environ.copy();environment['FCP_CF7_PEERS']=json.dumps(peers)
p=subprocess.run([str(root/'.venv/bin/python'),'-m','scripts.acceptance.cf7_physical_readiness','--checkout',str(root),'--evidence-root','evidence','preflight','--machine','school-control','--commit',sha],cwd=root,env=environment,capture_output=True,text=True,timeout=30)
(root.parent/'inputs/readiness-preflight.private.log').write_text(p.stdout+'\n'+p.stderr)
result={'candidate':sha,'physical_pass':False,'preflight_exit_code':p.returncode,'peer_selection':'Two nonlocal peers from the active explicitly configured control-plane topology'}
if p.returncode==0:result['preflight']=json.loads(p.stdout)
summary=root/'evidence/school-control/gate-summary.json'
result['gate_summary']=json.loads(summary.read_text()) if summary.exists() else {'status':'RUNNING'}
print(json.dumps(result))
