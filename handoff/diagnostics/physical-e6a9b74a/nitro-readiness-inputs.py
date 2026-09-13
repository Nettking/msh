import json,pathlib,subprocess,platform
assert platform.node().casefold()=='nitro'
new=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913')
old=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910')
def run(args):return subprocess.check_output(args,text=True,timeout=30).strip()
r={'dependency_install_log_tail':(new/'inputs/dependency-install.log').read_text()[-600:], 'venv_ready': subprocess.run([str(new/'source/.venv/bin/python'),'-m','pip','check'],capture_output=True).returncode==0}
stage=old/'inputs/c03-activation'
receipt=json.loads((stage/'supported-start-staging-receipt.json').read_text())
environment=json.loads(pathlib.Path(receipt['environment_file']).read_text())
r['environment_keys']=sorted(environment)
r['compose_config_files']=environment.get('COMPOSE_FILE')
r['binding_files']=[str(p) for p in (old/'inputs').glob('*binding*.json')]
for service in ['flask','relay']:
 cid=run(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service='+service]); assert cid and '\n' not in cid
 c=json.loads(run(['docker','inspect',cid]))[0]
 env=dict(v.split('=',1) for v in c['Config']['Env'] if '=' in v)
 r[service+'_environment_keys']=sorted(k for k in env if k.startswith('FCP_'))
print(json.dumps(r))
