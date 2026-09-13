import json,pathlib,subprocess,os,shutil,datetime
root=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
def run(args):return subprocess.check_output(args,text=True,timeout=35).strip()
docker_root=pathlib.Path(run(['docker','info','--format','{{.DockerRootDir}}']))
r={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':'e6a9b74a1d555609eed6bf40c800e1258f1c9077','activation':json.loads((root/'.acceptance/runtime-control/activation-status.json').read_text()),'docker_backing_free_bytes':shutil.disk_usage(docker_root).free,'docker_backing_total_bytes':shutil.disk_usage(docker_root).total,'owned_builders':[],'running_services':[],'protected_recorder_data_accessed':False}
ids=run(['docker','ps','-aq','--filter','name=buildx_buildkit_fcp-build-']).split()
for cid in ids:
 c=json.loads(run(['docker','inspect',cid]))[0]
 env=dict(v.split('=',1) for v in c['Config']['Env'] if '=' in v)
 stamp=env.get('FCP_BUILDER_ROOT_HEX','')
 try:owned=bytes.fromhex(stamp).decode()==str(root)
 except (ValueError,UnicodeDecodeError):owned=False
 if owned:r['owned_builders'].append({'id':c['Id'],'running':c['State']['Running'],'status':c['State']['Status'],'oom_killed':c['State']['OOMKilled']})
for service in ['flask','relay','recorder']:
 cid=run(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.oneoff=False','--filter','label=com.docker.compose.service='+service]);assert cid and '\n' not in cid
 c=json.loads(run(['docker','inspect',cid]))[0];img=json.loads(run(['docker','image','inspect',c['Image']]))[0]
 r['running_services'].append({'service':service,'build_commit':img['Config'].get('Labels',{}).get('no.fcp.build_commit'),'running':c['State']['Running']})
(root.parent/'inputs/aborted-build-safety.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps(r))
