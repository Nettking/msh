import hashlib,json,pathlib,platform,subprocess
def run(args):return subprocess.check_output(args,text=True,timeout=25).strip()
assert platform.node().casefold()=='nitro'
root=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source')
r={'fingerprint':hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16],'os':platform.system(),'wsl': 'microsoft' in platform.release().casefold(),'source':run(['git','-C',str(root),'rev-parse','HEAD']),'source_clean':not run(['git','-C',str(root),'status','--porcelain']),'services':[]}
for svc in ['flask','relay']:
 cid=run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service='+svc]);assert cid and '\n' not in cid
 c=json.loads(run(['docker','inspect',cid]))[0];img=json.loads(run(['docker','image','inspect',c['Image']]))[0]
 r['services'].append({'service':svc,'container_id':cid,'image':c['Image'],'build_commit':img['Config'].get('Labels',{}).get('no.fcp.build_commit'),'running':c['State']['Running'],'oom_killed':c['State']['OOMKilled'],'started_at':c['State']['StartedAt']})
r['python']=run(['python3','-V'])
r['disk_free_bytes']=__import__('shutil').disk_usage(root).free
print(json.dumps(r))
