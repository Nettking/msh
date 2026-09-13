"""Read only named acceptance runtime metadata; never inspect Recorder data."""
import datetime, hashlib, json, pathlib, platform, subprocess

OUT = pathlib.Path(__file__).parent / 'physical-e6a9b74a'
OUT.mkdir(exist_ok=True)
SHA = 'e6a9b74a1d555609eed6bf40c800e1258f1c9077'
OLD = pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
def run(args, timeout=25, input=None):
    p = subprocess.run(args, input=input, capture_output=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError('metadata command failed: ' + args[0] + ' exit ' + str(p.returncode))
    return p.stdout.decode('utf-8', errors='replace').strip()
binding = json.loads((OLD / 'nettking-73c779-runtime-control/runtime-binding-9b286f93.json').read_text())
assert platform.node().casefold() == 'nettking'
result = {'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'candidate': SHA, 'physical_pass': False, 'protected_recorder_data_accessed': False}
result['nettking'] = {'fingerprint': hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16], 'runtime_binding_candidate': binding['target_candidate_sha'], 'services': []}
wd = binding['runtime']['working_directory']
result['nettking']['source'] = run(['git','-C',wd,'rev-parse','HEAD'])
result['nettking']['source_clean'] = not run(['git','-C',wd,'status','--porcelain'])
for service in ['flask','recorder','relay','ollama']:
    cid = run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project='+binding['runtime']['project'],'--filter','label=com.docker.compose.service='+service])
    if not cid:
        result['nettking']['services'].append({'service':service,'running':False}); continue
    assert '\n' not in cid
    c = json.loads(run(['docker','inspect',cid]))[0]
    img = json.loads(run(['docker','image','inspect',c['Image']]))[0]
    result['nettking']['services'].append({'service': service, 'container_id': cid, 'image': c['Image'], 'build_commit': img['Config'].get('Labels',{}).get('no.fcp.build_commit'), 'running': c['State']['Running'], 'oom_killed': c['State']['OOMKilled'], 'started_at': c['State']['StartedAt']})
result['nettking']['gpu'] = run(['nvidia-smi','--query-gpu=name,memory.total,memory.used,utilization.gpu','--format=csv,noheader'])
for p in [OLD.parent/'venv/Scripts/python.exe', pathlib.Path('C:/wsl/msh/venv/Scripts/python.exe'), pathlib.Path('C:/wsl/fcp-v1-73c779-nettking-runtime-20260910/venv/Scripts/python.exe')]:
    if p.exists():
        result['nettking'].setdefault('available_python_versions',[]).append(run([str(p),'-B','-c','import sys; print(sys.version.split()[0])']))
remote = '''import hashlib,json,pathlib,platform,subprocess
def run(args):return subprocess.check_output(args,text=True,timeout=25).strip()
assert platform.node().casefold()=='nitro'
root=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source')
r={'fingerprint':hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16],'os':platform.system(),'wsl': 'microsoft' in platform.release().casefold(),'source':run(['git','-C',str(root),'rev-parse','HEAD']),'source_clean':not run(['git','-C',str(root),'status','--porcelain']),'services':[]}
for svc in ['flask','relay']:
 cid=run(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro','--filter','label=com.docker.compose.service='+svc]);assert cid and '\\n' not in cid
 c=json.loads(run(['docker','inspect',cid]))[0];img=json.loads(run(['docker','image','inspect',c['Image']]))[0]
 r['services'].append({'service':svc,'container_id':cid,'image':c['Image'],'build_commit':img['Config'].get('Labels',{}).get('no.fcp.build_commit'),'running':c['State']['Running'],'oom_killed':c['State']['OOMKilled'],'started_at':c['State']['StartedAt']})
r['python']=run(['python3','-V'])
r['disk_free_bytes']=__import__('shutil').disk_usage(root).free
print(json.dumps(r))
'''
script = OUT / 'nitro-admission-readonly.py'
script.write_text(remote)
probe = subprocess.run(['C:/Python314/python.exe',str(OLD/'ssh_campaign_script.py'),'nitro',str(script),'--timeout','55'],capture_output=True,timeout=60)
if probe.returncode:
    result['nitro'] = {'metadata_read':'FAILED','exit_code':probe.returncode}
else:
    result['nitro'] = json.loads(probe.stdout)
(OUT/'runtime-admission-metadata.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
