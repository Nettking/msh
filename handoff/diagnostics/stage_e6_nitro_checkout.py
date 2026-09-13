"""Stage a clean new acceptance checkout on Nitro; no product/runtime mutation."""
import hashlib,json,pathlib,subprocess
SHA='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
BASE='9b286f931497bf6291e215f6340443c5162826b0'
ROOT=pathlib.Path(__file__).parent
BUNDLE=pathlib.Path('C:/wsl/fcp-v1-e6a9b74a-main-20260913/.acceptance/source-e6a9b74a.bundle')
DEST='/home/martin/fcp-v1-e6a9b74a-nitro-20260913'
tail=json.loads(subprocess.check_output(['tailscale','status','--json']))
peer=next(p for p in tail['Peer'].values() if p['HostName'].casefold()=='nitro' and p['Online'])
host='martin@'+peer['TailscaleIPs'][0]
opts=['-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/mnt/c/Users/Martin/.ssh/known_hosts','-o','ConnectTimeout=5']
def ssh(code,timeout=55):
    p=subprocess.run(['wsl','-d','Ubuntu','--exec','/usr/bin/ssh',*opts,host,'python3 -B -'],input=code.encode(),capture_output=True,timeout=timeout)
    if p.returncode:raise RuntimeError('Nitro staging failed: '+p.stderr.decode(errors='replace')[-900:].replace(peer['TailscaleIPs'][0],'<private-peer>'))
    return p.stdout.decode()
print(ssh("import pathlib,platform; assert platform.node().casefold()=='nitro'; p=pathlib.Path("+repr(DEST)+"); assert not p.exists(); p.mkdir(); (p/'inputs').mkdir(); print('Created new owned acceptance staging directory')"))
digest=hashlib.sha256(BUNDLE.read_bytes()).hexdigest()
p=subprocess.run(['wsl','-d','Ubuntu','--exec','/usr/bin/scp',*opts,'/mnt/c/wsl/fcp-v1-e6a9b74a-main-20260913/.acceptance/source-e6a9b74a.bundle',host+':'+DEST+'/inputs/source.bundle'],capture_output=True,timeout=55)
assert p.returncode==0,'Bundle transfer failed'
code='''import hashlib,json,pathlib,platform,subprocess
root=pathlib.Path(DEST);old=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source')
def run(args):return subprocess.check_output(args,text=True,timeout=45).strip()
assert platform.node().casefold()=='nitro'
assert hashlib.sha256((root/'inputs/source.bundle').read_bytes()).hexdigest()==DIGEST
assert run(['git','-C',str(old),'rev-parse','HEAD'])==BASE
assert not (root/'source').exists()
run(['git','clone','--no-hardlinks',str(old),str(root/'source')])
run(['git','-C',str(root/'source'),'fetch',str(root/'inputs/source.bundle'),'HEAD'])
run(['git','-C',str(root/'source'),'checkout','--detach',SHA])
assert run(['git','-C',str(root/'source'),'rev-parse','HEAD'])==SHA
assert not run(['git','-C',str(root/'source'),'status','--porcelain'])
py=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/host-venv/bin/python3')
info=json.loads(run([str(py),'-B','-c','import json,sys;print(json.dumps({"version":sys.version.split()[0],"base_executable":sys._base_executable}))']))
assert info['version'].startswith('3.12.')
run([info['base_executable'],'-m','venv',str(root/'source/.venv')])
log=(root/'inputs/dependency-install.log').open('wb')
process=subprocess.Popen([str(root/'source/.venv/bin/python'),'-m','pip','install','-r','requirements.txt','-c','constraints-release.txt','pytest==9.1.1','ruff==0.16.3'],cwd=root/'source',stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
print(json.dumps({'candidate':SHA,'source_clean':True,'bundle_sha256':DIGEST,'native_python_version':info['version'],'dependency_install_pid':process.pid,'runtime_changed':False,'physical_pass':False}))
'''
result=json.loads(ssh('DEST='+repr(DEST)+'\nSHA='+repr(SHA)+'\nBASE='+repr(BASE)+'\nDIGEST='+repr(digest)+'\n'+code))
(ROOT/'physical-e6a9b74a/nitro-checkout-staged.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
