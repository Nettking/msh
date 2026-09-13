"""Read owned backing-resource mappings and isolation capabilities; no fault/mount."""
import hashlib,json,os,pathlib,shutil,subprocess
windows=os.name=='nt';h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913' if windows else '/home/martin/fcp-v1-1aac6148-main-20260913/source');c=h/'.acceptance/runtime-control'
binding=json.loads((c/'runtime-binding.json').read_text());env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((c/'environment.private.json').read_text()))
r=pathlib.Path(binding['runtime']['working_directory'])
def run(args):return subprocess.run(args,cwd=r,env=env,capture_output=True,text=True,timeout=25)
def measure(path):
 p=pathlib.Path(path);u=shutil.disk_usage(p)
 return {'path_digest':hashlib.sha256(str(p.resolve()).encode()).hexdigest()[:16],'device':p.stat().st_dev,'total_bytes':u.total,'free_bytes':u.free}
roots={'checkout':r,'data':binding['runtime']['data_root'],'results':binding['runtime']['results_root']};mounts=[]
for svc in ['flask','relay','recorder','ollama']:
 p=run(['docker','compose','ps','-q',svc]);assert p.returncode==0
 if not p.stdout.strip():continue
 item=json.loads(run(['docker','inspect',p.stdout.strip()]).stdout)[0]
 for m in item.get('Mounts',[]):
  rec={'service':svc,'type':m['Type'],'target':m['Destination'],'source_digest':hashlib.sha256(m['Source'].encode()).hexdigest()[:16],'writable':m['RW']}
  if not windows and pathlib.Path(m['Source']).exists():rec['backing']=measure(m['Source'])
  mounts.append(rec)
info=run(['docker','info','--format','{{json .DockerRootDir}}']);assert info.returncode==0;docker_root=json.loads(info.stdout)
if not windows:roots['docker']=docker_root
out={'candidate':binding['target_candidate_sha'],'host':'nettking' if windows else 'nitro','roots':{k:measure(v) for k,v in roots.items()},'owned_mounts':mounts,'runtime_mutated':False,'pressure_injected':False,'protected_data_accessed':False}
if not windows:
 out['isolation_tools']={name:shutil.which(name) is not None for name in ['sudo','mount','umount','losetup','mkfs.ext4','fallocate','truncate','unshare','findmnt']}
 p=run(['sudo','-n','true']);out['noninteractive_sudo_available']=p.returncode==0
 out['loop_control_available']=pathlib.Path('/dev/loop-control').exists()
print(json.dumps(out))
