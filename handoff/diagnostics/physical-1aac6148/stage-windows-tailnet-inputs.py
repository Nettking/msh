"""Prepare reviewed tailnet bindings for the owned Windows P03 installation only."""
import copy,hashlib,json,os,pathlib,socket,subprocess
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');base=h/'.acceptance/runtime-control';target=h/'.acceptance/p03-tailnet';sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
assert not target.exists(),'Inspect existing reviewed tailnet inputs before continuing'
prior=json.loads((base/'P03-launchers-status.json').read_text());assert prior['status']=='STOPPED' and prior['error']=='Owned web binding does not expose the actual tailnet path'
assert [x['assertion'] for x in prior['results']]==['start-cmd'] and prior['results'][0]['verdict']=='pass'
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(json.loads((base/'environment.private.json').read_text()));env.pop('FCP_HOST_MUTATION_LEASE_ACTIVE',None);env['FCP_BUILD_COMMIT']=sha
binding=json.loads((base/'runtime-binding.json').read_text());r=pathlib.Path(binding['runtime']['working_directory'])
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip()==sha and not subprocess.check_output(['git','status','--porcelain'],cwd=r,text=True).strip()
tail=subprocess.check_output(['tailscale','ip','-4'],text=True,timeout=20).strip().splitlines();assert len(tail)==1;tail=tail[0]
def config(e):return json.loads(subprocess.check_output(['docker','compose','config','--format','json'],cwd=r,env=e,text=True,timeout=30))
before=config(env);web=[p for p in before['services']['flask']['ports'] if p['target']==5000];relay=[p for p in before['services']['relay']['ports'] if p['target']==8765]
assert len(web)==len(relay)==1 and web[0]['host_ip']==relay[0]['host_ip']=='127.0.0.1'
join_port=int(env.get('FCP_AUTO_JOIN_PORT') or int(web[0]['published'])+1)
for port in [int(web[0]['published']),int(relay[0]['published']),join_port]:
 with socket.socket() as s:s.bind((tail,port))
target.mkdir()
override={'services':{'flask':{'ports':[{**web[0],'host_ip':tail}],'environment':{'FCP_HUMAN_AUTH_BASE_URL':'http://'+tail+':'+str(web[0]['published'])}},'relay':{'ports':[{**relay[0],'host_ip':tail}]}}}
file=target/'tailnet.compose.private.json';file.write_text(json.dumps(override,indent=2)+'\n')
desired=env.copy();desired.update(FCP_WEB_BIND=tail,FCP_RELAY_BIND=tail,FCP_WEB_PORT=str(web[0]['published']),FCP_AUTO_JOIN_PORT=str(join_port),FCP_HUMAN_AUTH_BASE_URL='http://'+tail+':'+str(web[0]['published']))
desired['COMPOSE_FILE']=env['COMPOSE_FILE']+';'+str(file)
after=config(desired)
assert set(before['services'])==set(after['services'])
review=[]
for name,old in before['services'].items():
 new=after['services'][name];a=copy.deepcopy(old);b=copy.deepcopy(new)
 if name in ['flask','relay']:
  old_ports=a.pop('ports',[]);new_ports=b.pop('ports',[])
  wanted=5000 if name=='flask' else 8765
  assert any(p['target']==wanted and p['host_ip']==tail for p in new_ports)
  assert all(p['host_ip'] in ['127.0.0.1',tail] for p in new_ports)
  assert {(p['target'],p['published'],p['protocol']) for p in old_ports}=={(p['target'],p['published'],p['protocol']) for p in new_ports}
  if name=='flask':
   a.setdefault('environment',{}).pop('FCP_HUMAN_AUTH_BASE_URL',None);b.setdefault('environment',{}).pop('FCP_HUMAN_AUTH_BASE_URL',None)
 assert a==b,'Unreviewed change beyond the owned tailnet bindings/human-auth URL: '+name
 assert old.get('volumes')==new.get('volumes') and old.get('user')==new.get('user')
 review.append({'service':name,'mounts_user_credentials_and_other_configuration_preserved':True})
binding['runtime']['config_files'].append(str(file))
(target/'runtime-binding.json').write_text(json.dumps(binding,indent=2)+'\n')
(target/'environment.private.json').write_text(json.dumps({k:v for k,v in desired.items() if k.startswith(('FCP_','COMPOSE_','OLLAMA_'))},indent=2)+'\n')
(target/'compose.resolved.private.json').write_text(json.dumps(after,indent=2)+'\n')
out={'candidate':sha,'host':'nettking','status':'INPUTS_REVIEWED','existing_mounts_user_credentials_and_source_preserved':True,'change':'Bind existing owned web and relay ports to the logged-in tailnet address; use the supported matching human-auth base URL and a free owned join-responder port.','web_port':int(web[0]['published']),'relay_port':int(relay[0]['published']),'join_port':join_port,'review':review,'runtime_activated':False,'physical_pass':False,'protected_data_accessed':False}
(target/'input-review.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
