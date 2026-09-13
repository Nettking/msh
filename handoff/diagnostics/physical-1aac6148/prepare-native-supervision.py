"""Expose the owned creator's actual relay address and use normal signed enrollment."""
import copy,datetime,hashlib,json,os,pathlib,subprocess,sys,time,urllib.request
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/onboarding-test';n=h/'.acceptance/native-faults';r=pathlib.Path('C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913');native=pathlib.Path('C:/wsl/fcp-v1-1aac6148-native-faults-20260913');sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
record=n/'P06-enrollment-status.json';assert not record.exists(),'Inspect existing enrollment; never mint another grant blindly'
prior=json.loads((n/'ingress-maximum-continuation-status.json').read_text());assert prior['status']=='COMPLETED' and prior['operator_stop_exit_code']==0
membership=n/'data/federation/onboarding/remote_pairing.json';assert not membership.exists(),'Use the existing membership; do not enroll again'
settings=json.loads((c/'environment.private.json').read_text());before=json.loads((c/'compose.private.json').read_text());after=copy.deepcopy(before)
address=settings['FCP_WEB_BIND'];port=58796;public_relay='ws://'+address+':'+str(port)
after['services']['flask']['environment']['FCP_RELAY_PORT']=str(port)
after['services']['flask']['environment']['FCP_PAIRING_RELAY_URL']=public_relay
check=copy.deepcopy(after)
for key in ['FCP_RELAY_PORT','FCP_PAIRING_RELAY_URL']:check['services']['flask']['environment'][key]=before['services']['flask']['environment'][key]
assert check==before
assert any(p['target']==8765 and p['published']==str(port) and p['host_ip']==address for p in before['services']['relay']['ports'])
settings.update(FCP_RELAY_PORT=str(port),FCP_PAIRING_RELAY_URL=public_relay)
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_'))};env.update(settings)
sys.path.insert(0,str(native))
from catalog.federation.host_mutation import host_mutation_lock
def docker(*args):return subprocess.check_output(['docker',*args],cwd=r,env=env,text=True,timeout=60).strip()
def cores():
 rows=json.loads(docker('inspect',*docker('compose','ps','-q').split()))
 return {x['Config']['Labels']['com.docker.compose.service']:{'container':x['Id'],'image':x['Image'],'running':x['State']['Running'],'restart_count':x['RestartCount'],'pid':x['State']['Pid'],'commit':x['Config']['Labels'].get('no.fcp.build_commit')} for x in rows}
status={'candidate':sha,'harness_sha':sha,'status':'RUNNING','phase':'PAIRING_CONFIGURATION','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'change':'Owned creator advertises its already-bound tailnet relay port/address. Only Flask operator environment is changed; source/image, credentials, mounts, coordinator authority and data remain unchanged.','source_changed':False,'physical_pass':False,'protected_data_accessed':False,'AQG_requested':False,'native_supervision_started':False}
def save():record.write_text(json.dumps(status,indent=2)+'\n')
save();node=None
try:
 old=cores();assert all(old[s]['running'] and old[s]['commit']==sha for s in ['flask','relay','recorder'])
 with host_mutation_lock(r,timeout_seconds=2):
  for name in ['compose.private.json','environment.private.json']:
   with (c/('before-native-pairing-'+name)).open('xb') as out:out.write((c/name).read_bytes())
  (c/'compose.private.json').write_text(json.dumps(after,indent=2)+'\n');(c/'environment.private.json').write_text(json.dumps(settings,indent=2)+'\n')
  result=subprocess.run(['docker','compose','up','-d','--no-deps','--no-build','--pull','never','flask'],cwd=r,env=env,capture_output=True,text=True,timeout=60)
  (n/'P06-pairing-configuration.private.log').write_text(result.stdout+result.stderr);status['configuration_activation_exit_code']=result.returncode;save();assert result.returncode==0
 deadline=time.monotonic()+30;advertisement=None
 while time.monotonic()<deadline:
  try:
   with urllib.request.urlopen('http://'+address+':'+settings['FCP_WEB_PORT']+'/onboarding/federation/discovery.json',timeout=10) as response:advertisement=json.load(response)
   if advertisement.get('relay_port')==port:break
  except OSError:pass
  time.sleep(0.5)
 assert advertisement and advertisement['relay_port']==port and advertisement['auto_join_port']==int(settings['FCP_AUTO_JOIN_PORT'])
 new=cores();assert new['flask']['image']==old['flask']['image'] and new['flask']['commit']==sha and new['flask']['running']
 for service in ['relay','recorder']:assert new[service]==old[service]
 status.update(creator_http_status=200,relay_port=port,creator_core_images={s:new[s]['image'] for s in ['flask','relay','recorder']},other_cores_unchanged=True,phase='SIGNED_NATIVE_ENROLLMENT');save();print(json.dumps({'phase':status['phase']}),flush=True)
 from catalog.federation.tailnet_join_client import request_join
 from catalog.flask_app.services.federation_pairing_service import PairingCodeCodec
 from catalog.mtconnect_recorder.federation_node import RecorderFederationNode
 grant,reason=request_join(address,int(settings['FCP_AUTO_JOIN_PORT']));assert grant,'Owned responder refused enrollment: '+reason
 offer=PairingCodeCodec().decode(grant);assert offer.relay_url==public_relay
 node=RecorderFederationNode(data_directory=n/'data',display_name='Federation v1 native fault acceptance',source_names=tuple(f's{i:02}' for i in range(1,9)))
 snapshot=node.bootstrap(grant);grant=None;offer=None
 status['native_federation_status']=snapshot.status;status['membership_saved']=membership.exists();save();assert membership.exists()
 snapshot=node.wait_until_sharing_ready(timeout_seconds=60)
 status.update(native_sharing_status=snapshot.storage_state,status='COMPLETED',phase='READY_FOR_SUPERVISION',finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save();print(json.dumps(status))
except BaseException as exc:
 status.update(status='STOPPED',error_type=type(exc).__name__,error_code=str(getattr(exc,'code','')),finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
 import traceback
 (n/'P06-enrollment.private.error').write_text(traceback.format_exc())
 print(json.dumps(status));raise SystemExit(1)
finally:
 if node is not None:node.stop()
