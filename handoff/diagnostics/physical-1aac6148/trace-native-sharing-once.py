"""One ten-second snapshot capture of the existing native publication failure path."""
import asyncio,dataclasses,datetime,json,pathlib,sys,time
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/native-faults';r=pathlib.Path('C:/wsl/fcp-v1-1aac6148-native-faults-20260913');record=c/'P06-sharing-trace.json'
assert not record.exists(),'The one bounded trace already exists; inspect it instead of rerunning'
assert json.loads((c/'P06-supervision-status.json').read_text())['status']=='STOPPED'
sys.path.insert(0,str(r))
from scripts.start_tailscale_recorder import _install_remote_session_creator_fallback
from catalog.mtconnect_recorder.federation_node import RecorderFederationNode,select_storage_authority
restore=_install_remote_session_creator_fallback();node=RecorderFederationNode(data_directory=c/'data',display_name='Federation v1 native fault acceptance',source_names=tuple(f's{i:02}' for i in range(1,9)))
out={'candidate':'1aac6148759d7b2fd488ec26b97e1a786bdafa80','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'new_grant_requested':False,'snapshots':[],'physical_pass':False,'protected_data_accessed':False}
try:
 node.bootstrap(None);start=time.monotonic()
 while time.monotonic()-start<10:
  s=node.snapshot();item={k:getattr(s,k) for k in ['status','storage_state','jsonl_state','pending_batches','last_committed_count','last_error_code']}
  if not out['snapshots'] or item!=out['snapshots'][-1]:out['snapshots'].append(item)
  time.sleep(0.5)
 out['status']='CAPTURED'
except Exception as exc:out.update(status='CAPTURE_REFUSED',error_type=type(exc).__name__,error_code=str(getattr(exc,'code','')))
finally:
 node.stop();restore();out['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();record.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
