"""Read only the owned native outbox and creator storage receipt summaries."""
import datetime
import json
import os
import pathlib
import sqlite3
import subprocess
import sys

h = pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913')
c = h / '.acceptance/onboarding-test'
n = h / '.acceptance/native-faults'
r = pathlib.Path('C:/wsl/fcp-v1-1aac6148-onboarding-runtime-20260913')
record = n / 'P06-storage-receipts.json'
assert not record.exists(), 'Keep the original read-only receipt'
sys.path.insert(0, str(h))
from scripts.acceptance.v1_physical_campaign import redact_text
env = os.environ.copy()
env.update(json.loads((c / 'environment.private.json').read_text()))
code = '''import json,os,sqlite3
with sqlite3.connect('file:'+os.environ['FCP_FEDERATION_COORDINATOR_DATABASE']+'?mode=ro',uri=True) as db:
 out={'intent_states':list(db.execute('SELECT state,count(*) FROM storage_manifest_intents GROUP BY state')),'committed_items':db.execute('SELECT count(*) FROM storage_manifest_items').fetchone()[0],'ack_modes':[x[0] for x in db.execute('SELECT mode FROM storage_ack_policies')],'events':[]}
 for event,when,payload in db.execute('SELECT event_type,occurred_at,payload_json FROM storage_control_events ORDER BY revision'):
  p=json.loads(payload)
  def clean(value):
   if isinstance(value,dict):return {k:(v if k in {'status','state','connected','enabled','reason_code','mode','term','fencing_token','revision','lease_expires_at','issued_at'} and isinstance(v,(str,int,bool,type(None))) else clean(v)) for k,v in value.items()}
   if isinstance(value,list):return [clean(v) for v in value]
   return '<value omitted>'
  out['events'].append({'event':event,'at':when,'shape_and_state':clean(p)})
print(json.dumps(out))
'''
result = subprocess.run(['docker', 'compose', 'exec', '-T', 'flask', 'python', '-c', code], cwd=r, env=env, capture_output=True, text=True, timeout=25)
assert result.returncode == 0, 'Owned read-only catalog query refused'
out = {'candidate': '1aac6148759d7b2fd488ec26b97e1a786bdafa80', 'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'creator': json.loads(result.stdout), 'protected_data_accessed': False, 'read_only': True}
path = n / 'data/federation/recorder_publication/outbox.sqlite3'
with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as db:
    out['outbox'] = {'states': list(db.execute('SELECT state,count(*),min(attempt_count),max(attempt_count) FROM outbox GROUP BY state')), 'errors': [{'error': redact_text(row[0] or '', cwd=h), 'rows': row[1]} for row in db.execute('SELECT last_error,count(*) FROM outbox GROUP BY last_error')]}
record.write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps(out))
