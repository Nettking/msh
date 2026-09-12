"""Read repository runner metadata without admission, labels or host-state changes."""
import datetime
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
api=client()
reply=api('/actions/runners?per_page=100')
assert reply['total_count']<=100
rows=[{**{k:r[k] for k in ['id','name','os','status','busy']},
       'labels':[x['name'] for x in r['labels']]} for r in reply['runners']]
native=[r for r in rows if 'aqg7ncc' in r['name'].lower() and 'Windows' in r['labels']]
linux=[r for r in rows if 'aqg7ncc' in r['name'].lower() and 'Linux' in r['labels']]
result=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    source_commit='17ab3a05c9c506e0f92adfaa4fa0bac231ac2c05',runners=rows,
    aqg7ncc_native_windows=native,aqg7ncc_linux=linux,
    native_windows_label_verified=any('fcp-windows' in r['labels'] for r in native),
    native_windows_available=any(r['status']=='online' and r['os'].lower()=='windows' for r in native),
    runtime_provenance='Metadata only: execution prerequisites must be verified by a checked-in replacement job before counting AQG Windows green evidence.',
    pool_admission='No fcp-test pool admission or label/account changes performed.',state_changed=False,
    protected_recorder_data_untouched=True)
(ROOT/'ci-migration-runner-capacity.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
