"""Read live main merge policy; do not change settings or bypass rules."""
import datetime
import json
from pathlib import Path
import sys
import urllib.error
root=Path(__file__).resolve().parent
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
api=client()
branch=api('/branches/main')
result=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    main=branch['commit']['sha'],protected=branch['protected'])
try:
    result['rules']=api('/rules/branches/main')
except urllib.error.HTTPError as error:
    result['rules_read_error']=dict(http_status=error.code,message=json.loads(error.read()).get('message'))
if branch['protected']:
    result['protection']=api('/branches/main/protection')
(root/'pr465-merge-policy.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
