"""Read failing premerge check annotations; do not rerun hosted workflows."""
import concurrent.futures
import datetime
import json
from pathlib import Path
import sys
root=Path(__file__).resolve().parent
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
api=client()
snapshot=json.loads((root/'pr465-premerge-review-input.json').read_text())
failed=[c for c in snapshot['checks'] if c['conclusion']=='failure']
def fetch(row):
    check=api('/check-runs/'+str(row['id']))
    annotations=api('/check-runs/'+str(row['id'])+'/annotations?per_page=100')
    return dict(id=row['id'],name=row['name'],started_at=check['started_at'],
        completed_at=check['completed_at'],details_url=check['details_url'],
        check_output=check['output'],annotations=annotations)
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
    rows=list(pool.map(fetch,failed))
result=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            source_sha=snapshot['head'],failed_checks=rows)
(root/'pr465-legacy-check-annotations.json').write_text(json.dumps(result,indent=2)+'\n')
messages=sorted({a['message'] for r in rows for a in r['annotations']})
print(json.dumps(dict(count=len(rows),messages=messages,
    output_summaries=sorted({str(r['check_output'].get('summary')) for r in rows}))))
