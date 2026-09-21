"""One exact failed synthetic CI fixture; never test, reset or read live state."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3

source = Path('/opt/actions-runner/_work/_temp/fcp-test-tmp/pytest-of-runner/pytest-2/test_live_catchup_repairs_only0/authority-catchup.sqlite3')
output = Path(os.environ['RUNNER_TEMP']) / ('catchup-original-106491443153-' + os.environ['GITHUB_RUN_ID'])
output.mkdir(mode=0o700, exist_ok=False)
result = {'observed_at': datetime.datetime.now(datetime.UTC).isoformat(), 'original_run':35647501286, 'original_job':106491443153, 'original_attempt':1, 'original_source':'3a908fd1d9503057f3520d8f2dbbf62401a6e6d3', 'test':'catalog/node/tests/test_live_storage_catchup.py::test_live_catchup_repairs_only_missing_batches_and_keeps_node_unassigned', 'diagnostic_source':os.environ['GITHUB_SHA'], 'status':'NOT_READ'}
try:
    if not source.is_file():
        result['status']='ORIGINAL_TEMP_NOT_PRESENT'
    else:
        assert source.resolve()==source, 'Unexpected redirected fixture path'
        paths=[p for suffix in ('','-wal','-shm') if (p:=Path(str(source)+suffix)).is_file()]
        assert sum(p.stat().st_size for p in paths)<=16*1024**2, 'Original fixture exceeds bounded retention'
        copies=[]
        for p in paths:
            before=p.stat()
            # Native fixture dates must match the original test interval.
            low=datetime.datetime.fromisoformat('2026-09-21T20:00:00+00:00').timestamp()
            high=datetime.datetime.fromisoformat('2026-09-21T20:04:00+00:00').timestamp()
            assert low <= before.st_mtime <= high, 'Fixture timestamp not in failed-test interval'
            dest=output/p.name
            shutil.copy2(p,dest)
            digest=lambda q:hashlib.sha256(q.read_bytes()).hexdigest()
            sha=digest(dest)
            after=p.stat()
            assert (before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns) and digest(p)==sha, 'Original fixture changed during copy'
            copies.append({'name':p.name,'bytes':before.st_size,'mtime':before.st_mtime,'sha256':sha})
        result['files']=copies
        with sqlite3.connect((output/source.name).as_uri()+'?mode=ro',uri=True) as db:
            rows=db.execute('SELECT record_json FROM storage_live_catchups LIMIT 10').fetchall()
        result['records']=[json.loads(r[0]) for r in rows]
        result['status']='RETAINED_ORIGINAL_RECORD'
except Exception as exc:
    result['status']='UNAVAILABLE'
    result['reason']=type(exc).__name__+': '+str(exc)
finally:
    (output/'observation.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='records'},indent=2))
    for record in result.get('records',[]):
        print('CATCHUP_STATE',record.get('state'),record.get('latest_error_code'),record.get('latest_error_reason'))
        for item in record.get('items',[]):
            print('ITEM',item.get('batch_id'),item.get('status'),item.get('last_error_code'),item.get('last_error_reason'))
