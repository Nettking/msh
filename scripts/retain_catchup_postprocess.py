"""Post-process one finished pytest invocation; no product-call instrumentation."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys

out = Path(os.environ['CATCHUP_EVIDENCE'])
start = float(sys.argv[1])
root = Path(os.environ['RUNNER_TEMP']) / 'fcp-test-tmp/pytest-of-runner'
result = {'utc': datetime.datetime.now(datetime.UTC).isoformat(),
          'run': os.environ['GITHUB_RUN_ID'], 'attempt': os.environ['GITHUB_RUN_ATTEMPT'],
          'product_sha': '3a908fd1d9503057f3520d8f2dbbf62401a6e6d3',
          'pytest_exit': int(sys.argv[2]), 'records': []}
paths = sorted(root.glob('pytest-[0-9]*/test_live_catchup_repairs_only0/authority-catchup.sqlite3'))
try:
    for path in paths:
        if path.is_symlink() or path.stat().st_mtime < start:
            continue
        if path.stat().st_size > 16 * 1024**2:
            raise RuntimeError('Fixture exceeds bounded retention size')
        dest = out / (path.parent.parent.name + '-authority-catchup.sqlite3')
        if dest.exists():
            raise RuntimeError('Refusing overwrite')
        # SQLite online backup includes committed WAL state in one consistent copy.
        with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5) as source:
            source.execute('PRAGMA query_only=ON')
            with sqlite3.connect(dest) as target:
                source.backup(target)
                assert target.execute('PRAGMA integrity_check').fetchone() == ('ok',)
                records = [json.loads(r[0]) for r in target.execute(
                    'SELECT record_json FROM storage_live_catchups LIMIT 10')]
        entry = {'source': str(path), 'copy': dest.name,
                 'sha256': hashlib.sha256(dest.read_bytes()).hexdigest(), 'records': records}
        result['records'].append(entry)
        for record in records:
            for item in record.get('items', []):
                print('CATCHUP_ITEM', json.dumps({k: item.get(k) for k in (
                    'item_id', 'attempt_count', 'status', 'last_error_code', 'last_error_reason')}))
    result['status'] = 'RETAINED' if result['records'] else 'NO_CURRENT_FIXTURE'
except Exception as exc:
    result['status'] = 'RETENTION_FAILED'
    result['error'] = type(exc).__name__ + ': ' + str(exc)
    raise
finally:
    (out / 'catchup-records.json').write_text(json.dumps(result, indent=2) + '\n')
    print('RETENTION_STATUS', result['status'])
