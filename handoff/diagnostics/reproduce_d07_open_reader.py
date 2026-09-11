"""Isolated real Windows reader/replacement probe using unchanged checked-in source."""
import datetime
import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile

root = pathlib.Path(__file__).resolve().parent
repo = pathlib.Path('C:/wsl/fcp-responder-exit-wait-20260911')
audit = pathlib.Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
sha = '83955f65b7e6bb36de8e90f34608b97070fed33b'
refs = [sha, '68f6e72c45bf1b0f709efb4ac90ff05fbde37ec7',
        '9b286f931497bf6291e215f6340443c5162826b0']
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip() == sha
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo, text=True).strip()
sources = {}
for path in ['catalog/capabilities/analysis/content_store.py',
             'catalog/capabilities/analysis/scheduler.py',
             'catalog/capabilities/tests/test_analysis_scheduling.py',
             'catalog/common/managed_temporary.py']:
    blobs = {ref: subprocess.check_output(['git', 'rev-parse', ref + ':' + path],
                                         cwd=repo, text=True).strip() for ref in refs}
    assert len(set(blobs.values())) == 1
    sources[path] = blobs
sys.path.insert(0, str(repo))
from catalog.capabilities.analysis.content_store import LocalArtifactContentStore

owned = audit / 'd07-owned-reproduction'
owned.mkdir(exist_ok=True)
assert owned.resolve().parent == audit.resolve()
result = dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    checked_in_sha=sha, host='Nettking Windows / Martin', source_blobs=sources,
    physical_candidate=refs[-1], product_source_modified=False,
    protected_recorder_data_untouched=True, physical_acceptance=False)
with tempfile.TemporaryDirectory(prefix='reader-', dir=owned) as directory:
    assert pathlib.Path(directory).resolve().parent == owned.resolve()
    store = LocalArtifactContentStore(pathlib.Path(directory) / 'artifacts', chunk_size=4)
    payload = b'{"plan":"owned-diagnostic-data"}'
    key = 'analysis/owned/session/plan.json'
    identity = store.write_bytes(key, payload)
    reader = store.stream(key, content_hash=identity.content_hash, size_bytes=identity.size_bytes)
    assert next(reader) == payload[:4]
    try:
        store.write_bytes(key, payload)
    except PermissionError as error:
        result['writer_with_active_reader'] = dict(type=type(error).__name__, winerror=error.winerror,
                                                  errno=error.errno, message=error.strerror)
    else:
        result['writer_with_active_reader'] = 'success'
    finally:
        reader.close()
    assert store.resolve(key).read_bytes() == payload
    result['original_content_preserved'] = True
    after = store.write_bytes(key, payload)
    assert after == identity
    result['writer_after_reader_closes'] = 'success'
result['owned_temporary_data_cleaned'] = True
xml = (audit / 'd07-unmodified-head-focused.xml').read_bytes()
result['existing_concurrent_test'] = dict(result='1 passed in6.38s; single run does not disprove race',
                                        junit_sha256=hashlib.sha256(xml).hexdigest())
(root / 'd07-open-reader-reproduction.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
