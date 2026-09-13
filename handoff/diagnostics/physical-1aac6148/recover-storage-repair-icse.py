"""One bounded failed-job recovery; preserve the first attempt disposition."""
import datetime
import hashlib
import json
import pathlib
import sys

sys.path.insert(0, 'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client

api = client()
outpath = pathlib.Path(__file__).parent / 'storage-repair-icse-recovery.json'
assert not outpath.exists(), 'Inspect existing recovery receipt; never repeat blindly'
sha = '76ad339f1631e136bba7a8a85973bddb1650d570'
merge = 'ec0bbd1d5ce45a97ddc10058bad38eea90f044b6'
run_id = 34777368640
assert api('/pulls/487')['head']['sha'] == sha
run = api(f'/actions/runs/{run_id}')
assert run['head_sha'] == sha and run['run_attempt'] == 1
assert run['status'] == 'completed' and run['conclusion'] == 'failure'
jobs = api(f'/actions/runs/{run_id}/jobs?filter=latest&per_page=100')['jobs']
assert {j['id'] for j in jobs if j['conclusion'] == 'failure'} == {103777830240, 103777830248}
private = pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913/.acceptance/p06-reproducer')
out = {
    'source': sha, 'native_checkout': merge, 'run_id': run_id,
    'requested_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'original_attempt': 1,
    'disposition': [
        {'job_id': 103777830240, 'classification': 'unresolved but non-demonstrated candidate defect',
         'observation': 'Linux reviewer components pass; network demo quorum bootstraps then voter-b announce operation times out. Current storage/analysis repair path is not demonstrated as cause.',
         'network_artifact_id': 10324271911},
        {'job_id': 103777830248, 'classification': 'infrastructure/host issue',
         'observation': 'Docker image export/unpack fails because a parent snapshot does not exist, before product Compose acceptance execution.'},
    ],
    'retained_private_logs': {name: hashlib.sha256((private / name).read_bytes()).hexdigest() for name in ['icse-first-linux.private.log', 'icse-first-compose.private.log']},
    'scope': 'Failed jobs only plus dependent publication; successful Windows job retained. No runner change, daemon repair, prune, product modification or full workflow rerun.',
    'status': 'requesting',
}
outpath.write_text(json.dumps(out, indent=2) + '\n')
out['response'] = api(f'/actions/runs/{run_id}/rerun-failed-jobs', {})
out['status'] = 'accepted'
outpath.write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps({'run_id': run_id, 'status': out['status'], 'response': out['response']}))
